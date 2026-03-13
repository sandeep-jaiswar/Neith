"""
PySpark OHLCV Price Adjuster.

Reads bronze.nse_equities_bhavcopy (Iceberg) and bronze.nse_corporate_actions,
applies split/bonus adjustments backwards in time, and writes to
silver.ohlcv_adjusted.

Adjustment logic:
  - SPLIT (e.g. 1:2): multiply all historical closing prices before ex-date by 0.5
  - BONUS (e.g. 1:1): multiply all historical closing prices before ex-date by 0.5
  - Both are "back-adjusted" so the most recent price is always the true price.

Run via: spark-submit --master spark://spark-master:7077 spark_jobs/ohlcv_adjuster.py
"""
from __future__ import annotations

import os
import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

MINIO_ENDPOINT  = os.getenv("NEITH_MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("NEITH_MINIO_ACCESS_KEY", "neith-access")
MINIO_SECRET    = os.getenv("NEITH_MINIO_SECRET_KEY", "neith-secret99")
ICEBERG_REST    = os.getenv("NEITH_ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("neith-ohlcv-adjuster")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.neith", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.neith.type", "rest")
        .config("spark.sql.catalog.neith.uri", ICEBERG_REST)
        .config("spark.sql.catalog.neith.warehouse", "s3://neith-lake/")
        .config("spark.sql.catalog.neith.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.neith.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", str(os.cpu_count() * 2 or 8))
        .getOrCreate()
    )


def parse_adjustment_factor(details_str: str | None) -> float:
    """
    Parse split/bonus ratio from action details string.
    Examples:
      'SPLIT-FROM RS 10 TO RS 1' → 0.1 (price drops 10×)
      'BONUS - 1:1'              → 0.5  (1 new share per existing → price halves)
    Returns 1.0 (no adjustment) if parsing fails.
    """
    if not details_str:
        return 1.0
    s = details_str.upper()
    # Bonus ratio: e.g. "1:1" or "3:2"
    bonus_match = re.search(r"BONUS.*?(\d+):(\d+)", s)
    if bonus_match:
        numerator, denominator = int(bonus_match.group(1)), int(bonus_match.group(2))
        return denominator / (denominator + numerator)  # 1:1 → 0.5, 1:2 → 0.67
    # Split ratio: Face value FROM–TO
    split_match = re.search(r"RS\s+([\d.]+)\s+TO\s+RS\s+([\d.]+)", s)
    if split_match:
        orig, new = float(split_match.group(1)), float(split_match.group(2))
        return new / orig if orig > 0 else 1.0
    return 1.0


parse_adj_udf = F.udf(parse_adjustment_factor, DoubleType())


def run(spark: SparkSession, start_date: str = "2020-01-01") -> None:
    # ── Load bronze data ──────────────────────────────────────────────────
    ohlcv = spark.table("neith.bronze.nse_equities_bhavcopy").filter(
        F.col("trade_date") >= start_date
    )

    actions = spark.table("neith.bronze.nse_corporate_actions").filter(
        F.col("action_type").rlike("(?i)bonus|split")
    ).select(
        F.col("symbol"),
        F.col("ex_date").cast("date").alias("ex_date"),
        F.col("action_type"),
        F.col("details"),
    )

    # ── Compute adjustment factor per corporate event ─────────────────────
    actions_with_factor = actions.withColumn(
        "adj_factor", parse_adj_udf(F.col("details"))
    ).filter(F.col("adj_factor") < 1.0)  # Only real adjustments

    # ── Cumulative adjustment factor per symbol, applied retroactively ────
    # For each (symbol, date), multiply by all adj_factors with ex_date > date
    adj_joined = (
        ohlcv.alias("ohlcv")
        .join(
            actions_with_factor.alias("act"),
            on=(F.col("ohlcv.symbol") == F.col("act.symbol"))
               & (F.col("ohlcv.trade_date").cast("date") < F.col("act.ex_date")),
            how="left",
        )
        .groupBy(
            "ohlcv.trade_date", "ohlcv.symbol", "ohlcv.series",
            "ohlcv.open", "ohlcv.high", "ohlcv.low", "ohlcv.close",
            "ohlcv.total_quantity", "ohlcv.total_turnover", "ohlcv.delivery_pct",
        )
        .agg(
            F.when(
                F.count("act.adj_factor") > 0,
                F.exp(F.sum(F.log(F.col("adj_factor"))))  # Product of factors via log-sum
            ).otherwise(1.0).alias("cumulative_adj_factor")
        )
    )

    # ── Apply adjustment to OHLCV prices ─────────────────────────────────
    silver = adj_joined.select(
        F.col("trade_date"),
        F.col("symbol"),
        F.col("series"),
        (F.col("open")  * F.col("cumulative_adj_factor")).cast(DoubleType()).alias("open_adj"),
        (F.col("high")  * F.col("cumulative_adj_factor")).cast(DoubleType()).alias("high_adj"),
        (F.col("low")   * F.col("cumulative_adj_factor")).cast(DoubleType()).alias("low_adj"),
        (F.col("close") * F.col("cumulative_adj_factor")).cast(DoubleType()).alias("close_adj"),
        F.col("open").cast(DoubleType()).alias("open_raw"),
        F.col("close").cast(DoubleType()).alias("close_raw"),
        F.col("total_quantity"),
        F.col("total_turnover"),
        F.col("delivery_pct"),
        F.col("cumulative_adj_factor"),
    )

    # ── Write to silver Iceberg table ─────────────────────────────────────
    (
        silver.writeTo("neith.silver.ohlcv_adjusted")
        .using("iceberg")
        .partitionedBy(F.years("trade_date"), F.col("symbol"))
        .createOrReplace()
    )
    print(f"[ohlcv_adjuster] Written {silver.count()} rows to silver.ohlcv_adjusted")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2020-01-01")
    args = parser.parse_args()
    spark = build_spark()
    run(spark, args.start_date)
    spark.stop()
