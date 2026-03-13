"""
PySpark Market Breadth Aggregation — Gold Layer.

Computes advanced/decline counts, sector rotation, India VIX trend,
FII/DII net flows, and PCR (Put-Call Ratio) from silver data.

Output: gold.market_breadth (Iceberg)
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

MINIO_ENDPOINT = os.getenv("NEITH_MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("NEITH_MINIO_ACCESS_KEY", "neith-access")
MINIO_SECRET   = os.getenv("NEITH_MINIO_SECRET_KEY", "neith-secret99")
ICEBERG_REST   = os.getenv("NEITH_ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("neith-market-breadth")
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
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    ohlcv = spark.table("neith.silver.ohlcv_adjusted")

    # ── Advance/Decline ratio ─────────────────────────────────────────────
    daily_ad = (
        ohlcv
        .withColumn(
            "direction",
            F.when(F.col("close_adj") > F.col("open_adj"), "advance")
             .when(F.col("close_adj") < F.col("open_adj"), "decline")
             .otherwise("unchanged"),
        )
        .groupBy("trade_date")
        .agg(
            F.count(F.when(F.col("direction") == "advance", 1)).alias("advances"),
            F.count(F.when(F.col("direction") == "decline", 1)).alias("declines"),
            F.count(F.when(F.col("direction") == "unchanged", 1)).alias("unchanged"),
            F.count_distinct("symbol").alias("total_stocks"),
            F.sum("total_quantity").alias("total_volume"),
            F.sum("total_turnover").alias("total_turnover_cr"),
            F.avg(
                (F.col("close_adj") - F.col("open_adj")) / F.col("open_adj") * 100
            ).cast(DoubleType()).alias("avg_return_pct"),
        )
        .withColumn(
            "ad_ratio",
            (F.col("advances") / F.col("declines")).cast(DoubleType()),
        )
    )

    # ── FO Put-Call Ratio ─────────────────────────────────────────────────
    fo = spark.table("neith.bronze.nse_fo_bhavcopy").filter(
        F.col("instrument_type").isin("OPTIDX", "OPTSTK")
    )
    pcr = (
        fo.groupBy("trade_date")
        .agg(
            F.sum(F.when(F.col("option_type") == "PE", F.col("open_interest"))).alias("put_oi"),
            F.sum(F.when(F.col("option_type") == "CE", F.col("open_interest"))).alias("call_oi"),
        )
        .withColumn(
            "pcr_oi",
            (F.col("put_oi") / F.col("call_oi")).cast(DoubleType()),
        )
    )

    # ── Join into gold breadth table ─────────────────────────────────────
    gold = daily_ad.join(pcr, on="trade_date", how="left").orderBy("trade_date")

    (
        gold.writeTo("neith.gold.market_breadth")
        .using("iceberg")
        .partitionedBy(F.years("trade_date"))
        .createOrReplace()
    )
    print(f"[market_breadth] Written {gold.count()} rows to gold.market_breadth")


if __name__ == "__main__":
    spark = build_spark()
    run(spark)
    spark.stop()
