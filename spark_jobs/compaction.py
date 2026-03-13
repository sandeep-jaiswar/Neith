"""
PySpark Iceberg Compaction Job.

Merges many small Parquet files written by Flink into optimally-sized files.
Also expires snapshots older than 7 days and removes orphan files.

Run nightly at 23:30 IST via Airflow.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("NEITH_MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("NEITH_MINIO_ACCESS_KEY", "neith-access")
MINIO_SECRET   = os.getenv("NEITH_MINIO_SECRET_KEY", "neith-secret99")
ICEBERG_REST   = os.getenv("NEITH_ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")

# Tables to compact in priority order (highest write volume first)
COMPACT_TABLES = [
    "neith.bronze.nse_fo_bhavcopy",
    "neith.bronze.nse_equities_bhavcopy",
    "neith.silver.ohlcv_adjusted",
    "neith.bronze.nse_option_chain",
    "neith.bronze.nse_index_history",
    "neith.bronze.nse_corporate_actions",
    "neith.bronze.nse_surveillance_asm",
]

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB target Parquet files
SNAPSHOT_EXPIRY_DAYS = 7


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("neith-iceberg-compaction")
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
        .config("spark.sql.shuffle.partitions", str(os.cpu_count() or 4))
        .getOrCreate()
    )


def compact_table(spark: SparkSession, table: str) -> None:
    print(f"[compaction] Rewriting data files: {table}")
    spark.sql(f"""
        CALL neith.system.rewrite_data_files(
            table => '{table}',
            strategy => 'sort',
            sort_order => 'zorder(symbol, trade_date)',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'min-input-files', '10',
                'partial-progress.enabled', 'true'
            )
        )
    """)

    print(f"[compaction] Expiring snapshots: {table}")
    spark.sql(f"""
        CALL neith.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{SNAPSHOT_EXPIRY_DAYS} days ago',
            retain_last => 3
        )
    """)

    print(f"[compaction] Removing orphan files: {table}")
    spark.sql(f"""
        CALL neith.system.remove_orphan_files(table => '{table}')
    """)
    print(f"[compaction] Done: {table}")


def run() -> None:
    spark = build_spark()
    for table in COMPACT_TABLES:
        try:
            compact_table(spark, table)
        except Exception as exc:
            print(f"[compaction] WARNING: failed for {table}: {exc}")
    spark.stop()


if __name__ == "__main__":
    run()
