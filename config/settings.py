"""
Centralised Pydantic Settings for Neith.
All values are read from environment variables (or .env file).
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class NeithSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="NEITH_",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Kafka ─────────────────────────────────────────────────────────────
    kafka_brokers: str = Field("localhost:9092", description="Comma-separated Kafka brokers")
    schema_registry_url: str = Field(
        "http://localhost:8081", description="Confluent Schema Registry URL"
    )
    kafka_producer_acks: str = "all"
    kafka_producer_linger_ms: int = 100
    kafka_producer_batch_size: int = 131072  # 128 KB
    kafka_producer_compression: str = "snappy"
    kafka_consumer_group_id: str = "neith-flink-consumers"
    kafka_max_poll_records: int = 500

    # ── MinIO / S3 ────────────────────────────────────────────────────────
    minio_endpoint: str = Field("http://localhost:9000", description="MinIO endpoint URL")
    minio_access_key: str = Field("neith-access")
    minio_secret_key: str = Field("neith-secret99")
    minio_bucket: str = Field("neith-lake")

    # ── Iceberg REST Catalog ──────────────────────────────────────────────
    iceberg_catalog_uri: str = Field("http://localhost:8181")
    iceberg_catalog_name: str = "neith"

    # ── Observability ─────────────────────────────────────────────────────
    log_level: str = "INFO"
    prometheus_port: int = 8000

    # ── Ingestion Behaviour ───────────────────────────────────────────────
    producer_rate_limit_per_sec: float = Field(2.0, description="Max NSE API calls/sec per domain")
    producer_max_workers: int = Field(6, description="OS processes for parallel domain producers")
    intraday_interval_minutes: int = Field(5, description="Intraday quote polling interval (mins)")
    backfill_start_date: str = Field("2020-01-01", description="ISO date for historical backfill start")


# Singleton — import and use everywhere
settings = NeithSettings()
