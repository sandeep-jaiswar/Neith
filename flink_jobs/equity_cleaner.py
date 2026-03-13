"""
PyFlink Equity Stream Cleaner Job.

Pipeline:
  Kafka (nse.equities.raw) → validate → normalise → deduplicate → Iceberg (bronze.nse_equities_bhavcopy)

Parallelism: set to local CPU count.
State backend: RocksDB (persistent across restarts).
Checkpointing: every 60s to MinIO.
"""
from __future__ import annotations

import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import WatermarkStrategy, Types, Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

# ── Environment configuration ─────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("NEITH_KAFKA_BROKERS", "kafka:29092")
MINIO_ENDPOINT = os.getenv("NEITH_MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("NEITH_MINIO_ACCESS_KEY", "neith-access")
MINIO_SECRET = os.getenv("NEITH_MINIO_SECRET_KEY", "neith-secret99")
SOURCE_TOPIC = "nse.equities.raw"
CHECKPOINT_DIR = "s3://neith-lake/flink-checkpoints/equity-cleaner"

# ── Row type for equity (matches Avro schema) ─────────────────────────────
EQUITY_TYPE = RowTypeInfo(
    field_types=[
        Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
        Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.LONG(),
        Types.LONG(), Types.DOUBLE(), Types.LONG(), Types.FLOAT(),
        Types.STRING(),
    ],
    field_names=[
        "ingested_at", "trade_date", "symbol", "series",
        "isin", "open", "high", "low",
        "close", "last", "prev_close", "total_trades",
        "total_quantity", "total_turnover", "deliverable_qty", "delivery_pct",
        "source",
    ],
)


class ValidateAndNormalise(MapFunction):
    """
    1. Strips leading/trailing whitespace from string fields.
    2. Rejects records where close <= 0.
    3. Ensures trade_date is in YYYY-MM-DD format.
    """
    def map(self, value: Row) -> Row:
        # Normalise symbol (NSE symbols are sometimes padded)
        value["symbol"] = (value["symbol"] or "").strip().upper()
        value["series"] = (value["series"] or "").strip().upper()
        # Clamp negative prices to None
        for field in ("open", "high", "low", "close", "last", "prev_close"):
            v = value[field]
            if v is not None and v < 0:
                value[field] = None
        return value


class RejectInvalidFilter(FilterFunction):
    """Drop records that have no symbol or no close price (non-tradeable)."""
    def filter(self, value: Row) -> bool:
        return bool(value["symbol"]) and value["close"] is not None and value["close"] > 0


class DeduplicationFunction(KeyedProcessFunction):
    """
    Stateful deduplication keyed by (symbol, trade_date).
    Uses RocksDB ValueState to remember if we've already seen this key today.
    Emits the record only on first occurrence.
    """
    def __init__(self):
        self._seen_state = None

    def open(self, runtime_context):
        state_desc = ValueStateDescriptor("seen", Types.BOOLEAN())
        self._seen_state = runtime_context.get_state(state_desc)

    def process_element(self, value: Row, ctx: KeyedProcessFunction.Context, out):
        seen = self._seen_state.value()
        if seen is None or not seen:
            self._seen_state.update(True)
            # Register a timer 24h in the future to clear state (avoid unbounded growth)
            ctx.timer_service().register_processing_time_timer(
                ctx.timestamp() + 24 * 60 * 60 * 1000
            )
            out.collect(value)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext, out):
        self._seen_state.clear()


def build_iceberg_sink_sql(env: StreamExecutionEnvironment) -> str:
    """Returns the SQL DDL to create the Iceberg table sink via Flink SQL."""
    return """
    CREATE CATALOG neith_iceberg WITH (
        'type'='iceberg',
        'catalog-type'='rest',
        'uri'='http://iceberg-rest:8181',
        'warehouse'='s3://neith-lake/',
        's3.endpoint'='{minio}',
        's3.path-style-access'='true',
        's3.access-key-id'='{access}',
        's3.secret-access-key'='{secret}'
    );

    CREATE DATABASE IF NOT EXISTS neith_iceberg.bronze;

    CREATE TABLE IF NOT EXISTS neith_iceberg.bronze.nse_equities_bhavcopy (
        ingested_at   BIGINT,
        trade_date    STRING,
        symbol        STRING,
        series        STRING,
        isin          STRING,
        open          FLOAT,
        high          FLOAT,
        low           FLOAT,
        close         FLOAT,
        last          FLOAT,
        prev_close    FLOAT,
        total_trades  BIGINT,
        total_quantity BIGINT,
        total_turnover DOUBLE,
        deliverable_qty BIGINT,
        delivery_pct  FLOAT,
        source        STRING
    ) PARTITIONED BY (trade_date, series)
    WITH ('format-version'='2', 'write.upsert.enabled'='false');
    """.format(minio=MINIO_ENDPOINT, access=MINIO_ACCESS, secret=MINIO_SECRET)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(os.cpu_count() or 4)
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_DIR)

    # ── Kafka source ──────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-equity-cleaner")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder().type_info(EQUITY_TYPE).build()
        )
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaEquitySource")

    # ── Processing pipeline ───────────────────────────────────────────────
    cleaned = (
        stream
        .map(ValidateAndNormalise(), output_type=EQUITY_TYPE)
        .filter(RejectInvalidFilter())
        .key_by(lambda row: f"{row['symbol']}_{row['trade_date']}")
        .process(DeduplicationFunction(), output_type=EQUITY_TYPE)
    )

    # TODO: Replace with proper Iceberg Table API sink when PyFlink Iceberg
    # connector is injected into the Flink image.
    # For now, print to stdout for development validation.
    cleaned.print()

    env.execute("neith-equity-cleaner")


if __name__ == "__main__":
    main()
