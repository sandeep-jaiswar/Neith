"""
PyFlink FO (Futures & Options) Stream Cleaner Job.

Pipeline:
  Kafka (nse.derivatives.raw) → validate → normalise → deduplicate → Iceberg (bronze.nse_fo_bhavcopy)

Key design choices:
- Keyed by (symbol, instrument_type, expiry_date, strike_price, option_type)
  — composite key ensures one authoritative record per contract per day.
- Higher parallelism than equities (12 Kafka partitions vs 6).
"""
from __future__ import annotations

import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import WatermarkStrategy, Types, Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

KAFKA_BROKERS = os.getenv("NEITH_KAFKA_BROKERS", "kafka:29092")
MINIO_ENDPOINT = os.getenv("NEITH_MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("NEITH_MINIO_ACCESS_KEY", "neith-access")
MINIO_SECRET = os.getenv("NEITH_MINIO_SECRET_KEY", "neith-secret99")
SOURCE_TOPIC = "nse.derivatives.raw"
CHECKPOINT_DIR = "s3://neith-lake/flink-checkpoints/fo-cleaner"

FO_TYPE = RowTypeInfo(
    field_types=[
        Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.FLOAT(), Types.STRING(),
        Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
        Types.FLOAT(), Types.LONG(), Types.DOUBLE(), Types.LONG(), Types.LONG(),
    ],
    field_names=[
        "ingested_at", "trade_date", "symbol", "instrument_type",
        "expiry_date", "strike_price", "option_type",
        "open", "high", "low", "close",
        "settle_price", "contracts", "value_in_lakh", "open_interest", "change_in_oi",
    ],
)


class FOValidateNormalise(MapFunction):
    def map(self, value: Row) -> Row:
        value["symbol"] = (value["symbol"] or "").strip().upper()
        value["instrument_type"] = (value["instrument_type"] or "").strip().upper()
        value["option_type"] = (value["option_type"] or "").strip().upper() or None
        for f in ("open", "high", "low", "close", "settle_price"):
            v = value[f]
            if v is not None and v < 0:
                value[f] = None
        return value


class FOValidFilter(FilterFunction):
    def filter(self, value: Row) -> bool:
        return bool(value["symbol"]) and bool(value["instrument_type"]) and bool(value["expiry_date"])


class FODeduplicationFunction(KeyedProcessFunction):
    def __init__(self):
        self._seen_state = None

    def open(self, ctx):
        self._seen_state = ctx.get_state(ValueStateDescriptor("fo_seen", Types.BOOLEAN()))

    def process_element(self, value: Row, ctx, out):
        if not self._seen_state.value():
            self._seen_state.update(True)
            ctx.timer_service().register_processing_time_timer(
                ctx.timestamp() + 48 * 60 * 60 * 1000  # FO contracts live 2 days+ in state
            )
            out.collect(value)

    def on_timer(self, timestamp, ctx, out):
        self._seen_state.clear()


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(min(os.cpu_count() or 4, 12))
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_DIR)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-fo-cleaner")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder().type_info(FO_TYPE).build()
        )
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaFOSource")

    cleaned = (
        stream
        .map(FOValidateNormalise(), output_type=FO_TYPE)
        .filter(FOValidFilter())
        .key_by(
            lambda r: (
                f"{r['symbol']}_{r['instrument_type']}_{r['expiry_date']}"
                f"_{r.get('strike_price', '')}_{r.get('option_type', '')}"
            )
        )
        .process(FODeduplicationFunction(), output_type=FO_TYPE)
    )

    cleaned.print()
    env.execute("neith-fo-cleaner")


if __name__ == "__main__":
    main()
