"""
Kafka topic constants for Neith.
All topic names are defined here to avoid magic strings scattered across producers/consumers.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaTopics:
    # ── Raw (directly from financeindia → Kafka) ──────────────────────────
    EQUITIES_RAW: str = "nse.equities.raw"
    DERIVATIVES_RAW: str = "nse.derivatives.raw"
    INDICES_RAW: str = "nse.indices.raw"
    CORPORATE_RAW: str = "nse.corporate.raw"
    SURVEILLANCE_RAW: str = "nse.surveillance.raw"
    MACRO_RAW: str = "nse.macro.raw"

    # ── Clean (Flink-processed, validated, deduped) ───────────────────────
    EQUITIES_CLEAN: str = "nse.equities.clean"
    DERIVATIVES_CLEAN: str = "nse.derivatives.clean"
    INDICES_CLEAN: str = "nse.indices.clean"
    CORPORATE_CLEAN: str = "nse.corporate.clean"
    SURVEILLANCE_CLEAN: str = "nse.surveillance.clean"
    MACRO_CLEAN: str = "nse.macro.clean"

    # ── Dead-letter queue ─────────────────────────────────────────────────
    DLQ: str = "nse.dlq"


TOPICS = KafkaTopics()

# Partition counts per topic — tuned to local parallelism
TOPIC_PARTITIONS: dict[str, int] = {
    TOPICS.EQUITIES_RAW: 6,
    TOPICS.DERIVATIVES_RAW: 12,   # Higher volume (option chains)
    TOPICS.INDICES_RAW: 2,
    TOPICS.CORPORATE_RAW: 3,
    TOPICS.SURVEILLANCE_RAW: 2,
    TOPICS.MACRO_RAW: 1,
    TOPICS.EQUITIES_CLEAN: 6,
    TOPICS.DERIVATIVES_CLEAN: 12,
    TOPICS.INDICES_CLEAN: 2,
    TOPICS.CORPORATE_CLEAN: 3,
    TOPICS.SURVEILLANCE_CLEAN: 2,
    TOPICS.MACRO_CLEAN: 1,
    TOPICS.DLQ: 3,
}
