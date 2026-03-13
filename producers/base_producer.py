"""
Abstract base class for all Neith Kafka producers.
Handles: Schema Registry registration, Avro serialization, retry with
tenacity, configurable rate limiting, Prometheus metrics.
"""
from __future__ import annotations

import json
import time
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import fastavro
import fastavro.schema
from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from config.settings import settings
import structlog

logger = structlog.get_logger(__name__)

# ── Prometheus metrics ─────────────────────────────────────────────────────
RECORDS_PRODUCED = Counter(
    "neith_records_produced_total",
    "Total records successfully produced to Kafka",
    ["topic", "domain"],
)
PRODUCE_ERRORS = Counter(
    "neith_produce_errors_total",
    "Total produce errors",
    ["topic", "domain"],
)
PRODUCE_LATENCY = Histogram(
    "neith_produce_latency_seconds",
    "Time to fetch + produce one batch",
    ["domain"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)
FETCH_LATENCY = Histogram(
    "neith_fetch_latency_seconds",
    "Time to fetch data from NSE",
    ["domain"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120],
)


class BaseProducer(ABC):
    """
    Base class for all Neith domain producers.

    Subclasses must implement:
        - ``topic`` property
        - ``schema_path`` property
        - ``domain`` property
        - ``fetch_records(date: str) -> list[dict]``
        - ``build_key(record: dict) -> str``
    """

    def __init__(self) -> None:
        self._schema = self._load_schema()
        self._kafka_producer = self._build_kafka_producer()
        self._avro_serializer = self._build_avro_serializer()
        self._rate_limiter_interval = 1.0 / settings.producer_rate_limit_per_sec

    # ── Abstract interface ────────────────────────────────────────────────
    @property
    @abstractmethod
    def topic(self) -> str: ...

    @property
    @abstractmethod
    def schema_path(self) -> Path: ...

    @property
    @abstractmethod
    def domain(self) -> str: ...

    @abstractmethod
    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        """Fetch data from NSE using financeindia. Return list of dicts."""
        ...

    @abstractmethod
    def build_key(self, record: dict[str, Any]) -> str:
        """Return the Kafka message key for a record (used for partitioning)."""
        ...

    # ── Main entry point ──────────────────────────────────────────────────
    def produce_for_date(self, date: str) -> int:
        """
        Fetch all records for *date* and produce them to Kafka.
        Returns the number of records produced.
        """
        logger.info("producer.start", domain=self.domain, date=date)
        start = time.perf_counter()

        with FETCH_LATENCY.labels(self.domain).time():
            records = self._fetch_with_retry(date)

        produced = 0
        for record in records:
            record.setdefault("ingested_at", int(time.time() * 1000))
            self._produce_record(record)
            produced += 1
            time.sleep(self._rate_limiter_interval)

        self._kafka_producer.flush(timeout=30)
        elapsed = time.perf_counter() - start
        PRODUCE_LATENCY.labels(self.domain).observe(elapsed)
        logger.info(
            "producer.done",
            domain=self.domain,
            date=date,
            records=produced,
            elapsed_s=round(elapsed, 2),
        )
        return produced

    # ── Internals ──────────────────────────────────────────────────────────
    def _produce_record(self, record: dict[str, Any]) -> None:
        key = self.build_key(record).encode("utf-8")
        try:
            value = self._avro_serializer(
                record, SerializationContext(self.topic, MessageField.VALUE)
            )
            self._kafka_producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                on_delivery=self._delivery_callback,
            )
            RECORDS_PRODUCED.labels(topic=self.topic, domain=self.domain).inc()
        except (KafkaException, Exception) as exc:
            PRODUCE_ERRORS.labels(topic=self.topic, domain=self.domain).inc()
            logger.error("producer.error", domain=self.domain, error=str(exc), record_key=key)
            # Non-fatal: log and continue (DLQ handling via delivery callback)

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.error("kafka.delivery_error", error=str(err))

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
    )
    def _fetch_with_retry(self, date: str) -> list[dict[str, Any]]:
        return self.fetch_records(date)

    def _load_schema(self) -> dict:
        return fastavro.schema.load_schema(str(self.schema_path))

    def _build_kafka_producer(self) -> Producer:
        return Producer(
            {
                "bootstrap.servers": settings.kafka_brokers,
                "acks": settings.kafka_producer_acks,
                "linger.ms": settings.kafka_producer_linger_ms,
                "batch.size": settings.kafka_producer_batch_size,
                "compression.type": settings.kafka_producer_compression,
                "enable.idempotence": True,
                "retries": 5,
                "delivery.timeout.ms": 120000,
            }
        )

    def _build_avro_serializer(self) -> AvroSerializer:
        sr_client = SchemaRegistryClient({"url": settings.schema_registry_url})
        schema_str = json.dumps(self._schema)
        return AvroSerializer(sr_client, schema_str)

    def close(self) -> None:
        self._kafka_producer.flush(timeout=30)
