"""
Kafka Topic Initialisation Script.
Creates all Neith topics with correct partition counts and retention settings.
Run once before first ingestion: python scripts/init_topics.py
"""
from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigSource
from config.settings import settings
from config.topics import TOPIC_PARTITIONS, TOPICS
import structlog

logger = structlog.get_logger(__name__)

RETENTION_MS = str(7 * 24 * 60 * 60 * 1000)  # 7 days
REPLICATION_FACTOR = 1  # Single-node local setup


def init_topics() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_brokers})

    existing_meta = admin.list_topics(timeout=10)
    existing = set(existing_meta.topics.keys())

    new_topics = []
    for topic_name, partitions in TOPIC_PARTITIONS.items():
        if topic_name in existing:
            logger.info("topic.exists", topic=topic_name)
            continue
        new_topics.append(
            NewTopic(
                topic=topic_name,
                num_partitions=partitions,
                replication_factor=REPLICATION_FACTOR,
                config={
                    "retention.ms": RETENTION_MS,
                    "compression.type": "snappy",
                    "min.insync.replicas": "1",
                    "message.max.bytes": "10485760",  # 10 MB max message
                },
            )
        )

    if not new_topics:
        logger.info("topics.all_exist")
        return

    results = admin.create_topics(new_topics)
    for topic, future in results.items():
        try:
            future.result()
            logger.info("topic.created", topic=topic)
        except Exception as exc:
            logger.error("topic.create_failed", topic=topic, error=str(exc))


if __name__ == "__main__":
    import logging
    import structlog
    structlog.configure(
        processors=[structlog.stdlib.add_log_level, structlog.dev.ConsoleRenderer()],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )
    init_topics()
