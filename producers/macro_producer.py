"""
Macro Producer — FII/DII Activity, Market Status, Holiday Calendar
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import financeindia as fi

from config.topics import TOPICS
from config.settings import settings
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import structlog

logger = structlog.get_logger(__name__)


class MacroProducer:
    """
    Produces macro-level records: FII/DII flows, market status.
    Uses plain JSON (no Avro) for macro data since volume is minimal.
    """

    TOPIC = TOPICS.MACRO_RAW

    def __init__(self) -> None:
        self._client = fi.FinanceClient()
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_brokers,
                "acks": "all",
                "compression.type": "snappy",
            }
        )

    def produce_for_date(self, date: str) -> int:
        logger.info("macro.fetch", date=date)
        records: list[dict[str, Any]] = []
        ts = int(time.time() * 1000)

        # FII/DII
        try:
            fii_dii = self._client.fii_dii_data() or []
            for item in fii_dii:
                records.append(
                    {
                        "ingested_at": ts,
                        "trade_date": date,
                        "record_type": "FII_DII",
                        "data": item,
                    }
                )
        except Exception as exc:
            logger.warning("macro.fii_dii_error", error=str(exc))

        # Market status
        try:
            status = self._client.market_status()
            records.append(
                {
                    "ingested_at": ts,
                    "trade_date": date,
                    "record_type": "MARKET_STATUS",
                    "data": status if isinstance(status, dict) else {"raw": str(status)},
                }
            )
        except Exception as exc:
            logger.warning("macro.status_error", error=str(exc))

        for rec in records:
            self._producer.produce(
                topic=self.TOPIC,
                key=f"{rec['record_type']}_{date}".encode(),
                value=json.dumps(rec).encode("utf-8"),
            )

        self._producer.flush(timeout=30)
        logger.info("macro.done", date=date, records=len(records))
        return len(records)
