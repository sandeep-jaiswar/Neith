"""
Corporate Actions Producer — Dividends, Splits, Bonus, Rights
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import financeindia as fi

from producers.base_producer import BaseProducer
from config.topics import TOPICS
import structlog

logger = structlog.get_logger(__name__)

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "nse_corporate_raw.avsc"


class CorporateActionProducer(BaseProducer):
    """
    Produces corporate action records (ex-date announcements).
    Fetches a rolling 30-day window to catch all pending actions.
    """

    domain = "corporate_actions"
    topic = TOPICS.CORPORATE_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.info("corporate.fetch", date=date)
        raw_list = self._client.corporate_actions(symbol="", action_type="") or []
        ts = int(time.time() * 1000)
        rows = []
        for item in raw_list:
            row: dict[str, Any] = {
                "ingested_at": ts,
                "symbol": str(item.get("symbol", "")).strip(),
                "company_name": str(item.get("comp", "")).strip() or None,
                "action_type": str(item.get("subject", item.get("type", "UNKNOWN"))).strip(),
                "ex_date": str(item.get("exDate", "")).strip() or None,
                "record_date": str(item.get("recDate", "")).strip() or None,
                "bc_start_date": str(item.get("bcStartDate", "")).strip() or None,
                "bc_end_date": str(item.get("bcEndDate", "")).strip() or None,
                "nd_start_date": str(item.get("ndStartDate", "")).strip() or None,
                "nd_end_date": str(item.get("ndEndDate", "")).strip() or None,
                "details": str(item.get("details", "")).strip() or None,
                "series": str(item.get("series", "EQ")).strip() or None,
            }
            rows.append(row)
        return rows

    def build_key(self, record: dict[str, Any]) -> str:
        return f"{record['symbol']}_{record['action_type']}_{record.get('ex_date', '')}"
