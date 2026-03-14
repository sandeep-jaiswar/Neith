"""
Index Producer — All Indices snapshot, Index history
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

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "nse_index_raw.avsc"


class IndexProducer(BaseProducer):
    """Produces snapshot of all NSE indices (NIFTY 50, Bank Nifty, VIX, etc.)."""

    domain = "indices"
    topic = TOPICS.INDICES_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.info("index.fetch", date=date)
        # Returns dict with 'data' key containing list of dicts
        raw_res = self._client.get_all_indices()
        raw_list = raw_res.get("data", [])
        ts = int(time.time() * 1000)
        rows = []
        for item in (raw_list or []):
            row: dict[str, Any] = {
                "ingested_at": ts,
                "trade_date": date,
                "index_name": str(item.get("index", item.get("indexSymbol", "UNKNOWN"))),
                "open": self._f(item.get("open")),
                "high": self._f(item.get("high")),
                "low": self._f(item.get("low")),
                "close": self._f(item.get("last") or item.get("indicativeClose")),
                "points_change": self._f(item.get("variation")),
                "pct_change": self._f(item.get("percentChange")),
                "volume": 0, # Volume not available in this snapshot
                "turnover_cr": 0.0,
                "pe_ratio": self._f(item.get("pe")),
                "pb_ratio": self._f(item.get("pb")),
                "div_yield": self._f(item.get("dy")),
            }
            rows.append(row)
        return rows

    def build_key(self, record: dict[str, Any]) -> str:
        return f"{record['index_name']}_{record['trade_date']}"

    @staticmethod
    def _f(val) -> float | None:
        try:
            return float(str(val).replace(",", "")) if val is not None else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _i(val) -> int | None:
        try:
            return int(float(str(val).replace(",", ""))) if val is not None else None
        except (ValueError, TypeError):
            return None
