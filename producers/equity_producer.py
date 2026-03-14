"""
Equity Producer — Bhavcopy, Deliverable Positions, Bulk/Block Deals
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

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "nse_equity_raw.avsc"


class EquityBhavProducer(BaseProducer):
    """Produces daily Bhavcopy records for all NSE equities (EQ series)."""

    domain = "equity_bhavcopy"
    topic = TOPICS.EQUITIES_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.info("equity_bhav.fetch", date=date)
        # financeindia returns a List[Dict] (row format) with XBRL fields
        raw_list = self._client.bhav_copy_equities(date)
        if not raw_list:
            return []

        rows = []
        ts = int(time.time() * 1000)
        for item in raw_list:
            row: dict[str, Any] = {
                "ingested_at": ts,
                "trade_date": date,
                "source": "bhavcopy",
                "symbol": item.get("TckrSymb"),
                "series": item.get("SctySrs"),
                "isin": item.get("ISIN"),
                "open": self._f(item.get("OpnPric")),
                "high": self._f(item.get("HghPric")),
                "low": self._f(item.get("LwPric")),
                "close": self._f(item.get("ClsPric")),
                "last": self._f(item.get("LastPric")),
                "prev_close": self._f(item.get("PrvsClsgPric")),
                "total_quantity": self._i(item.get("TtlTradgVol")),
                "total_turnover": self._f(item.get("TtlTrfVal")),
                "total_trades": self._i(item.get("TtlNbOfTxsExctd")),
            }
            rows.append(row)
        return rows

    def build_key(self, record: dict[str, Any]) -> str:
        return f"{record.get('symbol', 'UNKNOWN')}_{record.get('trade_date', '')}"

    @staticmethod
    def _f(val) -> float | None:
        try:
            return float(val) if val is not None and str(val).strip() else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _i(val) -> int | None:
        try:
            return int(float(val)) if val is not None and str(val).strip() else None
        except (ValueError, TypeError):
            return None


class EquityDeliverableProducer(BaseProducer):
    """Produces deliverable position records (% delivery) for NSE equities."""

    domain = "equity_deliverable"
    topic = TOPICS.EQUITIES_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.warning("equity_deliverable.unavailable", date=date, reason="FinanceClient.deliverable_position_data requires symbol")
        return [] # Currently not supported market-wide in this library version

    def build_key(self, record: dict[str, Any]) -> str:
        return f"{record.get('symbol', 'UNKNOWN')}_{record.get('trade_date', '')}_DEL"

    @staticmethod
    def _columnar_to_rows(columnar: dict[str, list], date: str) -> list[dict[str, Any]]:
        if not columnar:
            return []
        length = len(next(iter(columnar.values())))
        ts = int(time.time() * 1000)
        rows = []
        for i in range(length):
            row: dict[str, Any] = {"ingested_at": ts, "trade_date": date, "source": "deliverable"}
            for col, vals in columnar.items():
                key = col.strip().lower().replace(" ", "_")
                row[key] = vals[i]
            rows.append(row)
        return rows
