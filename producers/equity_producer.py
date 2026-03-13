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
        # financeindia returns a Dict[str, List] (columnar format)
        raw = self._client.bhav_copy_equities(date)
        return self._columnar_to_rows(raw, date)

    def build_key(self, record: dict[str, Any]) -> str:
        return f"{record.get('symbol', 'UNKNOWN')}_{record.get('trade_date', '')}"

    @staticmethod
    def _columnar_to_rows(columnar: dict[str, list], date: str) -> list[dict[str, Any]]:
        """Convert financeindia columnar dict to list of row dicts."""
        if not columnar:
            return []

        # Normalise column names to our schema field names
        col_map = {
            "SYMBOL": "symbol",
            "SERIES": "series",
            "ISIN": "isin",
            "OPEN": "open",
            "HIGH": "high",
            "LOW": "low",
            "CLOSE": "close",
            "LAST": "last",
            "PREVCLOSE": "prev_close",
            "TOTTRDQTY": "total_quantity",
            "TOTTRDVAL": "total_turnover",
            "TOTALTRADES": "total_trades",
        }

        columns = list(columnar.keys())
        length = len(next(iter(columnar.values())))
        rows = []
        ts = int(time.time() * 1000)

        for i in range(length):
            row: dict[str, Any] = {
                "ingested_at": ts,
                "trade_date": date,
                "source": "bhavcopy",
            }
            for raw_col, val_list in columnar.items():
                mapped = col_map.get(raw_col.strip().upper(), raw_col.lower().strip())
                val = val_list[i]
                # Type coercion
                if mapped in ("open", "high", "low", "close", "last", "prev_close",
                               "total_turnover", "delivery_pct"):
                    try:
                        row[mapped] = float(val) if val else None
                    except (ValueError, TypeError):
                        row[mapped] = None
                elif mapped in ("total_quantity", "total_trades", "deliverable_qty"):
                    try:
                        row[mapped] = int(float(val)) if val else None
                    except (ValueError, TypeError):
                        row[mapped] = None
                else:
                    row[mapped] = str(val).strip() if val else None
            rows.append(row)

        return rows


class EquityDeliverableProducer(BaseProducer):
    """Produces deliverable position records (% delivery) for NSE equities."""

    domain = "equity_deliverable"
    topic = TOPICS.EQUITIES_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.info("equity_deliverable.fetch", date=date)
        raw = self._client.deliverable_position(date)
        return self._columnar_to_rows(raw, date)

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
