"""
Derivatives Producer — FO Bhavcopy, Option Chain per symbol, FO Ban List
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

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "nse_fo_raw.avsc"


class FOBhavProducer(BaseProducer):
    """Produces full FO Bhavcopy (all contracts: FUTSTK, FUTIDX, OPTSTK, OPTIDX)."""

    domain = "fo_bhavcopy"
    topic = TOPICS.DERIVATIVES_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.info("fo_bhav.fetch", date=date)
        raw = self._client.fo_bhavcopy(date)
        return self._columnar_to_rows(raw, date)

    def build_key(self, record: dict[str, Any]) -> str:
        return (
            f"{record.get('symbol', '')}_{record.get('instrument_type', '')}"
            f"_{record.get('expiry_date', '')}_{record.get('strike_price', '')}"
            f"_{record.get('option_type', '')}"
        )

    @staticmethod
    def _columnar_to_rows(columnar: dict[str, list], date: str) -> list[dict[str, Any]]:
        if not columnar:
            return []
        col_map = {
            "INSTRUMENT": "instrument_type",
            "SYMBOL": "symbol",
            "EXPIRY_DT": "expiry_date",
            "STRIKE_PR": "strike_price",
            "OPTION_TYP": "option_type",
            "OPEN": "open",
            "HIGH": "high",
            "LOW": "low",
            "CLOSE": "close",
            "SETTLE_PR": "settle_price",
            "CONTRACTS": "contracts",
            "VAL_INLAKH": "value_in_lakh",
            "OPEN_INT": "open_interest",
            "CHG_IN_OI": "change_in_oi",
        }
        length = len(next(iter(columnar.values())))
        ts = int(time.time() * 1000)
        rows = []
        for i in range(length):
            row: dict[str, Any] = {"ingested_at": ts, "trade_date": date}
            for raw_col, vals in columnar.items():
                mapped = col_map.get(raw_col.strip().upper(), raw_col.lower().strip())
                val = vals[i]
                if mapped in ("open", "high", "low", "close", "settle_price",
                               "value_in_lakh", "strike_price"):
                    try:
                        row[mapped] = float(val) if val else None
                    except (ValueError, TypeError):
                        row[mapped] = None
                elif mapped in ("contracts", "open_interest", "change_in_oi"):
                    try:
                        row[mapped] = int(float(val)) if val else None
                    except (ValueError, TypeError):
                        row[mapped] = None
                else:
                    row[mapped] = str(val).strip() if val else None
            rows.append(row)
        return rows
