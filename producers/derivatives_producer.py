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
        # financeindia returns list of dicts for derivatives with segment param
        raw_list = self._client.bhav_copy_derivatives(date, segment="FO")
        if not raw_list:
            return []

        rows = []
        ts = int(time.time() * 1000)
        for item in raw_list:
            row: dict[str, Any] = {
                "ingested_at": ts,
                "trade_date": date,
                "instrument_type": item.get("FinInstrmTp"),
                "symbol": item.get("TckrSymb"),
                "expiry_date": item.get("XpryDt"),
                "strike_price": self._f(item.get("StrkPric")),
                "option_type": item.get("OptnTp"),
                "open": self._f(item.get("OpnPric")),
                "high": self._f(item.get("HghPric")),
                "low": self._f(item.get("LwPric")),
                "close": self._f(item.get("ClsPric")),
                "settle_price": self._f(item.get("SttlmPric")),
                "contracts": self._i(item.get("TtlNbOfTxsExctd")),
                "value_in_lakh": self._f(item.get("TtlTrfVal")),
                "open_interest": self._i(item.get("OpnIntrst")),
                "change_in_oi": self._i(item.get("ChngInOpnIntrst")),
            }
            rows.append(row)
        return rows

    def build_key(self, record: dict[str, Any]) -> str:
        return (
            f"{record.get('symbol', '')}_{record.get('instrument_type', '')}"
            f"_{record.get('expiry_date', '')}_{record.get('strike_price', '')}"
            f"_{record.get('option_type', '')}"
        )

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
