"""
Surveillance Producer — ASM (Long/Short), GSM, FO Ban List
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

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "nse_surveillance_raw.avsc"


class SurveillanceProducer(BaseProducer):
    """
    Produces records for all surveillance lists:
      - ASM Long-term / Short-term
      - GSM (Graded Surveillance Measure)
      - FO Ban list
    All are emitted on the same topic, differentiated by ``list_type`` field.
    """

    domain = "surveillance"
    topic = TOPICS.SURVEILLANCE_RAW
    schema_path = SCHEMA_PATH

    def __init__(self) -> None:
        super().__init__()
        self._client = fi.FinanceClient()

    def fetch_records(self, date: str) -> list[dict[str, Any]]:
        logger.info("surveillance.fetch", date=date)
        rows: list[dict[str, Any]] = []
        ts = int(time.time() * 1000)

        # ASM Long-term
        rows.extend(self._wrap(self._client.asm_list(), "ASM_LT", ts, date))
        # ASM Short-term
        rows.extend(self._wrap(self._client.short_term_asm_list(), "ASM_ST", ts, date))
        # GSM
        rows.extend(self._wrap(self._client.gsm_list(), "GSM", ts, date))
        # FO Ban
        rows.extend(self._wrap(self._client.fo_ban_list(date), "FO_BAN", ts, date))

        return rows

    def build_key(self, record: dict[str, Any]) -> str:
        return f"{record['symbol']}_{record['list_type']}_{record['as_of_date']}"

    @staticmethod
    def _wrap(raw_list: list | None, list_type: str, ts: int, date: str) -> list[dict[str, Any]]:
        if not raw_list:
            return []
        rows = []
        for item in raw_list:
            if not item:
                continue
            rows.append(
                {
                    "ingested_at": ts,
                    "as_of_date": date,
                    "symbol": str(item.get("symbol", item.get("SYMBOL", ""))).strip(),
                    "isin": str(item.get("isin", item.get("ISIN", ""))).strip() or None,
                    "company_name": str(item.get("companyName", item.get("NAME", ""))).strip() or None,
                    "list_type": list_type,
                    "stage": int(item["stage"]) if item.get("stage") is not None else None,
                    "reason": str(item.get("reason", "")).strip() or None,
                }
            )
        return rows
