"""
Parallel Producer Runner.

Launches all domain producers in parallel using ProcessPoolExecutor.
Each domain runs in its own OS process to:
  1. Bypass Python GIL (even though financeindia releases it, multiple domains
     can now run fully independently)
  2. Isolate NSE session state per domain
  3. Provide fault isolation (one domain crash doesn't kill others)

Usage:
    python -m producers.runner --date 2025-03-13
    python -m producers.runner --date today
    python -m producers.runner --date today --domains equity fo index
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Callable

import structlog
from prometheus_client import start_http_server

from config.settings import settings

logger = structlog.get_logger(__name__)

# ── Domain registry ────────────────────────────────────────────────────────
# Each entry: (name, producer_factory_callable, date_required)
DOMAIN_REGISTRY: dict[str, tuple[str, Callable, bool]] = {
    "equity":       ("equity_bhavcopy",    _import_equity,       True),
    "deliverable":  ("equity_deliverable", _import_deliverable,  True),
    "fo":           ("fo_bhavcopy",        _import_fo,           True),
    "index":        ("indices",            _import_index,        True),
    "corporate":    ("corporate_actions",  _import_corporate,    True),
    "surveillance": ("surveillance",       _import_surveillance, True),
    "macro":        ("macro",              _import_macro,        True),
}


def _import_equity():
    from producers.equity_producer import EquityBhavProducer
    return EquityBhavProducer()

def _import_deliverable():
    from producers.equity_producer import EquityDeliverableProducer
    return EquityDeliverableProducer()

def _import_fo():
    from producers.derivatives_producer import FOBhavProducer
    return FOBhavProducer()

def _import_index():
    from producers.index_producer import IndexProducer
    return IndexProducer()

def _import_corporate():
    from producers.corporate_producer import CorporateActionProducer
    return CorporateActionProducer()

def _import_surveillance():
    from producers.surveillance_producer import SurveillanceProducer
    return SurveillanceProducer()

def _import_macro():
    from producers.macro_producer import MacroProducer
    return MacroProducer()


# ── Worker function (runs in subprocess) ──────────────────────────────────
def _run_domain(domain_name: str, factory_name: str, trade_date: str) -> dict:
    """
    Entry-point for each subprocess worker.
    Returns a summary dict so the parent can log results.
    """
    import logging
    import structlog
    structlog.configure(
        processors=[structlog.stdlib.add_log_level, structlog.dev.ConsoleRenderer()],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )
    log = structlog.get_logger(domain_name)

    factory_map = {
        "equity":       _import_equity,
        "deliverable":  _import_deliverable,
        "fo":           _import_fo,
        "index":        _import_index,
        "corporate":    _import_corporate,
        "surveillance": _import_surveillance,
        "macro":        _import_macro,
    }

    start = time.perf_counter()
    try:
        producer = factory_map[domain_name]()
        count = producer.produce_for_date(trade_date)
        elapsed = time.perf_counter() - start
        return {"domain": domain_name, "status": "ok", "records": count, "elapsed": elapsed}
    except Exception as exc:
        elapsed = time.perf_counter() - start
        log.error("domain.failed", domain=domain_name, error=str(exc))
        return {"domain": domain_name, "status": "error", "error": str(exc), "elapsed": elapsed}


# ── Main orchestrator ──────────────────────────────────────────────────────
def run(trade_date: str, domains: list[str] | None = None) -> None:
    """
    Run all (or selected) domain producers in parallel processes.
    """
    if domains is None:
        domains = list(DOMAIN_REGISTRY.keys())

    logger.info(
        "runner.start",
        date=trade_date,
        domains=domains,
        max_workers=settings.producer_max_workers,
    )

    results = []
    with ProcessPoolExecutor(max_workers=min(len(domains), settings.producer_max_workers)) as pool:
        futures = {
            pool.submit(_run_domain, domain, domain, trade_date): domain
            for domain in domains
            if domain in DOMAIN_REGISTRY
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if result["status"] == "ok":
                logger.info(
                    "runner.domain_ok",
                    domain=result["domain"],
                    records=result["records"],
                    elapsed_s=round(result["elapsed"], 2),
                )
            else:
                logger.error(
                    "runner.domain_failed",
                    domain=result["domain"],
                    error=result.get("error"),
                )

    failed = [r for r in results if r["status"] != "ok"]
    total_records = sum(r.get("records", 0) for r in results)
    logger.info(
        "runner.complete",
        date=trade_date,
        total_records=total_records,
        failed_domains=[r["domain"] for r in failed],
    )

    if failed:
        sys.exit(1)


def main() -> None:
    import logging
    import structlog
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )

    parser = argparse.ArgumentParser(description="Neith parallel producer runner")
    parser.add_argument(
        "--date",
        default="today",
        help="Trade date YYYY-MM-DD or 'today' (default: today)",
    )
    parser.add_argument(
        "--domains",
        nargs="*",
        default=None,
        help="Domains to run (default: all). Options: equity fo index corporate surveillance macro",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=settings.prometheus_port,
        help="Port for Prometheus metrics server",
    )
    args = parser.parse_args()

    trade_date = (
        date.today().isoformat() if args.date == "today" else args.date
    )

    # Start Prometheus metrics HTTP server
    start_http_server(args.metrics_port)
    logger.info("prometheus.started", port=args.metrics_port)

    run(trade_date=trade_date, domains=args.domains)


if __name__ == "__main__":
    main()
