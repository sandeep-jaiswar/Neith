"""
Neith FastAPI Application — Serving Layer.

Endpoints:
  GET /equities/{symbol}/ohlcv          - Adjusted OHLCV time-series
  GET /equities/{symbol}/ohlcv/raw      - Raw (unadjusted) bhavcopy
  GET /fo/{symbol}/bhavcopy             - FO contracts for a given date
  GET /indices/breadth                  - Market breadth (A/D, PCR, turnover)
  GET /surveillance                     - ASM/GSM/FO-Ban lists
  GET /health                           - Liveness check
  GET /metrics                          - Prometheus metrics (text)
"""
from __future__ import annotations

from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Histogram
import time
import structlog

from api.query_engine import get_ohlcv, get_market_breadth, get_fo_bhavcopy, get_surveillance

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="Neith — Indian Stock Datalake API",
    description="Query NSE equity, FO, index, and surveillance data from the local Iceberg lake.",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Prometheus metrics ─────────────────────────────────────────────────────
REQUEST_COUNT = Counter("neith_api_requests_total", "API request count", ["endpoint"])
REQUEST_LATENCY = Histogram("neith_api_latency_seconds", "API request latency", ["endpoint"])


@app.get("/health", tags=["Meta"])
def health():
    return {"status": "ok", "service": "neith-api"}


@app.get("/metrics", response_class=PlainTextResponse, tags=["Meta"])
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/equities/{symbol}/ohlcv", tags=["Equities"])
def equity_ohlcv(
    symbol: str,
    from_date: str = Query(..., description="Start date YYYY-MM-DD", example="2024-01-01"),
    to_date: str = Query(..., description="End date YYYY-MM-DD",   example="2025-03-13"),
):
    """
    Returns split/bonus-adjusted OHLCV time-series for the given NSE symbol.
    Prices are back-adjusted so the latest price is always the true price.
    """
    t = time.perf_counter()
    REQUEST_COUNT.labels("/equities/{symbol}/ohlcv").inc()
    try:
        rows = get_ohlcv(symbol.upper(), from_date, to_date)
    except Exception as exc:
        logger.error("api.ohlcv_error", symbol=symbol, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))
    REQUEST_LATENCY.labels("/equities/{symbol}/ohlcv").observe(time.perf_counter() - t)
    return {"symbol": symbol.upper(), "data": rows, "count": len(rows)}


@app.get("/fo/{symbol}/bhavcopy", tags=["Derivatives"])
def fo_bhavcopy(
    symbol: str,
    trade_date: str = Query(..., description="Trade date YYYY-MM-DD"),
):
    """Returns all FO contracts (Futures and Options) for a symbol on the given date."""
    REQUEST_COUNT.labels("/fo/{symbol}/bhavcopy").inc()
    try:
        rows = get_fo_bhavcopy(symbol.upper(), trade_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"symbol": symbol.upper(), "trade_date": trade_date, "data": rows, "count": len(rows)}


@app.get("/indices/breadth", tags=["Indices"])
def market_breadth(
    from_date: str = Query(..., description="Start date YYYY-MM-DD"),
    to_date: str = Query(..., description="End date YYYY-MM-DD"),
):
    """Returns market breadth: advance/decline ratio, PCR, turnover, average returns."""
    REQUEST_COUNT.labels("/indices/breadth").inc()
    try:
        rows = get_market_breadth(from_date, to_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"data": rows, "count": len(rows)}


@app.get("/surveillance", tags=["Surveillance"])
def surveillance(
    list_type: Optional[str] = Query(
        None,
        description="Filter by list type: ASM_LT, ASM_ST, GSM, FO_BAN",
    ),
    as_of_date: Optional[str] = Query(None, description="Specific date YYYY-MM-DD"),
):
    """Returns stocks currently on ASM, GSM, or FO-Ban surveillance lists."""
    REQUEST_COUNT.labels("/surveillance").inc()
    try:
        rows = get_surveillance(list_type, as_of_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"data": rows, "count": len(rows)}


def start():
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=18000, reload=False, workers=2)


if __name__ == "__main__":
    start()
