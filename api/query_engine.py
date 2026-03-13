"""
DuckDB Query Engine — Direct reads on Iceberg/Parquet over MinIO (S3).
Provides fast ad-hoc analytical querying without a running Spark cluster.
"""
from __future__ import annotations

from functools import lru_cache
import duckdb
from config.settings import settings


@lru_cache(maxsize=1)
def _get_conn() -> duckdb.DuckDBPyConnection:
    """Singleton DuckDB in-memory connection configured for S3/MinIO access."""
    con = duckdb.connect(database=":memory:", read_only=False)

    # Install and load required extensions
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")

    # Configure S3 credentials pointing to local MinIO
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    con.execute(f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{settings.minio_access_key}';
        SET s3_secret_access_key='{settings.minio_secret_key}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)
    return con


def query(sql: str) -> list[dict]:
    """Execute a SQL query against Iceberg/Parquet tables and return rows as dicts."""
    con = _get_conn()
    result = con.execute(sql)
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def get_ohlcv(symbol: str, from_date: str, to_date: str) -> list[dict]:
    sql = f"""
        SELECT trade_date, symbol, open_adj, high_adj, low_adj, close_adj,
               total_quantity, total_turnover, delivery_pct, cumulative_adj_factor
        FROM iceberg_scan('s3://{settings.minio_bucket}/silver/ohlcv_adjusted')
        WHERE symbol = '{symbol.upper()}'
          AND trade_date BETWEEN '{from_date}' AND '{to_date}'
        ORDER BY trade_date ASC
    """
    return query(sql)


def get_market_breadth(from_date: str, to_date: str) -> list[dict]:
    sql = f"""
        SELECT *
        FROM iceberg_scan('s3://{settings.minio_bucket}/gold/market_breadth')
        WHERE trade_date BETWEEN '{from_date}' AND '{to_date}'
        ORDER BY trade_date ASC
    """
    return query(sql)


def get_fo_bhavcopy(symbol: str, trade_date: str) -> list[dict]:
    sql = f"""
        SELECT *
        FROM iceberg_scan('s3://{settings.minio_bucket}/bronze/nse_fo_bhavcopy')
        WHERE symbol = '{symbol.upper()}'
          AND trade_date = '{trade_date}'
        ORDER BY expiry_date, strike_price, option_type
    """
    return query(sql)


def get_surveillance(list_type: str | None = None, as_of_date: str | None = None) -> list[dict]:
    filters = []
    if list_type:
        filters.append(f"list_type = '{list_type.upper()}'")
    if as_of_date:
        filters.append(f"as_of_date = '{as_of_date}'")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT *
        FROM iceberg_scan('s3://{settings.minio_bucket}/bronze/nse_surveillance_asm')
        {where}
        ORDER BY as_of_date DESC, symbol
    """
    return query(sql)
