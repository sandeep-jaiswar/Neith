"""
PyIceberg catalog configuration for Neith.
Provides a factory to obtain the shared Iceberg REST catalog instance.
"""
from __future__ import annotations

from functools import lru_cache
from pyiceberg.catalog import load_catalog

from config.settings import settings


@lru_cache(maxsize=1)
def get_catalog():
    """Return the singleton PyIceberg REST catalog."""
    return load_catalog(
        settings.iceberg_catalog_name,
        **{
            "uri": settings.iceberg_catalog_uri,
            "s3.endpoint": settings.minio_endpoint,
            "s3.access-key-id": settings.minio_access_key,
            "s3.secret-access-key": settings.minio_secret_key,
            "s3.path-style-access": "true",
            "warehouse": f"s3://{settings.minio_bucket}/",
        },
    )


# Namespaces (databases) in the catalog
NAMESPACE_BRONZE = "bronze"
NAMESPACE_SILVER = "silver"
NAMESPACE_GOLD = "gold"

# Iceberg table full names
class IcebergTables:
    EQUITIES_BHAVCOPY = f"{NAMESPACE_BRONZE}.nse_equities_bhavcopy"
    FO_BHAVCOPY = f"{NAMESPACE_BRONZE}.nse_fo_bhavcopy"
    OPTION_CHAIN = f"{NAMESPACE_BRONZE}.nse_option_chain"
    INDEX_HISTORY = f"{NAMESPACE_BRONZE}.nse_index_history"
    CORPORATE_ACTIONS = f"{NAMESPACE_BRONZE}.nse_corporate_actions"
    FINANCIAL_RESULTS = f"{NAMESPACE_BRONZE}.nse_financial_results"
    SURVEILLANCE_ASM = f"{NAMESPACE_BRONZE}.nse_surveillance_asm"
    SURVEILLANCE_GSM = f"{NAMESPACE_BRONZE}.nse_surveillance_gsm"
    FII_DII = f"{NAMESPACE_BRONZE}.nse_fii_dii"

    OHLCV_ADJUSTED = f"{NAMESPACE_SILVER}.ohlcv_adjusted"
    FO_AGGREGATE = f"{NAMESPACE_SILVER}.fo_aggregate"

    MARKET_BREADTH = f"{NAMESPACE_GOLD}.market_breadth"
