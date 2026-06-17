"""
API key authentication for the serving layer.
Keys are stored in the Databricks Secret scope `sales-companion`.
For local dev, set the env var SALES_COMPANION_API_KEYS as a comma-separated list.
"""

import os
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_valid_keys: set[str] | None = None


def _load_keys() -> set[str]:
    global _valid_keys
    if _valid_keys is not None:
        return _valid_keys

    raw = os.environ.get("SALES_COMPANION_API_KEYS", "")
    if not raw:
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            dbutils = DBUtils(SparkSession.builder.getOrCreate())
            raw = dbutils.secrets.get("sales-companion", "api-keys")
        except Exception:
            pass

    _valid_keys = {k.strip() for k in raw.split(",") if k.strip()}
    return _valid_keys


async def require_api_key(api_key: str | None = Security(_header)) -> str:
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )
    if api_key not in _load_keys():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )
    return api_key
