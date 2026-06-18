"""
LLM client — Claude 3.5 Sonnet via Databricks external model endpoint.
Single module so the endpoint URL and token are resolved in one place.
"""

import os
import time
from typing import Any

import requests

_ENDPOINT = os.environ.get(
    "CLAUDE_ENDPOINT",
    "claude-3-5-sonnet",   # Databricks external model endpoint name
)
_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
_TOKEN: str | None = None


def _get_token() -> str:
    global _TOKEN
    if _TOKEN is None:
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            dbutils = DBUtils(SparkSession.builder.getOrCreate())
            _TOKEN = dbutils.secrets.get("sales-companion", "databricks-token")
        except Exception:
            _TOKEN = os.environ["DATABRICKS_TOKEN"]
    return _TOKEN


def chat(
    messages: list[dict],
    *,
    max_tokens: int = 1024,
    temperature: float = 0.0,
    system: str | None = None,
) -> dict:
    """
    Call the Databricks-hosted Claude endpoint.
    Returns a dict with: content, input_tokens, output_tokens.
    """
    payload: dict[str, Any] = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if system:
        payload["system"] = system

    t0 = time.perf_counter()
    resp = requests.post(
        f"{_HOST}/serving-endpoints/{_ENDPOINT}/invocations",
        headers={
            "Authorization": f"Bearer {_get_token()}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    latency_ms = int((time.perf_counter() - t0) * 1000)

    data = resp.json()
    usage = data.get("usage", {})
    return {
        "content": data["choices"][0]["message"]["content"],
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
        "latency_ms": latency_ms,
    }
