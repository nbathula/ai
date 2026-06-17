"""
Sprint 1 — Monitoring Core
Decorator that wraps every agent call to capture:
  - Total / retrieval / LLM latency
  - Groundedness score (Claude Haiku judge)
  - Confidence score (from agent)
  - Token count + estimated cost

Writes traces asynchronously to sales.monitoring.agent_traces.
"""

import asyncio
import functools
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from pyspark.sql import SparkSession

TRACE_TABLE = "sales.monitoring.agent_traces"
ISSUES_TABLE = "sales.monitoring.groundedness_issues"
SQL_LOG_TABLE = "sales.monitoring.sql_log"

AGENT_VERSION = os.environ.get("AGENT_VERSION", "0.1.0")
MODEL = "claude-3-5-sonnet"

# Anthropic pricing (per 1M tokens, as of 2024-06)
COST_PER_1M_INPUT = 3.0
COST_PER_1M_OUTPUT = 15.0


@dataclass
class AgentTrace:
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    agent_type: Optional[str] = None
    query: Optional[str] = None
    response: Optional[str] = None
    generated_sql: Optional[str] = None
    retrieved_chunk_ids: list[str] = field(default_factory=list)
    total_latency_ms: int = 0
    retrieval_latency_ms: int = 0
    llm_latency_ms: int = 0
    confidence_score: float = 0.0
    groundedness_score: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_usd: float = 0.0
    agent_version: str = AGENT_VERSION
    model: str = MODEL
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def calculate_cost(self):
        self.estimated_cost_usd = (
            self.input_tokens / 1_000_000 * COST_PER_1M_INPUT
            + self.output_tokens / 1_000_000 * COST_PER_1M_OUTPUT
        )


def _write_trace(trace: AgentTrace):
    """Fire-and-forget write to Delta. Called in a background thread."""
    try:
        spark = SparkSession.builder.getOrCreate()
        row = [(
            trace.trace_id, trace.session_id, trace.user_id,
            trace.agent_type, trace.query, trace.response,
            trace.generated_sql, trace.retrieved_chunk_ids,
            trace.total_latency_ms, trace.retrieval_latency_ms,
            trace.llm_latency_ms, trace.confidence_score,
            trace.groundedness_score, trace.input_tokens,
            trace.output_tokens, trace.estimated_cost_usd,
            trace.agent_version, trace.model, trace.ts,
        )]
        cols = [
            "trace_id", "session_id", "user_id", "agent_type",
            "query", "response", "generated_sql", "retrieved_chunk_ids",
            "total_latency_ms", "retrieval_latency_ms", "llm_latency_ms",
            "confidence_score", "groundedness_score", "input_tokens",
            "output_tokens", "estimated_cost_usd", "agent_version",
            "model", "ts",
        ]
        df = spark.createDataFrame(row, cols)
        df.write.format("delta").mode("append").saveAsTable(TRACE_TABLE)
    except Exception as e:
        print(f"[monitor] Warning: failed to write trace {trace.trace_id}: {e}")


def _write_groundedness_issues(trace_id: str, query: str, issues: list[str]):
    if not issues:
        return
    try:
        spark = SparkSession.builder.getOrCreate()
        rows = [(trace_id, query, claim, datetime.now(timezone.utc)) for claim in issues]
        df = spark.createDataFrame(rows, ["trace_id", "query", "unsupported_claim", "ts"])
        df.write.format("delta").mode("append").saveAsTable(ISSUES_TABLE)
    except Exception as e:
        print(f"[monitor] Warning: failed to write groundedness issues: {e}")


def _write_sql_log(
    trace_id: str, query: str, sql: str,
    success: bool, error: Optional[str],
    rows: int, ms: int,
):
    try:
        spark = SparkSession.builder.getOrCreate()
        row = [(
            trace_id, query, sql, success, error, rows, ms,
            AGENT_VERSION, datetime.now(timezone.utc),
        )]
        cols = [
            "trace_id", "query", "generated_sql", "executed_successfully",
            "error_message", "rows_returned", "execution_ms", "agent_version", "ts",
        ]
        df = spark.createDataFrame(row, cols)
        df.write.format("delta").mode("append").saveAsTable(SQL_LOG_TABLE)
    except Exception as e:
        print(f"[monitor] Warning: failed to write sql log: {e}")


# ── Groundedness scoring ──────────────────────────────────────────────────────

from agents.monitoring.groundedness import score_groundedness  # noqa: E402 (circular-safe)


# ── Decorator ─────────────────────────────────────────────────────────────────

def monitor(agent_type: str):
    """
    Decorator for agent call functions. Wraps the call with full observability.

    Decorated function must return a dict with at least:
      - "response": str
      - "confidence_score": float
      - "retrieved_chunk_ids": list[str]
      - "retrieved_context": str  (used for groundedness check)
      - "generated_sql": str | None
      - "input_tokens": int
      - "output_tokens": int
      - "retrieval_latency_ms": int
      - "llm_latency_ms": int

    Session context injected via thread-local or passed as kwargs:
      - session_id, user_id
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> dict:
            trace = AgentTrace(
                agent_type=agent_type,
                session_id=kwargs.pop("session_id", None),
                user_id=kwargs.pop("user_id", None),
                query=kwargs.get("query") or (args[0] if args else None),
            )

            t0 = time.perf_counter()
            try:
                result: dict = fn(*args, **kwargs)
            except Exception as exc:
                trace.total_latency_ms = int((time.perf_counter() - t0) * 1000)
                trace.response = f"ERROR: {exc}"
                _write_trace(trace)
                raise

            trace.total_latency_ms = int((time.perf_counter() - t0) * 1000)
            trace.response = result.get("response", "")
            trace.confidence_score = result.get("confidence_score", 0.0)
            trace.retrieved_chunk_ids = result.get("retrieved_chunk_ids", [])
            trace.generated_sql = result.get("generated_sql")
            trace.input_tokens = result.get("input_tokens", 0)
            trace.output_tokens = result.get("output_tokens", 0)
            trace.retrieval_latency_ms = result.get("retrieval_latency_ms", 0)
            trace.llm_latency_ms = result.get("llm_latency_ms", 0)
            trace.calculate_cost()

            # Groundedness: async to avoid blocking the response
            context = result.get("retrieved_context", "")
            if trace.response and context:
                try:
                    g_result = score_groundedness(trace.response, context)
                    trace.groundedness_score = g_result["score"]
                    if g_result.get("unsupported_claims"):
                        _write_groundedness_issues(
                            trace.trace_id,
                            trace.query or "",
                            g_result["unsupported_claims"],
                        )
                except Exception as e:
                    print(f"[monitor] Groundedness scoring failed: {e}")

            # SQL log
            if trace.generated_sql:
                _write_sql_log(
                    trace.trace_id, trace.query or "",
                    trace.generated_sql,
                    result.get("sql_success", True),
                    result.get("sql_error"),
                    result.get("sql_rows", 0),
                    result.get("sql_latency_ms", 0),
                )

            # Non-blocking Delta write
            import threading
            threading.Thread(target=_write_trace, args=(trace,), daemon=True).start()

            # Attach trace_id to result for downstream use (e.g., UI feedback)
            result["trace_id"] = trace.trace_id
            result["groundedness_score"] = trace.groundedness_score
            result["estimated_cost_usd"] = trace.estimated_cost_usd
            return result

        return wrapper
    return decorator
