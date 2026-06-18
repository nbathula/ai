"""
Text-to-SQL Tool
Translates a natural language query into SQL against Delta Lake metrics tables,
executes it, and returns structured results.

Safety: only SELECT is allowed; DML is rejected before execution.
"""

import re
import time
from typing import Any

from agents.llm import chat
from pyspark.sql import SparkSession

# ── Table schema context injected into the prompt ─────────────────────────────

SCHEMA_CONTEXT = """
Available Delta Lake tables in catalog `sales`:

-- sales.realtime.opportunity_current
opportunity_id STRING, account_id STRING, account_name STRING,
owner_id STRING, owner_name STRING, name STRING,
amount DECIMAL(18,2), acv DECIMAL(18,2),
stage STRING,          -- e.g. 'Prospecting','Discovery','Proposal','Negotiation','Closed Won','Closed Lost'
close_date DATE, probability INTEGER,
forecast_category STRING,   -- 'Commit','Best Case','Pipeline','Omitted'
days_in_stage INTEGER, last_activity_date DATE,
health_score INTEGER, created_date DATE, updated_at TIMESTAMP

-- sales.realtime.account_current
account_id STRING, name STRING, industry STRING,
segment STRING,     -- 'Enterprise','Mid-Market','SMB'
owner_id STRING, csm_id STRING,
arr DECIMAL(18,2), health_score INTEGER,
risk_tier STRING,   -- 'Green','Yellow','Red'
contract_end DATE, updated_at TIMESTAMP

-- sales.metrics.arr
account_id STRING, account_name STRING,
arr DECIMAL(18,2), mrr DECIMAL(18,2), arr_growth_pct DOUBLE,
new_logo_arr DECIMAL(18,2), expansion_arr DECIMAL(18,2),
churn_arr DECIMAL(18,2), segment STRING,
snapshot_date DATE, synced_at TIMESTAMP

-- sales.metrics.acv
opportunity_id STRING, account_id STRING, account_name STRING,
acv DECIMAL(18,2), tcv DECIMAL(18,2), term_months INTEGER,
close_date DATE, segment STRING, synced_at TIMESTAMP

-- sales.metrics.health
account_id STRING, account_name STRING,
health_score INTEGER, nps_score INTEGER, csat_score DOUBLE,
risk_tier STRING, dau_30d INTEGER, feature_adoption DOUBLE,
last_login_date DATE, snapshot_date DATE, synced_at TIMESTAMP

-- sales.metrics.forecast
period STRING, segment STRING, owner_id STRING,
committed DECIMAL(18,2), best_case DECIMAL(18,2),
pipeline DECIMAL(18,2), closed_won DECIMAL(18,2),
quota DECIMAL(18,2), attainment_pct DOUBLE,
snapshot_date DATE, synced_at TIMESTAMP

-- sales.metrics.nrr
period STRING, segment STRING,
nrr_pct DOUBLE, gross_retention DOUBLE,
expansion_pct DOUBLE, churn_pct DOUBLE,
snapshot_date DATE, synced_at TIMESTAMP
"""

SQL_SYSTEM_PROMPT = f"""You are a SQL expert for a B2B SaaS sales analytics platform.
Generate a single Spark SQL SELECT query to answer the user's question.

Rules:
- Only use tables listed in the schema. Never invent table or column names.
- Only generate SELECT statements. Never generate INSERT, UPDATE, DELETE, DROP, or CREATE.
- For current quarter, use: QUARTER(CURRENT_DATE()) and YEAR(CURRENT_DATE())
- For "at risk" accounts use: health_score < 60 OR risk_tier IN ('Yellow','Red')
- Format monetary output with ROUND(..., 2).
- Limit results to 100 rows unless the user asks for all.
- Return only the SQL — no explanation, no markdown fences.

Schema:
{SCHEMA_CONTEXT}
"""

UNSAFE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)
MAX_ROWS = 200


def generate_sql(query: str) -> dict:
    """Ask Claude to generate SQL for the natural language query."""
    llm_result = chat(
        messages=[{"role": "user", "content": query}],
        system=SQL_SYSTEM_PROMPT,
        max_tokens=512,
        temperature=0.0,
    )
    sql = llm_result["content"].strip()
    # Strip any accidental markdown fences
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*```$", "", sql)
    return {
        "sql": sql,
        "input_tokens": llm_result["input_tokens"],
        "output_tokens": llm_result["output_tokens"],
        "llm_latency_ms": llm_result["latency_ms"],
    }


def is_safe(sql: str) -> bool:
    return not UNSAFE_PATTERN.search(sql)


def execute_sql(sql: str) -> dict:
    """Execute the generated SQL and return rows as a list of dicts."""
    if not is_safe(sql):
        return {
            "success": False,
            "error": "SQL contains disallowed statement type",
            "rows": [],
            "row_count": 0,
            "execution_ms": 0,
        }

    spark = SparkSession.builder.getOrCreate()
    t0 = time.perf_counter()
    try:
        df = spark.sql(sql)
        rows = [row.asDict() for row in df.limit(MAX_ROWS).collect()]
        ms = int((time.perf_counter() - t0) * 1000)
        return {
            "success": True,
            "error": None,
            "rows": rows,
            "row_count": len(rows),
            "execution_ms": ms,
        }
    except Exception as e:
        ms = int((time.perf_counter() - t0) * 1000)
        return {
            "success": False,
            "error": str(e),
            "rows": [],
            "row_count": 0,
            "execution_ms": ms,
        }


def rows_to_text(rows: list[dict]) -> str:
    """Convert SQL result rows to a compact text block for the LLM context."""
    if not rows:
        return "Query returned no results."
    keys = list(rows[0].keys())
    header = " | ".join(keys)
    lines = [header, "-" * len(header)]
    for row in rows[:50]:   # cap context at 50 rows
        lines.append(" | ".join(str(row.get(k, "")) for k in keys))
    if len(rows) > 50:
        lines.append(f"... and {len(rows) - 50} more rows")
    return "\n".join(lines)


def run(query: str) -> dict:
    """
    Full Text-to-SQL pipeline: generate → validate → execute → format.
    Returns everything the monitor decorator needs.
    """
    gen = generate_sql(query)
    sql = gen["sql"]
    exec_result = execute_sql(sql)

    return {
        "sql": sql,
        "rows": exec_result["rows"],
        "row_count": exec_result["row_count"],
        "context_text": rows_to_text(exec_result["rows"]),
        "sql_success": exec_result["success"],
        "sql_error": exec_result["error"],
        "sql_rows": exec_result["row_count"],
        "sql_latency_ms": exec_result["execution_ms"],
        "input_tokens": gen["input_tokens"],
        "output_tokens": gen["output_tokens"],
        "llm_latency_ms": gen["llm_latency_ms"],
    }
