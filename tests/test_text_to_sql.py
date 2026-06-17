"""
Unit tests for the Text-to-SQL safety layer.
These run without Spark or a real LLM — they test the guard logic only.
"""

import pytest
from agents.tools.text_to_sql import is_safe, rows_to_text

SAFE_QUERIES = [
    "SELECT * FROM sales.metrics.arr",
    "SELECT account_id, arr FROM sales.metrics.arr WHERE segment = 'Enterprise'",
    "SELECT COUNT(*) FROM sales.realtime.opportunity_current WHERE stage = 'Negotiation'",
    "WITH cte AS (SELECT * FROM sales.metrics.forecast) SELECT * FROM cte",
]

UNSAFE_QUERIES = [
    "DELETE FROM sales.metrics.arr",
    "DROP TABLE sales.realtime.opportunity_current",
    "UPDATE sales.metrics.health SET health_score = 0",
    "TRUNCATE TABLE sales.monitoring.agent_traces",
    "INSERT INTO sales.metrics.arr VALUES ('x', 0)",
    "ALTER TABLE sales.metrics.arr ADD COLUMN foo STRING",
    # Injection via comment bypass attempt
    "SELECT 1; DELETE FROM sales.metrics.arr --",
]


def test_safe_queries_pass():
    for sql in SAFE_QUERIES:
        assert is_safe(sql), f"Expected safe: {sql}"


def test_unsafe_queries_blocked():
    for sql in UNSAFE_QUERIES:
        assert not is_safe(sql), f"Expected blocked: {sql}"


def test_rows_to_text_empty():
    result = rows_to_text([])
    assert result == "Query returned no results."


def test_rows_to_text_formats_header():
    rows = [{"account_name": "Acme", "arr": 100000}, {"account_name": "Globex", "arr": 250000}]
    result = rows_to_text(rows)
    assert "account_name" in result
    assert "Acme" in result
    assert "Globex" in result


def test_rows_to_text_caps_at_50():
    rows = [{"id": str(i)} for i in range(100)]
    result = rows_to_text(rows)
    assert "50 more rows" in result
