"""
FastAPI endpoint tests using httpx TestClient.
Mocks the agent router so tests run without Databricks.
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

import os
os.environ["SALES_COMPANION_API_KEYS"] = "test-key"

from serving.main import app

client = TestClient(app)

MOCK_RESULT = {
    "response": "Your Q2 pipeline is $12.4M across 48 deals.",
    "confidence_score": 0.91,
    "groundedness_score": 0.88,
    "agent_type": "pipeline_health",
    "generated_sql": "SELECT SUM(amount) FROM sales.realtime.opportunity_current",
    "retrieved_chunk_ids": [],
    "trace_id": "test-trace-001",
    "estimated_cost_usd": 0.0012,
}


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_query_requires_auth():
    resp = client.post("/query", json={"query": "What is our pipeline?"})
    assert resp.status_code in (401, 403)


def test_query_invalid_key():
    resp = client.post(
        "/query",
        json={"query": "What is our pipeline?"},
        headers={"X-API-Key": "wrong-key"},
    )
    assert resp.status_code == 403


@patch("serving.main.route", return_value=MOCK_RESULT)
def test_query_success(mock_route):
    resp = client.post(
        "/query",
        json={"query": "What is our Q2 pipeline?", "user_id": "test-user"},
        headers={"X-API-Key": "test-key"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["response"] == MOCK_RESULT["response"]
    assert data["confidence_score"] == 0.91
    assert data["agent_type"] == "pipeline_health"
    assert data["generated_sql"] is not None
    mock_route.assert_called_once()


@patch("serving.main.route", return_value=MOCK_RESULT)
def test_query_empty_fails_validation(mock_route):
    resp = client.post(
        "/query",
        json={"query": ""},
        headers={"X-API-Key": "test-key"},
    )
    assert resp.status_code == 422


@patch("serving.main.SparkSession", side_effect=Exception("No Spark"))
def test_feedback_handles_spark_failure(mock_spark):
    resp = client.post(
        "/feedback",
        json={"trace_id": "abc", "user_id": "u1", "rating": 2},
        headers={"X-API-Key": "test-key"},
    )
    # Feedback is best-effort — should still return 200
    assert resp.status_code == 200
    assert resp.json()["status"] == "recorded"
