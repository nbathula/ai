"""
Unit tests for the query router's fast keyword classification.
These run without LLM calls — testing the regex shortcut path only.
"""

import pytest
from agents.router import _fast_route

PIPELINE_QUERIES = [
    "What is our total pipeline for Q2?",
    "Which deals are stuck in Negotiation?",
    "Show me the forecast vs quota attainment",
    "What is our win rate this quarter?",
    "Which opportunities will miss their close date?",
    "What is the ACV of deals closing this month?",
]

CUSTOMER_QUERIES = [
    "Which accounts are at risk of churning?",
    "What is our current NRR?",
    "Show contracts expiring in 90 days",
    "Which accounts have low health scores?",
    "What are the renewal terms for Acme Corp?",
    "Show me expansion opportunities for existing customers",
]

AMBIGUOUS_QUERIES = [
    # Neither keyword group fires exclusively → returns None → falls to LLM
    "Give me a summary of everything",
    "What should I focus on today?",
]


def test_pipeline_fast_route():
    for q in PIPELINE_QUERIES:
        result = _fast_route(q)
        assert result == "PIPELINE", f"Expected PIPELINE for: {q!r}, got {result!r}"


def test_customer_fast_route():
    for q in CUSTOMER_QUERIES:
        result = _fast_route(q)
        assert result == "CUSTOMER", f"Expected CUSTOMER for: {q!r}, got {result!r}"


def test_ambiguous_returns_none():
    for q in AMBIGUOUS_QUERIES:
        result = _fast_route(q)
        assert result is None, f"Expected None for: {q!r}, got {result!r}"
