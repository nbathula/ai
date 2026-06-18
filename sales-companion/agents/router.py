"""
Agent Router
Top-level entry point for Sales Companion.
Classifies the incoming query and dispatches to the right agent.

Query → Router → PipelineHealthAgent | CustomerHealthAgent
"""

import re
from agents.llm import chat

ROUTE_SYSTEM = """You are a routing classifier for a B2B SaaS sales AI assistant.
Given a user query, determine which agent should handle it:

  PIPELINE  — pipeline totals, deal stages, forecast, quota attainment, win rate,
              ACV, ARR new logo, stuck deals, close date slippage, stage progression

  CUSTOMER  — account health, churn risk, NRR, renewal dates, contract terms,
              customer sentiment, adoption, at-risk accounts, expansion

  AMBIGUOUS — clearly about both (e.g. "which enterprise accounts in pipeline are at risk?")

Reply with exactly one word: PIPELINE, CUSTOMER, or AMBIGUOUS."""

# Fast keyword shortcuts — bypass LLM classification for obvious cases
_PIPELINE_KEYWORDS = re.compile(
    r"\b(pipeline|forecast|quota|win rate|deal|stage|close date|acv|new logo|slipp|stuck)\b",
    re.IGNORECASE,
)
_CUSTOMER_KEYWORDS = re.compile(
    r"\b(health|churn|nrr|renewal|contract|at.risk|adoption|retention|csm|expansion|sentiment)\b",
    re.IGNORECASE,
)


def _fast_route(query: str) -> str | None:
    has_pipeline = bool(_PIPELINE_KEYWORDS.search(query))
    has_customer = bool(_CUSTOMER_KEYWORDS.search(query))
    if has_pipeline and not has_customer:
        return "PIPELINE"
    if has_customer and not has_pipeline:
        return "CUSTOMER"
    return None   # fall through to LLM


def classify_query(query: str) -> str:
    fast = _fast_route(query)
    if fast:
        return fast

    result = chat(
        messages=[{"role": "user", "content": query}],
        system=ROUTE_SYSTEM,
        max_tokens=10,
        temperature=0.0,
    )
    label = result["content"].strip().upper()
    if label not in ("PIPELINE", "CUSTOMER", "AMBIGUOUS"):
        label = "PIPELINE"
    return label


def route(query: str, session_id: str, user_id: str) -> dict:
    """
    Route the query to the appropriate agent and return its full response dict.
    For AMBIGUOUS queries, calls both agents and merges responses.
    """
    label = classify_query(query)

    if label == "PIPELINE":
        from agents.pipeline_health import run
        return run(query, session_id=session_id, user_id=user_id)

    if label == "CUSTOMER":
        from agents.customer_health import run
        return run(query, session_id=session_id, user_id=user_id)

    # AMBIGUOUS — run both and combine
    from agents.pipeline_health import run as pipeline_run
    from agents.customer_health import run as customer_run

    pipeline_result = pipeline_run(query, session_id=session_id, user_id=user_id)
    customer_result = customer_run(query, session_id=session_id, user_id=user_id)

    combined_response = (
        "**Pipeline View:**\n" + pipeline_result["response"]
        + "\n\n---\n\n**Customer Health View:**\n" + customer_result["response"]
    )
    confidence = min(
        pipeline_result.get("confidence_score", 0.9),
        customer_result.get("confidence_score", 0.9),
    )

    return {
        "response": combined_response,
        "confidence_score": confidence,
        "agent_type": "ambiguous",
        "trace_id": pipeline_result.get("trace_id"),
        "retrieved_chunk_ids": (
            pipeline_result.get("retrieved_chunk_ids", [])
            + customer_result.get("retrieved_chunk_ids", [])
        ),
        "input_tokens": pipeline_result.get("input_tokens", 0) + customer_result.get("input_tokens", 0),
        "output_tokens": pipeline_result.get("output_tokens", 0) + customer_result.get("output_tokens", 0),
    }
