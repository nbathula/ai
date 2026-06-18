"""
Pipeline Health Agent
Answers questions about sales pipeline, forecast, deals, ARR, ACV, and quota.

Routing logic:
  - Pure metrics question  → Text-to-SQL only
  - Document question      → Vector Search only (proposals, call notes)
  - Mixed                  → both, merged context
"""

import time
from typing import TypedDict

from agents.llm import chat
from agents.monitoring.monitor import monitor
from agents.tools import text_to_sql, vector_search
from langgraph.graph import END, StateGraph

AGENT_TYPE = "pipeline_health"

CLASSIFY_SYSTEM = """You are a query classifier for a B2B SaaS sales assistant.
Classify the query into one of three categories:
  SQL      — needs live metrics from database (pipeline totals, deal stages, forecast, ARR, ACV, quota, win rate)
  DOCS     — needs unstructured documents (proposals, call transcripts, emails)
  HYBRID   — needs both

Reply with exactly one word: SQL, DOCS, or HYBRID."""

RESPONSE_SYSTEM = """You are Sales Companion, an AI assistant for B2B SaaS sales teams.
Answer the user's question using ONLY the data provided below.
Be concise and specific. Include relevant numbers. Use bullet points for lists.
If the data is insufficient, say so — never invent numbers.
Always end with a one-line "Key Takeaway:" summary."""


class AgentState(TypedDict):
    query: str
    classification: str
    sql_result: dict
    vs_result: dict
    combined_context: str
    response: str
    confidence_score: float
    input_tokens: int
    output_tokens: int
    retrieval_latency_ms: int
    llm_latency_ms: int
    retrieved_chunk_ids: list[str]
    generated_sql: str | None
    sql_success: bool
    sql_error: str | None
    sql_rows: int
    sql_latency_ms: int


# ── Graph nodes ────────────────────────────────────────────────────────────────

def classify(state: AgentState) -> AgentState:
    result = chat(
        messages=[{"role": "user", "content": state["query"]}],
        system=CLASSIFY_SYSTEM,
        max_tokens=10,
        temperature=0.0,
    )
    classification = result["content"].strip().upper()
    if classification not in ("SQL", "DOCS", "HYBRID"):
        classification = "SQL"
    state["classification"] = classification
    state["input_tokens"] = result["input_tokens"]
    state["output_tokens"] = result["output_tokens"]
    return state


def run_sql(state: AgentState) -> AgentState:
    t0 = time.perf_counter()
    result = text_to_sql.run(state["query"])
    state["retrieval_latency_ms"] = int((time.perf_counter() - t0) * 1000)
    state["sql_result"] = result
    state["generated_sql"] = result["sql"]
    state["sql_success"] = result["sql_success"]
    state["sql_error"] = result.get("sql_error")
    state["sql_rows"] = result.get("sql_rows", 0)
    state["sql_latency_ms"] = result.get("sql_latency_ms", 0)
    state["input_tokens"] += result.get("input_tokens", 0)
    state["output_tokens"] += result.get("output_tokens", 0)
    return state


def run_vector_search(state: AgentState) -> AgentState:
    t0 = time.perf_counter()
    result = vector_search.search(
        state["query"],
        doc_type="proposals",
        num_results=6,
    )
    elapsed = int((time.perf_counter() - t0) * 1000)
    state["vs_result"] = result
    state["retrieved_chunk_ids"] = result.get("chunk_ids", [])
    state["retrieval_latency_ms"] = state.get("retrieval_latency_ms", 0) + elapsed
    return state


def build_context(state: AgentState) -> AgentState:
    parts = []
    classification = state.get("classification", "SQL")

    if classification in ("SQL", "HYBRID") and state.get("sql_result"):
        sql_ctx = state["sql_result"].get("context_text", "")
        if sql_ctx:
            parts.append(f"## Live Metrics Data\n{sql_ctx}")

    if classification in ("DOCS", "HYBRID") and state.get("vs_result"):
        doc_ctx = state["vs_result"].get("context_text", "")
        if doc_ctx:
            parts.append(f"## Relevant Documents\n{doc_ctx}")

    state["combined_context"] = "\n\n".join(parts) if parts else "No data available."
    return state


def generate_response(state: AgentState) -> AgentState:
    context = state["combined_context"]
    user_prompt = f"Question: {state['query']}\n\nData:\n{context}"

    t0 = time.perf_counter()
    result = chat(
        messages=[{"role": "user", "content": user_prompt}],
        system=RESPONSE_SYSTEM,
        max_tokens=1024,
        temperature=0.0,
    )
    state["llm_latency_ms"] = int((time.perf_counter() - t0) * 1000)
    state["response"] = result["content"]
    state["input_tokens"] += result["input_tokens"]
    state["output_tokens"] += result["output_tokens"]

    # Confidence: penalize missing data or SQL failure
    confidence = 0.9
    if not state.get("sql_success", True):
        confidence -= 0.2
    if "no data" in state["combined_context"].lower():
        confidence -= 0.2
    if "no results" in state["combined_context"].lower():
        confidence -= 0.1
    state["confidence_score"] = max(0.1, round(confidence, 2))

    return state


# ── Router ────────────────────────────────────────────────────────────────────

def route_after_classify(state: AgentState) -> str:
    c = state.get("classification", "SQL")
    if c == "DOCS":
        return "vector_search"
    if c == "HYBRID":
        return "sql_and_search"
    return "sql"


# ── Build the graph ────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("classify", classify)
    graph.add_node("sql", run_sql)
    graph.add_node("vector_search", run_vector_search)
    graph.add_node("build_context", build_context)
    graph.add_node("generate", generate_response)

    # Parallel SQL + vector search for HYBRID
    def run_both(state: AgentState) -> AgentState:
        state = run_sql(state)
        state = run_vector_search(state)
        return state

    graph.add_node("sql_and_search", run_both)

    graph.set_entry_point("classify")
    graph.add_conditional_edges(
        "classify",
        route_after_classify,
        {
            "sql": "sql",
            "vector_search": "vector_search",
            "sql_and_search": "sql_and_search",
        },
    )
    graph.add_edge("sql", "build_context")
    graph.add_edge("vector_search", "build_context")
    graph.add_edge("sql_and_search", "build_context")
    graph.add_edge("build_context", "generate")
    graph.add_edge("generate", END)

    return graph.compile()


_graph = None


def _get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


# ── Public entry point (wrapped with monitoring) ──────────────────────────────

@monitor(AGENT_TYPE)
def run(query: str, **kwargs) -> dict:
    initial_state: AgentState = {
        "query": query,
        "classification": "",
        "sql_result": {},
        "vs_result": {},
        "combined_context": "",
        "response": "",
        "confidence_score": 0.0,
        "input_tokens": 0,
        "output_tokens": 0,
        "retrieval_latency_ms": 0,
        "llm_latency_ms": 0,
        "retrieved_chunk_ids": [],
        "generated_sql": None,
        "sql_success": True,
        "sql_error": None,
        "sql_rows": 0,
        "sql_latency_ms": 0,
    }

    final_state = _get_graph().invoke(initial_state)

    return {
        "response": final_state["response"],
        "confidence_score": final_state["confidence_score"],
        "retrieved_context": final_state["combined_context"],
        "retrieved_chunk_ids": final_state["retrieved_chunk_ids"],
        "generated_sql": final_state.get("generated_sql"),
        "input_tokens": final_state["input_tokens"],
        "output_tokens": final_state["output_tokens"],
        "retrieval_latency_ms": final_state["retrieval_latency_ms"],
        "llm_latency_ms": final_state["llm_latency_ms"],
        "sql_success": final_state.get("sql_success", True),
        "sql_error": final_state.get("sql_error"),
        "sql_rows": final_state.get("sql_rows", 0),
        "sql_latency_ms": final_state.get("sql_latency_ms", 0),
    }
