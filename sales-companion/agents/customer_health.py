"""
Customer Health Agent
Answers questions about account health, churn risk, NRR, renewals,
contract terms, and customer sentiment.

Routing logic:
  - Metrics question   → Text-to-SQL (health scores, NRR, ARR, churn)
  - Contract/doc query → Vector Search (contract terms, renewal clauses)
  - Mixed              → both
"""

import time
from typing import TypedDict

from agents.llm import chat
from agents.monitoring.monitor import monitor
from agents.tools import text_to_sql, vector_search
from langgraph.graph import END, StateGraph

AGENT_TYPE = "customer_health"

CLASSIFY_SYSTEM = """You are a query classifier for a B2B SaaS customer success assistant.
Classify the query into one of three categories:
  SQL      — needs live data (health scores, NRR, churn rate, ARR, contract dates, renewal status)
  DOCS     — needs documents (contract terms, renewal clauses, call transcripts, proposals, emails)
  HYBRID   — needs both

Reply with exactly one word: SQL, DOCS, or HYBRID."""

RESPONSE_SYSTEM = """You are Sales Companion, an AI assistant for B2B SaaS customer success teams.
Answer using ONLY the data provided. Be specific — include account names, scores, and dates.
For at-risk accounts, clearly explain WHY they are at risk.
For renewal questions, always state the contract end date.
Never invent data. If data is missing, say so explicitly.
End with "Key Takeaway:" summarizing the most important action to take."""

# ── Thresholds for risk classification ────────────────────────────────────────

RISK_SQL = """
SELECT
  a.account_id,
  a.name AS account_name,
  a.health_score,
  a.risk_tier,
  a.arr,
  a.contract_end,
  a.segment,
  h.nps_score,
  h.feature_adoption,
  h.last_login_date
FROM sales.realtime.account_current a
LEFT JOIN sales.metrics.health h USING (account_id)
WHERE a.risk_tier IN ('Yellow','Red') OR a.health_score < 60
ORDER BY a.health_score ASC, a.arr DESC
LIMIT 25
"""


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


# ── Nodes ──────────────────────────────────────────────────────────────────────

def classify(state: AgentState) -> AgentState:
    # Keyword shortcuts to avoid unnecessary LLM call
    q_lower = state["query"].lower()
    if any(kw in q_lower for kw in ["contract", "renewal", "clause", "term", "nda", "agreement"]):
        if any(kw in q_lower for kw in ["score", "nrr", "churn", "arr", "risk", "health"]):
            state["classification"] = "HYBRID"
        else:
            state["classification"] = "DOCS"
        state["input_tokens"] = 0
        state["output_tokens"] = 0
        return state

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

    # For generic churn/risk queries, use the pre-built risk SQL
    q_lower = state["query"].lower()
    if any(kw in q_lower for kw in ["at risk", "churn", "churning"]) and "?" not in state["query"]:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        rows = [r.asDict() for r in spark.sql(RISK_SQL).collect()]
        ms = int((time.perf_counter() - t0) * 1000)
        from agents.tools.text_to_sql import rows_to_text
        state["sql_result"] = {
            "sql": RISK_SQL.strip(),
            "rows": rows,
            "row_count": len(rows),
            "context_text": rows_to_text(rows),
            "sql_success": True,
            "sql_error": None,
            "sql_rows": len(rows),
            "sql_latency_ms": ms,
            "input_tokens": 0,
            "output_tokens": 0,
        }
        state["generated_sql"] = RISK_SQL.strip()
        state["sql_success"] = True
        state["sql_error"] = None
        state["sql_rows"] = len(rows)
        state["sql_latency_ms"] = ms
        state["retrieval_latency_ms"] = ms
    else:
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

    # Try to extract account name from query for targeted search
    account_name = None
    q = state["query"]
    for phrase in ["for ", "of ", "about ", "with "]:
        idx = q.lower().find(phrase)
        if idx != -1:
            candidate = q[idx + len(phrase):].split()[0].rstrip("?.,")
            if len(candidate) > 3 and candidate[0].isupper():
                account_name = candidate
                break

    result = vector_search.search(
        state["query"],
        doc_type="contracts",
        num_results=6,
        account_name=account_name,
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
        ctx = state["sql_result"].get("context_text", "")
        if ctx:
            parts.append(f"## Live Account & Health Data\n{ctx}")

    if classification in ("DOCS", "HYBRID") and state.get("vs_result"):
        ctx = state["vs_result"].get("context_text", "")
        if ctx:
            parts.append(f"## Contract & Document Context\n{ctx}")

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

    confidence = 0.9
    if not state.get("sql_success", True):
        confidence -= 0.2
    if "no data" in context.lower() or "no results" in context.lower():
        confidence -= 0.15
    if "no relevant documents" in context.lower() and state["classification"] in ("DOCS", "HYBRID"):
        confidence -= 0.1
    state["confidence_score"] = max(0.1, round(confidence, 2))

    return state


# ── Graph ──────────────────────────────────────────────────────────────────────

def route_after_classify(state: AgentState) -> str:
    c = state.get("classification", "SQL")
    if c == "DOCS":
        return "vector_search"
    if c == "HYBRID":
        return "sql_and_search"
    return "sql"


def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    def run_both(state: AgentState) -> AgentState:
        state = run_sql(state)
        state = run_vector_search(state)
        return state

    graph.add_node("classify", classify)
    graph.add_node("sql", run_sql)
    graph.add_node("vector_search", run_vector_search)
    graph.add_node("sql_and_search", run_both)
    graph.add_node("build_context", build_context)
    graph.add_node("generate", generate_response)

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


# ── Public entry point ────────────────────────────────────────────────────────

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
