"""
Vector Search Tool
Retrieves relevant document chunks from Databricks Vector Search
for unstructured document queries (contracts, proposals, transcripts).
"""

import os
import time
from typing import Any

import requests
from databricks.vector_search.client import VectorSearchClient

VS_ENDPOINT = os.environ.get("VS_ENDPOINT", "sales-companion-vs")
VS_INDEX = "sales.documents.chunks_index"
EMBED_ENDPOINT = os.environ.get("EMBED_ENDPOINT", "bge-large-en")

_DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
_TOKEN: str | None = None
_VS_CLIENT: VectorSearchClient | None = None


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


def _get_vs_client() -> VectorSearchClient:
    global _VS_CLIENT
    if _VS_CLIENT is None:
        _VS_CLIENT = VectorSearchClient(
            workspace_url=_DATABRICKS_HOST,
            personal_access_token=_get_token(),
        )
    return _VS_CLIENT


def embed_query(query: str) -> list[float]:
    """Embed the query using the same BGE model used during ingestion."""
    resp = requests.post(
        f"{_DATABRICKS_HOST}/serving-endpoints/{EMBED_ENDPOINT}/invocations",
        headers={
            "Authorization": f"Bearer {_get_token()}",
            "Content-Type": "application/json",
        },
        json={"input": [query]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["data"][0]["embedding"]


def search(
    query: str,
    *,
    num_results: int = 8,
    doc_type: str | None = None,
    account_id: str | None = None,
    account_name: str | None = None,
) -> dict:
    """
    Search the Vector Search index for chunks relevant to the query.

    Filters:
      doc_type     -- e.g. 'contracts', 'proposals', 'transcripts'
      account_id   -- scope to a specific account
      account_name -- alternative to account_id (substring match via filter)

    Returns:
      chunks       -- list of matching chunks with metadata
      context_text -- concatenated chunk content for LLM context
      chunk_ids    -- list of chunk_ids for monitoring
      latency_ms   -- retrieval time
    """
    t0 = time.perf_counter()

    query_vector = embed_query(query)

    # Build filter expression for Databricks Vector Search
    filters: list[str] = []
    if doc_type:
        filters.append(f"doc_type = '{doc_type}'")
    if account_id:
        filters.append(f"account_id = '{account_id}'")

    filter_expr = " AND ".join(filters) if filters else None

    vsc = _get_vs_client()
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

    search_kwargs: dict[str, Any] = {
        "query_vector": query_vector,
        "columns": [
            "chunk_id", "doc_id", "doc_type", "account_name",
            "opportunity_id", "doc_date", "content", "source_page",
        ],
        "num_results": num_results,
    }
    if filter_expr:
        search_kwargs["filters"] = filter_expr

    # account_name is not in the index filter — apply post-search
    raw_results = index.similarity_search(**search_kwargs)

    chunks = raw_results.get("result", {}).get("data_array", [])
    cols = [c["name"] for c in raw_results.get("result", {}).get("columns", [])]

    parsed_chunks = []
    for row in chunks:
        chunk = dict(zip(cols, row))
        if account_name and account_name.lower() not in (chunk.get("account_name") or "").lower():
            continue
        parsed_chunks.append(chunk)

    latency_ms = int((time.perf_counter() - t0) * 1000)

    # Build context text for LLM
    context_parts = []
    for i, chunk in enumerate(parsed_chunks, 1):
        source = f"[{i}] {chunk.get('doc_type', 'doc')} | {chunk.get('account_name', '')} | page {chunk.get('source_page', '?')}"
        context_parts.append(f"{source}\n{chunk.get('content', '')}")
    context_text = "\n\n---\n\n".join(context_parts) if context_parts else "No relevant documents found."

    return {
        "chunks": parsed_chunks,
        "context_text": context_text,
        "chunk_ids": [c.get("chunk_id", "") for c in parsed_chunks],
        "latency_ms": latency_ms,
        "result_count": len(parsed_chunks),
    }
