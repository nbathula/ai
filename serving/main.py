"""
Sprint 4 — FastAPI Serving Layer
Exposes Sales Companion agents as a REST API.

Endpoints:
  POST /query      — ask a question, get a response
  POST /feedback   — submit thumbs up/down
  GET  /health     — liveness check
"""

import os
import time
import uuid
from datetime import datetime, timezone

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from serving.auth import require_api_key
from serving.models import (
    FeedbackRequest, FeedbackResponse,
    HealthResponse, QueryRequest, QueryResponse,
)

AGENT_VERSION = os.environ.get("AGENT_VERSION", "0.1.0")

app = FastAPI(
    title="Sales Companion API",
    version=AGENT_VERSION,
    docs_url="/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health():
    return HealthResponse(status="ok", version=AGENT_VERSION)


@app.post("/query", response_model=QueryResponse, tags=["agent"])
async def query(
    request: QueryRequest,
    _: str = Depends(require_api_key),
):
    session_id = request.session_id or str(uuid.uuid4())
    user_id = request.user_id or "anonymous"

    t0 = time.perf_counter()

    from agents.router import route
    result = route(
        query=request.query,
        session_id=session_id,
        user_id=user_id,
    )

    total_ms = int((time.perf_counter() - t0) * 1000)

    return QueryResponse(
        trace_id=result.get("trace_id", str(uuid.uuid4())),
        response=result["response"],
        confidence_score=result.get("confidence_score", 0.0),
        groundedness_score=result.get("groundedness_score", 0.0),
        agent_type=result.get("agent_type", "unknown"),
        generated_sql=result.get("generated_sql"),
        retrieved_chunk_ids=result.get("retrieved_chunk_ids", []),
        total_latency_ms=total_ms,
        estimated_cost_usd=result.get("estimated_cost_usd", 0.0),
    )


@app.post("/feedback", response_model=FeedbackResponse, tags=["agent"])
async def feedback(
    request: FeedbackRequest,
    _: str = Depends(require_api_key),
):
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        row = [(
            request.trace_id,
            request.user_id,
            request.rating,
            request.comment,
            datetime.now(timezone.utc),
        )]
        df = spark.createDataFrame(row, ["trace_id", "user_id", "rating", "comment", "ts"])
        df.write.format("delta").mode("append").saveAsTable("sales.monitoring.user_feedback")
    except Exception as e:
        # Don't fail the request if Delta write fails — log and continue
        print(f"[feedback] Warning: {e}")

    return FeedbackResponse()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "serving.main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=os.environ.get("ENV", "prod") == "dev",
        workers=int(os.environ.get("WORKERS", 1)),
    )
