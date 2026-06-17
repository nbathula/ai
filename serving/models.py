from pydantic import BaseModel, Field
from typing import Optional


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    session_id: Optional[str] = None
    user_id: Optional[str] = None


class QueryResponse(BaseModel):
    trace_id: str
    response: str
    confidence_score: float
    groundedness_score: float
    agent_type: str
    generated_sql: Optional[str] = None
    retrieved_chunk_ids: list[str] = []
    total_latency_ms: int
    estimated_cost_usd: float


class FeedbackRequest(BaseModel):
    trace_id: str
    user_id: str
    rating: int = Field(..., ge=1, le=2)   # 1 = thumbs down, 2 = thumbs up
    comment: Optional[str] = None


class FeedbackResponse(BaseModel):
    status: str = "recorded"


class HealthResponse(BaseModel):
    status: str
    version: str
