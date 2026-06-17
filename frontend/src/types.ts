export interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  traceId?: string;
  confidenceScore?: number;
  groundednessScore?: number;
  agentType?: string;
  generatedSql?: string;
  totalLatencyMs?: number;
  estimatedCostUsd?: number;
  feedback?: 1 | 2;   // 1 = thumbs down, 2 = thumbs up
}

export interface QueryResponse {
  trace_id: string;
  response: string;
  confidence_score: number;
  groundedness_score: number;
  agent_type: string;
  generated_sql?: string;
  retrieved_chunk_ids: string[];
  total_latency_ms: number;
  estimated_cost_usd: number;
}
