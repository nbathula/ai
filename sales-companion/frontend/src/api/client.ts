import axios from "axios";
import { QueryResponse } from "../types";

const http = axios.create({
  baseURL: "/api",
  headers: {
    "Content-Type": "application/json",
    "X-API-Key": import.meta.env.VITE_API_KEY ?? "",
  },
});

export async function sendQuery(
  query: string,
  sessionId: string,
  userId: string
): Promise<QueryResponse> {
  const { data } = await http.post<QueryResponse>("/query", {
    query,
    session_id: sessionId,
    user_id: userId,
  });
  return data;
}

export async function sendFeedback(
  traceId: string,
  userId: string,
  rating: 1 | 2,
  comment?: string
): Promise<void> {
  await http.post("/feedback", { trace_id: traceId, user_id: userId, rating, comment });
}
