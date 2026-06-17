import React, { useEffect, useRef, useState } from "react";
import { v4 as uuidv4 } from "uuid";
import { sendQuery } from "./api/client";
import { MessageBubble } from "./components/MessageBubble";
import { QueryInput } from "./components/QueryInput";
import { Message } from "./types";

const SESSION_ID = uuidv4();
const USER_ID = "user-" + Math.random().toString(36).slice(2, 8);

const WELCOME: Message = {
  id: uuidv4(),
  role: "assistant",
  content: "Hi! I'm **Sales Companion**. Ask me about your pipeline, forecast, account health, renewals, or contracts.",
  timestamp: new Date(),
};

export default function App() {
  const [messages, setMessages] = useState<Message[]>([WELCOME]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function handleQuery(query: string) {
    setError(null);

    const userMsg: Message = {
      id: uuidv4(),
      role: "user",
      content: query,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setLoading(true);

    try {
      const result = await sendQuery(query, SESSION_ID, USER_ID);
      const assistantMsg: Message = {
        id: uuidv4(),
        role: "assistant",
        content: result.response,
        timestamp: new Date(),
        traceId: result.trace_id,
        confidenceScore: result.confidence_score,
        groundednessScore: result.groundedness_score,
        agentType: result.agent_type,
        generatedSql: result.generated_sql ?? undefined,
        totalLatencyMs: result.total_latency_ms,
        estimatedCostUsd: result.estimated_cost_usd,
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err: any) {
      setError(err?.response?.data?.detail ?? "Something went wrong. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100vh",
        maxWidth: 860,
        margin: "0 auto",
        padding: "0 16px",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "16px 0 12px",
          borderBottom: "1px solid #e2e8f0",
        }}
      >
        <div
          style={{
            width: 32,
            height: 32,
            background: "#0f172a",
            borderRadius: 8,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "#fff",
            fontSize: 16,
          }}
        >
          S
        </div>
        <div>
          <div style={{ fontWeight: 700, fontSize: 15, color: "#0f172a" }}>Sales Companion</div>
          <div style={{ fontSize: 11, color: "#64748b" }}>AI · Pipeline & Customer Health</div>
        </div>
      </div>

      {/* Messages */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "20px 0",
        }}
      >
        {messages.map((msg) => (
          <MessageBubble key={msg.id} message={msg} userId={USER_ID} />
        ))}

        {loading && (
          <div style={{ display: "flex", justifyContent: "flex-start", marginBottom: 20 }}>
            <div
              style={{
                background: "#fff",
                border: "1px solid #e2e8f0",
                borderRadius: "4px 16px 16px 16px",
                padding: "12px 20px",
                fontSize: 20,
                letterSpacing: 4,
                color: "#94a3b8",
              }}
            >
              ···
            </div>
          </div>
        )}

        {error && (
          <div
            style={{
              margin: "8px 0 16px",
              padding: "10px 14px",
              background: "#fef2f2",
              border: "1px solid #fca5a5",
              borderRadius: 8,
              fontSize: 13,
              color: "#b91c1c",
            }}
          >
            {error}
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div style={{ padding: "12px 0 20px", borderTop: "1px solid #e2e8f0" }}>
        <QueryInput onSubmit={handleQuery} disabled={loading} />
      </div>
    </div>
  );
}
