import React from "react";
import ReactMarkdown from "react-markdown";
import { Message } from "../types";
import { ConfidenceBadge } from "./ConfidenceBadge";
import { FeedbackBar } from "./FeedbackBar";
import { SqlDrawer } from "./SqlDrawer";

interface Props {
  message: Message;
  userId: string;
}

const AGENT_LABEL: Record<string, string> = {
  pipeline_health: "Pipeline Health",
  customer_health: "Customer Health",
  ambiguous: "Combined",
};

export function MessageBubble({ message, userId }: Props) {
  const isUser = message.role === "user";

  if (isUser) {
    return (
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 16 }}>
        <div
          style={{
            maxWidth: "70%",
            background: "#0f172a",
            color: "#f8fafc",
            borderRadius: "16px 16px 4px 16px",
            padding: "10px 16px",
            fontSize: 14,
            lineHeight: 1.5,
          }}
        >
          {message.content}
        </div>
      </div>
    );
  }

  const lowConfidence = (message.confidenceScore ?? 1) < 0.6;

  return (
    <div style={{ display: "flex", justifyContent: "flex-start", marginBottom: 20 }}>
      <div style={{ maxWidth: "80%" }}>
        {/* Agent badge */}
        {message.agentType && (
          <div style={{ marginBottom: 4, fontSize: 11, color: "#64748b" }}>
            {AGENT_LABEL[message.agentType] ?? message.agentType} Agent
          </div>
        )}

        {/* Response card */}
        <div
          style={{
            background: "#fff",
            border: lowConfidence ? "1px solid #fca5a5" : "1px solid #e2e8f0",
            borderRadius: "4px 16px 16px 16px",
            padding: "12px 16px",
            fontSize: 14,
            lineHeight: 1.6,
            color: "#1e293b",
          }}
        >
          <ReactMarkdown>{message.content}</ReactMarkdown>

          {lowConfidence && (
            <div
              style={{
                marginTop: 10,
                padding: "6px 10px",
                background: "#fef2f2",
                border: "1px solid #fca5a5",
                borderRadius: 4,
                fontSize: 12,
                color: "#b91c1c",
              }}
            >
              Low confidence — please verify with your team.
            </div>
          )}
        </div>

        {/* Metadata row */}
        <div style={{ display: "flex", gap: 8, marginTop: 6, flexWrap: "wrap", alignItems: "center" }}>
          {message.confidenceScore !== undefined && (
            <ConfidenceBadge score={message.confidenceScore} />
          )}
          {message.groundednessScore !== undefined && (
            <ConfidenceBadge score={message.groundednessScore} label="Grounded" />
          )}
          {message.totalLatencyMs !== undefined && (
            <span style={{ fontSize: 11, color: "#94a3b8" }}>
              {(message.totalLatencyMs / 1000).toFixed(1)}s
            </span>
          )}
          {message.estimatedCostUsd !== undefined && message.estimatedCostUsd > 0 && (
            <span style={{ fontSize: 11, color: "#94a3b8" }}>
              ${message.estimatedCostUsd.toFixed(4)}
            </span>
          )}
        </div>

        {/* SQL drawer */}
        {message.generatedSql && <SqlDrawer sql={message.generatedSql} />}

        {/* Feedback */}
        {message.traceId && (
          <FeedbackBar traceId={message.traceId} userId={userId} />
        )}
      </div>
    </div>
  );
}
