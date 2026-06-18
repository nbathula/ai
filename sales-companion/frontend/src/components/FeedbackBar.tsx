import React, { useState } from "react";
import { sendFeedback } from "../api/client";

interface Props {
  traceId: string;
  userId: string;
  onFeedback?: (rating: 1 | 2) => void;
}

export function FeedbackBar({ traceId, userId, onFeedback }: Props) {
  const [submitted, setSubmitted] = useState<1 | 2 | null>(null);

  async function handle(rating: 1 | 2) {
    if (submitted) return;
    setSubmitted(rating);
    onFeedback?.(rating);
    try {
      await sendFeedback(traceId, userId, rating);
    } catch {
      // feedback is best-effort — don't surface errors to user
    }
  }

  const btnStyle = (active: boolean, positive: boolean): React.CSSProperties => ({
    background: active ? (positive ? "#16a34a" : "#dc2626") : "none",
    color: active ? "#fff" : "#94a3b8",
    border: `1px solid ${active ? "transparent" : "#e2e8f0"}`,
    borderRadius: 4,
    cursor: submitted ? "default" : "pointer",
    fontSize: 14,
    padding: "2px 8px",
    transition: "all 0.15s",
  });

  return (
    <div style={{ display: "flex", gap: 6, marginTop: 10, alignItems: "center" }}>
      <span style={{ fontSize: 11, color: "#94a3b8" }}>Helpful?</span>
      <button style={btnStyle(submitted === 2, true)} onClick={() => handle(2)} disabled={!!submitted}>
        👍
      </button>
      <button style={btnStyle(submitted === 1, false)} onClick={() => handle(1)} disabled={!!submitted}>
        👎
      </button>
      {submitted && (
        <span style={{ fontSize: 11, color: "#64748b" }}>Thanks for the feedback</span>
      )}
    </div>
  );
}
