import React, { KeyboardEvent, useRef } from "react";

interface Props {
  onSubmit: (query: string) => void;
  disabled?: boolean;
}

const SUGGESTIONS = [
  "What is our total pipeline for Q2?",
  "Which accounts are at risk of churning?",
  "Show deals stuck in Negotiation for 30+ days.",
  "What is our current NRR?",
  "Which contracts expire in the next 90 days?",
];

export function QueryInput({ onSubmit, disabled }: Props) {
  const ref = useRef<HTMLTextAreaElement>(null);

  function submit() {
    const val = ref.current?.value.trim();
    if (!val || disabled) return;
    onSubmit(val);
    if (ref.current) ref.current.value = "";
  }

  function onKey(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      submit();
    }
  }

  return (
    <div>
      {/* Suggestion chips */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 10 }}>
        {SUGGESTIONS.map((s) => (
          <button
            key={s}
            onClick={() => { if (ref.current) { ref.current.value = s; ref.current.focus(); } }}
            style={{
              background: "#f1f5f9",
              border: "1px solid #e2e8f0",
              borderRadius: 14,
              padding: "3px 10px",
              fontSize: 12,
              color: "#475569",
              cursor: "pointer",
              whiteSpace: "nowrap",
            }}
          >
            {s}
          </button>
        ))}
      </div>

      {/* Input row */}
      <div
        style={{
          display: "flex",
          gap: 8,
          alignItems: "flex-end",
          background: "#fff",
          border: "1px solid #e2e8f0",
          borderRadius: 12,
          padding: "8px 8px 8px 14px",
          boxShadow: "0 1px 3px rgba(0,0,0,0.06)",
        }}
      >
        <textarea
          ref={ref}
          onKeyDown={onKey}
          placeholder="Ask about pipeline, forecast, customer health, contracts…"
          rows={2}
          disabled={disabled}
          style={{
            flex: 1,
            border: "none",
            outline: "none",
            resize: "none",
            fontSize: 14,
            lineHeight: 1.5,
            color: "#1e293b",
            background: "transparent",
            fontFamily: "inherit",
          }}
        />
        <button
          onClick={submit}
          disabled={disabled}
          style={{
            background: disabled ? "#e2e8f0" : "#0f172a",
            color: disabled ? "#94a3b8" : "#fff",
            border: "none",
            borderRadius: 8,
            padding: "8px 16px",
            fontSize: 13,
            fontWeight: 600,
            cursor: disabled ? "default" : "pointer",
            transition: "background 0.15s",
            flexShrink: 0,
          }}
        >
          {disabled ? "…" : "Ask"}
        </button>
      </div>
      <div style={{ marginTop: 4, fontSize: 11, color: "#94a3b8", textAlign: "right" }}>
        Enter to send · Shift+Enter for new line
      </div>
    </div>
  );
}
