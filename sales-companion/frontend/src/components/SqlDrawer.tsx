import React, { useState } from "react";

interface Props {
  sql: string;
}

export function SqlDrawer({ sql }: Props) {
  const [open, setOpen] = useState(false);

  return (
    <div style={{ marginTop: 8 }}>
      <button
        onClick={() => setOpen((v) => !v)}
        style={{
          background: "none",
          border: "1px solid #e2e8f0",
          borderRadius: 4,
          cursor: "pointer",
          fontSize: 11,
          color: "#64748b",
          padding: "2px 8px",
        }}
      >
        {open ? "Hide SQL" : "View SQL"}
      </button>
      {open && (
        <pre
          style={{
            marginTop: 6,
            padding: "10px 12px",
            background: "#1e1e2e",
            color: "#cdd6f4",
            borderRadius: 6,
            fontSize: 12,
            overflowX: "auto",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {sql}
        </pre>
      )}
    </div>
  );
}
