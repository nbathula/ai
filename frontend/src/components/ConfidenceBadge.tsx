import React from "react";

interface Props {
  score: number;
  label?: string;
}

function color(score: number): string {
  if (score >= 0.85) return "#16a34a";
  if (score >= 0.6)  return "#ca8a04";
  return "#dc2626";
}

export function ConfidenceBadge({ score, label = "Confidence" }: Props) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        fontSize: 11,
        fontWeight: 600,
        color: color(score),
        background: `${color(score)}18`,
        borderRadius: 4,
        padding: "2px 7px",
      }}
    >
      {label}: {Math.round(score * 100)}%
    </span>
  );
}
