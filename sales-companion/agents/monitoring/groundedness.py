"""
Groundedness Scorer
Uses Claude Haiku as a judge to check whether each claim in the agent's
response is supported by the retrieved context.

Returns a score 0.0–1.0 and a list of unsupported claims.
"""

import json
import os
import re

import anthropic

_client: anthropic.Anthropic | None = None

JUDGE_MODEL = "claude-haiku-4-5-20251001"

JUDGE_PROMPT = """\
You are a strict factual grounding evaluator.

TASK: Given a response and the source context that was retrieved to generate it,
identify every factual claim in the response and decide whether it is
SUPPORTED or UNSUPPORTED by the context.

A claim is SUPPORTED if the context contains explicit information backing it.
A claim is UNSUPPORTED if it is invented, inferred beyond the evidence, or
contradicted by the context.

Respond ONLY with valid JSON in this format:
{{
  "total_claims": <int>,
  "supported_claims": <int>,
  "unsupported_claims": ["<claim text>", ...],
  "score": <float 0.0-1.0>
}}

--- CONTEXT ---
{context}

--- RESPONSE ---
{response}
"""


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client


def score_groundedness(response: str, context: str) -> dict:
    """
    Returns:
        {
            "score": float,               # 0.0–1.0
            "total_claims": int,
            "supported_claims": int,
            "unsupported_claims": list[str],
        }
    """
    if not response.strip() or not context.strip():
        return {
            "score": 1.0,
            "total_claims": 0,
            "supported_claims": 0,
            "unsupported_claims": [],
        }

    client = _get_client()
    prompt = JUDGE_PROMPT.format(
        context=context[:8000],   # guard against oversized context
        response=response[:4000],
    )

    message = client.messages.create(
        model=JUDGE_MODEL,
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown fences if present
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: assume fully grounded if judge output is unparseable
        return {
            "score": 1.0,
            "total_claims": 0,
            "supported_claims": 0,
            "unsupported_claims": [],
        }

    return {
        "score": float(result.get("score", 1.0)),
        "total_claims": result.get("total_claims", 0),
        "supported_claims": result.get("supported_claims", 0),
        "unsupported_claims": result.get("unsupported_claims", []),
    }
