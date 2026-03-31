"""
Complexity routing for multi-model replies.

Uses Gemini Flash (ROUTER_MODEL) to classify low | medium | high, with
heuristic overrides for length and sensitive signals.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Literal

from google import genai

from app.config import (
    GEMINI_API_KEY,
    ROUTER_MODEL,
    ROUTER_SKIP_MAX_CHARS,
    ROUTER_SKIP_MAX_WORDS,
)

logger = logging.getLogger(__name__)

RoutingTier = Literal["low", "medium", "high"]

_client = genai.Client(api_key=GEMINI_API_KEY)

_SENSITIVE_HINTS = re.compile(
    r"\b(suicid|kill myself|end my life|self[- ]harm|hurt myself|"
    r"abuse|trauma|panic attack|depression|anxiety attack|"
    r"legal advice|lawyer|lawsuit|immigration status|visa)\b",
    re.IGNORECASE,
)


def _heuristic_floor(message: str) -> RoutingTier | None:
    t = message.strip()
    if not t:
        return None
    if len(t) > 900 or t.count("\n") > 8:
        return "high"
    if len(t) > 420 or t.count("?") >= 3 or t.count("\n") > 3:
        return "medium"
    if _SENSITIVE_HINTS.search(t):
        return "high"
    return None


def try_router_skip_safe(message: str) -> bool:
    """
    True when we can treat the message as routing tier *low* without calling
    the router model. Conservative: any heuristic medium/high floor, question
    mark, long text, or multi-line chat blocks the skip.
    """
    h = _heuristic_floor(message)
    if h is not None:
        return False
    t = message.strip()
    if not t:
        return True
    if "?" in t:
        return False
    if len(t) > ROUTER_SKIP_MAX_CHARS:
        return False
    if t.count("\n") > 1:
        return False
    words = t.split()
    if len(words) > ROUTER_SKIP_MAX_WORDS:
        return False
    return True


def _router_prompt(message: str, history: List[dict], fan_memory: str) -> str:
    hist_lines: list[str] = []
    for m in history[-4:]:
        role = m.get("role", "")
        text = (m.get("text") or "")[:400]
        hist_lines.append(f"{role}: {text}")
    hist_block = "\n".join(hist_lines) if hist_lines else "(none)"
    mem = (fan_memory or "").strip()[:500] or "(none)"

    return f"""Classify how hard this SMS fan message is to answer well.

Tiers:
- low: hi/bye/thanks, very short banter, tiny acknowledgment, simple one-liner joke ask.
- medium: normal chat, one clear question, moderate personalization, joke + context.
- high: long or multi-part message, nuanced advice, emotional weight, conflicting context, or sensitive topics.

Recent turns:
{hist_block}

Fan background (may be empty): {mem}

Latest message: {message}

Reply with ONLY a JSON object, no markdown:
{{"tier":"low"|"medium"|"high","confidence":0.0,"reason":"short"}}"""


def _parse_router_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    # Strip markdown code fence if present
    if text.startswith("```"):
        text = re.sub(r"^```\w*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def classify_routing_tier(
    message: str,
    history: List[dict],
    fan_memory: str = "",
) -> RoutingTier:
    """
    Return low | medium | high. Never raises — falls back to medium on errors.
    """
    h = _heuristic_floor(message)
    if h == "high":
        return "high"
    if try_router_skip_safe(message):
        return "low"

    prompt = _router_prompt(message, history, fan_memory)

    try:
        response = _client.models.generate_content(model=ROUTER_MODEL, contents=prompt)
        raw = (response.text or "").strip()
        data = _parse_router_json(raw)
        tier = str(data.get("tier", "medium")).lower().strip()
        conf = float(data.get("confidence", 0.7))
        if tier not in ("low", "medium", "high"):
            tier = "medium"
        # Low needs confidence; otherwise don’t under-shoot difficult asks
        if tier == "low" and conf < 0.72:
            tier = "medium"
        if tier == "high" and conf < 0.45 and h != "high":
            tier = "medium"
    except Exception as e:
        logger.warning("routing classifier fallback to medium: %s", e)
        tier = "medium"
        conf = 0.0

    # Raise floor from heuristics
    if h == "medium":
        if tier == "low":
            tier = "medium"
    return tier  # type: ignore[return-value]
