"""
Lightweight tone-mode classification for conversational steering.

This is intentionally heuristic and fast: it avoids an extra model call while
giving generation a clear primary mode for the current message.
"""

from __future__ import annotations

import re
from typing import List, Literal

from app.brain.intent import Intent

ToneMode = Literal[
    "roast_playful",
    "warm_supportive",
    "direct_answer",
    "celebratory",
    "sensitive_care",
]

_VULNERABLE_RE = re.compile(
    r"\b(sad|anxious|anxiety|depress|grief|grieving|panic|hurt|heartbroken|"
    r"loss|cancer|illness|scared|not okay|overwhelmed|lonely)\b",
    re.IGNORECASE,
)
_CELEBRATORY_RE = re.compile(
    r"\b(congrats|congratulations|amazing|awesome|love(d)?|proud|great show|"
    r"fantastic|brilliant|so good|killed it)\b",
    re.IGNORECASE,
)
_ROAST_FAMILY_RE = re.compile(
    r"\b(shalabh|husband|mother[- ]in[- ]law|mil|baba\s*ramdev)\b",
    re.IGNORECASE,
)
_DIRECT_Q_RE = re.compile(
    r"(^\s*(what|why|how|where|when|who|do|does|did|is|are|can|could|should)\b)|\?",
    re.IGNORECASE,
)
_THANKS_RE = re.compile(r"\b(thanks|thank you|appreciate)\b", re.IGNORECASE)


def classify_tone_mode(
    user_message: str,
    intent: Intent,
    history: List[dict] | None = None,
) -> ToneMode:
    text = (user_message or "").strip()
    if not text:
        return "direct_answer"

    if _VULNERABLE_RE.search(text):
        return "sensitive_care"
    if intent == Intent.FEEDBACK or _CELEBRATORY_RE.search(text):
        return "celebratory"
    if intent == Intent.JOKE or _ROAST_FAMILY_RE.search(text):
        return "roast_playful"
    if intent == Intent.QUESTION or _DIRECT_Q_RE.search(text):
        return "direct_answer"
    if intent == Intent.GREETING:
        return "roast_playful"
    if _THANKS_RE.search(text):
        return "warm_supportive"

    # If recent user turn was vulnerable and this is a short follow-up, stay warm.
    if history:
        for m in reversed(history[-4:]):
            if m.get("role") == "user" and _VULNERABLE_RE.search(m.get("text", "")):
                if len(text.split()) <= 8:
                    return "warm_supportive"
                break

    return "roast_playful"
