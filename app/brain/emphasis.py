"""
Emphasis (*italics*) policy for SMS replies.

- Distress in the fan message → strip all asterisk emphasis (any intent).
- Conversation throttle (non-JOKE) → if any of the last K assistant
  messages used *emphasis*, strip all on this reply so italics stay rare.
- JOKE intent bypasses the throttle only (not distress).
"""
import re
from typing import List

from app.brain.intent import Intent

_EMPHASIS_SPAN = re.compile(r"\*[^\*\n]+\*")

# Multi-word phrases (substring match is OK)
_DISTRESS_PHRASES = (
    "feeling sad",
    "feel sad",
    "very sad",
    "so sad",
    "i'm sad",
    "im sad",
    "i am sad",
    "depressed",
    "depression",
    "anxious",
    "anxiety",
    "heartbroken",
    "overwhelmed",
    "not okay",
    "not ok",
    "lonely",
    "hopeless",
    "grief",
    "grieving",
    "crying",
    "self-harm",
    "suicidal",
    "want to die",
    "kill myself",
    "end it all",
    "mental health crisis",
    "panic attack",
    "can't cope",
    "cant cope",
    "going bad",
    "day is bad",
    "bad day",
    "rough day",
    "terrible day",
    "awful day",
    "horrible day",
    "feeling low",
    "feeling down",
    "i'm down",
    "im down",
    "feel down",
    "in a funk",
    "really down",
)


def user_signals_distress(message: str) -> bool:
    """True when the fan message suggests sadness, anxiety, or crisis (heuristic)."""
    if not message or not message.strip():
        return False
    lower = message.lower()
    if any(p in lower for p in _DISTRESS_PHRASES):
        return True
    if re.search(r"\bsad\b", lower) and not re.search(r"\bnot\s+sad\b", lower):
        return True
    if re.search(r"\bbad\b", lower) and any(
        p in lower for p in ("day", "week", "mood", "time", "going")
    ):
        return True
    if re.search(r"\bblue\b", lower) and any(
        w in lower for w in ("feeling", "feel", "so ", "very ", "i'm ", "im ")
    ):
        return True
    return False


def recent_assistant_replies_used_emphasis(assistant_texts: List[str], k: int = 3) -> bool:
    """True if any of the last k assistant message bodies contain *emphasis*."""
    if k <= 0 or not assistant_texts:
        return False
    tail = assistant_texts[-k:]
    return any(_EMPHASIS_SPAN.search(t or "") for t in tail)


def strip_all_emphasis(text: str) -> str:
    """Remove all **bold** and *single-asterisk* spans; leave plain text."""
    if not text:
        return text
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*([^\*\n]+?)\*", r"\1", text)
    return text


def should_suppress_all_emphasis(
    user_message: str,
    intent: Intent,
    assistant_texts: List[str],
    *,
    throttle_window: int = 3,
) -> bool:
    """Combine distress override + per-thread throttle (JOKE bypasses throttle only)."""
    if user_signals_distress(user_message):
        return True
    if intent == Intent.JOKE:
        return False
    return recent_assistant_replies_used_emphasis(assistant_texts, k=throttle_window)
