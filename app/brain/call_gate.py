"""
CALL gate — deterministic reply when a fan asks about calling.

Creators with the phone-voice feature enabled (creator_config "voice" block)
answer real inbound phone calls on the same number fans text. Fans regularly
text the bare word "CALL" or ask "can I call you?", and the LLM — which knows
nothing about the voice feature — used to deny it exists. This gate
short-circuits those messages with a fixed, channel-aware reply:

  * SMS fans:      confirm they can call this exact number right now.
  * WhatsApp fans: texting works, but WhatsApp calling isn't supported yet.

The matcher is deliberately narrow — the bare keyword plus unambiguous
"can I call you?"-style asks. Naming questions ("what should I call you?",
"can I call you aunty?") are explicitly excluded. Anything ambiguous falls
through to the LLM, which is the safe failure mode.

Only fires when the creator's voice feature is enabled, so pure-SMS creators
keep their behaviour unchanged. Never raises to callers.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

_logger = logging.getLogger(__name__)

# Fixed replies — verbatim, no LLM.
SMS_CALL_REPLY = (
    "Yes — you can call me! Dial this exact number and I'll pick up. "
    "It's still AI me, just out loud instead of texting. Go ahead, try it."
)
WHATSAPP_CALL_REPLY = (
    "Texting me here works great — but WhatsApp calling doesn't work yet, "
    "unfortunately. For now you get me in messages only, so keep them coming!"
)

# Bare-keyword forms after normalization (lowercased, punctuation stripped).
_EXACT_FORMS = frozenset({
    "call",
    "call me",
    "call now",
    "call you",
    "call u",
    "phone call",
    "can i call",
    "can i call you",
    "can i call u",
})

# Call-asks that are unambiguous even with trailing words ("wanna call you so
# bad"). Checked BEFORE the naming veto — none of these can be naming questions.
_STRONG_ASK_PATTERNS = re.compile(
    r"("
    r"\bhow\s+(do|can|would)\s+(i|we|u)\s+call\b"
    r"|\b(want|wanna|would\s+love)\s+(to\s+)?call\b"
    r"|\bgive\s+(you|u)\s+a\s+call\b"
    r"|\btalk\s+(to\s+you\s+)?on\s+the\s+phone\b"
    r"|\b(is|does)\s+(the\s+)?call(ing)?\s*(feature|option|thing)?\s*"
    r"(work|working|available|real)\b"
    r"|\bcall(ing)?\s+(feature|option)\b"
    r")",
    re.IGNORECASE,
)

# "can I call ..." — a real ask, but also the shape of "can I call you aunty?",
# so it's only checked after the naming veto.
_CAN_I_CALL_PATTERN = re.compile(
    r"\b(can|could|may)\s+(i|we|u|you)\s+(please\s+)?(call|phone)\b",
    re.IGNORECASE,
)

# Naming questions — "what should I call you?", "can I call you aunty?".
# These are about names, not phone calls, and must fall through to the LLM.
_NAMING_PATTERNS = re.compile(
    r"("
    r"\bwhat\s+(should|do|can|shall|may)\s+(i|we)\s+call\b"
    r"|\bcall\s+(you|u|me|him|her|it|them)\s+[a-z]"
    r")",
    re.IGNORECASE,
)

_NORMALIZE_RE = re.compile(r"[^a-z0-9\s]+")


def is_call_request(message: str) -> bool:
    """True if the message is clearly asking about calling. Never raises."""
    try:
        text = (message or "").strip()
        if not text:
            return False
        if _STRONG_ASK_PATTERNS.search(text):
            return True
        if _NAMING_PATTERNS.search(text):
            return False
        normalized = _NORMALIZE_RE.sub(" ", text.lower())
        normalized = " ".join(normalized.split())
        if normalized in _EXACT_FORMS:
            return True
        return bool(_CAN_I_CALL_PATTERN.search(text))
    except Exception:
        _logger.exception("[ZARNA] call gate matcher failed — treating as no match")
        return False


def try_call_gate(
    phone_number: str, message_text: str, creator_config
) -> Optional[str]:
    """Return the fixed call reply if the gate should fire, else None.

    Fires only when the creator's phone-voice feature is enabled and the
    message is unambiguously about calling. WhatsApp senders (phone numbers
    carry the "whatsapp:" prefix end-to-end) get the not-supported-yet reply.
    """
    voice = getattr(creator_config, "voice", None)
    if not bool(getattr(voice, "enabled", False)):
        return None
    if not is_call_request(message_text):
        return None
    if str(phone_number or "").lower().startswith("whatsapp:"):
        return WHATSAPP_CALL_REPLY
    return SMS_CALL_REPLY
