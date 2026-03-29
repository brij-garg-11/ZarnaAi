"""
Messages that close a thread — fan isn't expecting another bot text (lol, thanks, ok).

Used to skip generation and outbound send while still logging the inbound user message.
"""
import re
import unicodedata

# Whole-message patterns (after normalize): reaction / ack only, no real content
_ENDER_RE = re.compile(
    r"""^[\s.!?,;'"“”]*(?:
        lol{1,3}z?|lmao|lmfao|rofl|rotfl|
        ha+ha*|bahaha+|hehe+|heh|
        ty|thx|tnx|
        np|no\s+problem|
        ok{1,3}|okay|kk|
        thanks?|thank\s*you|thanku|
        got\s*it|gotcha|
        cool|nice|sweet|perfect|awesome|love\s*it|love\s*this|
        yep|yup|sure|will\s*do|sounds\s+good|makes\s+sense
    )[\s.!?,;'"“”]*$""",
    re.IGNORECASE | re.VERBOSE,
)

# Single-letter / minimal ack (whole message)
_MINIMAL_RE = re.compile(r"^[\s.!?,]*[kK][\s.!?,]*$")


def _letters_or_digits(s: str) -> bool:
    for ch in s:
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("N"):
            return True
    return False


def _is_emoji_only_ack(s: str) -> bool:
    """True if message has no letters/digits — emoji or punctuation only (reaction)."""
    s = s.strip()
    if not s:
        return False
    return not _letters_or_digits(s)


def _laugh_plus_trailing_reaction(s: str) -> bool:
    """e.g. 'lol 😂', 'haha!!' — laugh token then only emoji/punct/space."""
    m = re.match(
        r"^(lol{1,3}z?|lmao|lmfao|ha+h+|hehe+|heh)\b(.*)$",
        s,
        re.I,
    )
    if not m:
        return False
    tail = m.group(2).strip()
    if not tail:
        return True
    return not _letters_or_digits(tail)


def is_conversation_ender(text: str) -> bool:
    """
    True when the fan message is only a conversational closer / reaction,
    not a hook for a new reply.
    """
    if not text or not text.strip():
        return False
    normalized = " ".join(text.strip().split())
    if _MINIMAL_RE.match(normalized):
        return True
    if _ENDER_RE.match(normalized):
        return True
    if _laugh_plus_trailing_reaction(normalized):
        return True
    if _is_emoji_only_ack(normalized):
        return True
    return False
