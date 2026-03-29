"""
Messages that close a thread — fan isn't expecting another bot text (lol, thanks, ok).

Two-step logic:
1) Must look like a possible ack (structure or strict phrase).
2) Must pass gates: no continuation intent, length/word caps.

Used to skip generation and outbound send while still logging the inbound user message.
"""
import re
import unicodedata

_MAX_ENDER_CHARS = 52
_MAX_ENDER_WORDS = 6

# If any of these appear, they're asking for more conversation — not a pure closer.
_CONTINUATION_RE = re.compile(
    r"[?]|"
    r"\b("
    r"but|however|although|though|except|actually|"
    r"tell|saying|explain|spill|more|another|again|keep|continu|"
    r"what|how|why|when|where|who|which|"
    r"please|can you|could you|would you|let me|"
    r"wait|hold on|one more|go on|and then"
    r")\b",
    re.IGNORECASE,
)

# Single-letter / minimal ack (whole message)
_MINIMAL_RE = re.compile(r"^[\s.!?,]*[kK][\s.!?,]*$")

# Whole message = allowed closer only (no nice/cool/yep — too often means "go on").
_STRICT_ACK_RE = re.compile(
    r"""^[\s.!?,;'"“”]*(?: 
        lol{1,3}z? | lmao | lmfao | rofl | rotfl |
        ha+ha* | bahaha+ | hehe+ | heh |
        ty | thx | tnx |
        kk | ok{1,3} | okay |
        thanks? | thank\s+you(\s+so\s+much|\s+a\s+lot)? |
        np | no\s+problem |
        got\s*it | gotcha
    )[\s.!?,;'"“”]*$""",
    re.IGNORECASE | re.VERBOSE,
)


def _letters_or_digits(s: str) -> bool:
    for ch in s:
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("N"):
            return True
    return False


def _is_emoji_only_ack(s: str) -> bool:
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


def _structural_ack(normalized: str) -> bool:
    """Non-text or minimal patterns that are clearly reactions."""
    return bool(
        _MINIMAL_RE.match(normalized)
        or _is_emoji_only_ack(normalized)
        or _laugh_plus_trailing_reaction(normalized)
    )


def _passes_gates(normalized: str) -> bool:
    if _CONTINUATION_RE.search(normalized):
        return False
    if len(normalized) > _MAX_ENDER_CHARS:
        return False
    if len(normalized.split()) > _MAX_ENDER_WORDS:
        return False
    return True


def is_conversation_ender(text: str) -> bool:
    """
    True when the fan message is only a conversational closer / reaction,
    not a hook for a new reply.
    """
    if not text or not text.strip():
        return False
    normalized = " ".join(text.strip().split())

    structural = _structural_ack(normalized)
    strict_phrase = bool(_STRICT_ACK_RE.match(normalized))

    if not structural and not strict_phrase:
        return False

    if not _passes_gates(normalized):
        return False

    return True
