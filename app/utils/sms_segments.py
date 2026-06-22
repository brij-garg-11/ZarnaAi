"""
SMS segment counting for credit billing.

Credit rules (1 credit = 1 SMS segment):
  - Standard SMS  : ≤160 GSM-7 chars  → 1 credit
  - Long SMS      : 161–306 chars      → 2 credits  (153-char segments for multi-part)
  - Very long SMS : 307–459 chars      → 3 credits
  - Each 153 chars beyond that adds 1 more credit
  - MMS (image/media attachment)       → 3 credits regardless of text length

Unicode note: messages containing non-GSM characters (emojis, accented chars, etc.)
  reduce the segment size to 70 chars (single) / 67 chars (multi-part). We detect
  the common emoji/unicode case and apply the tighter limit.

These rules mirror Twilio's billing and SlickText's credit model.
"""

import math
import re

# GSM-7 basic character set (single byte per char in SMS encoding)
_GSM7_CHARS = set(
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./"
    "0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§"
    "¿abcdefghijklmnopqrstuvwxyzäöñüà"
)

# Extended GSM-7 chars (counted as 2 chars in SMS, but rare in practice)
_GSM7_EXTENDED = set("^{}\\[~]|€")

_HAS_UNICODE_RE = re.compile(r"[^\x00-\x7F]")


def _is_gsm7(text: str) -> bool:
    """Return True if the entire string fits in the GSM-7 charset."""
    for ch in text:
        if ch not in _GSM7_CHARS and ch not in _GSM7_EXTENDED:
            return False
    return True


def count_sms_segments(text: str, has_media: bool = False) -> int:
    """
    Count the number of SMS billing segments for a given message.

    Args:
        text:      The message body string.
        has_media: True if the message includes an image / MMS attachment.

    Returns:
        Number of credits (segments) to charge.
    """
    if has_media:
        return 3  # Twilio MMS = 3 credits

    if not text:
        return 1

    length = len(text)

    if _is_gsm7(text):
        # GSM-7: 160 chars single, 153 chars per part when multi-part
        if length <= 160:
            return 1
        return math.ceil(length / 153)
    else:
        # Unicode (emoji, etc.): 70 chars single, 67 chars per part
        if length <= 70:
            return 1
        return math.ceil(length / 67)


def segments_for_length(char_count: int, has_media: bool = False) -> int:
    """
    Estimate segments from a pre-computed character count (no text available).
    Uses GSM-7 limits — conservative but fast for billing queries.
    """
    if has_media:
        return 3
    if char_count <= 0:
        return 1
    if char_count <= 160:
        return 1
    return math.ceil(char_count / 153)


# Boundary where SlickText starts rejecting a single outbound body. Anything
# longer is delivered as multiple sequential texts (see split_for_sms) so we
# never cut a reply off mid-thought.
_SENTENCE_END_RE = re.compile(r"[.!?]\s")


def split_for_sms(text: str, limit: int = 400, max_parts: int = 4):
    """Split a long reply into <=``limit``-char parts on natural boundaries.

    Long-but-legitimate replies (e.g. a "give me 3 jokes" list) exceed what a
    single SMS can carry, so we deliver them as several sequential texts instead
    of truncating. Splits prefer line breaks, then sentence ends, then word
    boundaries. Only the final part is hard-trimmed, and only if the text is so
    long it would exceed ``max_parts`` (a guard against runaway output).

    Returns a list of non-empty parts. A short message returns ``[text]``;
    empty input returns ``[]``.
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    parts = []
    remaining = text
    while remaining:
        # Last part we're allowed to emit: take the rest, hard-trimming only if
        # it still overflows the single-message limit.
        if len(remaining) <= limit or len(parts) == max_parts - 1:
            if len(remaining) > limit:
                remaining = remaining[:limit].rsplit(" ", 1)[0].rstrip() + "…"
            parts.append(remaining.strip())
            break

        window = remaining[:limit]
        cut = window.rfind("\n")
        if cut < limit // 2:
            # No usable line break — fall back to the last sentence end.
            last = None
            for m in _SENTENCE_END_RE.finditer(window):
                last = m
            cut = (last.end() - 1) if last else -1
        if cut < limit // 2:
            cut = window.rfind(" ")
        if cut <= 0:
            cut = limit  # no boundary at all — hard cut to keep making progress
        parts.append(remaining[:cut].strip())
        remaining = remaining[cut:].strip()

    return [p for p in parts if p]
