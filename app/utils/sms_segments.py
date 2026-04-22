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
