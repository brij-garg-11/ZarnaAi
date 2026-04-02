"""Keyword matching for live show signups (no DB imports)."""

from __future__ import annotations

import difflib
import os
import re

_FUZZY_RATIO = float(os.getenv("LIVE_SHOW_KEYWORD_FUZZY_RATIO", "0.74"))
_MAX_LEN_DELTA = int(os.getenv("LIVE_SHOW_KEYWORD_FUZZY_MAX_LEN_DELTA", "2"))
_MIN_LEN_FUZZY = 3


def _levenshtein(a: str, b: str) -> int:
    if len(a) < len(b):
        a, b = b, a
    prev = range(len(b) + 1)
    for i, ca in enumerate(a):
        cur = [i + 1]
        for j, cb in enumerate(b):
            cur.append(min(prev[j + 1] + 1, cur[j] + 1, prev[j] + (ca != cb)))
        prev = cur
    return prev[-1]


def fuzzy_match_tokens(candidate: str, kw: str) -> bool:
    """Typo-tolerant match (both lowercased). Short keywords are exact-only."""
    if not candidate or not kw:
        return False
    if candidate == kw:
        return True
    if len(kw) < _MIN_LEN_FUZZY:
        return False
    if abs(len(candidate) - len(kw)) > _MAX_LEN_DELTA:
        return False
    # Same first letter avoids false positives (e.g. glue ~ blue via ratio alone).
    if candidate[0] != kw[0]:
        return False
    # Avoid matching truncated words (e.g. "bl" for "blue").
    if len(candidate) < len(kw) - 1:
        return False
    ratio = difflib.SequenceMatcher(None, candidate, kw).ratio()
    dist = _levenshtein(candidate, kw)
    return ratio >= _FUZZY_RATIO or dist <= 2


def _normalize(s: str) -> str:
    """Lowercase, strip punctuation edges, collapse all whitespace."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s]", "", s)   # drop punctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _compact(s: str) -> str:
    """Like _normalize but also removes all spaces — for spacing-tolerant compare."""
    return _normalize(s).replace(" ", "")


def body_matches_keyword(body: str, keyword: str) -> bool:
    """
    Return True if `body` is a signup attempt matching `keyword`.

    Handles:
    - Single or multi-word keywords ("FLORIDA", "ZARNA TEST", "COMEDY SHOW NYC")
    - Case-insensitive exact match
    - Fan texts the phrase with different spacing ("zarnatest" vs "zarna test")
    - Fan texts just the start of a multi-word phrase ("ZARNA" for "ZARNA TEST")
    - Typos / minor misspellings via fuzzy matching
    - Extra text after the keyword ("FLORIDA yes sign me up")
    """
    body_n = _normalize(body)
    kw_n   = _normalize(keyword)

    if not kw_n:
        return True

    # 1. Exact match (handles "zarna test" == "zarna test")
    if body_n == kw_n:
        return True

    # 2. Body starts with the keyword (fan typed extra words after)
    if body_n.startswith(kw_n + " ") or body_n.startswith(kw_n):
        return True

    # 3. Space-collapsed comparison — "zarnatest" matches "zarna test"
    body_c = _compact(body)
    kw_c   = _compact(keyword)
    if body_c == kw_c:
        return True
    if body_c.startswith(kw_c):
        return True

    # 4. Fuzzy match on the space-collapsed forms (catches typos in multi-word phrases)
    if fuzzy_match_tokens(body_c, kw_c):
        return True

    # 5. Legacy single-token path — first word fuzzy-matched against keyword
    #    (keeps backward compat for single-word keywords like "FLORIDA")
    parts = body_n.split()
    if parts:
        first_core = re.sub(r"^[^\w]+|[^\w]+$", "", parts[0], flags=re.UNICODE)
        if first_core:
            if first_core == kw_n:
                return True
            if fuzzy_match_tokens(first_core, kw_n):
                return True

    return False


def is_keyword_only_join(body: str, show_kw: str) -> bool:
    """
    True if the message is clearly just the signup keyword (no extra content).
    Used to silence the AI reply for pure keyword joins.
    Handles single and multi-word keywords, spacing variants.
    """
    raw  = (body or "").strip()
    if not raw:
        return False

    body_n = _normalize(raw)
    kw_n   = _normalize(show_kw)
    if not kw_n:
        return False

    # Exact phrase match
    if body_n == kw_n:
        return True

    # Space-collapsed match ("zarnatest" for "zarna test")
    if _compact(raw) == _compact(show_kw):
        return True

    # Fuzzy on collapsed forms
    if fuzzy_match_tokens(_compact(raw), _compact(show_kw)):
        return True

    # Single-token legacy path
    parts = raw.split()
    if len(parts) == 1:
        token = re.sub(r"^[^\w]+|[^\w]+$", "", parts[0], flags=re.UNICODE).lower()
        kw_c  = _compact(show_kw)
        if token and (token == kw_c or fuzzy_match_tokens(token, kw_c)):
            return True

    return False
