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


def body_matches_keyword(body: str, keyword: str) -> bool:
    body = (body or "").strip().lower()
    kw = (keyword or "").strip().lower()
    if not kw:
        return True
    if body == kw:
        return True
    parts = body.split()
    if not parts:
        return False
    first = parts[0]
    # allow trailing punctuation on first token (e.g. "blue!")
    first_core = re.sub(r"^[^\w]+|[^\w]+$", "", first, flags=re.UNICODE).lower()
    if not first_core:
        return False
    if first_core == kw:
        return True
    return fuzzy_match_tokens(first_core, kw)


def is_keyword_only_join(body: str, show_kw: str) -> bool:
    """Single-token join message (for silencing the AI reply)."""
    raw = (body or "").strip()
    if not raw:
        return False
    parts = raw.split()
    if len(parts) != 1:
        return False
    token = re.sub(r"^[^\w]+|[^\w]+$", "", parts[0], flags=re.UNICODE)
    token_l = token.lower()
    kw = (show_kw or "").strip().lower()
    if not token_l or not kw:
        return False
    if token_l == kw:
        return True
    return fuzzy_match_tokens(token_l, kw)
