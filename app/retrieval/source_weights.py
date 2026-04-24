"""
Source-specific weight functions for retrieval re-ranking.

Why this exists:
    EmbeddingRetriever (Zarna's legacy, file-backed path) multiplies each
    candidate chunk's cosine similarity by a per-source weight before
    picking the top-K. Facts get a 1.35× boost, the book gets 1.18×,
    multi-speaker podcast transcripts get 0.74×, etc. Those weights were
    hand-tuned against real fan replies and are load-bearing for reply
    quality.

    When we swap Zarna over to PgRetriever, raw pgvector cosine distance
    alone would lose this ranking signal. So we re-apply the same weights
    inside PgRetriever — just with a pluggable callable so new creators
    (who don't have hand-tuned weights yet) still get uniform weighting.

Contract:
    A "weight function" is ``Callable[[str], float]`` that receives a
    chunk's source label and returns a multiplier. Return 1.0 for
    "no boost". Return 0.0 to *exclude* a source from results entirely.

Usage:
    >>> from app.retrieval.source_weights import zarna_weight_fn
    >>> weight = zarna_weight_fn(podcast_transcript_ids={...})
    >>> weight("zarna_facts")
    1.35
    >>> weight("monday_motivations.json")
    0.82

Keep this module dependency-free so both EmbeddingRetriever and PgRetriever
can use it without pulling each other into their import graphs.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Callable, Iterable, Optional

_log = logging.getLogger(__name__)


# Viral skits — confirmed high-engagement shorts that should surface readily.
# Kept in sync with EmbeddingRetriever._VIRAL_SKIT_WEIGHTS.
_VIRAL_SKIT_WEIGHTS: dict[str, float] = {
    # HUGE
    "mxaocinn3hc_transcript.json": 1.18,   # Texting My Son's Girlfriend
    "d_zg2sb7rhg_transcript.json": 1.18,   # Green juice / margarita (mega viral)
    # Strong viral
    "jg7b9xctyhs_transcript.json": 1.13,   # Dating Advice
    "rdkkeekl9icq_transcript.json": 1.13,   # Every New Year's Message (Dadhi)
    "emfhjv6qw9y_transcript.json": 1.13,   # Relaxing Bedtime Story
    "_7j8jfshce8_transcript.json": 1.13,   # The Perfect Date
    # Notable viral
    "fmtzk34eswy_transcript.json": 1.08,   # Hair Oil Vs Body Oil
    "tnfzdxwhfam_transcript.json": 1.08,   # How to Leave the House
    "kwbmgntdirm_transcript.json": 1.13,   # My Son's Girlfriend
}

_TRANSCRIPT_RE = re.compile(r"^[a-z0-9_-]{8,}_transcript\.json$")


def load_podcast_transcript_ids(base_dir: Optional[str] = None) -> set[str]:
    """
    Read Processed/youtube/video_metadata.json and return the set of
    transcript source filenames that belong to "Zarna Garg Family Podcast"
    episodes — we down-weight those because multi-speaker podcast
    transcripts bleed non-Zarna facts into replies.

    Silent on failure (returns empty set) so retrieval still works without
    the metadata file.
    """
    if base_dir is None:
        # app/retrieval/source_weights.py → app/retrieval → app → repo root
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
    metadata_path = os.path.join(base_dir, "Processed", "youtube", "video_metadata.json")
    if not os.path.exists(metadata_path):
        return set()
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            videos = json.load(f)
        ids: set[str] = set()
        for v in videos:
            title = str(v.get("title", "")).lower()
            vid = str(v.get("video_id", "")).strip()
            if not vid:
                continue
            if "zarna garg family podcast" in title:
                ids.add(f"{vid}_transcript.json".lower())
        return ids
    except Exception as exc:
        _log.warning("load_podcast_transcript_ids: failed — %s", exc)
        return set()


def zarna_weight_fn(
    podcast_transcript_ids: Iterable[str] = (),
    podcast_mode: str = "exclude",
    monday_mode: str = "include",
) -> Callable[[str], float]:
    """
    Build the Zarna-specific source-weight function.

    Mirrors EmbeddingRetriever._source_weight + the exclusion/filter logic
    in its _load() method — so PgRetriever('zarna') with this weight_fn
    behaves exactly like EmbeddingRetriever at the candidate-ranking level.

    Args:
      podcast_transcript_ids: transcript filenames known to be podcast
        episodes (pass load_podcast_transcript_ids() for production).
      podcast_mode: 'exclude' (default, returns 0.0 for podcast transcripts
        so they're dropped entirely) or 'include' (returns 1.0).
      monday_mode: 'include' (default, returns 0.82 per legacy weight)
        or 'exclude' (returns 0.0 so those chunks never surface).
    """
    podcast_ids = frozenset(s.strip().lower() for s in podcast_transcript_ids)
    podcast_mode = (podcast_mode or "exclude").strip().lower()
    monday_mode = (monday_mode or "include").strip().lower()

    def _weight(source: str) -> float:
        src = (source or "").strip().lower()
        if not src:
            return 1.0

        # ── Hard filters first: if a mode excludes a source type, zero it. ──
        if monday_mode == "exclude" and src == "monday_motivations.json":
            return 0.0
        if src in podcast_ids:
            if podcast_mode == "exclude":
                return 0.0
            return 1.0

        # ── Per-source multipliers (mirrors EmbeddingRetriever._source_weight) ──
        if src.startswith("podcast_zarna_"):
            return 1.24
        if src == "zarna_facts":
            return 1.35
        if src.endswith(".pdf"):
            return 1.18  # book / memoir chunks
        if src in ("one_in_a_billion.json", "practical_people_win.json", "nervous_in_new_york.json"):
            return 1.22
        if src == "monday_motivations.json":
            return 0.82
        if src == "podcast_episodes":
            return 0.90
        if src in _VIRAL_SKIT_WEIGHTS:
            return _VIRAL_SKIT_WEIGHTS[src]
        if _TRANSCRIPT_RE.match(src):
            return 0.92  # regular skit / youtube transcript
        return 1.0

    return _weight


def uniform_weight_fn() -> Callable[[str], float]:
    """No-op weight function for new creators without hand-tuned ranking."""
    return lambda source: 1.0
