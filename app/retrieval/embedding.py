import gzip
import json
import logging
import math
import os
import re
from functools import lru_cache
from typing import List

from google import genai

from app.config import (
    GEMINI_API_KEY,
    EMBEDDING_MODEL,
    EMBEDDINGS_PATH,
    MONDAY_MOTIVATION_MODE,
    PODCAST_TRANSCRIPTS_MODE,
    TOP_K_CHUNKS,
)
from app.retrieval.base import BaseRetriever

logger = logging.getLogger(__name__)


class EmbeddingRetriever(BaseRetriever):
    """
    Retrieves chunks using cosine similarity over pre-built Gemini embeddings.

    Optimisations:
    - Eager-loads the embeddings file at construction time (no first-request
      penalty).
    - LRU-caches the top-k results for repeated queries (common during shows).
    """

    def __init__(self, embeddings_path: str = EMBEDDINGS_PATH):
        self._path = embeddings_path
        self._chunks: list = []
        self._client = genai.Client(api_key=GEMINI_API_KEY)
        self._podcast_transcript_sources: set[str] = set()
        self._podcast_transcripts_mode = PODCAST_TRANSCRIPTS_MODE
        self._monday_motivation_mode = MONDAY_MOTIVATION_MODE
        self._load_podcast_transcript_sources()
        self._load()  # eager load at startup

    def _load(self):
        logger.info("Loading embeddings from %s …", self._path)
        open_fn = gzip.open if self._path.endswith(".gz") else open
        with open_fn(self._path, "rt", encoding="utf-8") as f:
            loaded = json.load(f)
        filtered = loaded
        if self._podcast_transcripts_mode == "exclude":
            filtered = [
                c for c in filtered if not self._is_podcast_transcript_source(str(c.get("source", "")))
            ]
            logger.info(
                "Filtered out %d podcast transcript chunks (mode=exclude)",
                len(loaded) - len(filtered),
            )
        if self._monday_motivation_mode == "exclude":
            before = len(filtered)
            filtered = [
                c for c in filtered if str(c.get("source", "")).lower() != "monday_motivations.json"
            ]
            logger.info(
                "Filtered out %d monday motivation chunks (mode=exclude)",
                before - len(filtered),
            )
        self._chunks = filtered
        logger.info("Embeddings loaded: %d chunks", len(self._chunks))

    def _load_podcast_transcript_sources(self) -> None:
        """
        Build a set of transcript source names that are known podcast episodes.
        This lets us down-rank multi-speaker transcript chunks that often cause
        factual bleed into Zarna-only replies.
        """
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        metadata_path = os.path.join(base_dir, "Processed", "youtube", "video_metadata.json")
        if not os.path.exists(metadata_path):
            return
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                videos = json.load(f)
            for v in videos:
                title = str(v.get("title", "")).lower()
                vid = str(v.get("video_id", "")).strip()
                if not vid:
                    continue
                if "zarna garg family podcast" in title:
                    self._podcast_transcript_sources.add(f"{vid}_transcript.json".lower())
            logger.info(
                "Loaded %d podcast transcript source IDs for retrieval weighting",
                len(self._podcast_transcript_sources),
            )
        except Exception as exc:
            logger.warning("Could not load podcast source metadata: %s", exc)

    def _source_weight(self, source: str) -> float:
        src = (source or "").strip().lower()
        if not src:
            return 1.0
        if src.startswith("podcast_zarna_"):
            return 1.24
        if src == "zarna_facts":
            return 1.35
        if src.endswith(".pdf"):
            # Book chunks (memoir) are a strong source of first-person facts.
            return 1.18
        if src in ("one_in_a_billion.json", "practical_people_win.json", "nervous_in_new_york.json"):
            return 1.22
        if src == "monday_motivations.json":
            # Advice/motivation content — present but deferential to comedy and facts.
            # Surfaces naturally when fans ask advice questions; won't beat humor chunks for joke queries.
            return 0.82
        if src == "podcast_episodes":
            # Episode blurbs are useful for podcast intent, but can be noisy for general replies.
            return 0.90
        if self._is_podcast_transcript_source(src):
            if self._podcast_transcripts_mode == "include":
                return 1.0
            return 0.74
        if re.match(r"^[a-z0-9_-]{8,}_transcript\.json$", src):
            # Unknown transcript files get a mild discount by default.
            return 0.92
        return 1.0

    def _is_podcast_transcript_source(self, source: str) -> bool:
        src = (source or "").strip().lower()
        if not src:
            return False
        if src.startswith("podcast_zarna_"):
            return False
        if src in self._podcast_transcript_sources:
            return True
        # Fallback: uploaded YouTube transcript filenames are usually video-id based.
        # In "exclude" mode we prefer facts/book/specials over generic transcript noise.
        return bool(re.match(r"^[a-z0-9_-]{8,}_transcript\.json$", src))

    def _embed(self, text: str) -> List[float]:
        result = self._client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=text,
        )
        return result.embeddings[0].values

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = math.sqrt(sum(x * x for x in a))
        mag_b = math.sqrt(sum(x * x for x in b))
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    def get_relevant_chunks(self, query: str, k: int = TOP_K_CHUNKS) -> List[str]:
        return self._cached_search(query, k)

    @lru_cache(maxsize=256)
    def _cached_search(self, query: str, k: int) -> List[str]:
        """
        LRU-cached search — identical queries (e.g. 'tell me a joke' from 50
        different people during a show) only hit the Gemini embedding API once.
        """
        query_embedding = self._embed(query)
        scored = []
        for c in self._chunks:
            base_score = self._cosine_similarity(query_embedding, c["embedding"])
            weighted = base_score * self._source_weight(str(c.get("source", "")))
            scored.append((weighted, c["text"]))
        scored.sort(reverse=True)
        return [text for _, text in scored[:k]]
