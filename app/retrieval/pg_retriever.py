"""
Multi-tenant pgvector retriever.

Implements BaseRetriever but pulls chunks from Postgres instead of a
pre-loaded .json.gz file. Scoped strictly by creator_slug — queries for
'haley' can never return rows belonging to 'zarna'.

Drop-in replacement for EmbeddingRetriever: create_brain() can hand either
to ZarnaBrain and the brain code is unchanged.

Requirements (already in place after Phase 1):
  - pgvector extension enabled on the Postgres instance
  - creator_embeddings table with `embedding vector(3072)` column
    (3072 matches gemini-embedding-001's native output)
  - HNSW halfvec cosine index on the embedding column (ivfflat caps at ~2000
    dims, so we cast to halfvec(3072) for the index)

Usage:
    retriever = PgRetriever("haley")
    chunks = retriever.get_relevant_chunks("when's your next show?")
"""

import logging
import os
import threading
from functools import lru_cache
from typing import Callable, List, Optional

import psycopg2
import psycopg2.extras
from google import genai

from app.config import EMBEDDING_MODEL, GEMINI_API_KEY, TOP_K_CHUNKS
from app.retrieval.base import BaseRetriever

logger = logging.getLogger(__name__)

# When a source-weight function is provided we can't use the HNSW index —
# a boosted zarna_facts chunk (1.35× weight) can beat a higher-raw-similarity
# transcript chunk (0.92× weight), so we MUST consider every row for that
# slug, not just the top-K by cosine. We therefore do a full sequential scan
# scoped by creator_slug and bring (text, source, distance) back to Python
# for the weighted rerank. The embedding column (~12KB each) is explicitly
# NOT selected, keeping the payload tiny (~200B/row).
#
# This matches EmbeddingRetriever._cached_search's behaviour, which iterates
# over every in-memory chunk. Safety cap keeps us from runaway memory on
# pathological cases (e.g. a creator with 500k rows who shouldn't be on the
# weighted path); normal creators sit well under this.
_WEIGHTED_SCAN_CAP = 20000


class PgRetriever(BaseRetriever):
    """
    pgvector-backed retriever, scoped by creator_slug.

    Every query:
      1. Embeds the query via Gemini (same model used at ingestion time)
      2. Runs `SELECT chunk_text FROM creator_embeddings
                 WHERE creator_slug = %s
                 ORDER BY embedding <=> %s::vector LIMIT %s`
      3. Returns the chunk texts (strings) in similarity order

    An LRU cache in front of the search avoids re-embedding identical queries
    during a show (same as EmbeddingRetriever's cache).
    """

    def __init__(
        self,
        creator_slug: str,
        dsn: Optional[str] = None,
        weight_fn: Optional[Callable[[str], float]] = None,
    ) -> None:
        """
        Args:
          creator_slug: Postgres scope key; every query is WHERE creator_slug=this.
          dsn:          optional DB connection string; defaults to env DATABASE_URL.
          weight_fn:    optional ``(source: str) -> float`` multiplier applied to
                        each candidate's cosine similarity. Return 0.0 to drop a
                        source entirely. When None, PgRetriever trusts raw
                        pgvector order (fine for new creators; Zarna passes in
                        ``zarna_weight_fn()`` to preserve legacy behaviour).
        """
        slug = (creator_slug or "").strip().lower()
        if not slug:
            raise ValueError("PgRetriever requires a non-empty creator_slug")
        self._slug = slug

        resolved_dsn = dsn or os.getenv("DATABASE_URL", "")
        if not resolved_dsn:
            raise RuntimeError(
                "PgRetriever needs a Postgres DSN — pass dsn=… or set DATABASE_URL"
            )
        self._dsn = resolved_dsn.replace("postgres://", "postgresql://", 1)

        self._client = genai.Client(api_key=GEMINI_API_KEY)
        self._weight_fn: Optional[Callable[[str], float]] = weight_fn

        # Serialize access to the single long-lived connection. Gemini embedding
        # is I/O-bound so contention here is minimal; a connection pool is only
        # needed if retrieval becomes a hot path (unlikely for SMS scale).
        self._conn_lock = threading.Lock()
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._ensure_conn()

        count = self._count_rows()
        logger.info(
            "PgRetriever ready: slug=%s rows=%d model=%s weighted=%s",
            self._slug, count, EMBEDDING_MODEL, bool(self._weight_fn),
        )

    def _ensure_conn(self) -> None:
        """Open (or reopen) the DB connection. Called on init and after failures."""
        if self._conn is not None:
            try:
                if self._conn.closed == 0:
                    return
            except Exception:
                pass
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = True

    def _count_rows(self) -> int:
        with self._conn_lock:
            self._ensure_conn()
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM creator_embeddings WHERE creator_slug = %s",
                    (self._slug,),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0

    def _embed(self, text: str) -> List[float]:
        result = self._client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=text,
        )
        return list(result.embeddings[0].values)

    def get_relevant_chunks(self, query: str, k: int = TOP_K_CHUNKS) -> List[str]:
        return self._cached_search(query, k)

    @lru_cache(maxsize=256)
    def _cached_search(self, query: str, k: int) -> List[str]:
        try:
            query_vec = self._embed(query)
        except Exception as exc:
            logger.exception("PgRetriever: embedding failed for slug=%s: %s", self._slug, exc)
            return []

        # pgvector accepts a string like "[0.1,0.2,…]" cast to vector.
        vec_literal = "[" + ",".join(f"{v:.8f}" for v in query_vec) + "]"

        # Two execution paths:
        #   Unweighted: let pgvector's HNSW halfvec index pick top-K directly —
        #               fast, approximate, uniform across all candidates.
        #   Weighted:   full per-slug scan (no ORDER BY on the distance operator
        #               means the index doesn't help anyway), return text/source/
        #               distance to Python for multiplicative rerank. See
        #               _WEIGHTED_SCAN_CAP docstring for the rationale.
        if self._weight_fn is None:
            sql = """
                SELECT chunk_text
                FROM creator_embeddings
                WHERE creator_slug = %s
                ORDER BY embedding::halfvec(3072) <=> %s::halfvec(3072)
                LIMIT %s
            """
            params = (self._slug, vec_literal, k)
        else:
            sql = """
                SELECT chunk_text,
                       source,
                       (embedding::halfvec(3072) <=> %s::halfvec(3072)) AS distance
                FROM creator_embeddings
                WHERE creator_slug = %s
                LIMIT %s
            """
            params = (vec_literal, self._slug, _WEIGHTED_SCAN_CAP)

        try:
            with self._conn_lock:
                self._ensure_conn()
                with self._conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall()
        except Exception as exc:
            logger.exception(
                "PgRetriever: query failed for slug=%s: %s — reopening connection",
                self._slug, exc,
            )
            with self._conn_lock:
                try:
                    if self._conn is not None:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
                self._ensure_conn()
            return []

        if self._weight_fn is None:
            return [r[0] for r in rows if r and r[0]]

        # Weighted path — pgvector returns cosine DISTANCE (0=identical, 2=opposite).
        # Convert to similarity (1 - distance), apply the source weight, drop
        # zero-weighted rows (excluded by source filter), sort, take top-k.
        weight_fn = self._weight_fn
        scored: list[tuple[float, str]] = []
        for text, source, distance in rows:
            if not text:
                continue
            w = weight_fn(source or "")
            if w <= 0.0:
                continue  # excluded source (e.g. podcast transcript when mode=exclude)
            similarity = 1.0 - float(distance)
            scored.append((similarity * w, text))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [text for _, text in scored[:k]]
