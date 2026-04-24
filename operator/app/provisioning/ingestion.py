"""
Content ingestion pipeline for a new creator.

Takes their onboarding form and produces a searchable knowledge base:
  1. Seed facts chunk from bio + extra_context (highest priority)
  2. Scrape website_url (home + /about + /faq)
  3. Process uploaded files (PDF + .txt) — bytes, NEVER disk
  4. (Optional) Podcast RSS / YouTube channel descriptions
  5. Chunk everything (~500 tokens, 50-token overlap)
  6. Batch embed via Gemini (same model as Zarna: gemini-embedding-001, 3072 dims)
  7. INSERT into creator_embeddings scoped by creator_slug

Idempotent: if any rows already exist for this slug, we skip entirely.
Re-ingest by deleting those rows first.

Dependencies (optional, with graceful fallback):
  - requests   (already in requirements)
  - bs4        (BeautifulSoup — optional, install for HTML extraction)
  - pypdf      (optional, for PDF uploads)
"""

from __future__ import annotations

import base64
import io
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from google import genai
from google.genai import errors as genai_errors

from ..db import get_conn

_log = logging.getLogger(__name__)

_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
_EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "gemini-embedding-001")
_CHUNK_TOKENS = 500
_CHUNK_OVERLAP_TOKENS = 50
_BATCH_SIZE = 50

# Very rough: 1 token ≈ 4 characters. Enough for the chunker; accurate
# tokenization would require the Gemini tokenizer and adds a dependency.
_CHARS_PER_TOKEN = 4
_CHUNK_CHARS = _CHUNK_TOKENS * _CHARS_PER_TOKEN
_OVERLAP_CHARS = _CHUNK_OVERLAP_TOKENS * _CHARS_PER_TOKEN


# ---------------------------------------------------------------------------
# Idempotency check
# ---------------------------------------------------------------------------

def _existing_row_count(slug: str) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM creator_embeddings WHERE creator_slug=%s",
                (slug,),
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Content collection
# ---------------------------------------------------------------------------

def _seed_facts_chunk(form: Dict[str, Any]) -> List[Tuple[str, str]]:
    """The (source, text) "facts" chunk assembled directly from the form."""
    display_name = (form.get("display_name") or "").strip()
    bio = (form.get("bio") or "").strip()
    extra = (form.get("extra_context") or "").strip()

    parts: List[str] = []
    if display_name:
        parts.append(f"Name: {display_name}.")
    if bio:
        parts.append(f"Bio: {bio}")
    if extra:
        parts.append(f"Additional context: {extra}")

    text = " ".join(parts).strip()
    if not text:
        return []
    return [("facts", text)]


def _scrape_website(url: str) -> List[Tuple[str, str]]:
    """
    Best-effort scrape of home + /about + /faq. Returns (source, text) tuples.
    Fails silently (logs a warning) — callers still get the facts chunk.
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except ImportError:
        _log.warning("ingestion: bs4 not installed — skipping website scrape")
        return []

    if not url or not url.strip():
        return []

    # Normalize to https:// when the user omitted scheme
    if not urlparse(url).scheme:
        url = "https://" + url.lstrip("/")

    paths_to_try = [
        ("website_general", url),
        ("website_about",   urljoin(url, "/about")),
        ("website_faq",     urljoin(url, "/faq")),
    ]

    collected: List[Tuple[str, str]] = []
    session = requests.Session()
    session.headers["User-Agent"] = "ZarnaBot-Ingestion/1.0"

    for source, candidate in paths_to_try:
        try:
            resp = session.get(candidate, timeout=15, allow_redirects=True)
            if resp.status_code != 200 or not resp.text:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) < 200:
                continue  # probably a thin page — not worth keeping
            collected.append((source, text))
            _log.info("ingestion: scraped %s (%d chars)", candidate, len(text))
        except Exception as exc:
            _log.warning("ingestion: scrape failed for %s — %s", candidate, exc)

    return collected


def _process_uploaded_files(files: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Files come in as a list of dicts: {filename, content_type, data_b64}.
    We decode in memory — NEVER write to disk (Railway FS is ephemeral).
    """
    if not files:
        return []

    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError:
        PdfReader = None  # type: ignore

    collected: List[Tuple[str, str]] = []
    for entry in files:
        name = str(entry.get("filename") or "uploaded")
        b64 = entry.get("data_b64") or ""
        if not b64:
            continue
        try:
            raw = base64.b64decode(b64)
        except Exception:
            _log.warning("ingestion: bad base64 for %s — skipping", name)
            continue

        lower = name.lower()
        if lower.endswith(".pdf"):
            if PdfReader is None:
                _log.warning("ingestion: pypdf not installed — skipping %s", name)
                continue
            try:
                reader = PdfReader(io.BytesIO(raw))
                text = "\n".join((p.extract_text() or "") for p in reader.pages)
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) >= 200:
                    collected.append(("doc_upload", text))
                    _log.info("ingestion: extracted %s (%d chars from PDF)", name, len(text))
            except Exception as exc:
                _log.warning("ingestion: PDF extract failed for %s — %s", name, exc)
        else:
            # Treat anything non-PDF as text.
            try:
                text = raw.decode("utf-8", errors="ignore").strip()
                if len(text) >= 200:
                    collected.append(("doc_upload", text))
                    _log.info("ingestion: read %s (%d chars from text)", name, len(text))
            except Exception as exc:
                _log.warning("ingestion: text decode failed for %s — %s", name, exc)

    return collected


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def _chunk_text(text: str, chunk_chars: int = _CHUNK_CHARS, overlap_chars: int = _OVERLAP_CHARS) -> List[str]:
    """
    Simple sliding-window chunker on characters. Prefers breaking at sentence
    boundaries within a ±100-char window.
    """
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_chars:
        return [text]

    out: List[str] = []
    start = 0
    while start < len(text):
        end = min(start + chunk_chars, len(text))
        if end < len(text):
            window = text[max(end - 100, start): min(end + 100, len(text))]
            match = re.search(r"[.!?]\s+[A-Z]", window)
            if match:
                end = max(end - 100, start) + match.start() + 1
        chunk = text[start:end].strip()
        if chunk:
            out.append(chunk)
        if end >= len(text):
            break
        start = max(end - overlap_chars, start + 1)
    return out


def _chunk_all(raw_sources: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """Apply chunker to every (source, text). Facts chunk is kept whole."""
    chunks: List[Tuple[str, str]] = []
    for source, text in raw_sources:
        if source == "facts":
            # Never split the facts chunk — it's short and high-priority.
            chunks.append((source, text))
            continue
        for piece in _chunk_text(text):
            chunks.append((source, piece))
    return chunks


# ---------------------------------------------------------------------------
# Embedding + storage
# ---------------------------------------------------------------------------

def _embed_batch_with_retry(client: genai.Client, texts: List[str], max_retries: int = 5) -> List[List[float]]:
    """
    Mirrors scripts/build_embeddings.py retry behaviour — exponential backoff
    on 429/RESOURCE_EXHAUSTED. Returns embeddings in input order.
    """
    delay = 10.0
    for attempt in range(max_retries):
        try:
            result = client.models.embed_content(
                model=_EMBEDDING_MODEL,
                contents=texts,
            )
            return [list(e.values) for e in result.embeddings]
        except genai_errors.ClientError as exc:
            msg = str(exc)
            if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                if attempt < max_retries - 1:
                    _log.warning("ingestion: rate limited — sleeping %.0fs", delay)
                    time.sleep(delay)
                    delay = min(delay * 2, 120)
                    continue
            raise
    raise RuntimeError("ingestion: exhausted embedding retries")


def _insert_chunks(slug: str, chunks: List[Tuple[str, str, List[float]]]) -> int:
    """Bulk insert (source, text, embedding) tuples for one slug."""
    if not chunks:
        return 0
    conn = get_conn()
    try:
        conn.autocommit = True
        inserted = 0
        with conn.cursor() as cur:
            for source, text, vec in chunks:
                vec_literal = "[" + ",".join(f"{v:.8f}" for v in vec) + "]"
                cur.execute(
                    """
                    INSERT INTO creator_embeddings
                        (creator_slug, chunk_text, source, embedding)
                    VALUES (%s, %s, %s, %s::vector)
                    """,
                    (slug, text, source, vec_literal),
                )
                inserted += 1
        return inserted
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def run(slug: str, form: Dict[str, Any]) -> int:
    """
    End-to-end ingestion for `slug` using `form` (the onboarding submission).
    Returns the number of chunks inserted.

    Idempotent: if creator_embeddings already has rows for this slug, skip.
    """
    existing = _existing_row_count(slug)
    if existing > 0:
        _log.info("ingestion[%s]: %d rows already exist — skipping", slug, existing)
        return existing

    if not _GEMINI_API_KEY:
        raise RuntimeError("ingestion: GEMINI_API_KEY not set — cannot embed")

    # 1-4: collect raw content
    raw_sources: List[Tuple[str, str]] = []
    raw_sources.extend(_seed_facts_chunk(form))

    website_url = (form.get("website_url") or "").strip()
    if website_url:
        raw_sources.extend(_scrape_website(website_url))

    uploaded = form.get("uploaded_files") or []
    raw_sources.extend(_process_uploaded_files(uploaded))

    if not raw_sources:
        _log.warning(
            "ingestion[%s]: no usable content found (no bio, no scrape, no uploads) "
            "— inserting 0 rows; bot will rely on prompt-only replies",
            slug,
        )
        return 0

    # 5. chunk everything
    chunks = _chunk_all(raw_sources)
    if not chunks:
        return 0
    _log.info(
        "ingestion[%s]: %d raw sources → %d chunks (avg %d chars)",
        slug, len(raw_sources), len(chunks),
        sum(len(t) for _, t in chunks) // max(len(chunks), 1),
    )

    # 6. embed in batches
    client = genai.Client(api_key=_GEMINI_API_KEY)
    embedded: List[Tuple[str, str, List[float]]] = []
    total = len(chunks)
    for i in range(0, total, _BATCH_SIZE):
        batch = chunks[i : i + _BATCH_SIZE]
        texts = [t for _, t in batch]
        vecs = _embed_batch_with_retry(client, texts)
        for (source, text), vec in zip(batch, vecs):
            embedded.append((source, text, vec))
        _log.info("ingestion[%s]: embedded %d/%d", slug, min(i + _BATCH_SIZE, total), total)
        if i + _BATCH_SIZE < total:
            time.sleep(0.3)

    # 7. store
    n = _insert_chunks(slug, embedded)
    _log.info("ingestion[%s]: inserted %d creator_embeddings rows", slug, n)
    return n
