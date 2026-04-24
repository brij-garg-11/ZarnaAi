"""
One-off migration: move Zarna's existing training data into the universal
multi-tenant tables.

Source                                     → Destination
─────────────────────────────────────────────────────────────────────────
creator_config/zarna.json                  → creator_configs       (slug=zarna)
training_data/zarna_embeddings.json.gz     → creator_embeddings    (slug=zarna)

Run:
  python scripts/migrate_zarna_to_pg.py              # idempotent dry-run summary
  python scripts/migrate_zarna_to_pg.py --apply      # actually write rows

After a successful --apply, the PgRetriever('zarna') path will return
semantically equivalent chunks to EmbeddingRetriever (verify with
scripts/test_phase5_quality_comparison.py).

Notes:
  - We insert ALL chunks regardless of PODCAST_TRANSCRIPTS_MODE /
    MONDAY_MOTIVATION_MODE. Filtering stays at retrieval time so those
    toggles remain runtime-switchable without a re-ingest.
  - --apply clears any existing rows for slug='zarna' first so reruns are
    deterministic (think: "the last apply wins"). Other slugs untouched.
  - Batches INSERTs in groups of 500 with execute_values() — takes ~60-90s
    for 3k chunks on a healthy Postgres, most of it network RTT.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from typing import Iterable, List, Tuple

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SLUG = "zarna"
CONFIG_PATH = os.path.join(ROOT, "creator_config", f"{SLUG}.json")
EMBEDDINGS_PATH = os.path.join(ROOT, "training_data", "zarna_embeddings.json.gz")

BATCH_SIZE = 500  # rows per execute_values() call


def _connect():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL not set — add it to .env before running this script")
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


# ---------------------------------------------------------------------------
# Config migration
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Drop any underscore-prefixed private keys like "_note" — keeping them
    # in JSONB is harmless but noisy. config_writer-produced rows never
    # contain them.
    return {k: v for k, v in data.items() if not k.startswith("_")}


def _upsert_config(conn, config: dict, apply: bool) -> None:
    print(f"\n[config] slug={SLUG}")
    print(f"         display_name = {config.get('display_name')!r}")
    print(f"         keys         = {len(config)} top-level fields")
    if not apply:
        print("         DRY-RUN — no write")
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO creator_configs (creator_slug, config_json)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (creator_slug)
            DO UPDATE SET
                config_json = EXCLUDED.config_json,
                updated_at  = NOW()
            """,
            (SLUG, json.dumps(config)),
        )
    print(f"         WROTE creator_configs row")


# ---------------------------------------------------------------------------
# Embeddings migration
# ---------------------------------------------------------------------------

def _iter_embedding_rows(path: str) -> Iterable[Tuple[str, str, List[float]]]:
    """
    Yield (chunk_text, source, embedding) tuples from the gz file.
    Lazy, so we don't hold the whole file in memory (though 110 MB is fine
    to fit — this just keeps peak RAM flat when we also batch-insert).
    """
    open_fn = gzip.open if path.endswith(".gz") else open
    with open_fn(path, "rt", encoding="utf-8") as f:
        data = json.load(f)
    for entry in data:
        text = entry.get("text") or ""
        source = entry.get("source") or ""
        embedding = entry.get("embedding") or []
        if not text or not embedding:
            continue
        yield text, source, embedding


def _clear_existing(conn, apply: bool) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM creator_embeddings WHERE creator_slug=%s", (SLUG,))
        existing = int(cur.fetchone()[0])
    print(f"\n[embeddings] current rows for slug={SLUG}: {existing}")
    if existing and apply:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM creator_embeddings WHERE creator_slug=%s", (SLUG,))
        print(f"             DELETED {existing} existing rows (fresh re-ingest)")
    return existing


def _bulk_insert(conn, rows: List[Tuple[str, str, List[float]]], apply: bool) -> int:
    """Batch insert chunks. Returns the number of rows actually written (0 in dry-run)."""
    if not rows:
        return 0

    # psycopg2's execute_values is dramatically faster than execute_many for
    # many-row inserts — it concatenates all VALUES into a single statement.
    # For a pgvector column we have to serialize the vector to its text
    # form "[x,y,z,…]" and cast in the VALUES template.
    def _vec_literal(v: List[float]) -> str:
        return "[" + ",".join(f"{x:.8f}" for x in v) + "]"

    payload = [(SLUG, text, source, _vec_literal(vec)) for text, source, vec in rows]

    if not apply:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO creator_embeddings (creator_slug, chunk_text, source, embedding)
            VALUES %s
            """,
            payload,
            template="(%s, %s, %s, %s::vector)",
            page_size=BATCH_SIZE,
        )
    return len(rows)


def _migrate_embeddings(conn, apply: bool) -> None:
    if not os.path.exists(EMBEDDINGS_PATH):
        raise FileNotFoundError(
            f"Embeddings file missing at {EMBEDDINGS_PATH}. "
            "Run scripts/build_embeddings.py first."
        )

    # Count upfront so we can report progress correctly.
    all_rows: List[Tuple[str, str, List[float]]] = list(_iter_embedding_rows(EMBEDDINGS_PATH))
    total = len(all_rows)
    print(f"[embeddings] loaded {total} rows from {os.path.relpath(EMBEDDINGS_PATH, ROOT)}")

    if total == 0:
        print("             nothing to insert — skipping")
        return

    _clear_existing(conn, apply)

    # Summarize source distribution so the user can sanity-check.
    from collections import Counter
    src_counts = Counter(src for _, src, _ in all_rows)
    print(f"             source distribution (top 10):")
    for src, n in src_counts.most_common(10):
        print(f"               {n:5d}  {src or '(blank)'}")
    if len(src_counts) > 10:
        print(f"               … and {len(src_counts) - 10} more sources")

    # Check embedding dimension — every row must be 3072 or the HNSW
    # halfvec(3072) index will reject the insert.
    first_dim = len(all_rows[0][2])
    if first_dim != 3072:
        raise RuntimeError(
            f"Embedding dimension mismatch — expected 3072 per gemini-embedding-001, "
            f"got {first_dim} in the source file."
        )

    if not apply:
        print("             DRY-RUN — no writes. Re-run with --apply to actually migrate.")
        return

    # Batch in chunks so progress is visible and memory stays predictable.
    written = 0
    t0 = time.time()
    for i in range(0, total, BATCH_SIZE):
        batch = all_rows[i : i + BATCH_SIZE]
        written += _bulk_insert(conn, batch, apply=True)
        done = min(i + BATCH_SIZE, total)
        elapsed = time.time() - t0
        rate = written / max(elapsed, 0.001)
        print(f"             inserted {done:5d}/{total}  ({rate:.0f} rows/s)")

    print(f"             INSERT complete — {written} rows in {time.time() - t0:.1f}s")


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write rows. Without this flag the script just reports what it would do.")
    args = parser.parse_args()

    print("=== Zarna → Postgres universal-tables migration ===")
    print(f"    mode      : {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"    slug      : {SLUG}")
    print(f"    config    : {os.path.relpath(CONFIG_PATH, ROOT)}")
    print(f"    embeddings: {os.path.relpath(EMBEDDINGS_PATH, ROOT)}")

    config = _load_config()

    conn = _connect()
    conn.autocommit = True
    try:
        _upsert_config(conn, config, apply=args.apply)
        _migrate_embeddings(conn, apply=args.apply)
    finally:
        conn.close()

    if not args.apply:
        print("\nDry-run complete. Re-run with --apply to actually write.")
    else:
        print("\nMigration complete. Verify with scripts/test_phase5_quality_comparison.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
