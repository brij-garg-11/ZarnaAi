"""
Phase 2 smoke test for PgRetriever.

What this verifies:
  1. PgRetriever can connect to the Railway Postgres and report 0 rows
     for a fresh slug (graceful empty-table handling).
  2. Gemini embeddings write successfully with vector(768) type.
  3. Similarity search returns chunks ranked by semantic relevance.
  4. Slug scoping is enforced — a different slug cannot see the test chunks.
  5. Cleanup removes every test row so no garbage sticks around.

Run:
  python scripts/test_phase2_pg_retriever.py
"""

import os
import sys
import traceback

from dotenv import load_dotenv
import psycopg2
from google import genai

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.config import EMBEDDING_MODEL, GEMINI_API_KEY  # noqa: E402
from app.retrieval.pg_retriever import PgRetriever  # noqa: E402


TEST_SLUG = "phase2_test"
OTHER_SLUG = "phase2_other"

# Deliberately varied chunks so relevance ranking is visible.
TEST_CHUNKS = [
    ("facts",   "Zarna Garg is an Indian-American comedian living in New York with her husband Shalabh and three kids."),
    ("about",   "Haley Johnson is a stand-up comedian based in Austin, Texas who specializes in observational humor about millennial life."),
    ("shows",   "Upcoming tour dates include Boston on May 10, Chicago on May 18, and Los Angeles on June 2."),
    ("podcast", "The Zarna Garg Family Podcast covers parenting, immigrant life, and marriage with husband Shalabh."),
    ("book",    "The memoir This American Woman tells the story of moving from Mumbai to New York and raising a comedy career at 44."),
]

OTHER_SLUG_CHUNK = (
    "facts",
    "This is a completely unrelated chunk belonging to a different creator — it should NEVER appear in phase2_test results.",
)


def _connect():
    url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def _seed_chunks(slug: str, chunks) -> int:
    """Embed each chunk via Gemini and insert into creator_embeddings."""
    client = genai.Client(api_key=GEMINI_API_KEY)
    conn = _connect()
    conn.autocommit = True
    inserted = 0
    try:
        with conn.cursor() as cur:
            for source, text in chunks:
                result = client.models.embed_content(
                    model=EMBEDDING_MODEL,
                    contents=text,
                )
                vec = list(result.embeddings[0].values)
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
    finally:
        conn.close()
    return inserted


def _cleanup(slug: str) -> int:
    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM creator_embeddings WHERE creator_slug = %s",
                (slug,),
            )
            return cur.rowcount
    finally:
        conn.close()


def main() -> int:
    print("=== Phase 2 Smoke Test — PgRetriever ===\n")

    # Start from a clean slate even if a previous run crashed mid-way.
    pre_a = _cleanup(TEST_SLUG)
    pre_b = _cleanup(OTHER_SLUG)
    if pre_a or pre_b:
        print(f"  (cleaned up {pre_a + pre_b} leftover rows from previous run)\n")

    failures = []

    # ── Test 1: Empty slug returns [] cleanly ─────────────────────────────
    try:
        print("[1] Empty slug → get_relevant_chunks returns []")
        retriever = PgRetriever(TEST_SLUG)
        result = retriever.get_relevant_chunks("when's your next show?", k=5)
        assert result == [], f"Expected [] for empty slug, got {result!r}"
        print("    OK\n")
    except Exception as e:
        failures.append(("Test 1", e))
        traceback.print_exc()
        print()

    # ── Seed data ─────────────────────────────────────────────────────────
    try:
        print(f"[seed] Embedding + inserting {len(TEST_CHUNKS)} chunks for slug={TEST_SLUG}")
        n = _seed_chunks(TEST_SLUG, TEST_CHUNKS)
        print(f"       inserted {n} rows")

        print(f"[seed] Embedding + inserting 1 chunk for slug={OTHER_SLUG}")
        m = _seed_chunks(OTHER_SLUG, [OTHER_SLUG_CHUNK])
        print(f"       inserted {m} rows\n")
    except Exception as e:
        print(f"SEED FAILED: {e}")
        traceback.print_exc()
        _cleanup(TEST_SLUG)
        _cleanup(OTHER_SLUG)
        return 2

    # ── Test 2: Semantic ranking works ────────────────────────────────────
    try:
        print("[2] Query 'when is the next show in Chicago' → shows chunk should rank #1")
        # Fresh retriever to pick up the new data (row count is logged on init).
        retriever = PgRetriever(TEST_SLUG)
        top = retriever.get_relevant_chunks("when is the next show in Chicago", k=3)
        assert len(top) > 0, "Expected at least one chunk"
        print("    top 3 results:")
        for i, t in enumerate(top, 1):
            print(f"      {i}. {t[:90]}{'…' if len(t) > 90 else ''}")
        assert "Chicago" in top[0] or "tour dates" in top[0].lower() or "show" in top[0].lower(), \
            f"Expected shows chunk to rank first, got: {top[0][:80]}"
        print("    OK — shows chunk ranked highest\n")
    except Exception as e:
        failures.append(("Test 2", e))
        traceback.print_exc()
        print()

    # ── Test 3: Different query → different ranking ───────────────────────
    try:
        print("[3] Query 'tell me about your family' → facts/podcast chunk should rank high")
        retriever = PgRetriever(TEST_SLUG)
        top = retriever.get_relevant_chunks("tell me about your family", k=3)
        print("    top 3 results:")
        for i, t in enumerate(top, 1):
            print(f"      {i}. {t[:90]}{'…' if len(t) > 90 else ''}")
        first = top[0].lower()
        assert ("shalabh" in first or "family" in first or "kids" in first or "husband" in first or "parenting" in first), \
            f"Expected family-related chunk first, got: {top[0][:80]}"
        print("    OK — family-related chunk ranked highest\n")
    except Exception as e:
        failures.append(("Test 3", e))
        traceback.print_exc()
        print()

    # ── Test 4: Slug isolation ────────────────────────────────────────────
    try:
        print("[4] OTHER slug retriever MUST NOT see TEST_SLUG chunks")
        other_retriever = PgRetriever(OTHER_SLUG)
        top = other_retriever.get_relevant_chunks("comedian tour dates Chicago", k=5)
        for chunk in top:
            assert "NEVER appear" in chunk or "unrelated" in chunk, \
                f"LEAK DETECTED — OTHER_SLUG retriever returned: {chunk[:80]}"
        assert len(top) == 1, f"OTHER slug should have exactly 1 chunk, got {len(top)}"
        print(f"    OK — returned only its own chunk ({len(top)} rows, no leak)\n")
    except Exception as e:
        failures.append(("Test 4", e))
        traceback.print_exc()
        print()

    # ── Test 5: Top-k bound ────────────────────────────────────────────────
    try:
        print("[5] Top-k honored (request 2, expect exactly 2)")
        retriever = PgRetriever(TEST_SLUG)
        top = retriever.get_relevant_chunks("anything", k=2)
        assert len(top) == 2, f"Expected 2 chunks, got {len(top)}"
        print(f"    OK — got {len(top)} chunks\n")
    except Exception as e:
        failures.append(("Test 5", e))
        traceback.print_exc()
        print()

    # ── Cleanup ───────────────────────────────────────────────────────────
    print("[cleanup] Removing test rows…")
    removed_a = _cleanup(TEST_SLUG)
    removed_b = _cleanup(OTHER_SLUG)
    print(f"    removed {removed_a + removed_b} rows\n")

    # ── Summary ───────────────────────────────────────────────────────────
    if failures:
        print(f"=== {len(failures)} TEST(S) FAILED ===")
        for name, err in failures:
            print(f"  - {name}: {err}")
        return 1

    print("=== All Phase 2 tests passed ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
