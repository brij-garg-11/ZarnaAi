"""
Phase 5 quality comparison — proves PgRetriever('zarna') returns
semantically equivalent chunks to EmbeddingRetriever for Zarna.

How it works:
  1. Instantiate both retrievers.
  2. For a fixed bank of realistic fan messages, fetch top-K chunks from each.
  3. Compare:
       - top-1 exact match rate (same single best chunk)
       - top-3 overlap  (|intersection| / 3)
       - top-K Jaccard  (|A∩B| / |A∪B|) with K = TOP_K_CHUNKS (7 in prod)
  4. Fail hard if:
       - top-1 match rate < 60%      (roughly: 6/10 queries must agree exactly)
       - average top-K Jaccard < 0.50 (at least half the chunks overlap)

Why these thresholds:
  Different retrieval paths will NEVER agree 100% on chunk order — pgvector
  uses halfvec precision which introduces minor distance ties; float32
  cosine in Python is exact. The thresholds are conservative: 60% top-1 +
  50% average Jaccard is the bar at which the generator, which only sees
  the combined chunks as plain text, produces behaviourally identical
  replies in manual A/B testing. (If you bump the thresholds up, you're
  asking for byte-for-byte equivalence, which pgvector can't give.)

Run:
  python scripts/test_phase5_quality_comparison.py

Prerequisites:
  - scripts/migrate_zarna_to_pg.py --apply  has been run
  - DATABASE_URL + GEMINI_API_KEY in .env
"""

from __future__ import annotations

import os
import statistics
import sys
import traceback

from dotenv import load_dotenv

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.retrieval.embedding import EmbeddingRetriever  # noqa: E402
from app.retrieval.pg_retriever import PgRetriever  # noqa: E402
from app.retrieval.source_weights import (  # noqa: E402
    load_podcast_transcript_ids,
    zarna_weight_fn,
)


TOP_K = 7  # matches TOP_K_CHUNKS default

# Representative fan queries spanning the main retrieval lanes: structured
# sell intents (SHOW/BOOK/MERCH/PODCAST/CLIP), conversational, empathy,
# family roast, and quiz-y bits. If these all overlap, the production
# conversation surface is covered.
TEST_QUERIES = [
    "What's your book called?",
    "When's your next show?",
    "Where can I buy your book?",
    "Tell me a joke",
    "What do you think of Shalabh?",
    "How do you deal with your mother in law?",
    "Do you have any merch?",
    "Tell me about your kids",
    "I'm feeling really sad today",
    "My MIL is driving me crazy",
    "What's your podcast called?",
    "Are you touring in Chicago?",
]

# Thresholds — see module docstring for rationale.
MIN_TOP1_RATE = 0.60
MIN_AVG_JACCARD = 0.50


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / max(len(sa | sb), 1)


def _overlap_at(a: list[str], b: list[str], n: int) -> float:
    if not a or not b:
        return 0.0
    return len(set(a[:n]) & set(b[:n])) / max(n, 1)


def main() -> int:
    print("=== Phase 5 Quality Comparison — EmbeddingRetriever vs PgRetriever('zarna') ===\n")

    print("[boot] loading EmbeddingRetriever (legacy file-backed)")
    legacy = EmbeddingRetriever()
    print(f"       chunks in memory: {len(legacy._chunks)}\n")

    print("[boot] loading PgRetriever('zarna') with zarna_weight_fn")
    weight_fn = zarna_weight_fn(
        podcast_transcript_ids=load_podcast_transcript_ids(),
        podcast_mode=os.getenv("PODCAST_TRANSCRIPTS_MODE", "exclude"),
        monday_mode=os.getenv("MONDAY_MOTIVATION_MODE", "include"),
    )
    pg = PgRetriever("zarna", weight_fn=weight_fn)
    print()

    per_query_top1 = []
    per_query_top3 = []
    per_query_jaccard = []
    failures: list[str] = []

    for i, q in enumerate(TEST_QUERIES, 1):
        try:
            legacy_chunks = legacy.get_relevant_chunks(q, k=TOP_K)
            pg_chunks = pg.get_relevant_chunks(q, k=TOP_K)
        except Exception as exc:
            failures.append(f"query {i} raised: {exc}")
            traceback.print_exc()
            continue

        top1_match = bool(legacy_chunks and pg_chunks and legacy_chunks[0] == pg_chunks[0])
        top3 = _overlap_at(legacy_chunks, pg_chunks, 3)
        jacc = _jaccard(legacy_chunks, pg_chunks)

        per_query_top1.append(1.0 if top1_match else 0.0)
        per_query_top3.append(top3)
        per_query_jaccard.append(jacc)

        flag = "✓" if top1_match else " "
        print(f"[{i:2d}/{len(TEST_QUERIES)}] {flag} top1={'yes' if top1_match else 'no ':3} "
              f"top3={top3:.2f}  jaccard={jacc:.2f}  q={q!r}")
        if not top1_match:
            print(f"       legacy[0]: {legacy_chunks[0][:110] if legacy_chunks else '(empty)'!r}")
            print(f"       pg[0]    : {pg_chunks[0][:110] if pg_chunks else '(empty)'!r}")

    if not per_query_top1:
        print("\nNo queries returned results — aborting.")
        return 1

    top1_rate = statistics.mean(per_query_top1)
    avg_top3 = statistics.mean(per_query_top3)
    avg_jacc = statistics.mean(per_query_jaccard)

    print("\n=== Aggregate ===")
    print(f"  queries             : {len(per_query_top1)}")
    print(f"  top-1 exact match   : {top1_rate:.1%}  (threshold {MIN_TOP1_RATE:.0%})")
    print(f"  top-3 overlap (avg) : {avg_top3:.1%}")
    print(f"  top-{TOP_K} jaccard (avg) : {avg_jacc:.1%}  (threshold {MIN_AVG_JACCARD:.0%})")

    if top1_rate < MIN_TOP1_RATE:
        failures.append(f"top-1 match rate {top1_rate:.1%} < {MIN_TOP1_RATE:.0%}")
    if avg_jacc < MIN_AVG_JACCARD:
        failures.append(f"average Jaccard {avg_jacc:.1%} < {MIN_AVG_JACCARD:.0%}")

    if failures:
        print(f"\n=== {len(failures)} FAILURE(S) ===")
        for f in failures:
            print(f"  - {f}")
        print("\nQuality regressed. Check source-weight function, migrated "
              "embeddings dimension, and PODCAST_TRANSCRIPTS_MODE before flipping "
              "Zarna to PgRetriever in production.")
        return 1

    print("\n=== PASS — PgRetriever('zarna') is quality-equivalent to EmbeddingRetriever ===")
    print("  Safe to switch Zarna's create_brain() default by setting "
          "PG_RETRIEVER_FOR_ZARNA=1 in the production environment.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
