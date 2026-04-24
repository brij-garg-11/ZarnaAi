"""
Multi-tenant isolation + edge-case coverage for PgRetriever, creator_configs,
and load_creator DB fallback.

Scenarios covered:
  T1. Two creators with overlapping chunk text — each retriever returns ONLY
      its own rows even when the query matches both.
  T2. creator_configs write for slug A does not affect slug B's row.
  T3. load_creator falls back to DB when no file exists on disk.
  T4. load_creator still prefers the file when both file + DB rows exist
      (Zarna's production behaviour is NOT changed by the DB fallback).
  T5. PgRetriever raises on empty slug.
  T6. PgRetriever returns [] on a slug with zero rows (new creator mid-ingest).
  T7. PgRetriever with weight_fn that returns 0.0 for everything drops all
      results (exclusion filter works).
  T8. PgRetriever weighted re-rank actually changes ordering vs unweighted.
  T9. Two retrievers for different slugs can coexist in the same process
      (separate connections, no shared state).

Each assertion prints its own OK/FAIL so partial successes are visible.
"""
from __future__ import annotations
import os, sys, json, tempfile
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import psycopg2
from google import genai

from app.config import GEMINI_API_KEY, EMBEDDING_MODEL
from app.retrieval.pg_retriever import PgRetriever

DSN = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
assert DSN, "DATABASE_URL required"

SLUG_A = "isolation_test_alpha"
SLUG_B = "isolation_test_beta"
failures: list[str] = []

def ok(label, cond, detail=""):
    mark = "✓" if cond else "✗"
    print(f"  [{mark}] {label}" + (f"  — {detail}" if detail else ""))
    if not cond:
        failures.append(label)

client = genai.Client(api_key=GEMINI_API_KEY)

def embed(text: str) -> list[float]:
    return list(client.models.embed_content(model=EMBEDDING_MODEL, contents=text).embeddings[0].values)

def vec_lit(v): return "[" + ",".join(f"{x:.8f}" for x in v) + "]"

# Representative chunks — deliberately similar text across tenants so a
# leaky retriever would return the wrong slug's row.
A_CHUNKS = [
    ("ALPHA fact: Alice leads a tour in Seattle this May at the alpha comedy hall.", "alpha_facts"),
    ("ALPHA_skit_1 content: Alice jokes about her parents in her alpha voice.", "alpha_skit_1.json"),
    ("ALPHA monday: Alice shares her morning motivation on Mondays.", "monday_motivations.json"),
]
B_CHUNKS = [
    ("BETA fact: Bob leads a tour in Austin this May at the beta comedy club.", "beta_facts"),
    ("BETA_skit_1 content: Bob jokes about his parents in his beta voice.", "beta_skit_1.json"),
]

def seed(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM creator_embeddings WHERE creator_slug IN (%s,%s)", (SLUG_A, SLUG_B))
        cur.execute("DELETE FROM creator_configs WHERE creator_slug IN (%s,%s)", (SLUG_A, SLUG_B))
        for slug, rows in ((SLUG_A, A_CHUNKS), (SLUG_B, B_CHUNKS)):
            for text, src in rows:
                v = vec_lit(embed(text))
                cur.execute(
                    "INSERT INTO creator_embeddings(creator_slug,chunk_text,source,embedding) VALUES (%s,%s,%s,%s::vector)",
                    (slug, text, src, v),
                )
        cur.execute(
            "INSERT INTO creator_configs(creator_slug,config_json) VALUES (%s,%s::jsonb)",
            (SLUG_A, json.dumps({"display_name":"Alpha Alice","slug":SLUG_A,"name_variants":["alice"]})),
        )
        cur.execute(
            "INSERT INTO creator_configs(creator_slug,config_json) VALUES (%s,%s::jsonb)",
            (SLUG_B, json.dumps({"display_name":"Beta Bob","slug":SLUG_B,"name_variants":["bob"]})),
        )

def cleanup(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM creator_embeddings WHERE creator_slug IN (%s,%s)", (SLUG_A, SLUG_B))
        cur.execute("DELETE FROM creator_configs WHERE creator_slug IN (%s,%s)", (SLUG_A, SLUG_B))

def main():
    conn = psycopg2.connect(DSN); conn.autocommit = True
    print("=== Setup: seeding two tenants ===")
    seed(conn)
    print(f"  seeded {len(A_CHUNKS)} rows for {SLUG_A}, {len(B_CHUNKS)} rows for {SLUG_B}")

    try:
        # --- T1 tenant isolation in retrieval ---
        print("\n=== T1: Retrieval tenant isolation ===")
        ra = PgRetriever(SLUG_A); rb = PgRetriever(SLUG_B)
        results_a = ra.get_relevant_chunks("tour this May", k=5)
        results_b = rb.get_relevant_chunks("tour this May", k=5)
        ok("Alpha retriever returns some rows", len(results_a) > 0, f"got {len(results_a)}")
        ok("Alpha retriever returns ONLY alpha content", all("ALPHA" in r for r in results_a), f"bad: {[r[:40] for r in results_a if 'ALPHA' not in r]}")
        ok("Beta retriever returns ONLY beta content", all("BETA" in r for r in results_b), f"bad: {[r[:40] for r in results_b if 'BETA' not in r]}")
        ok("Alpha top-1 is Alice", "Alice" in (results_a[0] if results_a else ""))
        ok("Beta top-1 is Bob", "Bob" in (results_b[0] if results_b else ""))

        # --- T2 creator_configs isolation ---
        print("\n=== T2: creator_configs row isolation ===")
        with conn.cursor() as cur:
            cur.execute("SELECT config_json FROM creator_configs WHERE creator_slug=%s", (SLUG_A,))
            a_cfg = cur.fetchone()[0]
            cur.execute("SELECT config_json FROM creator_configs WHERE creator_slug=%s", (SLUG_B,))
            b_cfg = cur.fetchone()[0]
        ok("Alpha row has Alpha Alice", a_cfg.get("display_name") == "Alpha Alice")
        ok("Beta row has Beta Bob", b_cfg.get("display_name") == "Beta Bob")
        ok("Rows don't share identity", a_cfg["slug"] != b_cfg["slug"])

        # --- T5 edge: empty slug must raise ---
        print("\n=== T5: Empty slug rejected ===")
        try:
            PgRetriever("")
            ok("empty slug raised", False, "PgRetriever('') did not raise")
        except ValueError as exc:
            ok("empty slug raises ValueError", True, str(exc))

        # --- T6 edge: unknown slug returns [] ---
        print("\n=== T6: Unknown slug returns empty list ===")
        r_unknown = PgRetriever("slug_that_definitely_does_not_exist_qxz999")
        chunks = r_unknown.get_relevant_chunks("anything", k=5)
        ok("unknown slug returns empty list", chunks == [], f"got {len(chunks)} rows")

        # --- T7 edge: all-zero weight_fn drops everything ---
        print("\n=== T7: weight_fn returning 0 for all drops every row ===")
        r_all_zero = PgRetriever(SLUG_A, weight_fn=lambda s: 0.0)
        chunks = r_all_zero.get_relevant_chunks("tour this May", k=5)
        ok("all-zero weight drops every row", chunks == [], f"got {len(chunks)} rows")

        # --- T8 edge: weighted rerank changes ordering ---
        print("\n=== T8: weighted rerank reorders results ===")
        r_unw = PgRetriever(SLUG_A)
        r_w = PgRetriever(SLUG_A, weight_fn=lambda s: 10.0 if s == "monday_motivations.json" else 1.0)
        # Query that semantically favours Alice's tour fact; weight should
        # push the monday_motivations row to the top instead.
        q = "tour this May"
        unw = r_unw.get_relevant_chunks(q, k=3)
        wtd = r_w.get_relevant_chunks(q, k=3)
        ok("unweighted top-1 is NOT monday_motivations", "monday" not in unw[0].lower(), f"unw[0]={unw[0][:60]!r}")
        ok("weighted top-1 IS monday_motivations (boost applied)", "monday" in wtd[0].lower(), f"wtd[0]={wtd[0][:60]!r}")

        # --- T9 two retrievers coexist ---
        print("\n=== T9: Two retrievers coexist (separate connections) ===")
        ra2 = PgRetriever(SLUG_A); rb2 = PgRetriever(SLUG_B)
        a = ra2.get_relevant_chunks("parents", k=1)
        b = rb2.get_relevant_chunks("parents", k=1)
        ok("both retrievers answer", len(a) == 1 and len(b) == 1)
        ok("each returns own content", "Alice" in a[0] and "Bob" in b[0])

        # --- T3/T4 load_creator DB fallback ---
        print("\n=== T3/T4: load_creator DB fallback ===")
        from app.brain.creator_config import load_creator, _CONFIG_DIR

        # T3 — no file on disk, slug only in DB → loads from DB
        # SLUG_A has no file on disk (we just DB-inserted it)
        expected_file = os.path.join(_CONFIG_DIR, f"{SLUG_A}.json")
        ok("no file exists for alpha", not os.path.exists(expected_file))
        cfg = load_creator(SLUG_A)
        ok("load_creator returned config from DB", cfg is not None and cfg.name == "Alpha Alice",
           f"got {cfg}")

        # T4 — slug with both file AND db — file should win
        zarna_cfg = load_creator("zarna")
        ok("zarna config loads from file (not DB)", zarna_cfg is not None and zarna_cfg.name == "Zarna Garg")

        # T3b — slug with neither file nor DB → returns None
        cfg_missing = load_creator("nonexistent_slug_qxz999")
        ok("load_creator returns None when neither file nor DB row exists", cfg_missing is None)

    finally:
        print("\n=== Cleanup ===")
        cleanup(conn)
        print("  removed test rows")
        conn.close()

    print("\n" + "=" * 50)
    if failures:
        print(f"FAIL — {len(failures)} check(s) failed:")
        for f in failures: print(f"  - {f}")
        sys.exit(1)
    print("PASS — all tenant isolation + edge-case checks green")

if __name__ == "__main__":
    main()
