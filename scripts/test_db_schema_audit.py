"""
Audit the universal-pipeline schema on whatever DB DATABASE_URL points at.

Verifies:
  - creator_configs table exists with (creator_slug, config_json, created_at, updated_at)
  - creator_embeddings table exists with (creator_slug, chunk_text, source, embedding)
  - pgvector extension enabled
  - HNSW halfvec cosine index present on creator_embeddings.embedding
  - bot_configs has provisioning_status + error_message columns
  - operator_users has phone_number column
  - Zarna rows actually migrated (creator_configs has slug=zarna, creator_embeddings has 3261 rows)
  - Embedding dimension is 3072 (prevents the 768 dim regression)

Exits 0 if everything's as expected, 1 on the first mismatch.
"""
from __future__ import annotations
import os, sys
from dotenv import load_dotenv
load_dotenv()
import psycopg2

dsn = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
assert dsn, "DATABASE_URL not set"
conn = psycopg2.connect(dsn); conn.autocommit = True

failures: list[str] = []
def check(label: str, ok: bool, detail: str = "") -> None:
    mark = "✓" if ok else "✗"
    print(f"  [{mark}] {label}" + (f"  — {detail}" if detail else ""))
    if not ok:
        failures.append(label)

with conn.cursor() as cur:
    print("=== Extensions ===")
    cur.execute("SELECT 1 FROM pg_extension WHERE extname='vector'")
    check("pgvector extension enabled", cur.fetchone() is not None)

    print("\n=== Tables ===")
    for tbl in ("creator_configs", "creator_embeddings", "bot_configs", "operator_users"):
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s", (tbl,))
        check(f"table {tbl} exists", cur.fetchone() is not None)

    print("\n=== creator_configs columns ===")
    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name='creator_configs' ORDER BY ordinal_position
    """)
    cols = {r[0]: r[1] for r in cur.fetchall()}
    for name, expect in [("creator_slug","text"),("config_json","jsonb"),("created_at","timestamp with time zone"),("updated_at","timestamp with time zone")]:
        check(f"  creator_configs.{name} ({expect})", cols.get(name) == expect, f"got {cols.get(name)}")

    print("\n=== creator_embeddings columns ===")
    cur.execute("""
        SELECT column_name, data_type, udt_name FROM information_schema.columns
        WHERE table_name='creator_embeddings' ORDER BY ordinal_position
    """)
    cols = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    check("  creator_slug (text)", cols.get("creator_slug", ("",""))[0] == "text", f"got {cols.get('creator_slug')}")
    check("  chunk_text (text)", cols.get("chunk_text", ("",""))[0] == "text", f"got {cols.get('chunk_text')}")
    check("  source (text)", cols.get("source", ("",""))[0] == "text", f"got {cols.get('source')}")
    check("  embedding (vector)", cols.get("embedding", ("",""))[1] == "vector", f"got {cols.get('embedding')}")

    print("\n=== Embedding dimension (must be 3072 not 768) ===")
    cur.execute("""
        SELECT atttypmod FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid
        WHERE c.relname='creator_embeddings' AND a.attname='embedding'
    """)
    row = cur.fetchone()
    # pgvector stores dims in atttypmod; for vector(N) it's N
    dim = int(row[0]) if row else 0
    check(f"  embedding dim = 3072", dim == 3072, f"got {dim}")

    print("\n=== Indexes on creator_embeddings ===")
    cur.execute("""
        SELECT indexname, indexdef FROM pg_indexes
        WHERE tablename='creator_embeddings'
    """)
    idx = {r[0]: r[1] for r in cur.fetchall()}
    has_slug_idx = any("creator_slug" in d for d in idx.values())
    has_hnsw = any("hnsw" in d.lower() and "halfvec" in d.lower() for d in idx.values())
    check("  slug index present", has_slug_idx, f"indexes: {list(idx.keys())}")
    check("  HNSW halfvec index present", has_hnsw)

    print("\n=== bot_configs provisioning columns ===")
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='bot_configs' AND column_name IN ('provisioning_status','error_message','status')
    """)
    bcols = {r[0] for r in cur.fetchall()}
    check("  bot_configs.provisioning_status", "provisioning_status" in bcols)
    check("  bot_configs.error_message", "error_message" in bcols)
    check("  bot_configs.status still present (not accidentally dropped)", "status" in bcols)

    print("\n=== operator_users.phone_number ===")
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='operator_users' AND column_name='phone_number'
    """)
    check("  operator_users.phone_number present", cur.fetchone() is not None)

    print("\n=== Zarna data migrated ===")
    cur.execute("SELECT COUNT(*) FROM creator_configs WHERE creator_slug='zarna'")
    n = cur.fetchone()[0]
    check(f"  creator_configs has zarna row", n == 1, f"count={n}")

    cur.execute("SELECT COUNT(*) FROM creator_embeddings WHERE creator_slug='zarna'")
    n = cur.fetchone()[0]
    check(f"  creator_embeddings has ~3261 zarna rows", n >= 3000, f"count={n}")

    cur.execute("""
        SELECT COUNT(DISTINCT source) FROM creator_embeddings WHERE creator_slug='zarna'
    """)
    n = cur.fetchone()[0]
    check(f"  creator_embeddings has multiple source labels preserved", n > 10, f"distinct sources={n}")

    cur.execute("""
        SELECT config_json->>'display_name' FROM creator_configs WHERE creator_slug='zarna'
    """)
    row = cur.fetchone()
    check(f"  creator_configs.zarna.display_name is 'Zarna Garg'", row and row[0] == "Zarna Garg", f"got {row}")

print("\n" + ("=" * 50))
if failures:
    print(f"FAIL — {len(failures)} check(s) failed")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("PASS — all schema & data checks green")
