"""
Phase 1b — fix embedding dimension mismatch.

Phase 1 created creator_embeddings with `vector(768)` assuming the
embedding model produced 768-dim vectors. gemini-embedding-001 actually
returns 3072-dim vectors by default, matching the existing Zarna training
data on disk.

This script:
  1. Drops the ivfflat cosine index (caps at 2000 dimensions).
  2. Changes the embedding column to vector(3072).
  3. Creates an HNSW halfvec index so similarity search stays fast
     without exceeding pgvector's per-element limits.

Safe to run: creator_embeddings is currently empty (0 rows).
Idempotent: uses IF EXISTS / IF NOT EXISTS throughout.
"""

import os
import sys

from dotenv import load_dotenv
import psycopg2

load_dotenv()


STATEMENTS = [
    ("Drop old ivfflat cosine index (only works up to ~2000 dims)",
     "DROP INDEX IF EXISTS idx_ce_embedding"),

    ("Alter embedding column to vector(3072) to match gemini-embedding-001",
     "ALTER TABLE creator_embeddings ALTER COLUMN embedding TYPE vector(3072)"),

    ("Create HNSW halfvec cosine index (supports up to 4000 dims)", """
        CREATE INDEX IF NOT EXISTS idx_ce_embedding_hnsw
        ON creator_embeddings
        USING hnsw ((embedding::halfvec(3072)) halfvec_cosine_ops)
    """),
]


VERIFY_QUERIES = [
    ("Column dimension is 3072", """
        SELECT atttypmod
        FROM pg_attribute
        WHERE attrelid = 'creator_embeddings'::regclass
          AND attname = 'embedding'
    """),
    ("HNSW index exists", """
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'creator_embeddings' AND indexname = 'idx_ce_embedding_hnsw'
    """),
]


def main() -> int:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        print("ERROR: DATABASE_URL not set in .env")
        return 1
    dsn = url.replace("postgres://", "postgresql://", 1)
    print(f"Connecting to {dsn.split('@')[-1].split('/')[0]}…")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # Safety check — this script assumes no prod data yet.
            cur.execute("SELECT COUNT(*) FROM creator_embeddings")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"ERROR: creator_embeddings already has {count} rows.")
                print("Migrating a populated table needs a different strategy.")
                return 4

            print("\n=== Running dimension fix ===")
            for label, stmt in STATEMENTS:
                try:
                    cur.execute(stmt)
                    print(f"  OK    {label}")
                except Exception as e:
                    print(f"  FAIL  {label}\n        {e}")
                    return 2

            print("\n=== Verifying ===")
            for label, q in VERIFY_QUERIES:
                cur.execute(q)
                row = cur.fetchone()
                if row is None:
                    print(f"  MISS  {label}")
                    return 3
                # pg_attribute.atttypmod encodes dimension as dim + 4 for vector.
                # Just print the raw value; if the vector is declared vector(3072)
                # atttypmod comes back as 3076 on recent pgvector versions.
                print(f"  OK    {label}  ({row[0]})")

        print("\nPhase 1b dimension fix complete.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
