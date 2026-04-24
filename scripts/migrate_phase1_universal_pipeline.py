"""
Phase 1 migration for the Universal Bot Pipeline.

Runs against the Railway Postgres pointed to by DATABASE_URL in .env.

Idempotent: safe to re-run. Every statement uses IF NOT EXISTS.

Steps:
  1. Enable pgvector extension
  2. Create creator_configs table (+ index)
  3. Create creator_embeddings table (+ slug index + ivfflat embedding index)
  4. Add error_message + provisioning_status columns to bot_configs
  5. Verify all objects exist

Usage:
  python scripts/migrate_phase1_universal_pipeline.py
"""

import os
import sys

from dotenv import load_dotenv
import psycopg2

load_dotenv()


STATEMENTS = [
    ("Enable pgvector extension",
     "CREATE EXTENSION IF NOT EXISTS vector"),

    ("Create creator_configs table", """
        CREATE TABLE IF NOT EXISTS creator_configs (
            id           BIGSERIAL PRIMARY KEY,
            creator_slug TEXT UNIQUE NOT NULL,
            config_json  JSONB NOT NULL DEFAULT '{}',
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    ("Index creator_configs(creator_slug)",
     "CREATE INDEX IF NOT EXISTS idx_cc_slug ON creator_configs(creator_slug)"),

    ("Create creator_embeddings table", """
        CREATE TABLE IF NOT EXISTS creator_embeddings (
            id           BIGSERIAL PRIMARY KEY,
            creator_slug TEXT NOT NULL,
            chunk_text   TEXT NOT NULL,
            source       TEXT NOT NULL DEFAULT 'general',
            embedding    vector(768),
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    ("Index creator_embeddings(creator_slug)",
     "CREATE INDEX IF NOT EXISTS idx_ce_slug ON creator_embeddings(creator_slug)"),
    ("IVFFLAT cosine index on creator_embeddings.embedding", """
        CREATE INDEX IF NOT EXISTS idx_ce_embedding
        ON creator_embeddings
        USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = 100)
    """),

    ("Add bot_configs.error_message",
     "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS error_message TEXT"),
    ("Add bot_configs.provisioning_status",
     "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS provisioning_status TEXT DEFAULT NULL"),
]


VERIFY_QUERIES = [
    ("pgvector installed",
     "SELECT 1 FROM pg_extension WHERE extname = 'vector'"),
    ("creator_configs exists",
     "SELECT 1 FROM information_schema.tables WHERE table_name = 'creator_configs'"),
    ("creator_embeddings exists",
     "SELECT 1 FROM information_schema.tables WHERE table_name = 'creator_embeddings'"),
    ("bot_configs.error_message exists",
     "SELECT 1 FROM information_schema.columns WHERE table_name='bot_configs' AND column_name='error_message'"),
    ("bot_configs.provisioning_status exists",
     "SELECT 1 FROM information_schema.columns WHERE table_name='bot_configs' AND column_name='provisioning_status'"),
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
            print("\n=== Running migration statements ===")
            for label, stmt in STATEMENTS:
                try:
                    cur.execute(stmt)
                    print(f"  OK    {label}")
                except Exception as e:
                    print(f"  FAIL  {label}\n        {e}")
                    return 2

            print("\n=== Verifying objects exist ===")
            all_ok = True
            for label, query in VERIFY_QUERIES:
                cur.execute(query)
                row = cur.fetchone()
                if row:
                    print(f"  OK    {label}")
                else:
                    print(f"  MISS  {label}")
                    all_ok = False

            if not all_ok:
                print("\nSome objects failed verification.")
                return 3

            cur.execute("SELECT COUNT(*) FROM creator_configs")
            cc_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM creator_embeddings")
            ce_count = cur.fetchone()[0]
            print(f"\ncreator_configs rows:    {cc_count}")
            print(f"creator_embeddings rows: {ce_count}")

        print("\nPhase 1 migration complete.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
