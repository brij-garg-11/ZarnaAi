"""
Shared Postgres connection helpers.
All queries here are safe — no raw phone numbers returned to routes.
"""

import os
import psycopg2
import psycopg2.extras


def get_conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    dsn = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def init_db():
    """Run idempotent migrations for operator-specific tables."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS operator_users (
            id          BIGSERIAL PRIMARY KEY,
            email       TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL DEFAULT '',
            password_hash TEXT NOT NULL,
            is_active   BOOLEAN DEFAULT TRUE,
            is_owner    BOOLEAN DEFAULT FALSE,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            last_login_at TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS broadcast_optouts (
            phone_number TEXT PRIMARY KEY,
            opted_out_at TIMESTAMPTZ DEFAULT NOW(),
            source       TEXT DEFAULT 'stop_reply'
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS blast_drafts (
            id                BIGSERIAL PRIMARY KEY,
            name              TEXT NOT NULL DEFAULT 'Untitled draft',
            body              TEXT NOT NULL DEFAULT '',
            channel           TEXT NOT NULL DEFAULT 'twilio',
            audience_type     TEXT NOT NULL DEFAULT 'all',
            audience_filter   TEXT DEFAULT '',
            audience_sample_pct INT DEFAULT 100,
            status            TEXT NOT NULL DEFAULT 'draft',
            scheduled_at      TIMESTAMPTZ,
            sent_at           TIMESTAMPTZ,
            sent_count        INT DEFAULT 0,
            failed_count      INT DEFAULT 0,
            total_recipients  INT DEFAULT 0,
            created_by        TEXT DEFAULT '',
            created_at        TIMESTAMPTZ DEFAULT NOW(),
            updated_at        TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        # Idempotent column additions for existing tables
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS media_url TEXT DEFAULT ''",
    ]
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                for stmt in ddl_statements:
                    cur.execute(stmt)
        conn.close()
    except Exception as e:
        import logging
        logging.warning("operator init_db error: %s", e)
