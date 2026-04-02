"""
Shared Postgres connection helpers.
All queries here are safe — no raw phone numbers returned to routes.
"""

import logging
import os
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def get_conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    dsn = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def init_db():
    """
    Run idempotent migrations for operator-specific tables.

    IMPORTANT: each statement runs with autocommit=True so a failure on one
    (e.g. dropping a constraint that doesn't exist on a fresh DB) does NOT
    roll back the others.  Previously all statements ran inside a single
    transaction, which meant the stale-URL cleanup at the end never executed
    whenever an earlier ALTER TABLE failed.
    """
    statements = [
        # ── Core tables ────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS operator_users (
            id            BIGSERIAL PRIMARY KEY,
            email         TEXT UNIQUE NOT NULL,
            name          TEXT NOT NULL DEFAULT '',
            password_hash TEXT NOT NULL,
            is_active     BOOLEAN DEFAULT TRUE,
            is_owner      BOOLEAN DEFAULT FALSE,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
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
            id                  BIGSERIAL PRIMARY KEY,
            name                TEXT NOT NULL DEFAULT 'Untitled draft',
            body                TEXT NOT NULL DEFAULT '',
            channel             TEXT NOT NULL DEFAULT 'twilio',
            audience_type       TEXT NOT NULL DEFAULT 'all',
            audience_filter     TEXT DEFAULT '',
            audience_sample_pct INT DEFAULT 100,
            status              TEXT NOT NULL DEFAULT 'draft',
            scheduled_at        TIMESTAMPTZ,
            sent_at             TIMESTAMPTZ,
            sent_count          INT DEFAULT 0,
            failed_count        INT DEFAULT 0,
            total_recipients    INT DEFAULT 0,
            created_by          TEXT DEFAULT '',
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS operator_blast_images (
            id         BIGSERIAL PRIMARY KEY,
            filename   TEXT NOT NULL,
            mime_type  TEXT NOT NULL DEFAULT 'image/jpeg',
            data       BYTEA,
            data_b64   TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,

        # ── Idempotent column additions ────────────────────────────────────
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS media_url TEXT DEFAULT ''",
        "ALTER TABLE operator_blast_images ADD COLUMN IF NOT EXISTS data_b64 TEXT",

        # Drop NOT NULL on legacy BYTEA column — only needed on old deployments
        # that created the table before we switched to data_b64.
        # Silently skipped on fresh DBs where the column is already nullable.
        "ALTER TABLE operator_blast_images ALTER COLUMN data DROP NOT NULL",

        # ── Data-cleanup on every startup ──────────────────────────────────
        # 1. Clear /tmp-based image URLs (ephemeral Railway filesystem, gone on redeploy)
        "UPDATE blast_drafts SET media_url='' WHERE media_url LIKE '%/operator/blast/uploads/%'",

        # 2. Clear any DB image URLs that reference a row with no valid data_b64
        #    (these came from failed uploads before the NOT NULL fix landed).
        #    We use a substring trick: extract the numeric ID from the URL path
        #    /operator/blast/img/<id>/<filename> and cross-check the images table.
        """
        UPDATE blast_drafts
        SET    media_url = ''
        WHERE  media_url LIKE '%/operator/blast/img/%'
          AND  NOT EXISTS (
                  SELECT 1
                  FROM   operator_blast_images oi
                  WHERE  blast_drafts.media_url LIKE '%/operator/blast/img/' || oi.id || '/%'
                    AND  oi.data_b64 IS NOT NULL
                    AND  LENGTH(oi.data_b64) > 0
               )
        """,
    ]

    conn = get_conn()
    try:
        conn.autocommit = True          # each statement is its own implicit txn
        with conn.cursor() as cur:
            for stmt in statements:
                label = stmt.strip()[:80].replace("\n", " ")
                try:
                    cur.execute(stmt)
                    rows = cur.rowcount if cur.rowcount >= 0 else 0
                    log.info("init_db OK  [rows=%d]: %s", rows, label)
                except Exception as e:
                    # Expected on fresh DBs (e.g. "column already not-null").
                    # Log as warning so it's visible in Railway deploy logs.
                    log.warning("init_db SKIP: %s — %s", label, e)
    finally:
        conn.close()
