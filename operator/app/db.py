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
        # link_url: raw URL entered by operator; tracked_link_slug: the /t/<slug> we created
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS link_url TEXT DEFAULT ''",
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS tracked_link_slug TEXT DEFAULT ''",
        # tracked_link_clicks: shared with main app — ensure it exists here too
        # so the operator's /t/<slug> redirect can log clicks independently
        """
        CREATE TABLE IF NOT EXISTS tracked_link_clicks (
            id         BIGSERIAL PRIMARY KEY,
            link_id    BIGINT NOT NULL REFERENCES tracked_links(id) ON DELETE CASCADE,
            clicked_at TIMESTAMPTZ DEFAULT NOW(),
            ip_hash    TEXT DEFAULT '',
            ua_short   TEXT DEFAULT ''
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_tlc_link_id ON tracked_link_clicks(link_id)",
        "CREATE INDEX IF NOT EXISTS idx_tlc_clicked_at ON tracked_link_clicks(clicked_at)",
        # sent_to on tracked_links: cumulative recipients across all blasts using this link
        "ALTER TABLE tracked_links ADD COLUMN IF NOT EXISTS sent_to INT DEFAULT 0",
        "ALTER TABLE operator_blast_images ADD COLUMN IF NOT EXISTS data_b64 TEXT",

        # Drop NOT NULL on legacy BYTEA column — only needed on old deployments
        # that created the table before we switched to data_b64.
        # Silently skipped on fresh DBs where the column is already nullable.
        "ALTER TABLE operator_blast_images ALTER COLUMN data DROP NOT NULL",

        # ── Quiz tables (shared with main app via same DATABASE_URL) ──────────
        """
        CREATE TABLE IF NOT EXISTS quiz_sessions (
            id             SERIAL PRIMARY KEY,
            show_id        INT,
            blast_draft_id BIGINT,
            question_text  TEXT NOT NULL,
            correct_answer TEXT NOT NULL,
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            expires_at     TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS quiz_responses (
            id           BIGSERIAL PRIMARY KEY,
            quiz_id      INT  NOT NULL REFERENCES quiz_sessions(id) ON DELETE CASCADE,
            phone_number TEXT NOT NULL,
            fan_answer   TEXT NOT NULL DEFAULT '',
            answered_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (quiz_id, phone_number)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_quiz_sessions_active ON quiz_sessions (expires_at, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_quiz_responses_lookup ON quiz_responses (quiz_id, phone_number)",

        # ── Quiz columns on blast_drafts ───────────────────────────────────
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS is_quiz BOOLEAN DEFAULT FALSE",
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS quiz_correct_answer TEXT DEFAULT ''",

        # ── Blast context columns + session table ─────────────────────────
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS blast_context_note TEXT DEFAULT ''",
        """
        CREATE TABLE IF NOT EXISTS blast_context_sessions (
            id             BIGSERIAL PRIMARY KEY,
            blast_draft_id BIGINT,
            context_note   TEXT NOT NULL,
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            expires_at     TIMESTAMPTZ
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_blast_context_sessions_active ON blast_context_sessions (expires_at, created_at)",

        # ── Analytics columns on blast_drafts ──────────────────────────────
        # started_at: recorded before the send loop begins so reply attribution
        # uses the blast START time, not the end time (sent_at).
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS opt_out_count INT DEFAULT 0",
        # manual_link_clicks: for external/SlickText blasts where we can't count from our DB
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS manual_link_clicks INT DEFAULT NULL",
        # blast_category: 'friendly' | 'sales' | 'show' — used to split Blast Performance table
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS blast_category TEXT DEFAULT NULL",

        # ── blast_recipients: per-fan record of every blast send ────────────
        # Foundation for Smart Blast frequency logic and per-fan tier cadence.
        """
        CREATE TABLE IF NOT EXISTS blast_recipients (
            id           BIGSERIAL PRIMARY KEY,
            blast_id     BIGINT NOT NULL REFERENCES blast_drafts(id) ON DELETE CASCADE,
            phone_number TEXT NOT NULL,
            sent_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (blast_id, phone_number)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_blast_recipients_phone ON blast_recipients (phone_number, sent_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_blast_recipients_blast ON blast_recipients (blast_id)",

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
