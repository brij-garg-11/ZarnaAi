"""
PostgreSQL storage — used in production when DATABASE_URL is set.

Railway adds a Postgres database via:
  Dashboard → your project → + New → Database → Add PostgreSQL
  The DATABASE_URL environment variable is injected automatically.

Tables are created on first startup — no migration step needed.
"""

import logging
import time
from typing import List, Optional

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from .base import BaseStorage
from .models import Contact, Message

# Cache for top-performing replies — keyed by (intent, tone_mode), expires after 5 min.
# Prevents a DB round-trip on every inbound message while still picking up new winners.
_REPLY_CACHE: dict = {}
_REPLY_CACHE_TTL = 300

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS contacts (
    phone_number TEXT PRIMARY KEY,
    source       TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS messages (
    id           BIGSERIAL PRIMARY KEY,
    phone_number TEXT        NOT NULL,
    role         TEXT        NOT NULL,
    text         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS messages_phone_created
    ON messages (phone_number, created_at);
"""

# Additive migrations — safe to run on every startup (idempotent)
_MIGRATIONS = """
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_memory   TEXT    DEFAULT '';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_tags     TEXT[]  DEFAULT '{}';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_location TEXT    DEFAULT '';
"""

# One statement per execute — psycopg2 limitation.
_LIVE_SHOW_MIGRATIONS = (
    """
    CREATE TABLE IF NOT EXISTS live_shows (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        keyword TEXT NOT NULL DEFAULT '',
        use_keyword_only BOOLEAN NOT NULL DEFAULT TRUE,
        window_start TIMESTAMPTZ,
        window_end TIMESTAMPTZ,
        deliver_as TEXT NOT NULL DEFAULT 'sms',
        status TEXT NOT NULL DEFAULT 'draft',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS live_show_signups (
        show_id INT NOT NULL REFERENCES live_shows(id) ON DELETE CASCADE,
        phone_number TEXT NOT NULL,
        channel TEXT NOT NULL DEFAULT '',
        signed_up_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (show_id, phone_number)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_live_show_signups_show
        ON live_show_signups(show_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS live_broadcast_jobs (
        id SERIAL PRIMARY KEY,
        show_id INT NOT NULL REFERENCES live_shows(id) ON DELETE CASCADE,
        body TEXT NOT NULL,
        provider TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        total_recipients INT DEFAULT 0,
        sent_count INT DEFAULT 0,
        failed_count INT DEFAULT 0,
        last_error TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_live_broadcast_jobs_show
        ON live_broadcast_jobs(show_id)
    """,
)

# Engagement analytics — idempotent column additions on messages.
_ENGAGEMENT_ANALYTICS_MIGRATIONS = (
    # Context columns written at reply generation time
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS intent              TEXT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS tone_mode           TEXT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS routing_tier        TEXT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_length_chars  INT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS has_link            BOOLEAN DEFAULT FALSE",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS conversation_turn   INT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS gen_ms              FLOAT",
    # Outcome columns backfilled asynchronously
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS did_user_reply      BOOLEAN",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_delay_seconds INT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS went_silent_after   BOOLEAN",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS link_clicked_1h     BOOLEAN",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS msgs_after_this     INT",
    # Index for fast analytics queries (filter to scored assistant rows)
    """
    CREATE INDEX IF NOT EXISTS idx_messages_analytics
        ON messages (role, intent, tone_mode, routing_tier)
        WHERE role = 'assistant' AND did_user_reply IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_messages_phone_role_created
        ON messages (phone_number, role, created_at)
    """,
)

# Quiz tables — created by main app so the inbound webhook can read them.
_QUIZ_MIGRATIONS = (
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
)

# Idempotent alters + audit log (runs after base live_shows DDL).
_LIVE_SHOW_ADDITIVE_MIGRATIONS = (
    """
    ALTER TABLE live_shows ADD COLUMN IF NOT EXISTS event_category TEXT NOT NULL DEFAULT 'other';
    """,
    """
    CREATE TABLE IF NOT EXISTS admin_audit_log (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        action TEXT NOT NULL,
        detail TEXT NOT NULL DEFAULT '',
        show_id INT REFERENCES live_shows(id) ON DELETE SET NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_admin_audit_show ON admin_audit_log(show_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_admin_audit_created ON admin_audit_log(created_at DESC)
    """,
    """
    ALTER TABLE live_shows ADD COLUMN IF NOT EXISTS event_timezone TEXT
    """,
)


class PostgresStorage(BaseStorage):
    """Thread-safe Postgres storage using a connection pool."""

    def __init__(self, dsn: str, minconn: int = 2, maxconn: int = 50):
        self._pool = ThreadedConnectionPool(minconn, maxconn, dsn)
        self._ensure_tables()
        logger.info("PostgresStorage initialised")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _acquire(self):
        return self._pool.getconn()

    def _release(self, conn):
        self._pool.putconn(conn)

    def _ensure_tables(self):
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    # Serialize migrations across gunicorn workers. Without this,
                    # multiple workers starting simultaneously all run ALTER TABLE
                    # on the same relations and deadlock each other.
                    # pg_advisory_xact_lock holds until the transaction commits/rolls back.
                    cur.execute("SELECT pg_advisory_xact_lock(1672394823)")
                    cur.execute(_DDL)
                    cur.execute(_MIGRATIONS)
                    for sql in _LIVE_SHOW_MIGRATIONS:
                        cur.execute(sql)
                    for sql in _LIVE_SHOW_ADDITIVE_MIGRATIONS:
                        cur.execute(sql)
                    for sql in _ENGAGEMENT_ANALYTICS_MIGRATIONS:
                        cur.execute(sql)
                    for sql in _QUIZ_MIGRATIONS:
                        cur.execute(sql)
                # conversation_sessions lives in session_manager — ensure it exists here too
                try:
                    from app.analytics.session_manager import ensure_session_tables
                    ensure_session_tables()
                except Exception:
                    pass
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.DeadlockDetected):
            # Another worker won the race and already ran the migrations.
            # All DDL is idempotent (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS)
            # so it is safe to continue — the tables exist.
            conn.rollback()
        finally:
            self._release(conn)

    # ------------------------------------------------------------------
    # BaseStorage interface
    # ------------------------------------------------------------------

    def save_contact(self, phone_number: str, source: Optional[str] = None) -> Contact:
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO contacts (phone_number, source)
                        VALUES (%s, %s)
                        ON CONFLICT (phone_number) DO NOTHING
                        """,
                        (phone_number, source),
                    )
        finally:
            self._release(conn)
        return Contact(phone_number=phone_number, source=source)

    def get_contact(self, phone_number: str) -> Optional[Contact]:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT phone_number, source FROM contacts WHERE phone_number = %s",
                    (phone_number,),
                )
                row = cur.fetchone()
                if row:
                    return Contact(phone_number=row["phone_number"], source=row["source"])
        finally:
            self._release(conn)
        return None

    def save_message(self, phone_number: str, role: str, text: str) -> Message:
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO messages (phone_number, role, text) VALUES (%s, %s, %s) RETURNING id",
                        (phone_number, role, text),
                    )
                    row_id = cur.fetchone()[0]
        finally:
            self._release(conn)
        return Message(phone_number=phone_number, role=role, text=text, id=row_id)

    def get_conversation_history(self, phone_number: str, limit: int = 10) -> List[Message]:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT role, text FROM (
                        SELECT role, text, created_at
                        FROM messages
                        WHERE phone_number = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    ) sub
                    ORDER BY created_at ASC
                    """,
                    (phone_number, limit),
                )
                return [
                    Message(phone_number=phone_number, role=r["role"], text=r["text"])
                    for r in cur.fetchall()
                ]
        finally:
            self._release(conn)
        return []

    def is_first_message(self, phone_number: str) -> bool:
        """Efficient single-row check — no need to fetch the full history."""
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM messages WHERE phone_number = %s LIMIT 1",
                    (phone_number,),
                )
                return cur.fetchone() is None
        finally:
            self._release(conn)

    def get_memory(self, phone_number: str) -> str:
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fan_memory FROM contacts WHERE phone_number = %s",
                    (phone_number,),
                )
                row = cur.fetchone()
                return (row[0] or "") if row else ""
        finally:
            self._release(conn)

    def update_memory(self, phone_number: str, memory: str, tags: list, location: str = "") -> None:
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE contacts
                        SET fan_memory = %s, fan_tags = %s, fan_location = COALESCE(NULLIF(%s, ''), fan_location)
                        WHERE phone_number = %s
                        """,
                        (memory[:400], tags, location[:100], phone_number),
                    )
        finally:
            self._release(conn)

    def get_fans_by_tag(self, tag: str) -> list:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                    """,
                    (tag.lower(),),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)

    def get_fans_by_location(self, location: str) -> list:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                    """,
                    (f"%{location.lower()}%",),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)

    # ------------------------------------------------------------------
    # Engagement analytics
    # ------------------------------------------------------------------

    def save_reply_context(
        self,
        message_id,
        intent=None,
        tone_mode=None,
        routing_tier=None,
        reply_length_chars=None,
        has_link=False,
        conversation_turn=None,
        gen_ms=None,
    ) -> None:
        """Write bot-reply context columns onto an existing message row."""
        if message_id is None:
            return
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE messages
                        SET intent             = %s,
                            tone_mode          = %s,
                            routing_tier       = %s,
                            reply_length_chars = %s,
                            has_link           = %s,
                            conversation_turn  = %s,
                            gen_ms             = %s
                        WHERE id = %s
                        """,
                        (
                            intent,
                            tone_mode,
                            routing_tier,
                            reply_length_chars,
                            has_link,
                            conversation_turn,
                            gen_ms,
                            message_id,
                        ),
                    )
        except Exception:
            logger.exception("save_reply_context failed for message_id=%s", message_id)
        finally:
            self._release(conn)

    def score_previous_bot_reply(self, phone_number: str) -> None:
        """
        Called when a new user message arrives.  Finds the most recent
        unscored assistant message for this fan and marks it as replied-to,
        recording the reply delay in seconds.
        """
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE messages
                        SET did_user_reply      = TRUE,
                            reply_delay_seconds = GREATEST(
                                0,
                                EXTRACT(EPOCH FROM (NOW() - created_at))::INT
                            )
                        WHERE id = (
                            SELECT id FROM messages
                            WHERE phone_number   = %s
                              AND role           = 'assistant'
                              AND did_user_reply IS NULL
                            ORDER BY created_at DESC
                            LIMIT 1
                        )
                        """,
                        (phone_number,),
                    )
        except Exception:
            logger.exception("score_previous_bot_reply failed for ...%s", phone_number[-4:] if phone_number else "?")
        finally:
            self._release(conn)

    def get_top_performing_replies(
        self,
        intent: str,
        tone_mode: str,
        limit: int = 4,
    ) -> list:
        """
        Return up to `limit` bot reply texts that performed best for this
        intent + tone_mode combo, ordered by follow-up depth then reply speed.
        Results are cached for 5 minutes so this never adds per-message latency.
        Requires at least 3 qualifying examples — returns [] otherwise.
        """
        cache_key = (intent, tone_mode)
        now = time.monotonic()
        cached, ts = _REPLY_CACHE.get(cache_key, (None, 0))
        if cached is not None and now - ts < _REPLY_CACHE_TTL:
            return cached

        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT text
                    FROM   messages
                    WHERE  role               = 'assistant'
                      AND  intent             = %s
                      AND  tone_mode          = %s
                      AND  did_user_reply     = TRUE
                      AND  reply_length_chars BETWEEN 40 AND 380
                      AND  source IS DISTINCT FROM 'blast'
                      AND  text NOT LIKE '%%zarnagarg.com%%'
                      AND  text NOT LIKE '%%amazon.com%%'
                      AND  text NOT LIKE '%%youtube.com%%'
                    ORDER BY COALESCE(msgs_after_this, 1) DESC,
                             reply_delay_seconds ASC NULLS LAST
                    LIMIT %s
                    """,
                    (intent, tone_mode, limit),
                )
                rows = [r[0] for r in cur.fetchall()]
            result = rows if len(rows) >= 3 else []
            _REPLY_CACHE[cache_key] = (result, now)
            return result
        except Exception:
            logger.exception("get_top_performing_replies failed intent=%s tone=%s", intent, tone_mode)
            return []
        finally:
            self._release(conn)
