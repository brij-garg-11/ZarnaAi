"""
Conversation session tracking.

A "session" is a contiguous window of fan ↔ bot messages.
A new session begins when a fan messages after SESSION_GAP_HOURS of silence.

This module is called from handler.py on every inbound message to:
  1. Find or create the active session for this fan.
  2. Increment the appropriate message counter.
  3. When a session ends (detected lazily or by nightly backfill), stamp ended_at.

The nightly backfill in scripts/backfill_silence.py also closes stale sessions.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

_logger = logging.getLogger(__name__)

# Gap after which a new message starts a fresh session (default 24h)
SESSION_GAP_HOURS: int = int(os.getenv("SESSION_GAP_HOURS", "24"))

# ---------------------------------------------------------------------------
# DB helpers (connect directly — SessionManager is standalone, not tied to
# PostgresStorage, to keep the call path simple and testable without the pool)
# ---------------------------------------------------------------------------

def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


# ---------------------------------------------------------------------------
# DDL — called once from ensure_session_tables()
# ---------------------------------------------------------------------------

_SESSION_DDL = (
    """
    CREATE TABLE IF NOT EXISTS conversation_sessions (
        id                  BIGSERIAL PRIMARY KEY,
        phone_number        TEXT NOT NULL,
        started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_active_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ended_at            TIMESTAMPTZ,
        user_message_count  INT  NOT NULL DEFAULT 0,
        bot_message_count   INT  NOT NULL DEFAULT 0,
        ended_by            TEXT,          -- 'user_silence' | 'user_stop' | NULL = ongoing
        came_back_within_7d BOOLEAN,       -- computed later by backfill
        first_intent        TEXT           -- intent of the first scored bot reply in this session
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sessions_phone_active
        ON conversation_sessions (phone_number, last_active_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sessions_ended
        ON conversation_sessions (ended_at, ended_by)
        WHERE ended_at IS NOT NULL
    """,
)


def ensure_session_tables() -> None:
    """Create the conversation_sessions table if it doesn't exist. Idempotent."""
    conn = _get_conn()
    if not conn:
        _logger.debug("ensure_session_tables: no DATABASE_URL, skipping")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                for sql in _SESSION_DDL:
                    cur.execute(sql)
        _logger.info("session_manager: conversation_sessions table ready")
    except Exception:
        _logger.exception("ensure_session_tables failed")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Core session logic
# ---------------------------------------------------------------------------

def get_or_create_session(phone_number: str, role: str) -> Optional[int]:
    """
    Called on every inbound message (user or assistant).

    Finds the most recent open session for this fan and checks whether the
    gap since last_active_at is within SESSION_GAP_HOURS.
    - If yes → bumps the appropriate counter and updates last_active_at.
    - If no  → closes the old session (user_silence) and opens a new one.

    Returns the session ID (int) or None if DB unavailable.
    Errors are swallowed — session tracking must never block a bot reply.
    """
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn:
            with conn.cursor() as cur:
                # Find most recent open session for this fan
                cur.execute(
                    """
                    SELECT id, last_active_at
                    FROM   conversation_sessions
                    WHERE  phone_number = %s
                      AND  ended_at IS NULL
                    ORDER  BY last_active_at DESC
                    LIMIT  1
                    """,
                    (phone_number,),
                )
                row = cur.fetchone()

                col = "user_message_count" if role == "user" else "bot_message_count"

                if row:
                    session_id, last_active = row
                    gap = datetime.now(timezone.utc) - last_active.replace(tzinfo=timezone.utc)
                    if gap <= timedelta(hours=SESSION_GAP_HOURS):
                        # Active session — bump counter + touch timestamp
                        cur.execute(
                            f"""
                            UPDATE conversation_sessions
                            SET    {col}      = {col} + 1,
                                   last_active_at = NOW()
                            WHERE  id = %s
                            """,
                            (session_id,),
                        )
                        return session_id
                    else:
                        # Gap exceeded → close old session, fall through to create new
                        cur.execute(
                            """
                            UPDATE conversation_sessions
                            SET    ended_at = last_active_at,
                                   ended_by = 'user_silence'
                            WHERE  id = %s
                            """,
                            (session_id,),
                        )

                # Create a fresh session
                user_cnt = 1 if role == "user" else 0
                bot_cnt  = 0 if role == "user" else 1
                cur.execute(
                    """
                    INSERT INTO conversation_sessions
                        (phone_number, user_message_count, bot_message_count)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (phone_number, user_cnt, bot_cnt),
                )
                return cur.fetchone()[0]
    except Exception:
        _logger.exception(
            "session_manager.get_or_create_session failed for ...%s",
            phone_number[-4:] if phone_number else "?",
        )
        return None
    finally:
        conn.close()


def close_stale_sessions(silence_hours: Optional[int] = None) -> int:
    """
    Close all open sessions where last_active_at is older than silence_hours.
    Called by the nightly backfill script.  Returns count of rows closed.
    """
    hours = silence_hours if silence_hours is not None else SESSION_GAP_HOURS
    conn = _get_conn()
    if not conn:
        return 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversation_sessions
                    SET    ended_at = last_active_at,
                           ended_by = 'user_silence'
                    WHERE  ended_at IS NULL
                      AND  last_active_at < NOW() - make_interval(hours => %s)
                    """,
                    (hours,),
                )
                return cur.rowcount
    except Exception:
        _logger.exception("close_stale_sessions failed")
        return 0
    finally:
        conn.close()


def backfill_came_back_within_7d() -> int:
    """
    For closed sessions, check if the same fan started a new session within
    7 days of the previous session ending. Fill came_back_within_7d.
    """
    conn = _get_conn()
    if not conn:
        return 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversation_sessions AS s
                    SET    came_back_within_7d = EXISTS (
                        SELECT 1 FROM conversation_sessions AS s2
                        WHERE  s2.phone_number = s.phone_number
                          AND  s2.id          != s.id
                          AND  s2.started_at  >  s.ended_at
                          AND  s2.started_at  <= s.ended_at + INTERVAL '7 days'
                    )
                    WHERE  s.ended_at IS NOT NULL
                      AND  s.came_back_within_7d IS NULL
                    """
                )
                return cur.rowcount
    except Exception:
        _logger.exception("backfill_came_back_within_7d failed")
        return 0
    finally:
        conn.close()
