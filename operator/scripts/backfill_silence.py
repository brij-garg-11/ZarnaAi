#!/usr/bin/env python3
"""
Nightly cron: mark bot replies that never received a fan response,
close stale conversation sessions, and fill came_back_within_7d.

Run on Railway as a cron job (or locally):
    python scripts/backfill_silence.py

All SQL is self-contained — no imports from the main app/ package.
Safe to re-run: all updates are idempotent.

Performance: uses composite indexes on messages(phone_number, role, created_at)
and a single-pass window for msgs_after_this (not per-row correlated counts).
"""

import logging
import os
import sys

try:
    from dotenv import load_dotenv
    _here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv()
    load_dotenv(os.path.join(_here, "..", ".env"))  # operator/.env
    load_dotenv(os.path.join(_here, "..", "..", ".env"))  # repo root
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger(__name__)

SILENCE_HOURS: int = int(os.getenv("SILENCE_HOURS", "24"))
SESSION_GAP_HOURS: int = int(os.getenv("SESSION_GAP_HOURS", "24"))


def _ensure_perf_indexes(conn) -> None:
    """Create indexes if missing (cron may run before main app migration). Idempotent."""
    stmts = [
        """
        CREATE INDEX IF NOT EXISTS idx_messages_phone_role_created
            ON messages (phone_number, role, created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sessions_phone_started
            ON conversation_sessions (phone_number, started_at)
        """,
    ]
    with conn:
        with conn.cursor() as cur:
            for sql in stmts:
                cur.execute(sql)
    _logger.info("ensure_perf_indexes: OK")


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        _logger.error("DATABASE_URL not set — nothing to do.")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def backfill_silence(conn) -> int:
    """Mark bot messages with no subsequent fan reply as went_silent_after=TRUE."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages AS bot_msg
                SET did_user_reply    = FALSE,
                    went_silent_after = TRUE
                WHERE bot_msg.role           = 'assistant'
                  AND bot_msg.did_user_reply IS NULL
                  AND bot_msg.created_at     < NOW() - make_interval(hours => %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM messages AS fan_msg
                      WHERE fan_msg.phone_number = bot_msg.phone_number
                        AND fan_msg.role         = 'user'
                        AND fan_msg.created_at   > bot_msg.created_at
                  )
                """,
                (SILENCE_HOURS,),
            )
            return cur.rowcount


def backfill_msgs_after_this(conn) -> int:
    """Fill msgs_after_this for bot replies that were replied-to but missing the count.

    One pass: window over all rows per phone (chronological), not N correlated COUNTs.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH ordered AS (
                    SELECT id,
                           COALESCE(
                               SUM(CASE WHEN role = 'user' THEN 1 ELSE 0 END) OVER (
                                   PARTITION BY phone_number
                                   ORDER BY created_at ASC, id ASC
                                   ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                               ),
                               0
                           )::int AS users_after
                    FROM messages
                )
                UPDATE messages AS m
                SET msgs_after_this = o.users_after
                FROM ordered AS o
                WHERE m.id = o.id
                  AND m.role = 'assistant'
                  AND m.did_user_reply = TRUE
                  AND m.msgs_after_this IS NULL
                """
            )
            return cur.rowcount


def close_stale_sessions(conn) -> int:
    """Close open sessions where last_active_at is older than SESSION_GAP_HOURS."""
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
                (SESSION_GAP_HOURS,),
            )
            return cur.rowcount


def backfill_came_back_within_7d(conn) -> int:
    """For closed sessions, fill came_back_within_7d based on subsequent sessions."""
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


def main():
    conn = _get_conn()
    try:
        _ensure_perf_indexes(conn)
        silence_count = backfill_silence(conn)
        _logger.info("backfill_silence: marked %d bot replies as went_silent_after=TRUE", silence_count)

        msgs_count = backfill_msgs_after_this(conn)
        _logger.info("backfill_msgs_after_this: filled msgs_after_this for %d rows", msgs_count)

        closed = close_stale_sessions(conn)
        _logger.info("close_stale_sessions: closed %d sessions", closed)

        came_back = backfill_came_back_within_7d(conn)
        _logger.info("backfill_came_back_within_7d: updated %d sessions", came_back)

        with conn:
            with conn.cursor() as cur:
                cur.execute("ANALYZE messages")
                cur.execute("ANALYZE conversation_sessions")
        _logger.info("ANALYZE messages, conversation_sessions — planner stats refreshed.")

        _logger.info("Backfill complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
