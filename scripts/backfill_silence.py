#!/usr/bin/env python3
"""
Nightly cron: mark bot replies that never received a fan response.

Run on Railway as a cron job (or locally):
    python scripts/backfill_silence.py

What it does:
  1. Finds every assistant message older than SILENCE_HOURS (default 24h)
     where did_user_reply IS NULL.
  2. Checks whether any user message came AFTER that bot message for the
     same phone number.
  3. If no reply ever came → sets did_user_reply=FALSE, went_silent_after=TRUE.
  4. Also backfills msgs_after_this for already-replied rows that are missing it.

Safe to re-run — all updates are idempotent (WHERE did_user_reply IS NULL).
"""

import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger(__name__)

SILENCE_HOURS = int(os.getenv("SILENCE_HOURS", "24"))


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        _logger.error("DATABASE_URL not set — nothing to do.")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def backfill_silence(conn) -> int:
    """
    Mark bot messages with no subsequent fan reply as went_silent_after=TRUE.
    Returns the count of rows updated.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages AS bot_msg
                SET did_user_reply    = FALSE,
                    went_silent_after = TRUE
                WHERE bot_msg.role           = 'assistant'
                  AND bot_msg.did_user_reply IS NULL
                  AND bot_msg.created_at     < NOW() - INTERVAL '%s hours'
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
    """
    For bot replies that were scored as replied-to (did_user_reply=TRUE) but
    are missing msgs_after_this, compute how many user messages came after.
    Returns the count of rows updated.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages AS bot_msg
                SET msgs_after_this = (
                    SELECT COUNT(*)
                    FROM messages AS fan_msg
                    WHERE fan_msg.phone_number = bot_msg.phone_number
                      AND fan_msg.role         = 'user'
                      AND fan_msg.source IS DISTINCT FROM 'reaction'
                      AND fan_msg.created_at   > bot_msg.created_at
                )
                WHERE bot_msg.role           = 'assistant'
                  AND bot_msg.did_user_reply  = TRUE
                  AND bot_msg.msgs_after_this IS NULL
                """
            )
            return cur.rowcount


def main():
    conn = _get_conn()
    try:
        silence_count = backfill_silence(conn)
        _logger.info("backfill_silence: marked %d bot replies as went_silent_after=TRUE", silence_count)

        msgs_count = backfill_msgs_after_this(conn)
        _logger.info("backfill_msgs_after_this: filled msgs_after_this for %d rows", msgs_count)
    finally:
        conn.close()

    # Close stale conversation sessions and fill came_back_within_7d
    from app.analytics.session_manager import (
        close_stale_sessions,
        backfill_came_back_within_7d,
    )
    closed = close_stale_sessions()
    _logger.info("close_stale_sessions: closed %d sessions", closed)

    came_back = backfill_came_back_within_7d()
    _logger.info("backfill_came_back_within_7d: updated %d sessions", came_back)

    _logger.info("Backfill complete.")


if __name__ == "__main__":
    main()
