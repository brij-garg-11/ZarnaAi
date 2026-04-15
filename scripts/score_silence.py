#!/usr/bin/env python3
"""
Dedicated silence-scoring cron — Pillar 1, Step 2.

Runs independently of other backfill logic so it can be scheduled,
monitored, and tuned without touching the session or fan-scoring crons.

What it does:
  1. score_silence        — marks assistant messages older than SILENCE_HOURS
                            where no fan reply ever came as:
                              did_user_reply    = FALSE
                              went_silent_after = TRUE
  2. score_msgs_after     — for already-replied messages missing msgs_after_this,
                            counts how many user messages came after them.

Both are idempotent — safe to re-run at any time.

Railway cron: operator/railway.score_silence.toml
Local:        python scripts/score_silence.py [--dry-run]
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SILENCE_HOURS = int(os.getenv("SILENCE_HOURS", "24"))


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        log.error("DATABASE_URL not set")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def score_silence(conn, dry_run: bool = False) -> int:
    """
    Find assistant messages older than SILENCE_HOURS with no subsequent fan
    reply and mark them went_silent_after=TRUE, did_user_reply=FALSE.
    Returns the number of rows updated (0 in dry-run mode).
    """
    sql = """
        UPDATE messages AS bot_msg
        SET did_user_reply    = FALSE,
            went_silent_after = TRUE
        WHERE bot_msg.role           = 'assistant'
          AND bot_msg.did_user_reply IS NULL
          AND bot_msg.msg_source IS DISTINCT FROM 'blast'
          AND bot_msg.created_at     < NOW() - INTERVAL '%s hours'
          AND NOT EXISTS (
              SELECT 1 FROM messages AS fan_msg
              WHERE fan_msg.phone_number = bot_msg.phone_number
                AND fan_msg.role         = 'user'
                AND fan_msg.created_at   > bot_msg.created_at
          )
    """
    if dry_run:
        # Run as SELECT COUNT so we can report without mutating
        count_sql = """
            SELECT COUNT(*) FROM messages AS bot_msg
            WHERE bot_msg.role           = 'assistant'
              AND bot_msg.did_user_reply IS NULL
              AND bot_msg.msg_source IS DISTINCT FROM 'blast'
              AND bot_msg.created_at     < NOW() - INTERVAL '%s hours'
              AND NOT EXISTS (
                  SELECT 1 FROM messages AS fan_msg
                  WHERE fan_msg.phone_number = bot_msg.phone_number
                    AND fan_msg.role         = 'user'
                    AND fan_msg.created_at   > bot_msg.created_at
              )
        """
        with conn.cursor() as cur:
            cur.execute(count_sql, (SILENCE_HOURS,))
            return cur.fetchone()[0]

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (SILENCE_HOURS,))
            return cur.rowcount


def score_msgs_after(conn, dry_run: bool = False) -> int:
    """
    For replied-to assistant messages missing msgs_after_this, count how many
    non-reaction user messages followed.  Returns the number of rows updated.
    """
    sql = """
        UPDATE messages AS bot_msg
        SET msgs_after_this = (
            SELECT COUNT(*)
            FROM   messages AS fan_msg
            WHERE  fan_msg.phone_number = bot_msg.phone_number
              AND  fan_msg.role         = 'user'
              AND  fan_msg.source IS DISTINCT FROM 'reaction'
              AND  fan_msg.created_at   > bot_msg.created_at
        )
        WHERE bot_msg.role           = 'assistant'
          AND bot_msg.did_user_reply  = TRUE
          AND bot_msg.msgs_after_this IS NULL
    """
    if dry_run:
        count_sql = """
            SELECT COUNT(*) FROM messages
            WHERE role = 'assistant' AND did_user_reply = TRUE AND msgs_after_this IS NULL
        """
        with conn.cursor() as cur:
            cur.execute(count_sql)
            return cur.fetchone()[0]

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount


def main():
    parser = argparse.ArgumentParser(description="Score silence on bot messages")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would be updated without writing to the DB",
    )
    args = parser.parse_args()

    conn = _get_conn()
    try:
        silence_count = score_silence(conn, dry_run=args.dry_run)
        msgs_count = score_msgs_after(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    prefix = "[DRY RUN] would update" if args.dry_run else "updated"
    log.info("score_silence:     %s %d rows → went_silent_after=TRUE", prefix, silence_count)
    log.info("score_msgs_after:  %s %d rows → msgs_after_this filled", prefix, msgs_count)
    log.info("score_silence cron complete.")


if __name__ == "__main__":
    main()
