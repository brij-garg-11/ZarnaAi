#!/usr/bin/env python3
"""
Weekly cron: automatically pick the Fan of the Week.

Runs every Monday at 3:05 AM UTC (5 minutes after fan scoring finishes)
so tiers and scores are fresh when we rank candidates.

Logic:
  - Ranks all fans by a composite score (fan_score, tier, reply activity,
    comeback sessions, fan memory depth)
  - Excludes anyone picked in the last 8 weeks
  - Saves the top candidate for the current week (idempotent — skips if
    this week already has a pick)
  - Tags the winner with 'fan_of_the_week' in contacts.fan_tags

Run on Railway as a weekly cron, or locally:
    python operator/scripts/pick_fan_of_the_week.py

All SQL is self-contained. Safe to re-run: skips if this week is already picked.
"""

import logging
import os
import sys
import time

try:
    from dotenv import load_dotenv
    _here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv()
    load_dotenv(os.path.join(_here, "..", ".env"))
    load_dotenv(os.path.join(_here, "..", "..", ".env"))
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("pick_fan_of_the_week")


def get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fan_of_the_week (
                id           BIGSERIAL PRIMARY KEY,
                phone_number TEXT        NOT NULL,
                week_of      DATE        NOT NULL,
                message_text TEXT        DEFAULT '',
                selected_at  TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (week_of)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fotw_week  ON fan_of_the_week (week_of DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fotw_phone ON fan_of_the_week (phone_number)")
    conn.commit()
    logger.info("ensure_table: fan_of_the_week ready")


def already_picked_this_week(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM fan_of_the_week
            WHERE week_of = DATE_TRUNC('week', CURRENT_DATE)::date
            LIMIT 1
        """)
        return cur.fetchone() is not None


def pick_top_candidate(conn) -> dict | None:
    """
    Returns the best fan candidate as a dict, or None if no one qualifies.
    Tries 7-day window first, then widens to 30 and 90 days.
    """
    sql = """
        WITH
        recent_replies AS (
            SELECT phone_number, COUNT(*) AS reply_count
            FROM   messages
            WHERE  role = 'user'
              AND  created_at >= NOW() - INTERVAL '7 days'
              AND  did_user_reply = true
            GROUP  BY phone_number
        ),
        came_back AS (
            SELECT phone_number, COUNT(*) > 0 AS did_come_back
            FROM   conversation_sessions
            WHERE  came_back_within_7d = true
              AND  created_at >= NOW() - INTERVAL '7 days'
            GROUP  BY phone_number
        ),
        best_msg AS (
            SELECT DISTINCT ON (m.phone_number)
                m.phone_number,
                m.text    AS message_text,
                m.created_at AS msg_at
            FROM messages m
            WHERE m.role = 'user'
              AND m.created_at >= NOW() - INTERVAL '1 day' * %s
              AND LENGTH(m.text) BETWEEN 50 AND 400
              AND m.text NOT ILIKE 'stop%%'
              AND m.text NOT ILIKE 'yes%%'
              AND m.text NOT ILIKE 'no%%'
              AND m.text NOT ILIKE 'ok%%'
              AND (m.intent IS NULL OR m.intent NOT IN ('STOP', 'OPTOUT'))
            ORDER BY m.phone_number, LENGTH(m.text) DESC
        )
        SELECT
            bm.phone_number,
            bm.message_text,
            c.fan_tier,
            COALESCE(c.fan_score, 0) AS fan_score,
            (
                COALESCE(c.fan_score, 0) * 0.4
              + CASE c.fan_tier
                    WHEN 'superfan' THEN 30
                    WHEN 'engaged'  THEN 15
                    ELSE 0
                END
              + LEAST(COALESCE(rr.reply_count, 0) * 5, 25)
              + CASE WHEN COALESCE(cb.did_come_back, false) THEN 20 ELSE 0 END
              + CASE WHEN c.fan_memory IS NOT NULL AND c.fan_memory != '' THEN 10 ELSE 0 END
            ) AS candidate_score
        FROM best_msg bm
        LEFT JOIN contacts           c  ON c.phone_number  = bm.phone_number
        LEFT JOIN recent_replies     rr ON rr.phone_number = bm.phone_number
        LEFT JOIN came_back          cb ON cb.phone_number = bm.phone_number
        WHERE bm.phone_number NOT IN (
            SELECT phone_number FROM fan_of_the_week
            WHERE week_of >= CURRENT_DATE - INTERVAL '8 weeks'
        )
        ORDER BY candidate_score DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        for days_back in (7, 30, 90):
            cur.execute(sql, (days_back,))
            row = cur.fetchone()
            if row:
                return {
                    "phone_number": row[0],
                    "message_text": row[1] or "",
                    "fan_tier": row[2],
                    "fan_score": row[3],
                    "candidate_score": float(row[4]),
                    "days_back": days_back,
                }
    return None


def save_pick(conn, phone_number: str, message_text: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fan_of_the_week (phone_number, week_of, message_text)
            VALUES (%s, DATE_TRUNC('week', CURRENT_DATE)::date, %s)
            ON CONFLICT (week_of) DO NOTHING
        """, (phone_number, message_text))

        cur.execute("""
            UPDATE contacts
            SET fan_tags = array_append(
                COALESCE(fan_tags, '{}'),
                'fan_of_the_week'
            )
            WHERE phone_number = %s
              AND NOT ('fan_of_the_week' = ANY(COALESCE(fan_tags, '{}')))
        """, (phone_number,))
    conn.commit()


def main():
    t0 = time.time()
    logger.info("=== pick_fan_of_the_week starting ===")

    conn = get_conn()
    try:
        ensure_table(conn)

        if already_picked_this_week(conn):
            logger.info("This week already has a Fan of the Week — skipping.")
            return

        candidate = pick_top_candidate(conn)
        if not candidate:
            logger.warning("No qualifying candidates found — no pick made this week.")
            return

        save_pick(conn, candidate["phone_number"], candidate["message_text"])

        logger.info(
            "Picked ***%s as Fan of the Week (tier=%s score=%.0f days_back=%d) in %.1fs",
            candidate["phone_number"][-4:],
            candidate["fan_tier"],
            candidate["candidate_score"],
            candidate["days_back"],
            time.time() - t0,
        )
    finally:
        conn.close()

    logger.info("=== pick_fan_of_the_week done in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
