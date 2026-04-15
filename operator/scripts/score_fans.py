#!/usr/bin/env python3
"""
Nightly cron: compute a fan engagement score and assign a tier to every contact.

Scoring signals (all sourced from existing DB tables):
  +25  signed up for a live show        (live_show_signups)
  +15  per bot message the fan replied  (messages.did_user_reply = true)
  +20  session where fan came back      (conversation_sessions.came_back_within_7d = true)
  +10  link clicked within 1h           (messages.link_clicked_1h = true)
  + 5  fast reply (<60s)                (messages.reply_delay_seconds < 60, non-null)
  -10  per silent bot message           (messages.went_silent_after = true)
  -20  no activity in 60–89 days        (decay)
  -40  no activity 90+ days             (decay)

Tiers assigned from final score:
  superfan  : score >= 60
  engaged   : score 25–59
  lurker    : score 10–24
  dormant   : score < 10

The tier and score are stored as ADD COLUMN IF NOT EXISTS on contacts:
  fan_score  INT
  fan_tier   TEXT

Run on Railway as a nightly cron, or locally:
    python scripts/score_fans.py

All SQL is self-contained — no imports from the main app/ package.
Safe to re-run: all updates are idempotent (upsert via UPDATE).
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
logger = logging.getLogger("score_fans")


def get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url)


def ensure_columns(conn):
    """Add fan_score and fan_tier columns to contacts if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_score INT DEFAULT 0")
        cur.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_tier  TEXT DEFAULT 'engaged'")
    conn.commit()
    logger.info("ensure_columns: fan_score and fan_tier ready on contacts")


def score_all_fans(conn) -> int:
    """
    Compute score + tier for every contact and write back to the DB.
    Returns the number of contacts updated.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH

            -- Signal: live show sign-ups (+25 each, capped at 2 shows = +50 max)
            show_points AS (
                SELECT phone_number,
                       LEAST(COUNT(*) * 25, 50) AS pts
                FROM   live_show_signups
                GROUP  BY phone_number
            ),

            -- Signal: bot messages the fan replied to (+15 each)
            reply_points AS (
                SELECT phone_number,
                       COUNT(*) * 15 AS pts
                FROM   messages
                WHERE  role = 'assistant'
                  AND  did_user_reply = true
                GROUP  BY phone_number
            ),

            -- Signal: sessions where fan came back within 7 days (+20 each)
            comeback_points AS (
                SELECT phone_number,
                       COUNT(*) * 20 AS pts
                FROM   conversation_sessions
                WHERE  came_back_within_7d = true
                GROUP  BY phone_number
            ),

            -- Signal: link clicked within 1h (+10 each)
            link_points AS (
                SELECT phone_number,
                       COUNT(*) * 10 AS pts
                FROM   messages
                WHERE  role = 'assistant'
                  AND  link_clicked_1h = true
                GROUP  BY phone_number
            ),

            -- Signal: fast replies < 60s (+5 each, max 3 = +15)
            fast_reply_points AS (
                SELECT phone_number,
                       LEAST(COUNT(*) * 5, 15) AS pts
                FROM   messages
                WHERE  role = 'assistant'
                  AND  reply_delay_seconds IS NOT NULL
                  AND  reply_delay_seconds < 60
                GROUP  BY phone_number
            ),

            -- Signal: went silent after bot message (-10 each)
            silence_penalty AS (
                SELECT phone_number,
                       COUNT(*) * 10 AS pts
                FROM   messages
                WHERE  role = 'assistant'
                  AND  went_silent_after = true
                GROUP  BY phone_number
            ),

            -- Signal: recency decay (based on most recent message from this fan)
            last_activity AS (
                SELECT phone_number,
                       MAX(created_at) AS last_msg
                FROM   messages
                WHERE  role = 'user'
                GROUP  BY phone_number
            ),

            decay AS (
                SELECT phone_number,
                       CASE
                           WHEN last_msg >= NOW() - INTERVAL '60 days' THEN 0
                           WHEN last_msg >= NOW() - INTERVAL '90 days' THEN 20
                           ELSE 40
                       END AS pts
                FROM   last_activity
            ),

            -- Combine all signals per contact
            raw_scores AS (
                SELECT
                    c.phone_number,
                    COALESCE(sp.pts, 0)
                  + COALESCE(rp.pts, 0)
                  + COALESCE(cb.pts, 0)
                  + COALESCE(lp.pts, 0)
                  + COALESCE(frp.pts, 0)
                  - COALESCE(sil.pts, 0)
                  - COALESCE(d.pts, 0)                 AS raw_score
                FROM   contacts c
                LEFT   JOIN show_points       sp  ON sp.phone_number  = c.phone_number
                LEFT   JOIN reply_points      rp  ON rp.phone_number  = c.phone_number
                LEFT   JOIN comeback_points   cb  ON cb.phone_number  = c.phone_number
                LEFT   JOIN link_points       lp  ON lp.phone_number  = c.phone_number
                LEFT   JOIN fast_reply_points frp ON frp.phone_number = c.phone_number
                LEFT   JOIN silence_penalty   sil ON sil.phone_number = c.phone_number
                LEFT   JOIN decay             d   ON d.phone_number   = c.phone_number
            )

            UPDATE contacts c
            SET    fan_score = GREATEST(rs.raw_score, 0),
                   fan_tier  = CASE
                                   WHEN rs.raw_score >= 60 THEN 'superfan'
                                   WHEN rs.raw_score >= 25 THEN 'engaged'
                                   WHEN rs.raw_score >= 10 THEN 'lurker'
                                   ELSE 'dormant'
                               END
            FROM   raw_scores rs
            WHERE  c.phone_number = rs.phone_number
        """)
        updated = cur.rowcount
    conn.commit()
    return updated


def log_tier_summary(conn):
    """Log a breakdown of tier counts for monitoring."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fan_tier, COUNT(*) as cnt
            FROM   contacts
            WHERE  fan_tier IS NOT NULL
            GROUP  BY fan_tier
            ORDER  BY cnt DESC
        """)
        rows = cur.fetchall()
    for tier, cnt in rows:
        logger.info("  tier=%-10s count=%d", tier, cnt)


def main():
    t0 = time.time()
    logger.info("=== score_fans starting ===")
    conn = get_conn()
    try:
        ensure_columns(conn)
        updated = score_all_fans(conn)
        logger.info("Scored %d contacts in %.1fs", updated, time.time() - t0)
        log_tier_summary(conn)
    finally:
        conn.close()
    logger.info("=== score_fans done in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
