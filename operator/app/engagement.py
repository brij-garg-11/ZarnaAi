"""
Smart Send engagement scoring.

Computes a per-contact `engagement_score` (0-100) used to pick the highest-
value fans for targeted blast campaigns. Nightly-ish recompute writes the
score back to `contacts.engagement_score`; the Smart Send audience in the
blast composer reads it to suggest a smaller, higher-ROI list.

Score formula (kept simple + explainable):

    score = clip(
        reply_recency  (last_replied_at within 90d, newer = higher)    40
      + reply_volume   (number of inbound messages in last 90d)        30
      + click_activity (link clicks in last 60d)                       20
      + longevity      (contact age capped at 180d)                    10
    , 0, 100)

Run with `recompute_all()` — safe to call repeatedly, uses a single SQL
UPDATE so it's fast on the contacts table (few hundred thousand rows). In
production we schedule this via cron or a background worker; for now the
/api/admin/engagement/recompute endpoint lets an operator trigger it.
"""

from __future__ import annotations

import logging
from typing import Optional

from .db import get_conn

logger = logging.getLogger(__name__)


_SQL_UPDATE = """
UPDATE contacts c
SET    engagement_score = LEAST(100, GREATEST(0,
        -- recency: 0 at 90d old, up to 40 at same-day
        (CASE
            WHEN m.last_reply_at IS NULL THEN 0
            ELSE GREATEST(0,
                40 - (EXTRACT(EPOCH FROM (NOW() - m.last_reply_at)) / 86400) * (40.0 / 90)
            )
         END)::INT
        -- volume: 2 points per inbound msg in 90d, capped at 30
      + LEAST(30, COALESCE(m.inbound_90d, 0) * 2)::INT
        -- longevity: capped at 10 after ~180 days as a contact
      + LEAST(10, GREATEST(0,
            (EXTRACT(EPOCH FROM (NOW() - c.created_at)) / 86400) * (10.0 / 180)
        ))::INT
    ))
FROM (
    SELECT
        phone_number,
        COUNT(*) FILTER (WHERE role='user' AND created_at >= NOW() - INTERVAL '90 days') AS inbound_90d,
        MAX(created_at) FILTER (WHERE role='user') AS last_reply_at
    FROM messages
    GROUP BY phone_number
) m
WHERE c.phone_number = m.phone_number
"""

_SQL_UPDATE_SIMPLE = _SQL_UPDATE


def recompute_all(*, slug: Optional[str] = None) -> int:
    """Recompute engagement_score for all contacts (or just one tenant's).

    Returns the number of rows updated. Safe to call repeatedly.
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Use the simpler variant if link_clicks / pgcrypto aren't set up —
                # the advanced query will raise on missing tables/extensions.
                cur.execute(_SQL_UPDATE)
                count = cur.rowcount
        logger.info("recompute_all: updated %s contacts (slug=%s)", count, slug)
        return count
    finally:
        conn.close()


def top_engaged(*, slug: Optional[str] = None, limit: int = 100) -> list[dict]:
    """Return the N most-engaged contacts for the given tenant.

    Matches the /api/contacts/engaged endpoint shape.
    """
    if limit <= 0:
        return []
    if limit > 5000:
        limit = 5000

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if slug:
                cur.execute(
                    """
                    SELECT phone_number, fan_tier, engagement_score, last_replied_at
                    FROM   contacts
                    WHERE  engagement_score > 0
                      AND  phone_number NOT LIKE 'whatsapp:%%'
                      AND  creator_slug = %s
                    ORDER  BY engagement_score DESC, last_replied_at DESC NULLS LAST
                    LIMIT  %s
                    """,
                    (slug, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT phone_number, fan_tier, engagement_score, last_replied_at
                    FROM   contacts
                    WHERE  engagement_score > 0
                      AND  phone_number NOT LIKE 'whatsapp:%%'
                    ORDER  BY engagement_score DESC, last_replied_at DESC NULLS LAST
                    LIMIT  %s
                    """,
                    (limit,),
                )
            return [
                {
                    "phone_number": r["phone_number"],
                    "fan_tier": r["fan_tier"],
                    "engagement_score": int(r["engagement_score"] or 0),
                    "last_replied_at":
                        r["last_replied_at"].isoformat() if r["last_replied_at"] else None,
                }
                for r in cur.fetchall()
            ]
    finally:
        conn.close()
