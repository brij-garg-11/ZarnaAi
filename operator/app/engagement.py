"""
Smart Send engagement scoring.

Computes a per-contact `engagement_score` (0-100) used to pick the highest-
value fans for targeted blast campaigns. Nightly-ish recompute writes the
score back to `contacts.engagement_score`; the Smart Send audience in the
blast composer reads it to suggest a smaller, higher-ROI list.

Score formula (kept simple + explainable, matches the SQL below):

    score = clip(
        reply_recency  (last_replied_at within 90d, newer = higher)    40
      + reply_volume   (number of inbound messages in last 90d, ×2)    30
      + longevity      (contact age in days × 10/180)                  10
    , 0, 100)

Note: an earlier draft of this docstring listed a `click_activity` term
worth 20 points, but the actual SQL never included it (no `link_clicks`
join). The score therefore caps at 80, not 100. If/when click data is
wired in, both this docstring and `_SQL_UPDATE` need to be updated.

Run with `recompute_all()` — safe to call repeatedly, uses a single SQL
UPDATE so it's fast on the contacts table (few hundred thousand rows). In
production we schedule this via cron or a background worker; for now the
/api/admin/engagement/recompute endpoint lets an operator trigger it.

`recompute_all(slug=...)` honors the slug — it only recomputes scores for
contacts belonging to that tenant. Without the slug the update covers all
tenants in the same DB.
"""

from __future__ import annotations

import logging
from typing import Optional

from .db import get_conn

logger = logging.getLogger(__name__)


_SQL_UPDATE_BASE = """
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
    {message_slug_filter}
    GROUP BY phone_number
) m
WHERE c.phone_number = m.phone_number
{contact_slug_filter}
"""


def recompute_all(*, slug: Optional[str] = None) -> int:
    """Recompute engagement_score for all contacts (or just one tenant's).

    When `slug` is provided, the UPDATE is constrained to that tenant in
    both the `messages` aggregation and the `contacts` row filter — so
    callers can safely re-score a single tenant without disturbing others
    on a shared DB. Returns the number of rows updated.
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if slug:
                    sql = _SQL_UPDATE_BASE.format(
                        message_slug_filter="WHERE creator_slug = %s",
                        contact_slug_filter="AND c.creator_slug = %s",
                    )
                    cur.execute(sql, (slug, slug))
                else:
                    sql = _SQL_UPDATE_BASE.format(
                        message_slug_filter="",
                        contact_slug_filter="",
                    )
                    cur.execute(sql)
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
