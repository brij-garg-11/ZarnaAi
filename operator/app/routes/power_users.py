"""
Power Users JSON API — consumed by the Lovable React dashboard.

Returns the creator's most active fans (the "power users" of the bot), ranked
by how many messages the fan has *sent* to the bot. Fan-initiated messages
(role='user') are the truest signal of engagement and naturally exclude
outbound blasts (which are stored as role='assistant', msg_source='blast').

Each row carries enough context (name, tier, location, counts, last activity)
plus phone_last4 so the dashboard can jump straight into the fan's thread.

All routes are tenant-scoped via slug_or_abort() and require an active session.
Registered via register_power_users_api_routes(api_bp) from
operator/app/routes/api.py.
"""

import logging

import psycopg2.extras
from flask import jsonify, request

from ..routes.auth import login_required
from ..db import get_conn

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 30
_MAX_LIMIT = 100


def register_power_users_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach power-user routes to the shared api blueprint."""

    @api_bp.route("/api/power-users")
    @login_required
    def power_users_list():
        require_performer_account()
        slug = slug_or_abort()

        try:
            limit = int(request.args.get("limit", _DEFAULT_LIMIT))
        except (TypeError, ValueError):
            limit = _DEFAULT_LIMIT
        limit = max(1, min(limit, _MAX_LIMIT))

        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        m.phone_number,
                        RIGHT(m.phone_number, 4) AS phone_last4,
                        MAX(m.created_at) AS last_message_at,
                        COUNT(*) FILTER (WHERE m.role = 'user') AS fan_messages,
                        COUNT(*) FILTER (
                            WHERE m.role = 'assistant'
                              AND (m.msg_source IS NULL OR m.msg_source <> 'blast')
                        ) AS bot_messages,
                        c.fan_name,
                        c.fan_tier,
                        c.fan_location,
                        COALESCE(c.engagement_score, 0) AS engagement_score
                    FROM messages m
                    LEFT JOIN contacts c
                           ON c.phone_number = m.phone_number
                          AND c.creator_slug = m.creator_slug
                    WHERE m.creator_slug = %s
                      AND m.phone_number NOT LIKE 'whatsapp:%%'
                    GROUP BY m.phone_number, c.fan_name, c.fan_tier,
                             c.fan_location, c.engagement_score
                    HAVING COUNT(*) FILTER (WHERE m.role = 'user') > 0
                    ORDER BY fan_messages DESC, last_message_at DESC
                    LIMIT %s
                    """,
                    (slug, limit),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("power_users_list failed for slug=%s", slug)
            return jsonify(success=False, error="Failed to load power users."), 500

        power_users = []
        for i, r in enumerate(rows):
            last_at = r["last_message_at"]
            power_users.append({
                "rank": i + 1,
                "phone_number": r["phone_number"],
                "phone_last4": r["phone_last4"],
                "fan_name": r.get("fan_name") or "",
                "fan_tier": r.get("fan_tier") or "",
                "fan_location": r.get("fan_location") or "",
                "fan_messages": r["fan_messages"] or 0,
                "bot_messages": r["bot_messages"] or 0,
                "engagement_score": r["engagement_score"] or 0,
                "last_message_at": last_at.isoformat() if last_at else None,
            })

        return jsonify(success=True, count=len(power_users), power_users=power_users)
