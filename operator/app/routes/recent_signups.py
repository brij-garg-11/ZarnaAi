"""
Recent Signups JSON API — consumed by the Lovable React dashboard.

Returns the creator's newest subscribers, ordered by when they joined
(contacts.created_at DESC, most recent first). Each row carries enough
context (name, tier, location, signup source, message count) plus
phone_last4 so the dashboard can jump straight into the fan's thread.

All routes are tenant-scoped via slug_or_abort() and require an active session.
Registered via register_recent_signups_api_routes(api_bp) from
operator/app/routes/api.py.
"""

import logging

import psycopg2.extras
from flask import jsonify, request

from ..routes.auth import login_required
from ..db import get_conn

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 200


def register_recent_signups_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach recent-signup routes to the shared api blueprint."""

    @api_bp.route("/api/recent-signups")
    @login_required
    def recent_signups_list():
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
                # Inner query picks the newest N contacts first so the
                # per-fan message count only runs on the rows we return.
                cur.execute(
                    """
                    SELECT
                        s.phone_number,
                        RIGHT(s.phone_number, 4) AS phone_last4,
                        s.signed_up_at,
                        s.fan_name,
                        s.fan_tier,
                        s.fan_location,
                        s.source,
                        (
                            SELECT COUNT(*)
                            FROM messages m
                            WHERE m.phone_number = s.phone_number
                              AND m.creator_slug = %s
                              AND m.role = 'user'
                        ) AS fan_messages
                    FROM (
                        SELECT c.phone_number,
                               c.created_at AS signed_up_at,
                               c.fan_name,
                               c.fan_tier,
                               c.fan_location,
                               c.source
                        FROM contacts c
                        WHERE c.creator_slug = %s
                          AND c.phone_number NOT LIKE 'whatsapp:%%'
                        ORDER BY c.created_at DESC NULLS LAST
                        LIMIT %s
                    ) s
                    ORDER BY s.signed_up_at DESC NULLS LAST
                    """,
                    (slug, slug, limit),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("recent_signups_list failed for slug=%s", slug)
            return jsonify(success=False, error="Failed to load recent signups."), 500

        signups = []
        for r in rows:
            joined = r["signed_up_at"]
            signups.append({
                "phone_number": r["phone_number"],
                "phone_last4": r["phone_last4"],
                "fan_name": r.get("fan_name") or "",
                "fan_tier": r.get("fan_tier") or "",
                "fan_location": r.get("fan_location") or "",
                "source": r.get("source") or "",
                "fan_messages": r["fan_messages"] or 0,
                "signed_up_at": joined.isoformat() if joined else None,
            })

        return jsonify(success=True, count=len(signups), signups=signups)
