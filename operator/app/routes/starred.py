"""
Starred fans JSON API — consumed by the Lovable React dashboard.

The creator can star a fan from the inbox (star icon on the fan profile) to
bookmark them. The Starred Fans page lists everyone they've saved, with enough
context (name, tier, location, last message) to jump straight into the thread.

All routes are tenant-scoped via _slug_or_abort() and require an active session.
The starred_fans table is shared on the same DATABASE_URL (created by both the
main app and operator init_db).

Registered via register_starred_api_routes(api_bp) from operator/app/routes/api.py.
"""

import logging

import psycopg2.extras
from flask import jsonify, request

from ..routes.auth import login_required
from ..db import get_conn

logger = logging.getLogger(__name__)


def register_starred_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach starred-fan routes to the shared api blueprint."""

    @api_bp.route("/api/starred-fans")
    @login_required
    def starred_fans_list():
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        sf.phone_number,
                        sf.created_at AS starred_at,
                        sf.note,
                        c.fan_name,
                        c.fan_tier,
                        c.fan_tags,
                        c.fan_location,
                        LEFT(c.fan_memory, 200) AS fan_memory_preview,
                        m.last_body,
                        m.last_role,
                        m.last_message_at,
                        m.fan_messages
                    FROM starred_fans sf
                    LEFT JOIN contacts c
                           ON c.phone_number = sf.phone_number AND c.creator_slug = sf.creator_slug
                    LEFT JOIN LATERAL (
                        SELECT
                            (ARRAY_AGG(m2.text ORDER BY m2.created_at DESC))[1] AS last_body,
                            (ARRAY_AGG(m2.role ORDER BY m2.created_at DESC))[1] AS last_role,
                            MAX(m2.created_at) AS last_message_at,
                            COUNT(*) FILTER (WHERE m2.role = 'user') AS fan_messages
                        FROM messages m2
                        WHERE m2.phone_number = sf.phone_number
                          AND m2.creator_slug = sf.creator_slug
                    ) m ON TRUE
                    WHERE sf.creator_slug = %s
                    ORDER BY sf.created_at DESC
                    """,
                    (slug,),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to list starred fans")
            return jsonify(fans=[]), 500

        fans = [{
            "phone_number": r["phone_number"],
            "phone_last4": (r["phone_number"] or "")[-4:],
            "starred_at": r["starred_at"].isoformat() if r["starred_at"] else None,
            "note": r["note"] or "",
            "fan_name": r["fan_name"] or "",
            "fan_tier": r["fan_tier"] or "",
            "fan_tags": (r["fan_tags"] or [])[:5],
            "fan_location": r["fan_location"] or "",
            "fan_memory_preview": (r["fan_memory_preview"] or "")[:200],
            "last_body": (r["last_body"] or "")[:120],
            "last_role": r["last_role"],
            "last_message_at": r["last_message_at"].isoformat() if r["last_message_at"] else None,
            "fan_messages": r["fan_messages"] or 0,
        } for r in rows]
        return jsonify(fans=fans, total=len(fans))

    @api_bp.route("/api/starred-fans", methods=["POST"])
    @login_required
    def starred_fans_star():
        require_performer_account()
        slug = slug_or_abort()
        data = request.get_json(force=True, silent=True) or {}
        phone = (data.get("phone") or "").strip()
        if not phone:
            return jsonify(error="phone is required"), 400
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO starred_fans (phone_number, creator_slug) "
                    "VALUES (%s, %s) ON CONFLICT (creator_slug, phone_number) DO NOTHING",
                    (phone, slug),
                )
            conn.close()
        except Exception:
            logger.exception("api: failed to star fan")
            return jsonify(error="failed to star fan"), 500
        return jsonify(success=True, starred=True)

    @api_bp.route("/api/starred-fans/unstar", methods=["POST"])
    @login_required
    def starred_fans_unstar():
        require_performer_account()
        slug = slug_or_abort()
        data = request.get_json(force=True, silent=True) or {}
        phone = (data.get("phone") or "").strip()
        if not phone:
            return jsonify(error="phone is required"), 400
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM starred_fans WHERE phone_number = %s AND creator_slug = %s",
                    (phone, slug),
                )
            conn.close()
        except Exception:
            logger.exception("api: failed to unstar fan")
            return jsonify(error="failed to unstar fan"), 500
        return jsonify(success=True, starred=False)

    @api_bp.route("/api/starred-fans/note", methods=["POST"])
    @login_required
    def starred_fans_set_note():
        """Save the operator's private note for a starred fan.

        Only affects a fan that's already starred (the note lives on the
        starred_fans row); an empty note clears it. Tenant-scoped by slug.
        """
        require_performer_account()
        slug = slug_or_abort()
        data = request.get_json(force=True, silent=True) or {}
        phone = (data.get("phone") or "").strip()
        # Cap length so a runaway paste can't bloat the row; trim whitespace.
        note = (data.get("note") or "").strip()[:2000]
        if not phone:
            return jsonify(error="phone is required"), 400
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE starred_fans SET note = %s "
                    "WHERE phone_number = %s AND creator_slug = %s",
                    (note, phone, slug),
                )
                updated = cur.rowcount
            conn.close()
        except Exception:
            logger.exception("api: failed to save starred-fan note")
            return jsonify(error="failed to save note"), 500
        if not updated:
            return jsonify(error="fan is not starred"), 404
        return jsonify(success=True, note=note)
