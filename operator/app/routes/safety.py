"""
Safety review JSON API — consumed by the Lovable React dashboard (zar.bot).

The main SMS app's crisis gate (app/brain/crisis.py) writes a row to the
``safety_flags`` table whenever a fan's message signals suicidal ideation or
self-harm and the fixed 988 response is sent. These endpoints let the operator
review those conversations: list flags (unreviewed first), see the fan's
message + the exact response sent, and mark flags reviewed.

The table lives on the same DATABASE_URL the main app writes to, so no
cross-service calls are needed. All routes are tenant-scoped via
_slug_or_abort() and require an active session.

Registered via register_safety_api_routes(api_bp) from operator/app/routes/api.py.
"""

import logging

import psycopg2.extras
from flask import jsonify, request

from ..routes.auth import login_required
from ..db import get_conn

logger = logging.getLogger(__name__)


def _table_exists(cur) -> bool:
    cur.execute("SELECT to_regclass('public.safety_flags')")
    return cur.fetchone()[0] is not None


def register_safety_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach the safety-review routes to the shared /api blueprint."""

    @api_bp.route("/api/safety-flags")
    @login_required
    def api_list_safety_flags():
        require_performer_account()
        slug = slug_or_abort()
        show = (request.args.get("show") or "unreviewed").strip().lower()
        try:
            limit = min(max(int(request.args.get("limit", 100)), 1), 500)
        except ValueError:
            limit = 100

        where = "creator_slug = %s"
        params = [slug]
        if show == "unreviewed":
            where += " AND reviewed = FALSE"
        elif show == "reviewed":
            where += " AND reviewed = TRUE"
        # show == "all": no extra filter

        try:
            conn = get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    if not _table_exists(cur):
                        # Main app hasn't recorded any flags yet — empty, not an error.
                        return jsonify(flags=[], unreviewed_count=0)
                    cur.execute(
                        f"""
                        SELECT id, phone_number, message_text, matched_pattern,
                               bot_response, created_at, reviewed, reviewed_at
                        FROM   safety_flags
                        WHERE  {where}
                        ORDER  BY reviewed ASC, created_at DESC
                        LIMIT  %s
                        """,
                        params + [limit],
                    )
                    rows = cur.fetchall()
                    cur.execute(
                        "SELECT COUNT(*) FROM safety_flags WHERE creator_slug = %s AND reviewed = FALSE",
                        (slug,),
                    )
                    unreviewed = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception:
            logger.exception("safety-flags list failed for slug=%s", slug)
            return jsonify(flags=[], unreviewed_count=0, error="query failed"), 500

        flags = [
            {
                "id": r["id"],
                "phone_number": r["phone_number"],
                "phone_last4": (r["phone_number"] or "")[-4:],
                "message_text": r["message_text"],
                "matched_pattern": r["matched_pattern"],
                "bot_response": r["bot_response"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "reviewed": bool(r["reviewed"]),
                "reviewed_at": r["reviewed_at"].isoformat() if r["reviewed_at"] else None,
            }
            for r in rows
        ]
        return jsonify(flags=flags, unreviewed_count=unreviewed)

    @api_bp.route("/api/safety-flags/<int:flag_id>/review", methods=["POST"])
    @login_required
    def api_review_safety_flag(flag_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE safety_flags
                            SET    reviewed = TRUE, reviewed_at = NOW()
                            WHERE  id = %s AND creator_slug = %s
                            """,
                            (flag_id, slug),
                        )
                        updated = cur.rowcount
            finally:
                conn.close()
        except Exception:
            logger.exception("safety-flag review failed id=%s slug=%s", flag_id, slug)
            return jsonify(success=False, error="update failed"), 500
        if not updated:
            return jsonify(success=False, error="not found"), 404
        return jsonify(success=True)
