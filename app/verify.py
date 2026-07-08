"""Public signup-verification endpoint for the live-show VIP bracelet page.

A lightweight, read-only check used by the on-site signup page: given a phone
number, has this person signed up for the *currently live* show via the SMS bot?

Auth: reuses the existing API_SECRET_KEY (same secret as POST /message), passed
as the X-Api-Key header — so no new environment variable is required.

Read-only: this blueprint never writes to the database.
"""

from __future__ import annotations

import logging
import os

import psycopg2.extras
from flask import Blueprint, jsonify, request

from app.admin_auth import get_db_connection
from app.inbound_security import timing_safe_equal
from app.messaging.broadcast import normalize_e164

logger = logging.getLogger(__name__)

verify_bp = Blueprint("verify", __name__)

_API_SECRET = (os.getenv("API_SECRET_KEY") or "").strip()


def _authorized() -> bool:
    """Same auth contract as POST /message: constant-time compare on X-Api-Key."""
    if not _API_SECRET:
        # No secret configured → treat as misconfigured, deny in production.
        return False
    got = (request.headers.get("X-Api-Key") or "").strip()
    return timing_safe_equal(_API_SECRET, got)


@verify_bp.route("/verify/signup", methods=["GET"])
def verify_signup():
    """Return whether a phone number signed up for the currently-live show.

    Query params:
        phone   – the fan's phone number, any format (normalized to E.164 here).

    Responses:
        200 {"subscribed": true|false}
        400 {"error": "..."}    – missing/invalid phone
        403 {"error": "Unauthorized"}
        503 {"error": "..."}    – secret not configured / no database
    """
    if not _API_SECRET:
        return jsonify(
            {
                "error": "Misconfigured",
                "detail": "Set API_SECRET_KEY in the host environment to use /verify/signup.",
            }
        ), 503

    if not _authorized():
        return jsonify({"error": "Unauthorized"}), 403

    raw_phone = (request.args.get("phone") or "").strip()
    if not raw_phone:
        return jsonify({"error": "phone is required"}), 400

    phone = normalize_e164(raw_phone)
    if not phone:
        return jsonify({"error": "invalid phone"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "No database"}), 503

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Signed up for the show that's live right now. Scoping to the active
            # show (not "ever in contacts") ties the check to the current event,
            # so an old contact can't verify without signing up today.
            cur.execute(
                """
                SELECT 1
                FROM   live_show_signups s
                JOIN   live_shows sh ON sh.id = s.show_id
                WHERE  s.phone_number = %s
                  AND  sh.status = 'live'
                LIMIT  1
                """,
                (phone,),
            )
            subscribed = cur.fetchone() is not None
        return jsonify({"subscribed": subscribed})
    except Exception:
        logger.exception("verify_signup: query failed for ...%s", phone[-4:])
        return jsonify({"error": "Internal error"}), 500
    finally:
        conn.close()