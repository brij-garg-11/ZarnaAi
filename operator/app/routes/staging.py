"""
Staging-only utility endpoints. Gated by ENVIRONMENT=staging — every route
in this blueprint returns 404 in production so this can never accidentally
be triggered against real fan data.

What lives here
===============

POST   /api/admin/staging/add-test-fan        Add YOUR phone (or anyone's) as
                                              a test fan for your creator
GET    /api/admin/staging/test-fans           List all 'staging-seed' or
                                              manually-added test fans for
                                              your creator
DELETE /api/admin/staging/test-fans/<phone>   Remove a single test fan

Why this exists
===============

The seed script populates fake phone numbers (+15550100xxxx, magic Twilio
numbers) which test the credit logic but never deliver real SMS. To verify
end-to-end blast delivery against your real phone, you need a way to add
your phone as a fan without poking the database directly. This is that.

Source-of-truth for "is this a test fan?": the contacts.source column.
Seeded fans use 'staging-seed', manually-added fans use 'staging-manual'.
The reset script wipes both.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Optional

from flask import Blueprint, abort, jsonify, request

from ..routes.auth import current_user, login_required, resolve_slug
from ..db import get_conn

logger = logging.getLogger(__name__)

staging_bp = Blueprint("staging_utils", __name__)


def _staging_only():
    """Abort with 404 if we're not running on the staging deployment.

    Returning 404 (not 403) so prod users can't even tell these endpoints
    exist.
    """
    if os.getenv("ENVIRONMENT", "").lower() != "staging":
        abort(404)


def _normalize_phone(raw: str) -> Optional[str]:
    """Coerce common US phone formats to E.164 (+1XXXXXXXXXX).

    Returns None if the digits don't add up to a plausible US 10-digit number.
    """
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    # Strip a leading '1' country code if the user typed one
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"+1{digits}"


# ── Get the slug we should attach test fans to ────────────────────────────────
# Falls back to the user's main creator_slug if no super-admin "viewing as"
# is active.

def _target_slug() -> Optional[str]:
    user = current_user()
    if not user:
        return None
    # If super-admin is currently "viewing as" another project, use that slug.
    # resolve_slug() centralises that logic.
    return resolve_slug() or user.get("creator_slug")


# ── POST /api/admin/staging/add-test-fan ──────────────────────────────────────

@staging_bp.route("/api/admin/staging/add-test-fan", methods=["POST"])
@login_required
def add_test_fan():
    _staging_only()

    data = request.get_json(silent=True) or {}
    raw_phone = (data.get("phone") or "").strip()
    name = (data.get("name") or "").strip()[:120] or "My phone (test)"
    location = (data.get("location") or "").strip()[:120] or ""
    memory = (data.get("memory") or "").strip()[:1000] or \
        "Manually added via /api/admin/staging/add-test-fan for end-to-end SMS testing."

    phone = _normalize_phone(raw_phone)
    if not phone:
        return jsonify(success=False, error="Provide a 10-digit US phone number."), 400

    slug = _target_slug()
    if not slug:
        return jsonify(success=False, error="No creator slug active — finish onboarding first."), 400

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO contacts (
                        phone_number, source, creator_slug,
                        fan_name, fan_location, fan_tags, fan_memory
                    )
                    VALUES (%s, 'staging-manual', %s, %s, %s, %s, %s)
                    ON CONFLICT (phone_number) DO UPDATE SET
                        source       = 'staging-manual',
                        creator_slug = EXCLUDED.creator_slug,
                        fan_name     = EXCLUDED.fan_name,
                        fan_location = EXCLUDED.fan_location,
                        fan_tags     = EXCLUDED.fan_tags,
                        fan_memory   = EXCLUDED.fan_memory
                    """,
                    (phone, slug, name, location, ["test", "manual"], memory),
                )
        logger.info("staging: added test fan phone=%s slug=%s name=%r", phone, slug, name)
        return jsonify(
            success=True,
            fan={"phone": phone, "name": name, "creator_slug": slug, "source": "staging-manual"},
        )
    except Exception:
        logger.exception("staging.add_test_fan failed")
        return jsonify(success=False, error="Failed to add test fan."), 500
    finally:
        conn.close()


# ── GET /api/admin/staging/test-fans ──────────────────────────────────────────

@staging_bp.route("/api/admin/staging/test-fans", methods=["GET"])
@login_required
def list_test_fans():
    _staging_only()

    slug = _target_slug()
    if not slug:
        return jsonify(success=False, error="No creator slug active."), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT phone_number, source, fan_name, fan_location, fan_tags
                FROM contacts
                WHERE creator_slug = %s
                  AND source IN ('staging-seed', 'staging-manual')
                ORDER BY source DESC, fan_name
                """,
                (slug,),
            )
            rows = [dict(r) for r in cur.fetchall()]
        return jsonify(success=True, fans=rows, count=len(rows))
    except Exception:
        logger.exception("staging.list_test_fans failed")
        return jsonify(success=False, error="Failed to list test fans."), 500
    finally:
        conn.close()


# ── DELETE /api/admin/staging/test-fans/<phone> ───────────────────────────────

@staging_bp.route("/api/admin/staging/test-fans/<phone>", methods=["DELETE"])
@login_required
def delete_test_fan(phone: str):
    _staging_only()

    slug = _target_slug()
    if not slug:
        return jsonify(success=False, error="No creator slug active."), 400

    normalized = _normalize_phone(phone)
    if not normalized:
        return jsonify(success=False, error="Invalid phone number."), 400

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Only permit deletion of a fan the user actually owns AND
                # one that was added via the staging tooling — never delete
                # a fan that came in via real Twilio inbound.
                cur.execute(
                    """
                    DELETE FROM contacts
                    WHERE phone_number = %s
                      AND creator_slug = %s
                      AND source IN ('staging-seed', 'staging-manual')
                    """,
                    (normalized, slug),
                )
                deleted = cur.rowcount
                if deleted:
                    cur.execute(
                        "DELETE FROM messages WHERE phone_number = %s",
                        (normalized,),
                    )
        if not deleted:
            return jsonify(success=False, error="Fan not found or not eligible for deletion."), 404
        return jsonify(success=True, phone=normalized)
    except Exception:
        logger.exception("staging.delete_test_fan failed")
        return jsonify(success=False, error="Failed to delete test fan."), 500
    finally:
        conn.close()
