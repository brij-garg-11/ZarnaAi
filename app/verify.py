"""Public signup-verification endpoint for the live-show VIP bracelet page.

A lightweight, read-only check used by the on-site signup page: given a phone
number, has this person signed up for the *currently live* show via the SMS bot?

Auth: a DEDICATED VERIFY_SECRET_KEY, passed as the X-Api-Key header. This is
deliberately NOT the POST /message secret (API_SECRET_KEY): that key can trigger
AI replies and outbound SMS, so a public, browser-facing signup page must never
be able to present it. If VERIFY_SECRET_KEY is unset the endpoint returns 503 —
it never falls back to any other secret.

Read-only: this blueprint never writes to the database.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Optional

import psycopg2.extras
from flask import Blueprint, jsonify, request

from app.admin_auth import get_db_connection
from app.inbound_security import timing_safe_equal

logger = logging.getLogger(__name__)

verify_bp = Blueprint("verify", __name__)

# Dedicated secret for this public-page-facing endpoint. Intentionally distinct
# from API_SECRET_KEY (POST /message) so the bracelet page never shares a key
# that can trigger AI replies / outbound SMS. No fallback: unset => 503.
_VERIFY_SECRET = (os.getenv("VERIFY_SECRET_KEY") or "").strip()

if not _VERIFY_SECRET:
    logger.warning(
        "[WEB] VERIFY_SECRET_KEY is not set — /verify/signup will return 503. "
        "Set a dedicated secret (do NOT reuse API_SECRET_KEY) to enable it."
    )

_DIGITS = re.compile(r"\D")

# Volume watcher (NON-blocking). We never rate-limit — a real fan must always
# get an answer. Instead we count requests in a rolling window and log ONE
# warning if the rate crosses an alert threshold, so log-based alerting can
# flag "something crazy" (e.g. a leaked key being abused) without ever turning
# a fan away. Threshold is intentionally generous; tune via VERIFY_ALERT_PER_MIN.
_VOLUME_WINDOW = 60
try:
    _VOLUME_ALERT_PER_MIN = max(1, int(os.getenv("VERIFY_ALERT_PER_MIN", "300")))
except ValueError:
    _VOLUME_ALERT_PER_MIN = 300
_volume_hits: list = []
_volume_lock = threading.Lock()
# -inf so the first alert is never throttled: time.monotonic() is time since
# boot, which can be < _VOLUME_WINDOW on a freshly booted host (e.g. CI runners,
# new containers), and 0.0 would wrongly suppress the first warning there.
_last_alert_at = float("-inf")


def _to_e164(raw: str) -> Optional[str]:
    """Normalize a user-typed phone number to E.164, or None if implausible.

    Works for SMS *and* WhatsApp, US *and* international, because it canonicalizes
    to the exact E.164 form Twilio stores in live_show_signups (WhatsApp inbound
    has its "whatsapp:" prefix stripped before storage, so both channels land as
    a plain "+<countrycode><number>").

    Three input shapes are accepted:
      * Already international — a leading "+" (e.g. "+44 20 7946 0958"). The
        country code the caller supplied is trusted; we only require a plausible
        E.164 length (8–15 digits, per ITU-T E.164).
      * "00" international access prefix (e.g. "0044 20 7946 0958") — the "00" is
        dropped and the rest treated as an international number.
      * Bare US/Canada (NANP) convenience — a 10-digit number, or 11 digits
        starting with "1", gets a "+1" prefix. This is a fallback for callers
        that don't send a country code; the VIP page's country picker always
        sends a full "+" number, so international fans never hit this branch.

    Over-permissive on purpose: a wrong-but-plausible number simply fails to
    match a real signup and returns {"subscribed": false} — never a false yes.
    """
    raw = (raw or "").strip()
    if not raw:
        return None

    compact = raw.replace(" ", "")
    digits = _DIGITS.sub("", raw)
    if not digits:
        return None

    if compact.startswith("+"):
        return "+" + digits if 8 <= len(digits) <= 15 else None

    if compact.startswith("00"):
        digits = digits[2:]
        return "+" + digits if 8 <= len(digits) <= 15 else None

    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def _client_ip() -> str:
    fwd = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
    return fwd.split(",")[0].strip()


def _note_volume(ip: str) -> None:
    """Record a request and log a throttled warning on an abnormal spike.

    Never blocks. Emits at most one warning per window so a real surge (or a
    leaked-key abuse) shows up in logs/alerts without dropping any fan traffic.
    """
    global _last_alert_at
    now = time.monotonic()
    with _volume_lock:
        _volume_hits[:] = [t for t in _volume_hits if now - t < _VOLUME_WINDOW]
        _volume_hits.append(now)
        count = len(_volume_hits)
        should_alert = (
            count > _VOLUME_ALERT_PER_MIN and now - _last_alert_at > _VOLUME_WINDOW
        )
        if should_alert:
            _last_alert_at = now
    if should_alert:
        logger.warning(
            "[WEB] verify_signup high volume: %d requests in ~%ds (threshold %d, "
            "recent ip=%s). Not blocking — check for a leaked VERIFY_SECRET_KEY.",
            count,
            _VOLUME_WINDOW,
            _VOLUME_ALERT_PER_MIN,
            ip,
        )


def _authorized() -> bool:
    """Constant-time compare on the X-Api-Key header against VERIFY_SECRET_KEY."""
    if not _VERIFY_SECRET:
        return False
    got = (request.headers.get("X-Api-Key") or "").strip()
    return timing_safe_equal(_VERIFY_SECRET, got)


@verify_bp.route("/verify/signup", methods=["POST"])
def verify_signup():
    """Return whether a phone number signed up for the currently-live show.

    The phone is sent in the POST body (JSON {"phone": "..."} or form field),
    NOT the URL — keeping the fan's number out of access logs, proxies, and
    browser history. Any common US or international format is accepted and
    normalized to E.164 here (works for both SMS and WhatsApp signups).

    Responses:
        200 {"subscribed": true|false}
        400 {"error": "..."}    – missing/invalid phone
        403 {"error": "Unauthorized"}
        503 {"error": "..."}    – secret not configured / no database
    """
    if not _VERIFY_SECRET:
        return jsonify(
            {
                "error": "Misconfigured",
                "detail": "Set a dedicated VERIFY_SECRET_KEY to use /verify/signup.",
            }
        ), 503

    if not _authorized():
        return jsonify({"error": "Unauthorized"}), 403

    _note_volume(_client_ip())

    body = request.get_json(silent=True) or {}
    raw_phone = (body.get("phone") or request.form.get("phone") or "").strip()
    if not raw_phone:
        return jsonify({"error": "phone is required"}), 400

    phone = _to_e164(raw_phone)
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
