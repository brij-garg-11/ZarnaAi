"""
Twilio phone number provisioning.

STUBBED for now — returns a deterministic fake number so the rest of the
pipeline can be built, tested, and demoed end-to-end without a real
A2P campaign SID. Flip PROVISIONING_PHONE_MODE=real in env once Twilio
approves the campaign.

Real implementation (commented inline) will:
  1. Search Twilio for available US local numbers
  2. Purchase one
  3. Set sms_url webhook to /smb/inbound?tenant=<slug>
  4. Add to the platform messaging service (A2P campaign)
  5. Save to operator_users.phone_number
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Optional
from urllib.parse import quote

from ..db import get_conn
from . import twilio_numbers

_log = logging.getLogger(__name__)


def _mode() -> str:
    """Read the mode lazily so tests / env flips take effect without reimport."""
    return os.getenv("PROVISIONING_PHONE_MODE", "stub").lower()


def _webhook_url(slug: str, account_type: str) -> str:
    """
    Build the inbound-SMS webhook URL Twilio should POST to for this number.

    TWILIO_WEBHOOK_BASE must point at the MAIN APP (where /twilio/webhook and
    /smb/inbound live), NOT the operator service.

    Performer numbers carry their tenant in `?slug=` so the multi-tenant main
    app can pick the right brain; business numbers use the existing SMB router.
    """
    base = os.getenv("TWILIO_WEBHOOK_BASE", "").rstrip("/")
    if not base:
        raise RuntimeError(
            "TWILIO_WEBHOOK_BASE is not set — cannot wire the inbound webhook. "
            "Set it to the main app's public URL (e.g. https://app.zar.bot)."
        )
    q = quote(slug, safe="")
    if account_type == "business":
        return f"{base}/smb/inbound?tenant={q}"
    return f"{base}/twilio/webhook?slug={q}"


def _stub_phone_for_slug(slug: str) -> str:
    """
    Deterministic fake number so the same slug always resolves to the same
    stub number. Format: +1555XXXXXXX where X is derived from the slug hash.
    Always uses the 555 area code so it's unmistakable as a test number.
    """
    h = hashlib.sha256(slug.encode("utf-8")).hexdigest()
    suffix = int(h, 16) % 10_000_000
    return f"+1555{suffix:07d}"


def _get_existing_phone(slug: str) -> Optional[str]:
    """
    Idempotency check: if this slug already has a phone number, return it.
    Looks up via operator_users joined through bot_configs.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ou.phone_number
                FROM operator_users ou
                JOIN bot_configs bc ON bc.operator_user_id = ou.id
                WHERE bc.creator_slug = %s
                  AND ou.phone_number IS NOT NULL
                  AND ou.phone_number <> ''
                LIMIT 1
                """,
                (slug,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _save_phone_to_user(slug: str, phone_number: str) -> None:
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE operator_users
                SET phone_number = %s
                WHERE id = (
                    SELECT operator_user_id FROM bot_configs
                    WHERE creator_slug = %s
                    LIMIT 1
                )
                """,
                (phone_number, slug),
            )
    finally:
        conn.close()


def _save_number_registry(
    phone_number: str,
    slug: str,
    number_sid: str = "",
    account_type: str = "performer",
) -> None:
    """
    Upsert the phone→slug routing row the main app reads on every inbound text.
    Idempotent: re-provisioning the same number updates its row in place.
    """
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO creator_numbers
                    (phone_number, creator_slug, number_sid, account_type, status)
                VALUES (%s, %s, %s, %s, 'active')
                ON CONFLICT (phone_number) DO UPDATE SET
                    creator_slug = EXCLUDED.creator_slug,
                    number_sid   = EXCLUDED.number_sid,
                    account_type = EXCLUDED.account_type,
                    status       = 'active'
                """,
                (phone_number, slug, number_sid, account_type),
            )
    except Exception:
        # Never let a registry write failure abort provisioning — the orchestrator
        # will still mark the slug live, and the main app falls back to the
        # operator_users.phone_number → slug lookup if this row is missing.
        _log.exception("phone[%s]: failed to write creator_numbers row", slug)
    finally:
        conn.close()


def _ensure_phone_number_column() -> None:
    """
    Idempotent safety net: add phone_number column to operator_users if it
    doesn't already exist. In production this lives in init_db — this check
    is cheap and makes the module self-contained for tests.
    """
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS phone_number TEXT"
            )
    except Exception:
        _log.exception("phone: could not ensure phone_number column exists")
    finally:
        conn.close()


def buy_and_configure(slug: str, account_type: str = "performer") -> str:
    """
    Get (or create) a dedicated phone number for this creator.

    Returns the E.164 phone number string. Idempotent: if a number is already
    on file for this slug, it is returned without buying another.
    """
    _ensure_phone_number_column()

    existing = _get_existing_phone(slug)
    if existing:
        _log.info("phone[%s]: already provisioned (%s) — skipping", slug, existing)
        return existing

    if _mode() == "real":
        return _buy_real_number(slug, account_type)

    stub = _stub_phone_for_slug(slug)
    _save_phone_to_user(slug, stub)
    _save_number_registry(stub, slug, number_sid="", account_type=account_type)
    _log.info("phone[%s]: STUB assigned %s (PROVISIONING_PHONE_MODE=stub)", slug, stub)
    return stub


def _twilio_client():
    """Build a Twilio REST client from env. Raises if credentials are missing."""
    from twilio.rest import Client  # deferred import — keeps module cheap for tests

    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
    if not account_sid or not auth_token:
        raise RuntimeError(
            "TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN not set — cannot provision a number."
        )
    return Client(account_sid, auth_token)


def _buy_real_number(slug: str, account_type: str = "performer") -> str:
    """
    Buy a real Twilio number, wire its inbound webhook, and attach it to our
    approved A2P messaging service. Active when PROVISIONING_PHONE_MODE=real.

    The actual Twilio calls live in the dependency-free `twilio_numbers` module
    (so they can be unit-tested with a fake client); this function supplies the
    env-derived config and persists the result.
    """
    client = _twilio_client()
    msg_svc_sid = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "")
    area_code = os.getenv("TWILIO_PROVISION_AREA_CODE", "").strip() or None
    webhook_url = _webhook_url(slug, account_type)

    phone_number, number_sid = twilio_numbers.provision_number(
        client,
        webhook_url=webhook_url,
        messaging_service_sid=msg_svc_sid or None,
        area_code=area_code,
    )

    _save_phone_to_user(slug, phone_number)
    _save_number_registry(phone_number, slug, number_sid=number_sid, account_type=account_type)
    _log.info(
        "phone[%s]: REAL number %s provisioned (sid=%s, account_type=%s, webhook=%s)",
        slug, phone_number, number_sid, account_type, webhook_url,
    )
    return phone_number


def _get_number_for_slug(slug: str):
    """Return (phone_number, number_sid) from the registry for this slug, or (None, None)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phone_number, number_sid FROM creator_numbers "
                "WHERE creator_slug = %s AND status = 'active' "
                "ORDER BY created_at LIMIT 1",
                (slug,),
            )
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)
    finally:
        conn.close()


def _mark_number_released(phone_number: str) -> None:
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE creator_numbers SET status = 'released' WHERE phone_number = %s",
                (phone_number,),
            )
    finally:
        conn.close()


def release_for_slug(slug: str) -> bool:
    """
    Release the dedicated number for `slug` back to Twilio and mark the
    registry row 'released'. Call on cancellation/downgrade so we stop paying
    for an idle number. Returns True if a number was found and released.

    Idempotent: a slug with no active number is a no-op (returns False).
    """
    phone_number, number_sid = _get_number_for_slug(slug)
    if not phone_number:
        _log.info("phone[%s]: no active number to release", slug)
        return False

    if _mode() == "real" and number_sid:
        try:
            twilio_numbers.release_number(_twilio_client(), number_sid)
        except Exception:
            # Mark released anyway so routing stops; an orphaned Twilio number
            # is a billing cleanup task, not a reason to keep routing to it.
            _log.exception("phone[%s]: Twilio release failed for sid=%s", slug, number_sid)

    _mark_number_released(phone_number)
    _log.info("phone[%s]: released number %s (sid=%s)", slug, phone_number, number_sid or "stub")
    return True
