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

from ..db import get_conn

_log = logging.getLogger(__name__)

_MODE = os.getenv("PROVISIONING_PHONE_MODE", "stub").lower()


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


def buy_and_configure(slug: str) -> str:
    """
    Get (or create) a dedicated phone number for this creator.

    Returns the E.164 phone number string.
    """
    _ensure_phone_number_column()

    existing = _get_existing_phone(slug)
    if existing:
        _log.info("phone[%s]: already provisioned (%s) — skipping", slug, existing)
        return existing

    if _MODE == "real":
        return _buy_real_number(slug)

    stub = _stub_phone_for_slug(slug)
    _save_phone_to_user(slug, stub)
    _log.info("phone[%s]: STUB assigned %s (PROVISIONING_PHONE_MODE=stub)", slug, stub)
    return stub


def _buy_real_number(slug: str) -> str:
    """
    Real Twilio provisioning. NOT ACTIVE until PROVISIONING_PHONE_MODE=real
    and TWILIO_MESSAGING_SERVICE_SID is set. Left as a clear TODO with the
    exact Twilio SDK calls spelled out.
    """
    # from twilio.rest import Client
    # account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    # auth_token  = os.getenv("TWILIO_AUTH_TOKEN")
    # msg_svc_sid = os.getenv("TWILIO_MESSAGING_SERVICE_SID")
    # webhook_base = os.getenv("TWILIO_WEBHOOK_BASE", "https://zarnaai.up.railway.app")
    # client = Client(account_sid, auth_token)
    #
    # # 1. Find an available number
    # available = client.available_phone_numbers("US").local.list(sms_enabled=True, limit=5)
    # if not available:
    #     raise RuntimeError("No Twilio numbers available in the US local pool")
    # number_to_buy = available[0].phone_number
    #
    # # 2. Purchase it with the webhook wired to the SMB tenant router
    # purchased = client.incoming_phone_numbers.create(
    #     phone_number=number_to_buy,
    #     sms_url=f"{webhook_base}/smb/inbound?tenant={slug}",
    #     sms_method="POST",
    # )
    #
    # # 3. Add it to the platform A2P messaging service
    # client.messaging.v1.services(msg_svc_sid).phone_numbers.create(
    #     phone_number_sid=purchased.sid,
    # )
    #
    # _save_phone_to_user(slug, purchased.phone_number)
    # return purchased.phone_number
    raise NotImplementedError(
        "Real Twilio provisioning not yet wired — "
        "set PROVISIONING_PHONE_MODE=stub or finish _buy_real_number()."
    )
