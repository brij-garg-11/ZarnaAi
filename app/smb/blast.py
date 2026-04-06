"""
SMB owner blast: command detection and subscriber broadcast.

When the business owner texts the bot's number, this module determines
whether their message is an availability/offer command and, if so,
kicks off a broadcast to all active subscribers.

Flow:
  1. Owner texts "opening tonight at 8pm — 20% off tickets"
  2. is_blast_command() checks message against the tenant's blast_triggers
  3. handle_owner_blast() fires an async thread and returns instant confirmation
  4. _run_blast_async() fetches active subscribers, formats body, sends via Twilio
  5. _record_blast() saves the blast to smb_blasts for reporting

Owner sends that aren't blast commands get a friendly help reply instead.
"""

import logging
import re
import threading
from typing import Optional

from app.admin_auth import get_db_connection
from app.messaging.broadcast import run_loop_broadcast, resolve_broadcast_provider
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def is_blast_command(text: str, tenant: BusinessTenant) -> bool:
    """
    Return True if the owner's message contains any of the tenant's blast triggers
    as whole words (not substrings), e.g. 'deal' matches 'great deal' but not 'idealized'.
    """
    if not tenant.blast_triggers:
        return False
    lower = text.strip().lower()
    return any(
        re.search(r"\b" + re.escape(t.strip().lower()) + r"\b", lower)
        for t in tenant.blast_triggers
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def handle_owner_blast(
    phone_number: str, message_text: str, tenant: BusinessTenant
) -> str:
    """
    Called when the registered owner sends a message to the bot.

    If it's a blast command, kicks off an async broadcast and returns
    an instant confirmation to the owner.

    If it's not a blast command, returns a short help reply showing
    example trigger words.
    """
    if not is_blast_command(message_text, tenant):
        sample = ", ".join(f'"{t}"' for t in tenant.blast_triggers[:4])
        return (
            f"To blast your subscribers, include a trigger word like {sample}. "
            f'Example: "Opening tonight 8pm — 20% off tickets"'
        )

    threading.Thread(
        target=_run_blast_async,
        args=(message_text, tenant),
        daemon=True,
    ).start()

    return (
        "Blast queued! Sending to your active subscribers now. "
        "Check your weekly report for results."
    )


# ---------------------------------------------------------------------------
# Async broadcast worker
# ---------------------------------------------------------------------------

def _run_blast_async(message_text: str, tenant: BusinessTenant) -> None:
    conn = get_db_connection()
    if not conn:
        logger.error("SMB blast: no DB connection for tenant=%s", tenant.slug)
        return

    try:
        with conn:
            subscribers = smb_storage.get_active_subscribers(conn, tenant.slug)
    finally:
        conn.close()

    if not subscribers:
        logger.info("SMB blast: no active subscribers for tenant=%s", tenant.slug)
        return

    phones = [s["phone_number"] for s in subscribers]
    body = _format_blast(message_text, tenant)
    provider = resolve_broadcast_provider()

    logger.info(
        "SMB blast starting: tenant=%s recipients=%d provider=%s",
        tenant.slug, len(phones), provider,
    )

    result = run_loop_broadcast(
        phones=phones,
        body=body,
        provider=provider,
        deliver_whatsapp=False,
        slicktext_send=_slicktext_send_one,
    )

    logger.info(
        "SMB blast complete: tenant=%s attempted=%d succeeded=%d failed=%d",
        tenant.slug, result.attempted, result.succeeded, result.failed,
    )

    _record_blast(tenant, message_text, body, result.attempted, result.succeeded)


def _slicktext_send_one(to: str, body: str) -> bool:
    try:
        from app.messaging.slicktext_adapter import create_slicktext_adapter
        return create_slicktext_adapter().send_reply(to, body)
    except Exception:
        logger.exception("SMB blast: SlickText send failed to %s", to[-4:] if to else "?")
        return False


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _format_blast(owner_message: str, tenant: BusinessTenant) -> str:
    """
    Format the owner's raw message as the outbound blast body.
    Prepends the business name if it's not already in the message so
    subscribers know who's texting them.
    """
    msg = owner_message.strip()
    if tenant.display_name.lower() not in msg.lower():
        msg = f"{tenant.display_name}: {msg}"
    return msg


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------

def _record_blast(
    tenant: BusinessTenant,
    owner_message: str,
    body: str,
    attempted: int,
    succeeded: int,
) -> None:
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO smb_blasts
                        (tenant_slug, owner_message, body, attempted, succeeded)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (tenant.slug, owner_message[:500], body[:500], attempted, succeeded),
                )
    except Exception:
        logger.exception("SMB blast: failed to record blast for tenant=%s", tenant.slug)
    finally:
        conn.close()
