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

    Supports optional segment targeting:
      "STANDUP: Great show tonight 8pm!" → only STANDUP fans
      "IMPROV: Jam session tonight!"     → only IMPROV fans
      "Opening tonight 8pm!"             → all active subscribers

    If it's not a blast command, returns a short help reply showing
    example trigger words.
    """
    if not is_blast_command(message_text, tenant):
        sample = ", ".join(f'"{t}"' for t in tenant.blast_triggers[:4])
        seg_hint = ""
        if tenant.segments:
            names = ", ".join(s["name"] for s in tenant.segments[:3])
            seg_hint = f' Prefix with a segment to target a group: "{tenant.segments[0]["name"]}: your message".'
        return (
            f"To blast your subscribers, include a trigger word like {sample}. "
            f'Example: "Opening tonight 8pm — 20% off tickets".{seg_hint}'
        )

    segment = _detect_segment(message_text, tenant)

    threading.Thread(
        target=_run_blast_async,
        args=(message_text, tenant, segment),
        daemon=True,
    ).start()

    if segment:
        return (
            f"Blast queued for your {segment['name']} subscribers! "
            "Check your weekly report for results."
        )
    return (
        "Blast queued for all your active subscribers! "
        "Check your weekly report for results."
    )


# ---------------------------------------------------------------------------
# Async broadcast worker
# ---------------------------------------------------------------------------

def _detect_segment(text: str, tenant: BusinessTenant) -> Optional[dict]:
    """
    Check if the owner's message starts with a known segment prefix.

    Format: "SEGMENT_NAME: rest of message"
    e.g.   "STANDUP: Great show tonight at 8pm!"

    Returns the matching segment dict (with name, question_key, answers)
    if found, or None for a broadcast to all subscribers.
    """
    if not tenant.segments:
        return None
    stripped = text.strip()
    for seg in tenant.segments:
        prefix = seg["name"].upper() + ":"
        if stripped.upper().startswith(prefix):
            body_after = stripped[len(prefix):].strip()
            if body_after:  # ignore bare "STANDUP:" with no message
                return seg
    return None


def _run_blast_async(
    message_text: str,
    tenant: BusinessTenant,
    segment: Optional[dict] = None,
) -> None:
    conn = get_db_connection()
    if not conn:
        logger.error("SMB blast: no DB connection for tenant=%s", tenant.slug)
        return

    try:
        with conn:
            if segment:
                subscribers = smb_storage.get_subscribers_by_segment(
                    conn, tenant.slug,
                    segment["question_key"],
                    segment["answers"],
                )
                logger.info(
                    "SMB blast: segment=%s matched %d subscribers for tenant=%s",
                    segment["name"], len(subscribers), tenant.slug,
                )
            else:
                subscribers = smb_storage.get_active_subscribers(conn, tenant.slug)
    finally:
        conn.close()

    if not subscribers:
        logger.info(
            "SMB blast: no matching subscribers for tenant=%s segment=%s",
            tenant.slug, segment["name"] if segment else "all",
        )
        return

    # Strip the segment prefix from the outbound message body
    body = _format_blast(_strip_segment_prefix(message_text, segment), tenant)
    provider = resolve_broadcast_provider()
    phones = [s["phone_number"] for s in subscribers]
    seg_name = segment["name"] if segment else None

    logger.info(
        "SMB blast starting: tenant=%s segment=%s recipients=%d provider=%s",
        tenant.slug, seg_name or "all", len(phones), provider,
    )

    result = run_loop_broadcast(
        phones=phones,
        body=body,
        provider=provider,
        deliver_whatsapp=False,
        slicktext_send=_slicktext_send_one,
    )

    logger.info(
        "SMB blast complete: tenant=%s segment=%s attempted=%d succeeded=%d failed=%d",
        tenant.slug, seg_name or "all",
        result.attempted, result.succeeded, result.failed,
    )

    _record_blast(tenant, message_text, body, result.attempted, result.succeeded, seg_name)


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

def _strip_segment_prefix(message_text: str, segment: Optional[dict]) -> str:
    """Remove the 'SEGMENT_NAME: ' prefix from the owner's message if present."""
    if not segment:
        return message_text.strip()
    prefix = segment["name"].upper() + ":"
    stripped = message_text.strip()
    if stripped.upper().startswith(prefix):
        return stripped[len(prefix):].strip()
    return stripped


def _format_blast(owner_message: str, tenant: BusinessTenant) -> str:
    """
    Format the (already prefix-stripped) owner message as the outbound body.
    Prepends the business name if it's not already there so subscribers
    know who's texting them.
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
    segment: Optional[str] = None,
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
                        (tenant_slug, owner_message, body, attempted, succeeded, segment)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (tenant.slug, owner_message[:500], body[:500], attempted, succeeded, segment),
                )
    except Exception:
        logger.exception("SMB blast: failed to record blast for tenant=%s", tenant.slug)
    finally:
        conn.close()
