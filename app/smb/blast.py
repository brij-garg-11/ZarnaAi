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
import time
from typing import Optional

from app.admin_auth import get_db_connection
from app.messaging.broadcast import run_loop_broadcast, resolve_broadcast_provider
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pending clarification state
# In-memory store: owner_phone → {message_text, tenant_slug, hinted_segment, ts}
# Expires after 10 minutes of inactivity.
# ---------------------------------------------------------------------------
_PENDING: dict = {}
_PENDING_TTL = 600  # seconds


def _set_pending(owner_phone: str, message_text: str, tenant: BusinessTenant, hinted_segment: dict) -> None:
    _PENDING[owner_phone] = {
        "message_text": message_text,
        "tenant_slug": tenant.slug,
        "hinted_segment": hinted_segment,
        "ts": time.monotonic(),
    }


def _get_pending(owner_phone: str) -> Optional[dict]:
    entry = _PENDING.get(owner_phone)
    if not entry:
        return None
    if time.monotonic() - entry["ts"] > _PENDING_TTL:
        _PENDING.pop(owner_phone, None)
        return None
    return entry


def _clear_pending(owner_phone: str) -> None:
    _PENDING.pop(owner_phone, None)


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

_AUDIENCE_QUERY_PATTERNS = re.compile(
    r"\b(how many|count|total|number of|stats|statistics|breakdown|"
    r"subscribers?|fans?|audience|segment|who likes?|who signed|how big)\b",
    re.IGNORECASE,
)


def _is_audience_query(text: str) -> bool:
    """Return True if the owner is asking about their subscriber counts/stats."""
    return bool(_AUDIENCE_QUERY_PATTERNS.search(text.strip()))


def _hint_segment(text: str, tenant: BusinessTenant) -> Optional[dict]:
    """
    Check if the blast message hints at a specific segment without explicitly
    prefixing it. Looks for the segment name or its hint_keywords in the body.

    e.g. "20% off standup tickets tonight" → hinted STANDUP segment
    """
    lower = text.strip().lower()
    for seg in tenant.segments:
        # Check the segment name itself
        name = seg["name"].lower()
        if re.search(r"\b" + re.escape(name) + r"\b", lower):
            return seg
        # Check any hint_keywords defined in config
        for kw in seg.get("hint_keywords", []):
            if re.search(r"\b" + re.escape(kw.lower()) + r"\b", lower):
                return seg
    return None


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

def _get_audience_stats(tenant: BusinessTenant) -> str:
    """Return a formatted audience breakdown for the owner."""
    conn = get_db_connection()
    if not conn:
        return "Sorry, I can't reach the database right now."
    try:
        with conn:
            all_subs = smb_storage.get_active_subscribers(conn, tenant.slug)
            total = len(all_subs)
            if not total:
                return f"You have 0 active subscribers on {tenant.display_name} yet."

            lines = [f"Your {tenant.display_name} audience ({total} active subscribers):"]
            for seg in tenant.segments:
                seg_subs = smb_storage.get_subscribers_by_segment(
                    conn, tenant.slug, seg["question_key"], seg["answers"]
                )
                pct = round((len(seg_subs) / total) * 100) if total else 0
                lines.append(f"  {seg['name']}: {len(seg_subs)} people ({pct}%)")

            return "\n".join(lines)
    except Exception:
        logger.exception("SMB: failed to get audience stats for tenant=%s", tenant.slug)
        return "Error fetching stats — check logs."
    finally:
        conn.close()


def handle_owner_blast(
    phone_number: str, message_text: str, tenant: BusinessTenant
) -> str:
    """
    Called when the registered owner sends a message to the bot.

    Routing logic (in order):
    1. Audience stats query   → return subscriber counts
    2. Pending clarification reply → owner answered SEGMENT or ALL → send blast
    3. Explicit segment prefix (STANDUP: ...) → send targeted blast immediately
    4. Blast command with hinted segment → ask for clarification
    5. Blast command, no hint → send to everyone
    6. Not a blast command → show help
    """
    text = message_text.strip()

    # ── 1. Audience stats query ──
    if _is_audience_query(text):
        return _get_audience_stats(tenant)

    # ── 2. Reply to a pending clarification ──
    pending = _get_pending(phone_number)
    if pending and pending["tenant_slug"] == tenant.slug:
        upper = text.upper()
        # Owner replied with a segment name (e.g. "STANDUP") or "ALL"/"EVERYONE"
        if upper in {"ALL", "EVERYONE", "ALL SUBSCRIBERS"}:
            _clear_pending(phone_number)
            threading.Thread(
                target=_run_blast_async,
                args=(pending["message_text"], tenant, None),
                daemon=True,
            ).start()
            return "Got it! Sending to all your active subscribers now."

        matched_seg = next(
            (s for s in tenant.segments if s["name"].upper() == upper), None
        )
        if matched_seg:
            _clear_pending(phone_number)
            threading.Thread(
                target=_run_blast_async,
                args=(pending["message_text"], tenant, matched_seg),
                daemon=True,
            ).start()
            return f"Got it! Sending to your {matched_seg['name']} subscribers now."

        # Unrecognised reply — nudge them
        seg_names = " / ".join(s["name"] for s in tenant.segments)
        return f"Reply with a segment ({seg_names}) or ALL to send to everyone."

    # ── Not a blast command ──
    if not is_blast_command(text, tenant):
        sample = ", ".join(f'"{t}"' for t in tenant.blast_triggers[:4])
        seg_hint = ""
        if tenant.segments:
            seg_hint = f' Tip: prefix with a segment to target a group — e.g. "{tenant.segments[0]["name"]}: your message".'
        return (
            f"To blast your subscribers, include a trigger word like {sample}. "
            f'Example: "Opening tonight 8pm — 20% off tickets".{seg_hint}'
        )

    # ── 3. Explicit segment prefix ──
    segment = _detect_segment(text, tenant)
    if segment:
        threading.Thread(
            target=_run_blast_async,
            args=(text, tenant, segment),
            daemon=True,
        ).start()
        return (
            f"Blast queued for your {segment['name']} subscribers! "
            "Check your weekly report for results."
        )

    # ── 4. No prefix — check for a hinted segment ──
    hinted = _hint_segment(text, tenant)
    if hinted and tenant.segments:
        # Store pending and ask for clarification
        _set_pending(phone_number, text, tenant, hinted)
        seg_names = " / ".join(s["name"] for s in tenant.segments)
        return (
            f'Would you like to send that to your {hinted["name"]} subscribers only, '
            f"or everyone? Reply: {hinted['name']} or ALL"
        )

    # ── 5. No hint — blast everyone ──
    threading.Thread(
        target=_run_blast_async,
        args=(text, tenant, None),
        daemon=True,
    ).start()
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
