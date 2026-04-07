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

def _ai_classify_audience_reply(reply: str, tenant: BusinessTenant) -> Optional[dict]:
    """
    Use AI to interpret the owner's free-text audience reply.

    Examples:
      "just standup fans" → STANDUP segment
      "everyone"          → None (all)
      "improv people"     → IMPROV segment
      "all of them"       → None (all)

    Returns the matching segment dict or None for all subscribers.
    """
    if not tenant.segments:
        return None

    seg_lines = "\n".join(
        f"- {s['name']}: {s.get('description', s['name'])}"
        for s in tenant.segments
    )
    seg_names = [s["name"] for s in tenant.segments]

    try:
        from google import genai
        from app.config import GEMINI_API_KEY, GENERATION_MODEL

        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set")

        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = (
            f"The owner of {tenant.display_name} was asked who they want to send a blast to. "
            f"They replied: \"{reply}\"\n\n"
            f"Available audience segments:\n{seg_lines}\n"
            f"- ALL: send to everyone\n\n"
            f"Which option best matches their intent? "
            f"Reply with ONLY one word: ALL, {', '.join(seg_names)}"
        )

        response = client.models.generate_content(model=GENERATION_MODEL, contents=prompt)
        result = (response.text or "").strip().upper()

        if result == "ALL":
            return None

        matched = next((s for s in tenant.segments if s["name"].upper() == result), None)
        if matched:
            logger.info(
                "SMB blast: AI classified audience reply '%s' → %s (tenant=%s)",
                reply[:40], matched["name"], tenant.slug,
            )
            return matched

        logger.warning("SMB blast: AI returned unknown audience '%s', defaulting to all", result)

    except Exception:
        logger.exception("SMB blast: AI audience classification failed, defaulting to all")

    return None


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
    1. Audience stats query    → return subscriber counts
    2. Pending clarification reply → AI interprets free-text → send blast
    3. Blast command           → always ask who to send to (AI interprets reply)
    4. Not a blast command     → show help
    """
    text = message_text.strip()

    # ── 1. Audience stats query ──
    if _is_audience_query(text):
        return _get_audience_stats(tenant)

    # ── 2. Reply to a pending clarification (AI-interpreted free text) ──
    pending = _get_pending(phone_number)
    if pending and pending["tenant_slug"] == tenant.slug:
        _clear_pending(phone_number)
        segment = _ai_classify_audience_reply(text, tenant)
        threading.Thread(
            target=_run_blast_async,
            args=(pending["message_text"], tenant, segment),
            daemon=True,
        ).start()
        if segment:
            return f"Got it! Sending to your {segment['name']} subscribers now."
        return "Got it! Sending to all your active subscribers now."

    # ── Not a blast command ──
    if not is_blast_command(text, tenant):
        sample = ", ".join(f'"{t}"' for t in tenant.blast_triggers[:4])
        return (
            f"To blast your subscribers, include a trigger word like {sample}. "
            f'Example: "Opening tonight 8pm — 20% off tickets".'
        )

    # ── Blast command — always ask who to send to ──
    _set_pending(phone_number, text, tenant, None)
    seg_examples = ", ".join(
        f'"{s["name"].lower()} fans"' for s in tenant.segments[:2]
    ) if tenant.segments else ""
    clarify = "Who would you like to send this to?"
    if seg_examples:
        clarify += f" (e.g. {seg_examples}, or everyone)"
    return clarify


# ---------------------------------------------------------------------------
# Async broadcast worker
# ---------------------------------------------------------------------------



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

    body = _format_blast(message_text.strip(), tenant)
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
