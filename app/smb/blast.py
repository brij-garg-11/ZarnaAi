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
import time  # still used in _run_blast_async for rate-limiting sleep
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb import ai as smb_ai
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pending clarification state — DB-backed so all gunicorn workers share it
# ---------------------------------------------------------------------------

def _set_pending(owner_phone: str, message_text: str, tenant: BusinessTenant) -> None:
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn:
            smb_storage.set_pending_blast(conn, owner_phone, tenant.slug, message_text)
    except Exception:
        logger.exception("SMB blast: failed to set pending state for %s", owner_phone[-4:])
    finally:
        conn.close()


def _get_pending(owner_phone: str) -> Optional[dict]:
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn:
            return smb_storage.get_pending_blast(conn, owner_phone)
    except Exception:
        logger.exception("SMB blast: failed to get pending state for %s", owner_phone[-4:])
        return None
    finally:
        conn.close()


def _clear_pending(owner_phone: str) -> None:
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn:
            smb_storage.clear_pending_blast(conn, owner_phone)
    except Exception:
        logger.exception("SMB blast: failed to clear pending state for %s", owner_phone[-4:])
    finally:
        conn.close()


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
    Falls back across Gemini → OpenAI → Anthropic automatically.
    """
    if not tenant.segments:
        return None

    seg_lines = "\n".join(
        f"- {s['name']}: {s.get('description', s['name'])}"
        for s in tenant.segments
    )
    seg_names = [s["name"] for s in tenant.segments]

    prompt = (
        f"The owner of {tenant.display_name} was asked who they want to send a blast to. "
        f"They replied: \"{reply}\"\n\n"
        f"Available audience segments:\n{seg_lines}\n"
        f"- ALL: send to everyone\n\n"
        f"Which option best matches their intent? "
        f"Reply with ONLY one word: ALL, {', '.join(seg_names)}"
    )

    result = smb_ai.generate(prompt).upper()
    if not result:
        logger.warning("SMB blast: AI audience classification returned nothing, defaulting to all")
        return None

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
    return None


def _get_audience_stats(tenant: BusinessTenant) -> str:
    """Fetch live subscriber data and return an AI-written natural reply in the tenant's tone."""
    conn = get_db_connection()
    if not conn:
        return "Sorry, can't reach the database right now — try again in a sec."
    try:
        with conn:
            all_subs = smb_storage.get_active_subscribers(conn, tenant.slug)
            total = len(all_subs)

            seg_data = []
            for seg in tenant.segments:
                seg_subs = smb_storage.get_subscribers_by_segment(
                    conn, tenant.slug, seg["question_key"], seg["answers"]
                )
                pct = round((len(seg_subs) / total) * 100) if total else 0
                seg_data.append(
                    {"name": seg["name"], "description": seg.get("description", ""), "count": len(seg_subs), "pct": pct}
                )
    except Exception:
        logger.exception("SMB: failed to get audience stats for tenant=%s", tenant.slug)
        return "Couldn't pull stats right now — check the logs."
    finally:
        conn.close()

    return _ai_narrate_stats(total, seg_data, tenant)


def _ai_narrate_stats(total: int, seg_data: list, tenant: BusinessTenant) -> str:
    """Use AI to write a natural, tone-matched stats update for the owner."""
    if not total:
        # Even a zero-subscriber message gets the AI treatment
        facts = f"{tenant.display_name} currently has 0 active subscribers."
    else:
        seg_lines = "\n".join(
            f"- {s['name']} ({s['description']}): {s['count']} people ({s['pct']}%)"
            for s in seg_data
        )
        facts = (
            f"{tenant.display_name} has {total} active subscribers.\n"
            f"Breakdown by segment:\n{seg_lines}"
        )

    prompt = (
        f"You are the SMS assistant for {tenant.display_name}. "
        f"Tone: {tenant.tone}.\n\n"
        f"The owner just asked about their audience. "
        f"Reply to them naturally — like a smart friend who knows the numbers — "
        f"using the following facts:\n\n{facts}\n\n"
        f"Keep it short (2–4 sentences max), conversational, no bullet points or headers. "
        f"SMS only — plain text."
    )

    result = smb_ai.generate(prompt)
    if result:
        return result

    # Hard fallback if every AI provider is down
    logger.warning("SMB: all AI providers failed for stats narration (tenant=%s)", tenant.slug)
    if not total:
        return f"No active subscribers on {tenant.display_name} yet — keep spreading the word!"
    lines = [f"{tenant.display_name} has {total} active subscribers."]
    for s in seg_data:
        lines.append(f"{s['name']}: {s['count']} ({s['pct']}%)")
    return " | ".join(lines)


def _ai_suggest_segment(message: str, tenant: BusinessTenant) -> Optional[dict]:
    """
    Look at the blast message and suggest the single most relevant segment,
    or return None if the message seems relevant to everyone.
    Used to make the clarification question specific: 'Everyone or just standup fans?'
    """
    if not tenant.segments:
        return None

    seg_lines = "\n".join(
        f"- {s['name']}: {s.get('description', s['name'])}"
        for s in tenant.segments
    )
    seg_names = [s["name"] for s in tenant.segments]

    prompt = (
        f"A comedy club owner wants to send this blast to their SMS subscribers:\n"
        f"\"{message}\"\n\n"
        f"Available audience segments:\n{seg_lines}\n"
        f"- ALL: relevant to everyone\n\n"
        f"Which segment is this message MOST relevant to? "
        f"If it's equally relevant to everyone, reply ALL. "
        f"Reply with ONLY one word: ALL, {', '.join(seg_names)}"
    )

    result = smb_ai.generate(prompt).strip().upper()
    if not result or result == "ALL":
        return None

    matched = next((s for s in tenant.segments if s["name"].upper() == result), None)
    return matched


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
            args=(pending["message_text"], tenant, segment, phone_number),
            daemon=True,
        ).start()
        if segment:
            return f"Sending to your {segment['name']} subscribers now."
        return "Sending to all your active subscribers now."

    # ── Not a blast command ──
    if not is_blast_command(text, tenant):
        sample = ", ".join(f'"{t}"' for t in tenant.blast_triggers[:4])
        return (
            f"To blast your subscribers, include a trigger word like {sample}. "
            f'Example: "Opening tonight 8pm — 20% off tickets".'
        )

    # ── Blast command — ask who to send to (AI suggests the relevant segment) ──
    _set_pending(phone_number, text, tenant)
    suggested = _ai_suggest_segment(text, tenant)
    if suggested:
        return f"Send to everyone or just your {suggested['name'].lower()} fans?"
    elif tenant.segments:
        options = " or ".join(f"{s['name'].lower()} fans" for s in tenant.segments[:2])
        return f"Send to everyone or just your {options}?"
    return "Send to everyone or a specific group?"


# ---------------------------------------------------------------------------
# Async broadcast worker
# ---------------------------------------------------------------------------



def _run_blast_async(
    message_text: str,
    tenant: BusinessTenant,
    segment: Optional[dict] = None,
    owner_phone: Optional[str] = None,
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

    body = _ai_enhance_blast(message_text.strip(), tenant)
    phones = [s["phone_number"] for s in subscribers]
    seg_name = segment["name"] if segment else None

    logger.info(
        "SMB blast starting: tenant=%s segment=%s recipients=%d",
        tenant.slug, seg_name or "all", len(phones),
    )

    attempted = succeeded = failed = 0
    for phone in phones:
        attempted += 1
        if _twilio_send_smb(phone, body, tenant.sms_number):
            succeeded += 1
        else:
            failed += 1
        if len(phones) > 1:
            time.sleep(0.35)

    logger.info(
        "SMB blast complete: tenant=%s segment=%s attempted=%d succeeded=%d failed=%d",
        tenant.slug, seg_name or "all", attempted, succeeded, failed,
    )

    _record_blast(tenant, message_text, body, attempted, succeeded, seg_name)

    if owner_phone and tenant.sms_number:
        audience = f"your {seg_name.lower()} subscribers" if seg_name else "all your subscribers"
        confirmation = f"Done! Blast sent to {succeeded}/{attempted} {audience}."
        _twilio_send_smb(owner_phone, confirmation, tenant.sms_number)


def _twilio_send_smb(to: str, body: str, from_number: str) -> bool:
    """
    Send a single SMS via Twilio from the tenant's dedicated number.

    Hard guard: refuses to send if from_number is not a registered SMB tenant
    number, preventing accidental blasts from Zarna's number or any other number.
    """
    if not from_number:
        logger.error("SMB blast: tenant has no sms_number configured — refusing to send")
        return False

    # Firewall: only send from a number that belongs to an SMB tenant.
    from app.smb.tenants import get_registry
    if not get_registry().is_smb_number(from_number):
        logger.error(
            "SMB blast: from_number ...%s is not a registered SMB number — "
            "refusing to send. This is a routing bug.",
            from_number[-4:],
        )
        return False

    try:
        from twilio.rest import Client
        from app.config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN

        if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
            logger.error("SMB blast: Twilio credentials not configured")
            return False

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        client.messages.create(to=to, from_=from_number, body=body)
        return True
    except Exception as exc:
        logger.warning("SMB blast: Twilio send to ...%s failed: %s", to[-4:] if to else "?", exc)
        return False


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------


def _ai_enhance_blast(owner_message: str, tenant: BusinessTenant) -> str:
    """
    Rewrite the owner's raw message into an engaging subscriber-facing SMS
    in the tenant's tone. Falls back to a clean plain version if AI fails.
    """
    prompt = (
        f"You are writing an SMS blast for {tenant.display_name} subscribers. "
        f"Tone: {tenant.tone}.\n\n"
        f"The owner wants to send this message:\n\"{owner_message}\"\n\n"
        f"Rewrite it as an engaging, natural SMS that subscribers will want to read. "
        f"Keep all the key facts (time, discount, details) intact. "
        f"Keep it short — 2 sentences max. Plain text only, no emojis unless the original has them. "
        f"Do NOT start with the business name — the sender ID handles that."
    )

    enhanced = smb_ai.generate(prompt)
    if enhanced:
        return enhanced

    # Plain fallback if all AI providers are down
    logger.warning("SMB blast: AI enhancement failed for tenant=%s, using raw message", tenant.slug)
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
