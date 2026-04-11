"""
SMB owner blast: command detection and subscriber broadcast.

When the business owner texts the bot's number, this module determines
whether their message is an availability/offer command and, if so,
kicks off a broadcast to all active subscribers.

Flow:
  1. Owner texts "opening tonight at 8pm — 20% off tickets"
  2. handle_owner_blast() routes: stats query → pending reply → new blast → help
  3. For a new blast: saves as pending, asks owner who to send to (with bullet list)
  4. Owner replies with audience choice (free text, AI-interpreted)
  5. _run_blast_async() fetches subscribers, AI-enhances body, sends via Twilio
  6. Owner receives a confirmation SMS with sent/attempted counts

Owner can reply "cancel" at any time to abort a pending blast.
Non-blast messages from the owner show a help text instead of entering the flow.
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


_CANCEL_RE = re.compile(
    r"^\s*(cancel|nevermind|never mind|stop|abort|don't send|dont send|forget it)\s*$",
    re.IGNORECASE,
)


def _is_cancel(text: str) -> bool:
    """Return True if the owner wants to abort a pending blast."""
    return bool(_CANCEL_RE.match(text.strip()))


def _looks_like_blast_intent(text: str, tenant: BusinessTenant) -> bool:
    """
    Return True if the owner's message looks like something to blast to subscribers.
    First tries the fast keyword check; falls back to AI for messages that don't
    match triggers but still look like a promo (e.g. '7pm 25% off').
    """
    if is_blast_command(text, tenant):
        return True

    prompt = (
        f"A business owner texted: \"{text}\"\n"
        f"Is this a message they want to send as an SMS blast to their subscribers "
        f"(a promo, announcement, deal, event info, etc.)?\n"
        f"Reply YES or NO only."
    )
    try:
        result = smb_ai.generate(prompt).strip().upper()
        return result.startswith("YES")
    except Exception:
        logger.warning("SMB blast: AI intent check failed, defaulting to False")
        return False


def _get_expired_pending(owner_phone: str) -> Optional[dict]:
    """
    Return the most recent pending blast for this owner if it expired within
    the last 10 minutes (i.e. created between 10–20 minutes ago).
    Used to inform the owner their window lapsed rather than silently restarting.
    """
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_slug, message_text, created_at
                    FROM smb_pending_blasts
                    WHERE owner_phone = %s
                      AND created_at <= NOW() - INTERVAL '%s seconds'
                      AND created_at >  NOW() - INTERVAL '1200 seconds'
                    """,
                    (owner_phone, smb_storage._PENDING_TTL_SECONDS),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {"tenant_slug": row[0], "message_text": row[1], "created_at": row[2]}
    except Exception:
        logger.exception("SMB blast: failed to check expired pending for %s", owner_phone[-4:])
        return None
    finally:
        conn.close()


def _seg_display_name(seg: dict) -> str:
    """Short, SMS-friendly label for a segment (trims long descriptions at parens/commas)."""
    desc = seg.get("description", seg["name"])
    desc = desc.split("(")[0].split(",")[0].strip()
    return desc if len(desc) <= 35 else seg["name"].title()


def _format_audience_question(message: str, tenant: BusinessTenant) -> str:
    """
    Build the audience clarification SMS with a bullet list of segment options.
    Highlights the AI-suggested best fit segment, if any.
    """
    if not tenant.segments:
        return (
            "Got it! Send to everyone, or a specific group?\n\n"
            "Reply with your choice, or 'cancel' to abort."
        )

    suggested = _ai_suggest_segment(message, tenant)
    suggested_name = suggested["name"] if suggested else None

    bullets = []
    for seg in tenant.segments:
        label = _seg_display_name(seg)
        marker = " ← suggested" if suggested_name and seg["name"] == suggested_name else ""
        bullets.append(f"• {label}{marker}")

    bullet_block = "\n".join(bullets)

    return (
        f"Got it! Who should get this blast?\n\n"
        f"Everyone, or just:\n"
        f"{bullet_block}\n\n"
        f"Reply with your choice, or 'cancel' to abort."
    )


def _owner_help(tenant: BusinessTenant) -> str:
    """Help text shown when the owner sends something that isn't a blast or stats query."""
    return (
        f"Hey! Here's what I can do:\n\n"
        f"• Send a blast — just text me your message (e.g. '7pm 25% off tonight')\n"
        f"• Audience stats — text 'stats' or 'how many subscribers'\n\n"
        f"I'll ask who to send to before anything goes out."
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
    1. Cancel command            → abort any pending blast
    2. Audience stats query      → only when no pending blast (avoids "fans" false-triggering)
    3. Expired pending blast     → inform owner the 10-min window lapsed
    4. Active pending reply      → AI interprets free-text audience choice → send blast
    5. Blast intent detected     → save as pending, ask who to send to (bullet list)
    6. Not a blast               → show help text
    """
    text = message_text.strip()

    # ── 2. Cancel command ──
    if _is_cancel(text):
        had_pending = _get_pending(phone_number)
        _clear_pending(phone_number)
        if had_pending:
            return "Got it, blast cancelled. Nothing was sent."
        return "No blast pending right now. Text me a message whenever you're ready to send one."

    # Fetch pending state once — used by steps 1, 3, and 4.
    active_pending = _get_pending(phone_number)

    # ── 1. Audience stats query — only when NOT mid-blast-flow ──
    # Must run after cancel check but before pending reply so that words like
    # "fans" or "audience" in an audience-selection reply don't trigger stats.
    if active_pending is None and _is_audience_query(text):
        return _get_audience_stats(tenant)

    # ── 3. Expired pending — inform the owner before doing anything else ──
    if active_pending is None:
        expired = _get_expired_pending(phone_number)
        if expired and expired["tenant_slug"] == tenant.slug:
            _clear_pending(phone_number)  # clean up the stale record
            short = expired["message_text"][:60]
            return (
                f"That blast window expired — it's been over 10 minutes since you drafted "
                f"\"{short}{'…' if len(expired['message_text']) > 60 else ''}\".\n\n"
                f"Send me the message again to start a new one."
            )

    # ── 4. Reply to an active pending clarification (AI-interpreted free text) ──
    if active_pending and active_pending["tenant_slug"] == tenant.slug:
        _clear_pending(phone_number)
        segment = _ai_classify_audience_reply(text, tenant)
        threading.Thread(
            target=_run_blast_async,
            args=(active_pending["message_text"], tenant, segment, phone_number),
            daemon=True,
        ).start()
        if segment:
            return f"Sending to your {_seg_display_name(segment).lower()} subscribers now."
        return "Sending to all your active subscribers now."

    # ── 5. New blast intent — set pending and ask who to send to ──
    if _looks_like_blast_intent(text, tenant):
        _set_pending(phone_number, text, tenant)
        return _format_audience_question(text, tenant)

    # ── 6. Not a blast — show help ──
    return _owner_help(tenant)


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
        f"The owner of {tenant.display_name} wants to send this SMS blast to subscribers:\n"
        f"\"{owner_message}\"\n\n"
        f"Lightly clean it up — fix typos, expand shorthand (e.g. 'tn' → 'tonight', 'tmrw' → 'tomorrow'), "
        f"and make it feel warm and human, like a text from a friend who wants you to come out. "
        f"CRITICAL: keep every fact exactly as stated — same discount %, same time, same event. "
        f"Do NOT add, invent, or change any details. "
        f"Do NOT make it sound like marketing or a brand. "
        f"1-2 sentences max. Plain text only. No emojis unless the original has them. "
        f"Return only the final message, nothing else.\n\n"
        f"Examples:\n"
        f"Input: '30% off standup tonight'\n"
        f"Output: 'Hey! 30% off standup tonight — would love to see you there :)'\n\n"
        f"Input: 'we have 25% off stand up comedy 7pm tn'\n"
        f"Output: 'Hey! We have 25% off stand up comedy tonight at 7pm — hope to see you there :)'\n\n"
        f"Input: 'last few seats for tonights show 8pm'\n"
        f"Output: 'Just a heads up — only a few seats left for tonight at 8. Grab one while you can.'\n\n"
        f"Input: 'improv night friday free drinks for first 20 people'\n"
        f"Output: 'Improv night this Friday — first 20 people get free drinks. Come early.'"
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
