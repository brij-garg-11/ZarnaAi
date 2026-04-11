"""
SMB owner blast: AI-driven command understanding and subscriber broadcast.

When the business owner texts the bot's number, this module reads the full
conversation history and uses a single AI call to determine what to do:

  CLARIFY   → owner wants to blast but we haven't asked who to send it to yet
  SEND_BLAST → previous bot turn was CLARIFY, owner just replied with audience
  STATS     → owner asking about subscriber counts
  CANCEL    → owner wants to abort
  HELP      → anything else

The 2-step confirmation (CLARIFY → SEND_BLAST) is always enforced — the AI
prompt makes SEND_BLAST conditional on seeing a prior CLARIFY in the history.

Flow:
  1. brain.py saves the owner's message and fetches conversation history
  2. handle_owner_blast(phone_number, message_text, history, tenant) is called
  3. _ai_decide_owner_action() reads the transcript and returns a JSON decision
  4. We execute the decision: send blast, return stats, clarify, etc.
  5. brain.py saves the bot's reply to history
"""

import json
import logging
import re
import threading
import time
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb import ai as smb_ai
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AI decision engine
# ---------------------------------------------------------------------------

def _ai_decide_owner_action(
    message_text: str,
    history: list,
    tenant: BusinessTenant,
) -> dict:
    """
    Single AI call: reads the full owner conversation history and returns a
    structured decision dict:
      {"action": "CLARIFY|SEND_BLAST|STATS|CANCEL|HELP",
       "blast_message": "<original blast text>",
       "segment": "ALL|<SEGMENT_NAME>",
       "reply": "<bot reply to send>"}

    The AI prompt enforces the 2-step rule: SEND_BLAST is only valid if the
    immediately preceding bot message was a CLARIFY asking about audience.
    """
    seg_names = [s["name"] for s in tenant.segments] if tenant.segments else []
    valid_segments = ", ".join(seg_names) if seg_names else "ALL"

    if tenant.segments:
        seg_lines = "\n".join(
            f"- {s['name']}: {s.get('description', s['name'])}"
            for s in tenant.segments
        )
    else:
        seg_lines = "No segments defined — send to everyone (ALL)."

    # History already includes the current message (saved by brain.py before
    # calling us). Build a readable transcript oldest-first.
    history_text = (
        "\n".join(
            f"{'Owner' if m['role'] == 'user' else 'Bot'}: {m['body']}"
            for m in history
        )
        if history
        else "(no prior messages)"
    )

    prompt = f"""You manage the SMS blast tool for {tenant.display_name} (tone: {tenant.tone}).

Conversation history (oldest first — the last Owner line is the current message):
{history_text}

Available audience segments:
{seg_lines}
- ALL: send to everyone

Your job: decide the correct action and return a JSON object ONLY (no markdown, no explanation).

ACTIONS and when to use them:
- CLARIFY: owner's message looks like a blast they want to send, but you have NOT yet asked
  who to send it to in this conversation. Write a reply listing the segment options as bullet
  points so the owner can pick. ALWAYS use CLARIFY before SEND_BLAST — never skip this step.
- SEND_BLAST: use ONLY when the immediately preceding Bot message was a CLARIFY asking about
  audience, AND the owner just replied with their audience choice. Extract the original blast
  message from the owner's turn that triggered the CLARIFY (not the audience reply).
  segment = one of: ALL, {valid_segments}
- STATS: owner is asking about subscriber counts, audience breakdown, or segment sizes.
  Leave reply blank — real stats will be fetched from the database separately.
- CANCEL: owner wants to cancel or abort. Confirm in reply.
- HELP: anything else. Briefly explain what you can do (send blasts, check stats).

Return this exact JSON (all fields required, use empty string if not applicable):
{{"action": "CLARIFY|SEND_BLAST|STATS|CANCEL|HELP", "blast_message": "", "segment": "ALL", "reply": ""}}"""

    raw = smb_ai.generate(prompt) or ""

    # Strip markdown code fences if the model wraps the JSON
    raw = re.sub(r"^```[a-z]*\n?", "", raw.strip(), flags=re.IGNORECASE)
    raw = raw.rstrip("`").strip()

    try:
        data = json.loads(raw)
        action = str(data.get("action", "HELP")).upper().strip()
        if action not in {"CLARIFY", "SEND_BLAST", "STATS", "CANCEL", "HELP"}:
            logger.warning("SMB blast: AI returned unknown action '%s', defaulting to HELP", action)
            action = "HELP"
        return {
            "action": action,
            "blast_message": str(data.get("blast_message", "")).strip(),
            "segment": str(data.get("segment", "ALL")).upper().strip(),
            "reply": str(data.get("reply", "")).strip(),
        }
    except Exception:
        logger.warning(
            "SMB blast: AI decision JSON parse failed, raw=%s",
            raw[:300],
        )
        return {"action": "HELP", "blast_message": "", "segment": "ALL", "reply": ""}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def handle_owner_blast(
    phone_number: str,
    message_text: str,
    history: list,
    tenant: BusinessTenant,
) -> str:
    """
    Called when the registered business owner sends a message to the bot.

    brain.py saves the owner's message and fetches conversation history before
    calling this function, so `history` already includes the current message.

    Routing is entirely AI-driven — no regex, no DB state machine.
    The 2-step confirmation (CLARIFY → SEND_BLAST) is enforced in the AI prompt.
    """
    decision = _ai_decide_owner_action(message_text, history, tenant)
    action = decision["action"]

    logger.info(
        "SMB blast: AI decision action=%s segment=%s (tenant=%s)",
        action, decision["segment"], tenant.slug,
    )

    # ── Stats query ──
    if action == "STATS":
        return _get_audience_stats(tenant)

    # ── Cancel ──
    if action == "CANCEL":
        return decision["reply"] or "No blast pending right now."

    # ── Help ──
    if action == "HELP":
        return decision["reply"] or _owner_help(tenant)

    # ── Clarify: ask who to send to ──
    if action == "CLARIFY":
        return decision["reply"] or _owner_help(tenant)

    # ── Send blast ──
    if action == "SEND_BLAST":
        blast_message = decision["blast_message"]
        if not blast_message:
            # Safety: AI couldn't extract the original blast message
            logger.warning(
                "SMB blast: SEND_BLAST action but no blast_message extracted (tenant=%s)",
                tenant.slug,
            )
            return (
                "I lost track of the message to blast — could you send it again?"
            )

        # Resolve segment
        seg_name = decision["segment"]
        segment = None
        if seg_name and seg_name != "ALL" and tenant.segments:
            segment = next(
                (s for s in tenant.segments if s["name"].upper() == seg_name),
                None,
            )
            if segment is None:
                logger.warning(
                    "SMB blast: AI returned unknown segment '%s', sending to all (tenant=%s)",
                    seg_name, tenant.slug,
                )

        threading.Thread(
            target=_run_blast_async,
            args=(blast_message, tenant, segment, phone_number),
            daemon=True,
        ).start()

        if segment:
            return f"Sending to your {_seg_display_name(segment).lower()} subscribers now."
        return "Sending to all your active subscribers now."

    return _owner_help(tenant)


# ---------------------------------------------------------------------------
# Audience stats
# ---------------------------------------------------------------------------

def _get_audience_stats(tenant: BusinessTenant) -> str:
    """Fetch live subscriber data and return an AI-written natural reply."""
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
                seg_data.append({
                    "name": seg["name"],
                    "description": seg.get("description", ""),
                    "count": len(seg_subs),
                    "pct": pct,
                })
    except Exception:
        logger.exception("SMB: failed to get audience stats for tenant=%s", tenant.slug)
        return "Couldn't pull stats right now — check the logs."
    finally:
        conn.close()

    return _ai_narrate_stats(total, seg_data, tenant)


def _ai_narrate_stats(total: int, seg_data: list, tenant: BusinessTenant) -> str:
    """Use AI to write a natural, tone-matched stats update for the owner."""
    if not total:
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

    logger.warning("SMB: all AI providers failed for stats narration (tenant=%s)", tenant.slug)
    if not total:
        return f"No active subscribers on {tenant.display_name} yet — keep spreading the word!"
    lines = [f"{tenant.display_name} has {total} active subscribers."]
    for s in seg_data:
        lines.append(f"{s['name']}: {s['count']} ({s['pct']}%)")
    return " | ".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seg_display_name(seg: dict) -> str:
    """Short, SMS-friendly label for a segment."""
    desc = seg.get("description", seg["name"])
    desc = desc.split("(")[0].split(",")[0].strip()
    return desc if len(desc) <= 35 else seg["name"].title()


def _owner_help(tenant: BusinessTenant) -> str:
    """Fallback help text shown when the owner sends something unrecognised."""
    return (
        f"Hey! Here's what I can do:\n\n"
        f"• Send a blast — just text me your message (e.g. '7pm 25% off tonight')\n"
        f"• Audience stats — text 'stats' or 'how many subscribers'\n\n"
        f"I'll ask who to send to before anything goes out."
    )


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
