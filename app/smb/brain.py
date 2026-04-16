"""
SMB brain: inbound message router.

Every SMS that arrives on an SMB Twilio number flows through here.
Routes to the correct handler based on who sent it and what state they're in.

Decision tree:
  1. Look up which business owns the destination (To) number.
     → Unknown To number: return None (caller should not route here).
  2. Is the sender the registered business owner?
     → blast.handle_owner_blast() — parse and broadcast, or return help.
  3. Is the message a signup keyword or mid-onboarding reply?
     → onboarding.get_onboarding_reply() — run the intake flow.
  4. Is the sender an active subscriber?
     → _conversational_reply() — short friendly AI response in business voice.
  5. Unknown sender who didn't opt in with this message?
     → _signup_nudge() — invite to join, reply YES (no STOP required).

Entry point: SMBBrain.handle_message(from_number, to_number, message_text)
Returns a reply string or None (None = message should be silently dropped).
"""

import logging
import threading
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb import ai as smb_ai
from app.smb import blast, onboarding, knowledge, tagging
from app.smb import storage as smb_storage
from app.smb.tenants import BusinessTenant, get_registry

logger = logging.getLogger(__name__)


class SMBBrain:

    def handle_message(
        self,
        from_number: str,
        to_number: str,
        message_text: str,
    ) -> Optional[str]:
        """
        Route an inbound SMS to the correct handler.

        from_number: the sender's phone (E.164)
        to_number:   the Twilio number that was texted (identifies the tenant)
        message_text: raw message body

        Returns reply text, or None if nothing should be sent back.
        """
        registry = get_registry()
        tenant = registry.get_by_to_number(to_number)

        if tenant is None:
            logger.warning(
                "SMB brain: unrecognised To number %s — dropping",
                to_number[-4:] if to_number else "?",
            )
            return None

        # --- Owner commands ---
        if registry.is_owner(from_number, tenant):
            logger.info("SMB brain: owner message → blast handler (tenant=%s)", tenant.slug)
            history = _save_and_get_history(from_number, tenant, message_text, role="user")
            reply = blast.handle_owner_blast(from_number, message_text, history, tenant)
            if reply:
                _persist_message(from_number, tenant, reply, role="assistant")
            return reply

        # --- Show check-in keyword ---
        # Check before onboarding so a fan who texts a show keyword mid-conversation
        # gets the check-in confirmation rather than a generic onboarding nudge.
        checkin_reply = _try_show_checkin(from_number, message_text, tenant)
        if checkin_reply is not None:
            logger.info(
                "SMB brain: show check-in keyword matched (tenant=%s phone=...%s)",
                tenant.slug, from_number[-4:] if from_number else "?",
            )
            return checkin_reply

        # --- Onboarding flow (keyword or mid-intake reply) ---
        onboarding_reply = onboarding.get_onboarding_reply(
            from_number, message_text, tenant
        )
        if onboarding_reply is not None:
            logger.info(
                "SMB brain: onboarding message → onboarding handler (tenant=%s phone=...%s)",
                tenant.slug, from_number[-4:] if from_number else "?",
            )
            return onboarding_reply

        # --- Regular subscriber or unknown sender ---
        subscriber = _get_subscriber(from_number, tenant)

        if subscriber is None:
            logger.info(
                "SMB brain: unknown sender → signup nudge (tenant=%s phone=...%s)",
                tenant.slug, from_number[-4:] if from_number else "?",
            )
            return _signup_nudge(tenant)

        logger.info(
            "SMB brain: active subscriber → conversational reply (tenant=%s phone=...%s)",
            tenant.slug, from_number[-4:] if from_number else "?",
        )

        # Persist inbound message and fetch recent history before generating reply
        history = _save_and_get_history(from_number, tenant, message_text, role="user")

        # Update engagement / intent tags in the background (never blocks the reply)
        inbound_count = sum(1 for m in history if m["role"] == "user")
        threading.Thread(
            target=tagging.tag_engagement_async,
            args=(from_number, subscriber["id"], tenant.slug, message_text, inbound_count),
            daemon=True,
        ).start()

        reply = _conversational_reply(message_text, tenant, history=history)

        # Persist the bot's reply
        if reply:
            _persist_message(from_number, tenant, reply, role="assistant")

        return reply


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_subscriber(phone_number: str, tenant: BusinessTenant) -> Optional[dict]:
    """Return subscriber row if they exist (any status), or None."""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn:
            return smb_storage.get_subscriber(conn, phone_number, tenant.slug)
    except Exception:
        logger.exception("SMB brain: DB error checking subscriber")
        return None
    finally:
        conn.close()


def _signup_nudge(tenant: BusinessTenant) -> str:
    """
    Invite an unknown sender to join — sent before they've opted in so no
    STOP line is required or appropriate here.
    Uses tenant-specific copy when configured; falls back to a generic line.
    """
    if tenant.signup_nudge:
        return tenant.signup_nudge
    return (
        f"Hey! {tenant.display_name} here. Want updates and exclusive offers? "
        "Reply YES to join — it's free!"
    )


def _conversational_reply(
    message_text: str,
    tenant: BusinessTenant,
    history: Optional[list] = None,
) -> Optional[str]:
    """
    Short friendly AI reply in the business's voice.
    Injects relevant club knowledge and recent conversation history.
    Falls back across Gemini → OpenAI → Anthropic automatically.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    venue_tz = getattr(tenant, "timezone", "America/New_York")
    try:
        local_tz = ZoneInfo(venue_tz)
    except Exception:
        local_tz = ZoneInfo("America/New_York")
    now_local = datetime.now(local_tz)
    today_label = now_local.strftime("%A, %B %-d, %Y")   # e.g. "Saturday, April 11, 2026"
    time_label  = now_local.strftime("%-I:%M %p %Z")     # e.g. "7:13 PM EDT"

    context = knowledge.build_context(tenant, message_text)

    prompt = (
        f"You are the friendly SMS assistant for {tenant.display_name}, a {tenant.business_type}. "
        f"Your tone: {tenant.tone}. "
        f"Keep replies very short (1-3 sentences), warm, and on-brand. "
        f"Never mention competitors. Do not use emojis unless the subscriber uses them first.\n\n"
        f"TODAY IS {today_label}. Current time: {time_label}.\n\n"
    )
    if context:
        prompt += (
            f"Use ONLY the following facts to answer. "
            f"Be strictly accurate about show dates and times — never guess, infer, or mix up days. "
            f"The schedule below uses day labels (Tonight, Tomorrow, Monday Apr 14, etc.) — "
            f"match those labels exactly to the question. "
            f"If asked about 'tonight' or 'today', only mention shows listed under 'Tonight'. "
            f"If asked about 'tomorrow', only mention shows listed under 'Tomorrow'. "
            f"Only include what is relevant to the question.\n"
            f"{context}\n\n"
        )
    # Add recent conversation so the AI can follow the thread
    if history:
        convo_lines = "\n".join(
            f"{'Subscriber' if m['role'] == 'user' else 'You'}: {m['body']}"
            for m in history[:-1]  # exclude the message we're about to answer
        )
        if convo_lines:
            prompt += f"Recent conversation:\n{convo_lines}\n\n"

    prompt += f"Subscriber: {message_text}"

    reply = smb_ai.generate(prompt)
    if not reply:
        logger.warning("SMB brain: all AI providers failed for conversational reply")
    return reply or None


def _save_and_get_history(
    phone_number: str,
    tenant: BusinessTenant,
    message_text: str,
    role: str,
) -> list:
    """Save a message and return the updated conversation history (oldest-first)."""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn:
            smb_storage.save_message(conn, tenant.slug, phone_number, role, message_text)
            return smb_storage.get_history(conn, tenant.slug, phone_number, limit=8)
    except Exception:
        logger.exception("SMB brain: failed to save/fetch history")
        return []
    finally:
        conn.close()


def _persist_message(phone_number: str, tenant: BusinessTenant, text: str, role: str) -> None:
    """Fire-and-forget: persist an assistant message to conversation history."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn:
            smb_storage.save_message(conn, tenant.slug, phone_number, role, text)
    except Exception:
        logger.exception("SMB brain: failed to persist assistant message")
    finally:
        conn.close()


def _try_show_checkin(
    phone_number: str,
    message_text: str,
    tenant: BusinessTenant,
) -> Optional[str]:
    """
    If the message exactly matches an active show's check-in keyword, record the
    attendance and return a warm confirmation reply. Returns None if no match.

    Keywords are short (e.g. "APR13", "FRIDAY", "SAT8PM") and matched case-insensitively.
    We only match single-word messages to avoid false positives with conversational text.
    """
    word = message_text.strip()
    # Only match single-word messages to avoid false positives
    if not word or len(word.split()) > 2:
        return None

    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn:
            show = smb_storage.get_show_by_keyword(conn, tenant.slug, word)
            if show is None:
                return None
            is_new = smb_storage.record_checkin(conn, show["id"], phone_number, tenant.slug)
    except Exception:
        logger.exception("SMB brain: error during show check-in (tenant=%s)", tenant.slug)
        return None
    finally:
        conn.close()

    if is_new:
        if tenant.checkin_confirmation:
            return tenant.checkin_confirmation
        return (
            f"You're checked in for {show['name']}! "
            "Thanks for coming — enjoy the event! 🎉"
        )
    return f"You're already checked in for {show['name']} — see you there! 🎉"


def create_smb_brain() -> SMBBrain:
    """Factory — returns the single SMBBrain instance for use in blueprint.py."""
    return SMBBrain()
