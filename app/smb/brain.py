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
  5. Unknown sender who hasn't signed up yet?
     → _signup_nudge() — "Text COMEDY to subscribe".

Entry point: SMBBrain.handle_message(from_number, to_number, message_text)
Returns a reply string or None (None = message should be silently dropped).
"""

import logging
from typing import Optional

from google import genai

from app.admin_auth import get_db_connection
from app.config import GEMINI_API_KEY, GENERATION_MODEL
from app.smb import blast, onboarding
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
            return blast.handle_owner_blast(from_number, message_text, tenant)

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
        return _conversational_reply(message_text, tenant)


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
    """Short prompt for someone who texted but hasn't signed up yet."""
    if tenant.keyword:
        return (
            f"Hey! Text {tenant.keyword} to subscribe to {tenant.display_name} "
            "for exclusive deals and updates."
        )
    return f"Hey! Ask {tenant.display_name} how to subscribe for exclusive deals and updates."


def _conversational_reply(message_text: str, tenant: BusinessTenant) -> Optional[str]:
    """
    Short friendly AI reply in the business's voice.
    Uses Gemini with a lightweight system prompt built from the tenant config.
    No RAG — keeps costs low and responses fast for subscriber conversation.
    """
    try:
        if not GEMINI_API_KEY:
            logger.warning("SMB brain: GEMINI_API_KEY not set — cannot generate reply")
            return None

        client = genai.Client(api_key=GEMINI_API_KEY)
        topics = ", ".join(tenant.value_content_topics) if tenant.value_content_topics else "general topics"
        prompt = (
            f"You are the friendly SMS assistant for {tenant.display_name}, a {tenant.business_type}. "
            f"Your tone: {tenant.tone}. "
            f"Keep replies very short (1-3 sentences), warm, and on-brand. "
            f"Stay on topic: {topics}. "
            f"Never mention competitors. Do not use emojis unless the subscriber uses them first.\n\n"
            f"Subscriber: {message_text}"
        )

        response = client.models.generate_content(model=GENERATION_MODEL, contents=prompt)
        reply = (response.text or "").strip()
        return reply if reply else None

    except Exception:
        logger.exception("SMB brain: conversational reply generation failed")
        return None


def create_smb_brain() -> SMBBrain:
    """Factory — returns the single SMBBrain instance for use in blueprint.py."""
    return SMBBrain()
