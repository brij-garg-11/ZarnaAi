"""
Twilio messaging adapter.

Inbound: Twilio POSTs form-encoded data to the webhook:
    From=+16467244908&Body=hello&To=+18557689537
or for WhatsApp:
    From=whatsapp:+16467244908&Body=hello&To=whatsapp:+14155238886

Outbound: Uses the Twilio REST API (twilio-python SDK) to send a reply.

Webhook security: Twilio signs every request with an X-Twilio-Signature
header. Call validate_signature() in the webhook route to reject spoofed
requests (optional but recommended in production).
"""

import logging
import os
import unicodedata
from typing import Optional, Tuple

from twilio.request_validator import RequestValidator
from twilio.rest import Client

from app.config import (
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_PHONE_NUMBER,
)

logger = logging.getLogger(__name__)

# Reaction prefixes — same set as SlickText adapter
_REACTION_PREFIXES = (
    "reacted ",
    "liked ",
    "loved ",
    "laughed at ",
    "emphasized ",
    "disliked ",
    "questioned ",
)

# Carrier / opt-out keywords to ignore
_RESERVED_KEYWORDS = {
    "stop", "stopall", "unsubscribe", "cancel", "end", "quit",
    "start", "yes", "unstop",
    "help", "info",
    "zarna",
}


def _is_reserved_keyword(message: str) -> bool:
    return message.strip().lower() in _RESERVED_KEYWORDS


def _is_reaction(message: str) -> bool:
    lower = message.lower()
    return any(lower.startswith(p) for p in _REACTION_PREFIXES)


def _is_emoji_only(message: str) -> bool:
    stripped = message.strip()
    if not stripped:
        return True
    for char in stripped:
        cat = unicodedata.category(char)
        if cat.startswith("L") or cat.startswith("N"):
            return False
    return True


def _is_whatsapp_number(value: str) -> bool:
    return str(value).startswith("whatsapp:")


def _ensure_whatsapp_prefix(value: str) -> str:
    value = str(value).strip()
    if value.startswith("whatsapp:"):
        return value
    return f"whatsapp:{value}"


def _strip_whatsapp_prefix(value: str) -> str:
    return str(value).replace("whatsapp:", "").strip()


class TwilioAdapter:

    def __init__(
        self,
        account_sid: str = TWILIO_ACCOUNT_SID,
        auth_token: str = TWILIO_AUTH_TOKEN,
        from_number: str = TWILIO_PHONE_NUMBER,
    ):
        self._account_sid = account_sid
        self._auth_token = auth_token
        self._from_number = from_number
        self._client = Client(account_sid, auth_token) if account_sid and auth_token else None
        self._validator = RequestValidator(auth_token) if auth_token else None
        logger.info("TwilioAdapter initialised (from=%s)", from_number or "not set")

    # ------------------------------------------------------------------
    # Webhook signature validation (call this in the route)
    # ------------------------------------------------------------------

    def validate_signature(self, url: str, post_data: dict, signature: str) -> bool:
        """Return True if the request is genuinely from Twilio."""
        if not self._validator:
            logger.warning("Twilio validator not configured — skipping signature check.")
            return True
        return self._validator.validate(url, post_data, signature)

    # ------------------------------------------------------------------
    # Inbound
    # ------------------------------------------------------------------

    def peek_inbound(self, form_data: dict) -> Tuple[Optional[str], Optional[str]]:
        """Raw From + Body for live-show signup (before AI filters)."""
        phone = form_data.get("From", "").strip() or None
        message = form_data.get("Body", "").strip() or None
        return phone, message

    def filter_inbound_for_ai(
        self, phone: Optional[str], message: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        if not phone or not message:
            return None, None

        if _is_reserved_keyword(message):
            logger.info("Ignoring reserved keyword from %s: %r", phone, message)
            return None, None

        if _is_reaction(message):
            logger.info("Ignoring reaction from %s: %.60s", phone, message)
            return None, None

        if _is_emoji_only(message):
            logger.info("Ignoring emoji-only message from %s: %.60s", phone, message)
            return None, None

        return phone, message

    def parse_inbound(self, form_data: dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract (phone_number, message_text) from a Twilio inbound webhook.
        form_data is request.form.to_dict() from Flask.
        """
        p, m = self.peek_inbound(form_data)
        return self.filter_inbound_for_ai(p, m)

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    def send_reply(self, to_number: str, body: str) -> bool:
        """Send an outbound reply via Twilio SMS or WhatsApp."""
        if not (body or "").strip():
            logger.info("[TWILIO REPLY TO %s]: skipped (empty body)", to_number)
            return False
        logger.info("[TWILIO REPLY TO %s]: %s", to_number, body)

        if not self._client:
            logger.warning("Twilio client not configured — reply not sent.")
            return False

        if not self._from_number:
            logger.warning("TWILIO_PHONE_NUMBER not configured — reply not sent.")
            return False

        try:
            is_whatsapp = _is_whatsapp_number(to_number)

            if is_whatsapp:
                whatsapp_from = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
                final_to = _ensure_whatsapp_prefix(to_number)
                final_from = _ensure_whatsapp_prefix(whatsapp_from)
            else:
                final_to = _strip_whatsapp_prefix(to_number)
                final_from = _strip_whatsapp_prefix(self._from_number)

            logger.info(
                "Sending Twilio reply via channel=%s from=%s to=%s",
                "whatsapp" if is_whatsapp else "sms",
                final_from,
                final_to,
            )

            msg = self._client.messages.create(
                to=final_to,
                from_=final_from,
                body=body,
            )
            logger.info("Twilio reply sent: SID=%s", msg.sid)
            return True

        except Exception as e:
            logger.error("Twilio send error: %s", e)
            return False


def create_twilio_adapter() -> TwilioAdapter:
    return TwilioAdapter()
