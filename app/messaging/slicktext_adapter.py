"""
SlickText messaging adapter — supports both v1 (legacy) and v2 (new) accounts.

Auto-detects version based on which credentials are set in .env:

  v1 (legacy accounts, dashboard at www.slicktext.com/dashboard/):
    Auth:     HTTP Basic (public_key : private_key)
    Base URL: https://api.slicktext.com/v1/
    Inbound:  payload["ChatMessage"]["FromNumber"] + ["Body"]
    Outbound: POST /messages/ with form data (action, textword, number, body)

  v2 (new accounts created after Jan 22, 2025, dashboard at app.slicktext.com):
    Auth:     Authorization: Bearer {api_key}
    Base URL: https://dev.slicktext.com/v1/
    Inbound:  payload["data"]["contact_id"] + ["last_message"]
              (requires GET /brands/{brand_id}/contacts/{id} to get phone number)
    Outbound: POST /brands/{brand_id}/messages with JSON (mobile_number, body)
              (requires a paid plan; test accounts return 409)
"""

import logging
import re
import time
import unicodedata
from typing import Optional, Tuple

import requests

from app.config import (
    SLICKTEXT_PUBLIC_KEY,
    SLICKTEXT_PRIVATE_KEY,
    SLICKTEXT_TEXTWORD_ID,
    SLICKTEXT_API_KEY,
    SLICKTEXT_BRAND_ID,
)

logger = logging.getLogger(__name__)

_V1_BASE = "https://api.slicktext.com/v1"
_V2_BASE = "https://dev.slicktext.com/v1"

# Reaction prefixes sent by iPhone (iOS) and Android when someone reacts to a message
_REACTION_PREFIXES = (
    "reacted ",          # iOS: "Reacted 😂 to "...""
    "liked ",            # Android: "Liked "...""
    "loved ",            # Android: "Loved "...""
    "laughed at ",       # Android: "Laughed at "...""
    "emphasized ",       # Android: "Emphasized "...""
    "disliked ",         # Android: "Disliked "...""
    "questioned ",       # Android: "Questioned "...""
)


# SlickText / carrier reserved keywords — handled automatically by SlickText,
# but the webhook still fires. We drop these so the AI never responds to them.
_RESERVED_KEYWORDS = {
    # Opt-out (carrier-mandated)
    "stop", "stopall", "unsubscribe", "cancel", "end", "quit",
    # Opt-in
    "start", "yes", "unstop",
    # Help
    "help", "info",
    # Common subscription keywords Zarna uses
    "zarna",
}


def _is_reserved_keyword(message: str) -> bool:
    """Return True if the entire message is a SlickText/carrier reserved keyword."""
    return message.strip().lower() in _RESERVED_KEYWORDS


def _is_reaction(message: str) -> bool:
    """Return True if the message is an iOS/Android emoji reaction to a previous text."""
    lower = message.lower()
    return any(lower.startswith(p) for p in _REACTION_PREFIXES)


def _is_emoji_only(message: str) -> bool:
    """Return True if the message contains no real words — just emoji, punctuation, or whitespace."""
    # Strip whitespace; if nothing left, it's empty
    stripped = message.strip()
    if not stripped:
        return True
    # If every character is emoji, punctuation, or symbol (no letters/digits), treat as emoji-only
    for char in stripped:
        cat = unicodedata.category(char)
        # L = letter, N = number — if any of these exist, it's a real word message
        if cat.startswith("L") or cat.startswith("N"):
            return False
    return True


class SlickTextAdapter:

    def __init__(
        self,
        # v1 credentials
        public_key: str = SLICKTEXT_PUBLIC_KEY,
        private_key: str = SLICKTEXT_PRIVATE_KEY,
        textword_id: str = SLICKTEXT_TEXTWORD_ID,
        # v2 credentials
        api_key: str = SLICKTEXT_API_KEY,
        brand_id: str = SLICKTEXT_BRAND_ID,
    ):
        self._public_key  = public_key
        self._private_key = private_key
        self._textword_id = textword_id
        self._api_key     = api_key
        self._brand_id    = brand_id

        # Use v1 if public/private keys are present, otherwise v2
        self._version = "v1" if public_key and private_key else "v2"
        logger.info(f"SlickTextAdapter using API {self._version}")

    # ------------------------------------------------------------------
    # Inbound
    # ------------------------------------------------------------------

    def peek_inbound(self, payload: dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Raw FromNumber + Body (or v2 equivalent) with no keyword/reaction filtering.
        Use for live-show signup before filter_inbound_for_ai drops marketing keywords.
        """
        if self._version == "v1":
            return self._peek_inbound_v1(payload)
        return self._peek_inbound_v2(payload)

    def filter_inbound_for_ai(
        self, phone: Optional[str], message: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Apply reserved-keyword / reaction / emoji-only rules for the AI reply path."""
        if not message:
            return phone, message

        if _is_reserved_keyword(message):
            logger.info(f"Ignoring reserved keyword from {phone}: {message!r}")
            return None, None

        if _is_reaction(message):
            logger.info(f"Ignoring reaction from {phone}: {message[:60]}")
            return None, None

        if _is_emoji_only(message):
            logger.info(f"Ignoring emoji-only message from {phone}: {message[:60]}")
            return None, None

        return phone, message

    def parse_inbound(self, payload: dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract (phone_number, message_text) from a SlickText webhook payload.
        Handles both v1 and v2 payload shapes automatically.
        """
        p, m = self.peek_inbound(payload)
        return self.filter_inbound_for_ai(p, m)

    def _peek_inbound_v1(self, payload: dict) -> Tuple[Optional[str], Optional[str]]:
        """
        v1 payload: SlickText sends a form POST with a single field "data"
        whose value is a JSON string:
        {
          "data": "{\"Event\":\"ChatMessageRecieved\",
                   \"ChatMessage\":{\"FromNumber\":\"+1...\",\"Body\":\"...\"}}"
        }
        """
        import json as _json

        raw = payload.get("data")
        if raw:
            try:
                payload = _json.loads(raw)
            except (ValueError, TypeError):
                pass

        chat = payload.get("ChatMessage", {})
        phone = chat.get("FromNumber", "").strip() or None
        message = chat.get("Body", "").strip() or None
        return phone, message

    def _peek_inbound_v2(self, payload: dict) -> Tuple[Optional[str], Optional[str]]:
        """
        v2 payload shape:
        {
          "name": "inbox_message_received",
          "data": {
            "contact_id": 1111111,
            "last_message": "tell me a joke",
            "last_message_direction": "incoming"
          }
        }
        """
        data = payload.get("data", {})

        if data.get("last_message_direction") == "outgoing":
            return None, None

        message_text = data.get("last_message", "").strip() or None
        contact_id = data.get("contact_id")

        if not contact_id or not message_text:
            return None, None

        phone_number = self._lookup_phone_v2(contact_id)
        return phone_number, message_text

    def _lookup_phone_v2(self, contact_id: int) -> Optional[str]:
        if not self._api_key or not self._brand_id:
            logger.warning("SlickText v2 credentials not configured — cannot look up contact.")
            return None
        url = f"{_V2_BASE}/brands/{self._brand_id}/contacts/{contact_id}"
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json().get("mobile_number")
            logger.error(f"Contact lookup failed: {resp.status_code} — {resp.text}")
        except requests.RequestException as e:
            logger.error(f"Contact lookup error: {e}")
        return None

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    # Hard cap: Unicode chars (—, …, emoji) force 67-char/segment encoding on SMS.
    # 400 chars keeps us comfortably within 6 segments; SlickText rejects anything longer.
    _SMS_HARD_CAP = 400

    def _enforce_sms_length(self, body: str) -> str:
        """Truncate to _SMS_HARD_CAP chars. Logs a warning if truncation occurs."""
        if len(body) <= self._SMS_HARD_CAP:
            return body
        truncated = body[: self._SMS_HARD_CAP].rsplit(" ", 1)[0] + "…"
        logger.warning(
            "SMS body truncated from %d to %d chars before send",
            len(body),
            len(truncated),
        )
        return truncated

    def send_reply(self, to_number: str, body: str) -> bool:
        """Send an outbound SMS reply. Routes to v1 or v2 automatically."""
        if not (body or "").strip():
            logger.info("[REPLY TO %s]: skipped (empty body)", to_number)
            return False
        body = self._enforce_sms_length(body)
        logger.info(f"[REPLY TO {to_number}]: {body}")
        if self._version == "v1":
            return self._send_v1(to_number, body)
        return self._send_v2(to_number, body)

    def _send_v1(self, to_number: str, body: str) -> bool:
        if not self._public_key or not self._private_key:
            logger.warning("SlickText v1 API keys not configured — reply not sent.")
            return False
        if not self._textword_id:
            logger.warning("SLICKTEXT_TEXTWORD_ID not configured — reply not sent.")
            return False

        url  = f"{_V1_BASE}/messages/"
        data = {
            "action":   "SEND",
            "textword": self._textword_id,
            "number":   to_number,
            "body":     body,
        }
        for attempt in range(3):
            try:
                resp = requests.post(
                    url,
                    data=data,
                    auth=(self._public_key, self._private_key),
                    timeout=10,
                )
                if resp.status_code == 200:
                    logger.info(f"Reply sent to {to_number} (v1)")
                    return True
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"SlickText v1 rate-limited (attempt {attempt+1}/3) — retrying in {wait}s")
                    time.sleep(wait)
                    continue
                logger.error(f"SlickText v1 send failed: {resp.status_code} — {resp.text}")
                return False
            except requests.RequestException as e:
                logger.error(f"SlickText v1 request error: {e}")
                return False
        logger.error(f"SlickText v1 send failed after 3 attempts (rate limited): {to_number}")
        return False

    def _send_v2(self, to_number: str, body: str) -> bool:
        if not self._api_key or not self._brand_id:
            logger.warning("SlickText v2 credentials not configured — reply not sent.")
            return False

        url = f"{_V2_BASE}/brands/{self._brand_id}/messages"
        for attempt in range(3):
            try:
                resp = requests.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self._api_key}",
                        "Content-Type":  "application/json",
                    },
                    json={"mobile_number": to_number, "body": body},
                    timeout=10,
                )
                if resp.status_code == 200:
                    logger.info(f"Reply sent to {to_number} (v2)")
                    return True
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"SlickText v2 rate-limited (attempt {attempt+1}/3) — retrying in {wait}s")
                    time.sleep(wait)
                    continue
                logger.error(f"SlickText v2 send failed: {resp.status_code} — {resp.text}")
                return False
            except requests.RequestException as e:
                logger.error(f"SlickText v2 request error: {e}")
                return False
        logger.error(f"SlickText v2 send failed after 3 attempts (rate limited): {to_number}")
        return False


def create_slicktext_adapter() -> SlickTextAdapter:
    return SlickTextAdapter()
