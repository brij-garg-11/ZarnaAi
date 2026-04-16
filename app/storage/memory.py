from datetime import datetime
from typing import Dict, List, Optional

from .base import BaseStorage
from .models import Contact, Message


class InMemoryStorage(BaseStorage):
    """
    In-memory storage implementation for development and testing.
    Swap this for a SQLite or Postgres implementation later without
    changing anything outside this file.
    """

    def __init__(self):
        self._contacts: Dict[str, Contact] = {}
        self._messages: Dict[str, List[Message]] = {}
        self._memory: Dict[str, str] = {}
        self._tags: Dict[str, list] = {}
        self._location: Dict[str, str] = {}
        self._next_id: int = 1
        # engagement context stored by message id
        self._reply_context: Dict[int, dict] = {}

    def save_contact(self, phone_number: str, source: Optional[str] = None) -> Contact:
        if phone_number not in self._contacts:
            self._contacts[phone_number] = Contact(
                phone_number=phone_number,
                source=source,
            )
        return self._contacts[phone_number]

    def get_contact(self, phone_number: str) -> Optional[Contact]:
        return self._contacts.get(phone_number)

    def save_message(self, phone_number: str, role: str, text: str) -> Message:
        msg = Message(phone_number=phone_number, role=role, text=text, id=self._next_id)
        self._next_id += 1
        self._messages.setdefault(phone_number, []).append(msg)
        return msg

    def get_conversation_history(self, phone_number: str, limit: int = 10) -> List[Message]:
        return self._messages.get(phone_number, [])[-limit:]

    def get_memory(self, phone_number: str) -> str:
        return self._memory.get(phone_number, "")

    def update_memory(self, phone_number: str, memory: str, tags: list, location: str = "") -> None:
        self._memory[phone_number] = memory[:400]
        self._tags[phone_number] = tags
        if location:
            self._location[phone_number] = location[:100]

    def get_fans_by_tag(self, tag: str) -> list:
        results = []
        for phone, t_list in self._tags.items():
            if tag.lower() in t_list:
                results.append({
                    "phone_number": phone,
                    "fan_memory": self._memory.get(phone, ""),
                    "fan_tags": t_list,
                    "fan_location": self._location.get(phone, ""),
                })
        return results

    def get_fan_location(self, phone_number: str) -> str:
        return self._location.get(phone_number, "")

    def get_fans_by_location(self, location: str) -> list:
        results = []
        for phone, loc in self._location.items():
            if location.lower() in loc.lower():
                results.append({
                    "phone_number": phone,
                    "fan_memory": self._memory.get(phone, ""),
                    "fan_tags": self._tags.get(phone, []),
                    "fan_location": loc,
                })
        return results

    # ------------------------------------------------------------------
    # Engagement analytics
    # ------------------------------------------------------------------

    def save_reply_context(
        self,
        message_id: Optional[int],
        intent: Optional[str] = None,
        tone_mode: Optional[str] = None,
        routing_tier: Optional[str] = None,
        reply_length_chars: Optional[int] = None,
        has_link: bool = False,
        conversation_turn: Optional[int] = None,
        gen_ms: Optional[float] = None,
        sell_variant: Optional[str] = None,
    ) -> None:
        if message_id is None:
            return
        self._reply_context[message_id] = {
            "intent": intent,
            "tone_mode": tone_mode,
            "routing_tier": routing_tier,
            "reply_length_chars": reply_length_chars,
            "has_link": has_link,
            "conversation_turn": conversation_turn,
            "gen_ms": gen_ms,
            "sell_variant": sell_variant,
            "did_user_reply": None,
            "reply_delay_seconds": None,
            "went_silent_after": None,
        }

    def score_previous_bot_reply(self, phone_number: str) -> None:
        """Find the last unscored assistant message and mark it as replied-to."""
        msgs = self._messages.get(phone_number, [])
        now = datetime.utcnow()
        for msg in reversed(msgs):
            if msg.role != "assistant":
                continue
            ctx = self._reply_context.get(msg.id or -1)
            if ctx is None:
                continue
            if ctx.get("did_user_reply") is not None:
                break  # already scored
            delay = int((now - msg.created_at).total_seconds())
            ctx["did_user_reply"] = True
            ctx["reply_delay_seconds"] = max(0, delay)
            return

    def get_reply_context(self, message_id: int) -> Optional[dict]:
        """Test helper — returns the stored context for a message ID."""
        return self._reply_context.get(message_id)
