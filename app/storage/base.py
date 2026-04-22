from abc import ABC, abstractmethod
from typing import List, Optional

from .models import Contact, Message


class BaseStorage(ABC):

    @abstractmethod
    def save_contact(self, phone_number: str, source: Optional[str] = None) -> Contact:
        pass

    @abstractmethod
    def get_contact(self, phone_number: str) -> Optional[Contact]:
        pass

    @abstractmethod
    def save_message(self, phone_number: str, role: str, text: str) -> Message:
        pass

    @abstractmethod
    def get_conversation_history(self, phone_number: str, limit: int = 10) -> List[Message]:
        pass

    def is_first_message(self, phone_number: str) -> bool:
        """Return True if this phone number has no prior messages. Override for efficiency."""
        return len(self.get_conversation_history(phone_number, limit=1)) == 0

    def get_memory(self, phone_number: str) -> str:
        return ""

    def update_memory(self, phone_number: str, memory: str, tags: list, location: str = "") -> None:
        pass

    def get_fans_by_tag(self, tag: str) -> list:
        return []

    def get_fans_by_location(self, location: str) -> list:
        return []

    def get_fan_location(self, phone_number: str) -> str:
        """Return the stored location string for this fan, or empty string."""
        return ""

    def get_fan_show_context(self, phone_number: str) -> Optional[str]:
        """
        Return a human-readable string describing the fan's most recent show
        attendance, e.g. "Fan attended 'Chicago Laugh Factory' on 2025-03-15."
        Returns None when no show history exists.
        """
        return None

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
        provider: Optional[str] = None,
        prompt_tokens: Optional[int] = None,
        completion_tokens: Optional[int] = None,
        ai_cost_usd: Optional[float] = None,
    ) -> None:
        """
        Persist context metadata for an assistant message so engagement
        outcomes can later be correlated against it.  Default is a no-op —
        override in concrete implementations.
        """

    def score_previous_bot_reply(self, phone_number: str) -> None:
        """
        Called when a new *user* message arrives.  Finds the most recent
        unscored assistant message for this phone and marks it as replied-to,
        recording how many seconds the fan took to reply.  No-op by default.
        """

    def get_top_performing_replies(
        self,
        intent: str,
        tone_mode: str,
        limit: int = 4,
    ) -> list:
        """
        Return up to `limit` high-engagement bot reply texts for a given
        intent + tone_mode combination, ordered by depth of follow-up
        conversation (msgs_after_this DESC) then reply speed (faster = better).
        Returns [] when not enough data exists or storage doesn't support it.
        """
        return []
