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
