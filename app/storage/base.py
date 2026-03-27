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
