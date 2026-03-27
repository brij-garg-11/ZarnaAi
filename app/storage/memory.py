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
        msg = Message(phone_number=phone_number, role=role, text=text)
        self._messages.setdefault(phone_number, []).append(msg)
        return msg

    def get_conversation_history(self, phone_number: str, limit: int = 10) -> List[Message]:
        return self._messages.get(phone_number, [])[-limit:]
