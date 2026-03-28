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
