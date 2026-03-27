from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Contact:
    phone_number: str
    source: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Message:
    phone_number: str
    role: str  # "user" or "assistant"
    text: str
    created_at: datetime = field(default_factory=datetime.utcnow)
