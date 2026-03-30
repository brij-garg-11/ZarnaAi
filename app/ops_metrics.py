"""In-process counters for admin observability (per worker)."""

from __future__ import annotations

import threading
from typing import Dict

_lock = threading.Lock()
_counters: Dict[str, int] = {
    "slicktext_webhook_401": 0,
    "twilio_signature_fail": 0,
    "ai_reply_error": 0,
    "ai_reply_capacity_reject": 0,
    "active_ai_replies": 0,
}


def bump(name: str, delta: int = 1) -> None:
    with _lock:
        _counters[name] = _counters.get(name, 0) + delta


def snapshot() -> Dict[str, int]:
    with _lock:
        return dict(_counters)


def ai_reply_enter() -> bool:
    """Return False if at capacity (caller should skip starting AI work)."""
    import os

    try:
        cap = int(os.getenv("AI_REPLY_MAX_CONCURRENT", "16"))
    except ValueError:
        cap = 16
    cap = max(1, min(cap, 256))
    with _lock:
        cur = _counters.get("active_ai_replies", 0)
        if cur >= cap:
            _counters["ai_reply_capacity_reject"] = _counters.get("ai_reply_capacity_reject", 0) + 1
            return False
        _counters["active_ai_replies"] = cur + 1
        return True


def ai_reply_leave() -> None:
    with _lock:
        cur = max(0, _counters.get("active_ai_replies", 0) - 1)
        _counters["active_ai_replies"] = cur
