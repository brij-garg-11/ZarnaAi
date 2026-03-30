"""Per-show event timezone: interpret signup windows in venue-local time, store UTC."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

# IANA ids only — safe for zoneinfo.
EVENT_TIMEZONE_CHOICES: Tuple[Tuple[str, str], ...] = (
    ("America/New_York", "Eastern (New York)"),
    ("America/Chicago", "Central (Chicago)"),
    ("America/Denver", "Mountain (Denver)"),
    ("America/Los_Angeles", "Pacific (Los Angeles)"),
    ("America/Phoenix", "Arizona (Phoenix)"),
    ("UTC", "UTC"),
)

_ALLOWED = {z for z, _ in EVENT_TIMEZONE_CHOICES}


def normalize_event_timezone(raw: Optional[str]) -> str:
    z = (raw or "").strip()
    return z if z in _ALLOWED else "America/New_York"


def effective_event_timezone(stored: Optional[str]) -> str:
    """NULL/empty = legacy rows: windows were entered as UTC."""
    if not (stored or "").strip():
        return "UTC"
    return normalize_event_timezone(stored)


def parse_local_datetime(value: str | None, tz_name: str) -> Optional[datetime]:
    """
    Parse datetime-local string (naive) as local time in tz_name, return UTC-aware.
    """
    if not value or not str(value).strip():
        return None
    v = str(value).strip()
    try:
        if len(v) >= 16 and "T" in v:
            naive = datetime.strptime(v[:16], "%Y-%m-%dT%H:%M")
        else:
            naive = datetime.fromisoformat(v.replace("Z", ""))
            if naive.tzinfo is not None:
                naive = naive.replace(tzinfo=None)
        tz = ZoneInfo(normalize_event_timezone(tz_name or "America/New_York"))
        aware_local = naive.replace(tzinfo=tz)
        return aware_local.astimezone(timezone.utc)
    except (ValueError, OSError):
        return None


def utc_to_datetime_local_value(dt: Optional[datetime], tz_name: str) -> str:
    """Value for HTML datetime-local input in the given zone."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    tz = ZoneInfo(effective_event_timezone(tz_name))
    local = dt.astimezone(tz)
    return local.strftime("%Y-%m-%dT%H:%M")


def timezone_select_value_from_show(show_event_tz: Optional[str]) -> str:
    """Default for <select>: legacy NULL → UTC; otherwise normalized IANA id."""
    if show_event_tz is None or not str(show_event_tz).strip():
        return effective_event_timezone(None)
    return normalize_event_timezone(show_event_tz)


def format_window_human(ws: Optional[datetime], we: Optional[datetime], tz_name: Optional[str]) -> str:
    """Short display line for admin (both in same zone)."""
    zid = effective_event_timezone(tz_name)
    lbl = next((n for z, n in EVENT_TIMEZONE_CHOICES if z == zid), zid)
    if ws is None and we is None:
        return f"No window set — signups not limited by time. Event timezone: <strong>{lbl}</strong>."
    if ws is None or we is None:
        return "Window incomplete — set both start and end."
    zi = ZoneInfo(zid)

    def fmt(d: datetime) -> str:
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(zi).strftime("%b %d, %Y %I:%M %p")

    return f"{fmt(ws)} → {fmt(we)} — <strong>{lbl}</strong>"
