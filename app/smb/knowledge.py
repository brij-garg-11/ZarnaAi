"""
SMB knowledge base: static FAQ + live calendar scraping.

Provides build_context(tenant, message) which returns the full club knowledge
base as a context string injected into the conversational AI prompt.  The AI
decides what facts are relevant and composes a natural reply — no keyword
routing on our side.

Calendar data is fetched live from the tenant's calendar_url (next 8 days)
and cached in-process for 2 hours to avoid hammering the website.
Each show includes its title, time, and a direct tracked ticket link.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-process calendar cache (per tenant slug)
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 7200  # 2 hours

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, object]] = {}   # slug → (fetched_at, data)


def _get_cached(slug: str) -> Optional[object]:
    with _cache_lock:
        entry = _cache.get(slug)
        if entry and (time.time() - entry[0]) < _CACHE_TTL_SECONDS:
            return entry[1]
    return None


def _set_cached(slug: str, data: object) -> None:
    with _cache_lock:
        _cache[slug] = (time.time(), data)


# ---------------------------------------------------------------------------
# Tracked URL helper
# ---------------------------------------------------------------------------

def _tracked_url(base_url: str, slug: str, link_key: str) -> str:
    """
    Return a tracked redirect URL if RAILWAY_PUBLIC_DOMAIN is set,
    otherwise fall back to the raw URL.
    """
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if domain:
        return f"https://{domain}/smb/r/{slug}/{link_key}"
    return base_url


# ---------------------------------------------------------------------------
# Calendar scraper
# ---------------------------------------------------------------------------

def _fetch_shows(calendar_url: str, slug: str, tz: str = "America/New_York") -> dict:
    """
    Fetch the club's calendar page and return shows for today + next 7 days.

    Returns a dict keyed by ISO date string ("2026-04-07") where each value is
    a sorted list of dicts: {"title", "time", "ticket_link", "sold_out", "dt_str"}.
    Returns {} on failure.  Result is cached per-slug for 2 hours.
    """
    cached = _get_cached(slug)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        resp = requests.get(
            calendar_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ZarnaBot/1.0)"},
            timeout=8,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        logger.warning("knowledge: failed to fetch calendar for %s: %s", slug, exc)
        _set_cached(slug, {})
        return {}

    result = _parse_shows(html, tz=tz)
    _set_cached(slug, result)
    logger.info(
        "knowledge: scraped %d show-dates for %s",
        len(result), slug,
    )
    return result


def _parse_shows(html: str, tz: str = "America/New_York") -> dict:
    """
    Parse shows from WSCC's Next.js page for the next 8 days.

    The site double-escapes JSON inside JS push() calls so field names appear as
    \\"field\\" in the raw HTML string.  We extract datetime, title, ticket_link,
    and is_sold_out from every event object in the page.
    Uses the venue's local timezone so "today" and "tomorrow" are correct for the subscriber.
    """
    try:
        local_tz = ZoneInfo(tz)
    except Exception:
        local_tz = ZoneInfo("America/New_York")
    today = datetime.now(local_tz).date()
    window = {str(today + timedelta(days=i)) for i in range(8)}  # today + 7 days

    # datetime always appears before title; ticket_link and is_sold_out may appear
    # anywhere in the same object.  Use a broad multi-field regex per object.
    raw_events = re.findall(
        r'\\\"datetime\\\":\\\"(20\d\d-\d\d-\d\dT[^\\\"]+)\\\"'
        r'(?:[^}]*\\\"is_sold_out\\\":(true|false))?'
        r'[^}]*\\\"title\\\":\\\"([^\\\"]+)\\\"'
        r'(?:[^}]*\\\"ticket_link\\\":\\\"([^\\\"]*?)\\\")?',
        html,
    )

    result: dict[str, list] = {}
    seen: set[str] = set()

    for dt_str, sold_out_raw, title, ticket_link in raw_events:
        date_part = dt_str[:10]
        if date_part not in window:
            continue
        dedup_key = f"{date_part}|{title}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        try:
            dt = datetime.fromisoformat(dt_str)
            hour, minute = dt.hour, dt.minute
            suffix = "am" if hour < 12 else "pm"
            hour12 = hour % 12 or 12
            time_str = f"{hour12}:{minute:02d}{suffix}" if minute else f"{hour12}{suffix}"
        except Exception:
            time_str = ""

        result.setdefault(date_part, []).append({
            "title": title.strip(),
            "time": time_str,
            "ticket_link": ticket_link.strip() if ticket_link else "",
            "sold_out": sold_out_raw == "true",
            "dt_str": dt_str,
        })

    for day_shows in result.values():
        day_shows.sort(key=lambda s: s["dt_str"])

    return result


def _format_show(show: dict, slug: str) -> str:
    """Format a single show as a short human-readable string with a tracked ticket link."""
    label = show["title"]
    if show["time"]:
        label += f" at {show['time']}"
    if show["sold_out"]:
        label += " (sold out)"
    elif show.get("ticket_link"):
        tracked = _tracked_url(show["ticket_link"], slug, "tickets")
        label += f" — tickets: {tracked}"
    return label


def _format_day_label(date_str: str, tz: str = "America/New_York") -> str:
    """Return a friendly label like 'Tonight', 'Tomorrow', 'Saturday Apr 11'.
    Uses the venue's local timezone so labels match the subscriber's experience."""
    try:
        local_tz = ZoneInfo(tz)
    except Exception:
        local_tz = ZoneInfo("America/New_York")
    today = datetime.now(local_tz).date()
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        return date_str
    delta = (d - today).days
    if delta == 0:
        return "Tonight"
    if delta == 1:
        return "Tomorrow"
    return d.strftime("%A %b %-d")


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def build_context(tenant, message: str) -> str:  # noqa: ARG001  message unused intentionally
    """
    Pass the full knowledge base to the AI every time.
    No keyword matching — let the AI decide what's relevant and how to respond naturally.
    Live show schedule (today + next 7 days with direct ticket links) is always fetched.
    """
    kb = tenant.raw.get("knowledge_base", {})
    if not kb:
        return ""

    cal_link = _tracked_url(kb.get("calendar_url", ""), tenant.slug, "calendar")
    map_link = _tracked_url(kb.get("maps_link", ""), tenant.slug, "map")

    # Fetch upcoming shows live — use venue's timezone for correct day labels
    calendar_url = kb.get("calendar_url", "")
    venue_tz = getattr(tenant, "timezone", "America/New_York")
    shows_by_date = _fetch_shows(calendar_url, tenant.slug, tz=venue_tz) if calendar_url else {}

    if shows_by_date:
        schedule_lines = []
        for date_str in sorted(shows_by_date.keys()):
            label = _format_day_label(date_str, tz=venue_tz)
            show_labels = [_format_show(s, tenant.slug) for s in shows_by_date[date_str]]
            schedule_lines.append(f"{label}: {', '.join(show_labels)}")
        schedule_block = "\n".join(schedule_lines)
    else:
        schedule_block = f"Check the live calendar at {cal_link}"

    lines = [
        f"Address: {kb.get('address', '')}",
        f"Hours: {kb.get('hours', '')}",
        f"Website: {kb.get('website', '')}",
        f"Calendar: {cal_link}",
        f"Map / directions: {map_link} — {kb.get('directions', '')}",
        f"Upcoming shows:\n{schedule_block}",
        f"Food & drinks: {kb.get('food_and_drinks', '')}",
        f"Policies: {kb.get('policies', '')}",
        f"Ticket support (lost / missing tickets): {kb.get('ticket_support', '')}",
        f"Cancellation policy: {kb.get('cancellation_policy', '')}",
        f"Show transfers (changing show time or date): {kb.get('show_transfers', '')}",
        f"Accidental purchase: {kb.get('accidental_purchase', '')}",
        f"Billing errors / overcharged: {kb.get('billing_errors', '')}",
        f"Accessibility / mobility needs: {kb.get('accessibility', '')}",
        f"Late arrival: {kb.get('late_arrival', '')}",
        f"Lost and found: {kb.get('lost_and_found', '')}",
        f"Private events: {kb.get('private_events', '')}",
        f"Notable performers: {kb.get('notable_performers', '')}",
    ]

    facts = "\n".join(line for line in lines if line.split(": ", 1)[-1].strip())
    return f"--- {tenant.display_name} facts ---\n{facts}\n---"
