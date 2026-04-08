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


def _extract_field(window: str, field: str) -> str:
    """Extract a double-escaped JSON string field from an event window.

    Matches: \\"field\\":\\"value\\" where value may contain escaped chars.
    Returns "" if not found or if value is JSON null.
    """
    m = re.search(
        r'\\\"' + re.escape(field) + r'\\\":\\\"((?:[^\\\"]|\\\\.)*?)\\\"',
        window,
    )
    return m.group(1) if m else ""


def _extract_bool(window: str, field: str) -> Optional[bool]:
    """Extract a boolean field from a double-escaped event window."""
    m = re.search(r'\\\"' + re.escape(field) + r'\\\":(true|false)', window)
    if m:
        return m.group(1) == "true"
    return None


def _parse_shows(html: str, tz: str = "America/New_York") -> dict:
    """
    Parse shows from WSCC's Next.js page for the next 8 days.

    The site double-escapes JSON inside JS push() calls so field names appear as
    \\\"field\\\" in the raw HTML string.

    Real field order per event object:
      ticket_link → datetime → is_sold_out → metadata_text → title → comedian.about

    We anchor on datetime and extract a ±800 char window per event so each field
    can be extracted independently of order.

    Uses the venue's local timezone so "today" and "tomorrow" are correct.
    """
    try:
        local_tz = ZoneInfo(tz)
    except Exception:
        local_tz = ZoneInfo("America/New_York")
    today = datetime.now(local_tz).date()
    window_dates = {str(today + timedelta(days=i)) for i in range(8)}  # today + 7 days

    # Find every event datetime position in the HTML
    dt_pattern = re.compile(
        r'\\\"datetime\\\":\\\"(20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)\\\"'
    )

    result: dict[str, list] = {}
    seen: set[str] = set()

    # Each event object starts with a UUID "id" field.  We anchor on the last
    # "id":"<uuid>" that appears within 700 chars before each datetime — that gives
    # us a clean per-event context window that includes ticket_link (before datetime)
    # as well as metadata_text and title (after datetime).
    id_anchor_re = re.compile(r'\\\"id\\\":\\\"[0-9a-f-]{36}\\\"')

    for m in dt_pattern.finditer(html):
        dt_str = m.group(1)
        date_part = dt_str[:10]
        if date_part not in window_dates:
            continue

        look_back = html[max(0, m.start() - 700):m.start()]
        id_matches = list(id_anchor_re.finditer(look_back))
        if not id_matches:
            continue  # no event object start found — skip stray datetime references
        last_id = id_matches[-1]
        abs_obj_start = (m.start() - len(look_back)) + last_id.end()

        end = min(len(html), m.end() + 1600)
        ctx = html[abs_obj_start:end]

        title = _extract_field(ctx, "title")
        if not title:
            continue

        dedup_key = f"{date_part}|{title}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        ticket_link = _extract_field(ctx, "ticket_link")
        sold_out = _extract_bool(ctx, "is_sold_out") or False

        # metadata_text describes the show; comedian.about is the comedian bio.
        # Prefer metadata_text; fall back to comedian.about.
        raw_desc = _extract_field(ctx, "metadata_text")
        if not raw_desc:
            raw_desc = _extract_field(ctx, "about")

        # Unescape \\n / \\t inserted by JSON double-escaping
        desc_clean = raw_desc.replace("\\\\n", " ").replace("\\\\t", " ").strip()
        # Trim to first 220 chars at a word boundary
        if len(desc_clean) > 220:
            desc_clean = desc_clean[:220].rsplit(" ", 1)[0] + "…"

        try:
            dt = datetime.fromisoformat(dt_str)
            hour, minute = dt.hour, dt.minute
            hour12 = hour % 12 or 12
            time_str = f"{hour12}:{minute:02d}pm" if minute else f"{hour12}pm"
            if hour < 12:
                time_str = f"{hour12}:{minute:02d}am" if minute else f"{hour12}am"
        except Exception:
            time_str = ""

        result.setdefault(date_part, []).append({
            "title": title.strip(),
            "time": time_str,
            "ticket_link": ticket_link.strip(),
            "sold_out": sold_out,
            "description": desc_clean,
            "dt_str": dt_str,
        })

    for day_shows in result.values():
        day_shows.sort(key=lambda s: s["dt_str"])

    return result


def _format_show(show: dict, slug: str) -> str:
    """Format a single show as a short human-readable string with description and ticket link."""
    label = show["title"]
    if show["time"]:
        label += f" at {show['time']}"
    if show.get("description"):
        label += f" ({show['description']})"
    if show["sold_out"]:
        label += " — SOLD OUT"
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
