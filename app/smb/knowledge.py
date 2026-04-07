"""
SMB knowledge base: static FAQ + live calendar scraping.

Provides build_context(tenant, message) which returns the full club knowledge
base as a context string injected into the conversational AI prompt.  The AI
decides what facts are relevant and composes a natural reply — no keyword
routing on our side.

Calendar data is fetched live from the tenant's calendar_url and cached
in-process for 2 hours to avoid hammering the website on every message.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-process calendar cache (per tenant slug)
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 7200  # 2 hours

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, str]] = {}   # slug → (fetched_at, parsed_text)


def _get_cached(slug: str) -> Optional[str]:
    with _cache_lock:
        entry = _cache.get(slug)
        if entry and (time.time() - entry[0]) < _CACHE_TTL_SECONDS:
            return entry[1]
    return None


def _set_cached(slug: str, text: str) -> None:
    with _cache_lock:
        _cache[slug] = (time.time(), text)


# ---------------------------------------------------------------------------
# Calendar scraper
# ---------------------------------------------------------------------------

def _fetch_todays_shows(calendar_url: str, slug: str) -> str:
    """
    Fetch the club's calendar page and extract today's shows.
    Returns a short plain-text string like:
      "Not Ripe Bananas 8:00pm, Friday Favs 10:00pm"
    or "" if nothing found / fetch fails.
    """
    cached = _get_cached(slug)
    if cached is not None:
        return cached

    try:
        resp = requests.get(
            calendar_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ZarnaBot/1.0)"},
            timeout=8,
        )
        resp.raise_for_status()
        text = resp.text
    except Exception as exc:
        logger.warning("knowledge: failed to fetch calendar for %s: %s", slug, exc)
        _set_cached(slug, "")
        return ""

    today = datetime.now(timezone.utc).strftime("%-d")   # day without leading zero
    # The calendar page renders like: "Not Ripe Bananas8:00pm" on the day's cell
    # We extract lines near today's date marker
    result = _parse_todays_shows(text, today)
    _set_cached(slug, result)
    return result


def _parse_todays_shows(html: str, today_day: str) -> str:
    """
    Parse show names + times from raw HTML/text for today's date.
    The calendar renders plain text with show names immediately followed by times.
    """
    # Strip HTML tags
    plain = re.sub(r"<[^>]+>", " ", html)
    plain = re.sub(r"\s+", " ", plain)

    # Pattern: look for the day number followed by show entries on the same day cell
    # Calendar text looks like: "7 Not Ripe Bananas8:00pm 8 SHOW: Comedy Idol7:00pm"
    # We find the segment starting with today's day number
    day_pattern = re.compile(
        r"(?<!\d)" + re.escape(today_day) + r"(?!\d)"
        r"\s*((?:[A-Za-z][^0-9]{2,60}?\d{1,2}:\d{2}(?:am|pm)\s*)+)"
    )
    match = day_pattern.search(plain)
    if not match:
        return ""

    segment = match.group(1)
    # Split individual shows: text followed by time
    shows = re.findall(r"([A-Za-z][^0-9]{2,60}?)(\d{1,2}:\d{2}(?:am|pm))", segment)
    if not shows:
        return ""

    parts = [f"{name.strip()} {time}" for name, time in shows]
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Context builder
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


def build_context(tenant, message: str) -> str:  # noqa: ARG001  message unused intentionally
    """
    Pass the full knowledge base to the AI every time.
    No keyword matching — let the AI decide what's relevant and how to respond naturally.
    Tonight's shows are always fetched live so the answer is accurate.
    """
    kb = tenant.raw.get("knowledge_base", {})
    if not kb:
        return ""

    ticket_link = _tracked_url(kb.get("website", ""), tenant.slug, "tickets")
    cal_link    = _tracked_url(kb.get("calendar_url", ""), tenant.slug, "calendar")
    map_link    = _tracked_url(kb.get("maps_link", ""), tenant.slug, "map")

    # Fetch tonight's shows live
    calendar_url = kb.get("calendar_url", "")
    todays_shows = _fetch_todays_shows(calendar_url, tenant.slug) if calendar_url else ""
    tonight_line = (
        f"Tonight's shows: {todays_shows}"
        if todays_shows
        else f"Tonight's shows: check the live calendar at {cal_link}"
    )

    lines = [
        f"Address: {kb.get('address', '')}",
        f"Hours: {kb.get('hours', '')}",
        f"Website: {kb.get('website', '')}",
        f"Tickets: {ticket_link}",
        f"Calendar: {cal_link}",
        f"Map / directions: {map_link} — {kb.get('directions', '')}",
        tonight_line,
        f"Food & drinks: {kb.get('food_and_drinks', '')}",
        f"Policies: {kb.get('policies', '')}",
        f"Private events: {kb.get('private_events', '')}",
        f"Notable performers: {kb.get('notable_performers', '')}",
    ]

    facts = "\n".join(line for line in lines if line.split(": ", 1)[-1].strip())
    return f"--- {tenant.display_name} facts ---\n{facts}\n---"
