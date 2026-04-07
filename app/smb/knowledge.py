"""
SMB knowledge base: static FAQ + live calendar scraping.

Provides build_context(tenant, message) which returns a context string
injected into the conversational AI prompt so the bot can answer questions
about location, hours, tonight's lineup, tickets, food, etc.

Calendar data is fetched live from the tenant's calendar_url and cached
in-process for 2 hours to avoid hammering the website on every message.
"""

from __future__ import annotations

import logging
import os
import re
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

# Keywords that suggest the subscriber is asking about tonight / the schedule
_TONIGHT_KEYWORDS = re.compile(
    r"\b(tonight|today|this week|show|performing|lineup|who.s on|schedule|calendar|on stage|"
    r"what.s happening|what.s on|any shows|shows tonight|shows today)\b",
    re.IGNORECASE,
)

# Keywords that suggest location / directions
_LOCATION_KEYWORDS = re.compile(
    r"\b(where|address|location|directions|how do i get|subway|parking|far|near|map|"
    r"upper west side|uws|75th)\b",
    re.IGNORECASE,
)

# Keywords about tickets / reservations
_TICKET_KEYWORDS = re.compile(
    r"\b(ticket|tickets|buy|book|reserve|reservation|cost|price|how much|admission|cover)\b",
    re.IGNORECASE,
)

# Keywords about food / drinks
_FOOD_KEYWORDS = re.compile(
    r"\b(food|eat|menu|drink|drinks|margarita|cocktail|dinner|kitchen|minimum|vegetarian|vegan)\b",
    re.IGNORECASE,
)

# Keywords about age / policy
_POLICY_KEYWORDS = re.compile(
    r"\b(age|how old|minimum age|kids|children|id|dress code|attire|policy|rules)\b",
    re.IGNORECASE,
)

# Keywords about private events
_PRIVATE_KEYWORDS = re.compile(
    r"\b(private|group|birthday|corporate|hire|event|party|buyout|book the|book for)\b",
    re.IGNORECASE,
)


def _tracked_url(base_url: str, slug: str, link_key: str) -> str:
    """
    Return a tracked redirect URL if RAILWAY_PUBLIC_DOMAIN is set,
    otherwise fall back to the raw URL.
    """
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if domain:
        return f"https://{domain}/smb/r/{slug}/{link_key}"
    return base_url


def build_context(tenant, message: str) -> str:
    """
    Return a context block injected into the AI prompt.
    Only includes sections relevant to the subscriber's question to keep prompts short.
    """
    kb = tenant.raw.get("knowledge_base", {})
    if not kb:
        return ""

    sections: list[str] = []

    # Always include core facts
    if kb.get("address"):
        sections.append(f"Address: {kb['address']}")
    if kb.get("hours"):
        sections.append(f"Hours: {kb['hours']}")
    if kb.get("website"):
        sections.append(f"Website: {kb['website']}")

    msg_lower = message.lower()

    # Tonight's lineup — fetch live from calendar
    if _TONIGHT_KEYWORDS.search(msg_lower):
        calendar_url = kb.get("calendar_url", "")
        if calendar_url:
            todays_shows = _fetch_todays_shows(calendar_url, tenant.slug)
            if todays_shows:
                sections.append(f"Tonight's shows: {todays_shows}")
            else:
                cal_link = _tracked_url(calendar_url, tenant.slug, "calendar")
                sections.append(f"Tonight's shows: Check the calendar at {cal_link}")
        ticket_link = _tracked_url(kb.get("website", ""), tenant.slug, "tickets")
        sections.append(f"Tickets: For official pricing and to grab tickets: {ticket_link}")

    # Location / directions
    if _LOCATION_KEYWORDS.search(msg_lower):
        if kb.get("directions"):
            sections.append(f"Directions / parking: {kb['directions']}")
        if kb.get("maps_link"):
            map_link = _tracked_url(kb["maps_link"], tenant.slug, "map")
            sections.append(f"Google Maps: {map_link}")

    # Tickets
    if _TICKET_KEYWORDS.search(msg_lower):
        ticket_link = _tracked_url(kb.get("website", ""), tenant.slug, "tickets")
        sections.append(f"Tickets: For official pricing and to grab tickets: {ticket_link}")

    # Food & drinks
    if _FOOD_KEYWORDS.search(msg_lower):
        if kb.get("food_and_drinks"):
            sections.append(f"Food & drinks: {kb['food_and_drinks']}")

    # Age / policy
    if _POLICY_KEYWORDS.search(msg_lower):
        if kb.get("policies"):
            sections.append(f"Policies: {kb['policies']}")

    # Private events
    if _PRIVATE_KEYWORDS.search(msg_lower):
        if kb.get("private_events"):
            sections.append(f"Private events: {kb['private_events']}")

    if not sections:
        return ""

    return "--- Club info ---\n" + "\n".join(sections) + "\n---"
