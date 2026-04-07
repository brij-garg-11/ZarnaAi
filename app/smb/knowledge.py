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

    result = _parse_todays_shows(text, "")
    _set_cached(slug, result)
    return result


def _parse_todays_shows(html: str, _unused: str) -> str:
    """
    Extract tonight's shows from WSCC's Next.js page.

    The site double-escapes JSON inside JS push() calls, so event objects look like:
        \\"datetime\\":\\"2026-04-07T20:00:00\\" ... \\"title\\":\\"Not Ripe Bananas\\"

    We extract every (datetime, title) pair for today's date, parse the time,
    and return a human-readable string like "Not Ripe Bananas 8pm, Friday Favs 10pm".
    """
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # The page embeds double-escaped JSON inside JS push() calls.
    # In the raw HTML string, field names appear as \"field\" (backslash + quote).
    # datetime always appears before title in each event object.
    raw_matches = re.findall(
        r'\\\"datetime\\\":\\\"(' + re.escape(today_iso) + r'[T0-9:+]+)\\\"[^}]*\\\"title\\\":\\\"([^\\\"]+)',
        html,
    )

    shows: list[tuple[str, str]] = []
    seen: set[str] = set()

    for dt_str, title in raw_matches:
        title = title.strip()
        if not title or title in seen:
            continue
        seen.add(title)

        try:
            dt = datetime.fromisoformat(dt_str)
            hour, minute = dt.hour, dt.minute
            suffix = "am" if hour < 12 else "pm"
            hour12 = hour % 12 or 12
            time_str = f"{hour12}:{minute:02d}{suffix}" if minute else f"{hour12}{suffix}"
        except Exception:
            time_str = ""

        shows.append((dt_str, f"{title} {time_str}".strip()))

    if not shows:
        return ""

    shows.sort(key=lambda x: x[0])
    return ", ".join(label for _, label in shows)


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
