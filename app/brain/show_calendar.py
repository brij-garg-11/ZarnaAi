"""
Show calendar — per-creator tour dates for SHOW-intent replies.

When a fan asks "when are you coming to <city>?" the bot should answer with the
*specific* upcoming show (venue + date) and that show's ticket link, opening with
a warm "I'd love to see you there!" — not a bare generic URL. If Zarna was just in
the fan's city, the bot should say "we were just there!" instead of recommending a
show whose date has already passed.

Data source priority (first that yields shows wins):
  1. Bandsintown public REST API — when the creator config sets ``bandsintown_artist``.
     No API key required; only an app_id string (BANDSINTOWN_APP_ID env, with a default).
  2. ``upcoming_shows`` array in the creator config — manual fallback that the
     operator curates by hand.
  3. Nothing — callers fall back to the generic ``links.tickets`` URL and the
     existing generic SHOW prompt, so behaviour is unchanged.

Results are cached in-process per creator slug for 4 hours, mirroring the SMB
calendar cache in ``app/smb/knowledge.py``. Network failures degrade gracefully:
a failed Bandsintown fetch falls through to the config ``upcoming_shows`` list.

This module never raises to its callers — every public function is wrapped so a
bug here can never break a fan's reply. The worst case is "no directive", which
means the bot falls back to the existing generic ticket reply.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import List, Optional

import requests

_LOGGER = logging.getLogger(__name__)

# US state/territory abbreviation -> full name, so a fan typing "Kentucky" matches
# a show whose config region is "KY" (and vice-versa). Lowercased at use sites.
_US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "Washington",
}

# Continent / multi-country regions a fan might ask about ("any shows in Europe?").
# Maps the region keyword to the set of show `region` (country) tokens that belong
# to it. Single countries (UK, Ireland, Australia, Canada, …) are already handled by
# _match_city via the show's region field, so only broader groupings live here.
_REGION_GROUPS = {
    "europe": {"uk", "ireland", "germany", "switzerland", "sweden", "france",
               "spain", "italy", "netherlands", "belgium", "norway", "denmark",
               "austria", "poland", "portugal"},
    "asia": {"singapore", "india", "japan", "china", "uae", "indonesia"},
    "scandinavia": {"sweden", "norway", "denmark", "finland"},
}

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

# Matches config date labels like "Jun 5-6, 2026", "Jun 9, 2026", "Jul 2-4, 2026".
_DATE_LABEL_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?,?\s*(\d{4})"
)


def _parse_label_dates(label: str) -> tuple[Optional[date], Optional[date]]:
    """('Jun 5-6, 2026') -> (date(2026,6,5), date(2026,6,6)). (None, None) if unparseable."""
    if not label:
        return None, None
    m = _DATE_LABEL_RE.search(label)
    if not m:
        return None, None
    month = _MONTHS.get(m.group(1)[:3].lower())
    if not month:
        return None, None
    try:
        year = int(m.group(4))
        start_day = int(m.group(2))
        end_day = int(m.group(3)) if m.group(3) else start_day
        return date(year, month, start_day), date(year, month, end_day)
    except ValueError:
        return None, None

# How long a fetched calendar stays warm before we re-fetch. 4h keeps the bot
# within a few hours of the artist's Bandsintown listings without hammering the API.
_CACHE_TTL_SECONDS = 4 * 60 * 60

# A past show is only worth mentioning ("we were just there!") for a short window.
_RECENT_PAST_DAYS = 60

# Bandsintown caps how far back the past feed goes; we only need a handful.
_PAST_PAGE_SIZE = 15

_BANDSINTOWN_BASE = "https://rest.bandsintown.com/artists"

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, "ShowCalendar"]] = {}  # slug -> (fetched_at, calendar)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Show:
    city: str = ""
    region: str = ""  # state / province
    venue: str = ""
    date_iso: str = ""  # "2026-07-12" (date part only)
    date_label: str = ""  # "Jul 12, 2026"
    ticket_url: str = ""


@dataclass
class ShowCalendar:
    upcoming: List[Show] = field(default_factory=list)
    recent_past: List[Show] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.upcoming and not self.recent_past


@dataclass
class ShowDirective:
    """Instruction + link injected into the SHOW prompt for a specific fan message."""
    instruction: str
    ticket_url: str


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _app_id() -> str:
    return os.getenv("BANDSINTOWN_APP_ID", "zarna-sms-bot").strip() or "zarna-sms-bot"


def _generic_tickets(creator_config) -> str:
    """The creator's general tickets page — used when a show has no specific link."""
    links = getattr(creator_config, "links", None)
    return getattr(links, "tickets", "") if links else ""


# ---------------------------------------------------------------------------
# Bandsintown fetch + parse  (fetch = network, parse = pure/testable)
# ---------------------------------------------------------------------------

def _fetch_bandsintown(artist: str, past: bool = False) -> list:
    """Call the Bandsintown events API. Returns the raw JSON list, or [] on any failure."""
    if not artist:
        return []
    # Bandsintown expects the artist name URL-encoded in the path.
    artist_path = requests.utils.quote(artist, safe="")
    url = f"{_BANDSINTOWN_BASE}/{artist_path}/events"
    params = {"app_id": _app_id()}
    if past:
        params["date"] = "past"
    try:
        resp = requests.get(url, params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        _LOGGER.warning("show_calendar: Bandsintown fetch failed for %r (past=%s): %s",
                        artist, past, exc)
        return []
    if not isinstance(data, list):
        # The API returns {"errorMessage": ...} when the artist is unknown.
        _LOGGER.info("show_calendar: Bandsintown returned non-list for %r: %r", artist, data)
        return []
    if past:
        # Past feed is newest-first; keep only the most recent handful.
        return data[:_PAST_PAGE_SIZE]
    return data


def _parse_bandsintown(events: list) -> List[Show]:
    """Normalise raw Bandsintown event objects into Show records. Pure function."""
    shows: List[Show] = []
    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        venue = ev.get("venue") or {}
        dt_raw = (ev.get("datetime") or "").strip()
        date_iso, date_label = _format_dt(dt_raw)
        ticket_url = ""
        for offer in ev.get("offers") or []:
            if isinstance(offer, dict) and (offer.get("url") or "").strip():
                # Prefer an explicit "Tickets" offer, else take the first link.
                if (offer.get("type") or "").lower() == "tickets":
                    ticket_url = offer["url"].strip()
                    break
                if not ticket_url:
                    ticket_url = offer["url"].strip()
        shows.append(Show(
            city=(venue.get("city") or "").strip(),
            region=(venue.get("region") or "").strip(),
            venue=(venue.get("name") or "").strip(),
            date_iso=date_iso,
            date_label=date_label,
            ticket_url=ticket_url,
        ))
    return shows


def _format_dt(dt_raw: str) -> tuple[str, str]:
    """('2026-07-12T20:00:00') -> ('2026-07-12', 'Jul 12, 2026'). Degrades gracefully."""
    if not dt_raw:
        return "", ""
    try:
        dt = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
    except ValueError:
        # Some feeds send a bare date.
        try:
            dt = datetime.strptime(dt_raw[:10], "%Y-%m-%d")
        except ValueError:
            return "", dt_raw
    # Avoid %-d (not portable); strip a leading zero from the day manually.
    label = dt.strftime("%b %d, %Y").replace(" 0", " ")
    return dt.date().isoformat(), label


# ---------------------------------------------------------------------------
# Config fallback parser
# ---------------------------------------------------------------------------

def _shows_from_config(creator_config) -> List[Show]:
    """Build Show records from the creator config ``upcoming_shows`` array."""
    raw = getattr(creator_config, "upcoming_shows", ()) or ()
    shows: List[Show] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        date_label = str(item.get("date", item.get("date_label", ""))).strip()
        date_iso = str(item.get("date_iso", "")).strip()
        # Configs usually only carry a human label ("Jun 5-6, 2026"); derive the
        # ISO start date so the calendar can sort + filter chronologically.
        if not date_iso and date_label:
            start, _end = _parse_label_dates(date_label)
            if start:
                date_iso = start.isoformat()
        shows.append(Show(
            city=str(item.get("city", "")).strip(),
            region=str(item.get("state", item.get("region", ""))).strip(),
            venue=str(item.get("venue", "")).strip(),
            date_iso=date_iso,
            date_label=date_label,
            ticket_url=str(item.get("ticket_url", "")).strip(),
        ))
    return shows


# ---------------------------------------------------------------------------
# Calendar assembly (cached)
# ---------------------------------------------------------------------------

def _build_calendar(creator_config) -> ShowCalendar:
    """Assemble a ShowCalendar from Bandsintown (preferred) or the config fallback."""
    artist = (getattr(creator_config, "bandsintown_artist", "") or "").strip()
    if artist:
        upcoming = _parse_bandsintown(_fetch_bandsintown(artist, past=False))
        recent_past = _parse_bandsintown(_fetch_bandsintown(artist, past=True))
        if upcoming or recent_past:
            return ShowCalendar(upcoming=upcoming, recent_past=_filter_recent_past(recent_past))
        _LOGGER.info("show_calendar: Bandsintown empty for %r — using config fallback", artist)
    # Manual fallback: split config shows into upcoming vs recent-past by date,
    # so we never recommend a show whose date has already passed and we *can*
    # say "we were just there" for very recent ones.
    return _calendar_from_config(creator_config)


def _calendar_from_config(creator_config) -> ShowCalendar:
    today = datetime.now(timezone.utc).date()
    upcoming: List[Show] = []
    recent_past: List[Show] = []
    for s in _shows_from_config(creator_config):
        _start, end = _parse_label_dates(s.date_label)
        if end is None:
            # Unparseable date — keep it as upcoming rather than silently dropping.
            upcoming.append(s)
        elif end >= today:
            upcoming.append(s)
        elif (today - end).days <= _RECENT_PAST_DAYS:
            recent_past.append(s)
    upcoming.sort(key=lambda s: s.date_iso or "9999-99-99")
    recent_past.sort(key=lambda s: s.date_iso or "", reverse=True)
    return ShowCalendar(upcoming=upcoming, recent_past=recent_past)


def _filter_recent_past(shows: List[Show]) -> List[Show]:
    """Keep only past shows within the recent window, so we don't surface stale tours."""
    today = datetime.now(timezone.utc).date()
    keep: List[Show] = []
    for s in shows:
        if not s.date_iso:
            continue
        try:
            d = datetime.strptime(s.date_iso, "%Y-%m-%d").date()
        except ValueError:
            continue
        age = (today - d).days
        if 0 <= age <= _RECENT_PAST_DAYS:
            keep.append(s)
    return keep


def get_calendar(creator_config) -> ShowCalendar:
    """Return the (cached) ShowCalendar for a creator. Never raises."""
    if creator_config is None:
        return ShowCalendar()
    slug = getattr(creator_config, "slug", "") or "default"
    now = time.time()
    with _cache_lock:
        entry = _cache.get(slug)
        if entry and (now - entry[0]) < _CACHE_TTL_SECONDS:
            return entry[1]
    try:
        cal = _build_calendar(creator_config)
    except Exception:
        _LOGGER.exception("show_calendar: build failed for slug=%s", slug)
        cal = ShowCalendar()
    with _cache_lock:
        _cache[slug] = (now, cal)
    return cal


def clear_cache() -> None:
    """Test/admin helper — drop all cached calendars."""
    with _cache_lock:
        _cache.clear()


# ---------------------------------------------------------------------------
# City matching + directive building
# ---------------------------------------------------------------------------

def _match_city(message: str, shows: List[Show]) -> Optional[Show]:
    """Return the first show whose city/region appears in the fan's message.

    City is matched as a substring. Region (state) is a weaker fallback: we match
    both the full state name and the 2-letter code, so "Kentucky", "KY", and a
    config region of either form all line up (the fan rarely types the code).
    """
    text = (message or "").lower()
    if not text:
        return None
    for show in shows:
        city = show.city.lower()
        if city and city in text:
            return show
    for show in shows:
        if _region_in_text(show.region, text):
            return show
    return None


def _region_in_text(region: str, text: str) -> bool:
    """True if a show's region matches the message, by full state name or code."""
    region = (region or "").strip()
    if not region:
        return False
    candidates = {region.lower()}
    full = _US_STATES.get(region.upper())
    if full:
        candidates.add(full.lower())
    for cand in candidates:
        if len(cand) <= 2:
            # 2-letter codes are noisy as substrings — require a whole-word match.
            if re.search(rf"\b{re.escape(cand)}\b", text):
                return True
        elif cand in text:
            return True
    return False


def _match_region(message: str, shows: List[Show]) -> tuple[Optional[str], List[Show]]:
    """If the fan named a continent/region (e.g. 'Europe'), return (label, matching shows).

    This catches broad geographic questions that _match_city can't, so the bot lists
    the real shows in that region instead of recommending an unrelated next date.
    """
    text = (message or "").lower()
    if not text:
        return None, []
    for key, countries in _REGION_GROUPS.items():
        if re.search(rf"\b{re.escape(key)}\b", text):
            matches = [s for s in shows if (s.region or "").strip().lower() in countries]
            if matches:
                return key, matches
    return None, []


def _upcoming_summary(shows: List[Show], limit: int = 3) -> str:
    """A compact 'City (date)' list of the next few shows for prompt context."""
    parts = []
    for s in shows[:limit]:
        if s.city and s.date_label:
            parts.append(f"{s.city} on {s.date_label}")
        elif s.city:
            parts.append(s.city)
    return "; ".join(parts)


def build_show_directive(message: str, creator_config) -> Optional[ShowDirective]:
    """
    Build a per-message SHOW directive, or None to use the generic ticket reply.

    Match priority:
      1. Upcoming show in the fan's city  -> "we'd love to see you at <venue> on <date>".
      2. Recent past show in the fan's city -> "we were just in <city>!" (no stale date).
      3. No city match but shows exist -> list the next few upcoming dates.
    """
    if creator_config is None:
        return None
    try:
        cal = get_calendar(creator_config)
    except Exception:
        _LOGGER.exception("show_calendar: get_calendar failed")
        return None
    if cal.is_empty():
        return None

    generic = _generic_tickets(creator_config)

    # Message shape is always: a clear verdict FIRST (are we coming / not coming /
    # were just there), then the warm "I'd love to see you there!" close LAST.
    upcoming_match = _match_city(message, cal.upcoming)
    if upcoming_match:
        when = upcoming_match.date_label or "the upcoming date"
        venue = upcoming_match.venue or "the venue"
        instruction = (
            f"The fan asked about {upcoming_match.city}. Zarna HAS an upcoming show there: "
            f"{venue} in {upcoming_match.city} on {when}. "
            f"OPEN the reply with the clear verdict up front — that you ARE coming to "
            f"{upcoming_match.city}, naming the venue and the exact date in the first sentence. "
            f"END with a warm invitation like 'I'd love to see you there!' "
            f"Keep it to 1-2 short sentences."
        )
        return ShowDirective(instruction=instruction,
                             ticket_url=upcoming_match.ticket_url or generic)

    past_match = _match_city(message, cal.recent_past)
    if past_match:
        venue = past_match.venue or "town"
        when = past_match.date_label or "recently"
        instruction = (
            f"The fan asked about {past_match.city}. Zarna was JUST there ({venue} on {when}) "
            f"and has no upcoming show booked there yet. "
            f"OPEN with the clear verdict up front — that you're NOT coming back to "
            f"{past_match.city} just yet because you were only just there. Do NOT invent or "
            f"promise a future date. Then warmly invite them to watch the tickets page for when "
            f"you're back."
        )
        return ShowDirective(instruction=instruction, ticket_url=generic)

    region_key, region_shows = _match_region(message, cal.upcoming)
    if region_shows:
        listed = "; ".join(
            f"{s.venue or s.city} in {s.city} on {s.date_label}" for s in region_shows[:4]
        )
        more = " (and more)" if len(region_shows) > 4 else ""
        instruction = (
            f"The fan asked about {region_key.title()}. Zarna HAS upcoming shows in {region_key.title()}: "
            f"{listed}{more}. OPEN with the clear verdict up front — yes, you ARE coming to {region_key.title()} — "
            f"then name those shows with their dates. END with a warm 'I'd love to see you there!'"
        )
        return ShowDirective(instruction=instruction, ticket_url=generic)

    if cal.upcoming:
        summary = _upcoming_summary(cal.upcoming)
        if not summary:
            return None
        nxt = cal.upcoming[0]
        when = nxt.date_label or "soon"
        where = nxt.city or "the next city"
        venue_part = f"{nxt.venue} in " if nxt.venue else ""
        instruction = (
            f"OPEN with the clear verdict up front: if the fan named a specific city, say plainly "
            f"in the first sentence whether you're coming there — and since that city is NOT in the "
            f"list below, tell them you don't have it booked yet. "
            f"THEN point them to your NEXT show: {venue_part}{where} on {when}, named explicitly with "
            f"its date. There are more upcoming dates ({summary}) — you may mention one or two. "
            f"END with a warm 'I'd love to see you there!' Never claim a show in a city that isn't "
            f"in this list."
        )
        return ShowDirective(instruction=instruction, ticket_url=generic)

    return None
