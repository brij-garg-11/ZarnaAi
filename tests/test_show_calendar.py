"""
Tests for the tour show calendar (Item 1 — show-specific ticket links).

Covers the pure pieces (Bandsintown parsing, date formatting, config fallback,
city matching, directive building), the in-process cache, the
Bandsintown-preferred-over-config priority, and the generator SHOW-prompt
integration. No network calls — the Bandsintown fetch is always monkeypatched.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import pytest

from app.brain import show_calendar as sc
from app.brain.creator_config import CreatorConfig, CreatorLinks
from app.brain.generator import _build_prompt
from app.brain.intent import Intent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(slug="testcal", artist="", shows=(), tickets="https://example.com/tickets/"):
    return CreatorConfig(
        slug=slug,
        name="Test Creator",
        links=CreatorLinks(tickets=tickets),
        bandsintown_artist=artist,
        upcoming_shows=tuple(shows),
    )


SAMPLE_EVENTS = [
    {
        "datetime": "2026-07-12T20:00:00",
        "title": "Million Dollar Excuses",
        "venue": {"name": "Zanies", "city": "Chicago", "region": "IL", "country": "United States"},
        "offers": [{"type": "Tickets", "url": "https://tix.example/chicago"}],
    },
    {
        "datetime": "2026-08-01T19:30:00",
        "venue": {"name": "Beacon Theatre", "city": "New York", "region": "NY"},
        "offers": [
            {"type": "Other", "url": "https://other.example/ny"},
            {"type": "Tickets", "url": "https://tix.example/ny"},
        ],
    },
]


@pytest.fixture(autouse=True)
def _clear_cache():
    sc.clear_cache()
    yield
    sc.clear_cache()


# ---------------------------------------------------------------------------
# Date formatting
# ---------------------------------------------------------------------------

class TestFormatDt:
    def test_iso_datetime_strips_leading_zero(self):
        assert sc._format_dt("2026-07-12T20:00:00") == ("2026-07-12", "Jul 12, 2026")

    def test_iso_with_z(self):
        date_iso, label = sc._format_dt("2026-07-12T20:00:00Z")
        assert date_iso == "2026-07-12"
        assert label == "Jul 12, 2026"

    def test_bare_date(self):
        assert sc._format_dt("2026-12-05") == ("2026-12-05", "Dec 5, 2026")

    def test_empty(self):
        assert sc._format_dt("") == ("", "")

    def test_garbage_falls_back_to_raw_label(self):
        assert sc._format_dt("not-a-date") == ("", "not-a-date")


# ---------------------------------------------------------------------------
# Bandsintown parsing
# ---------------------------------------------------------------------------

class TestParseBandsintown:
    def test_normalizes_fields(self):
        shows = sc._parse_bandsintown(SAMPLE_EVENTS)
        assert len(shows) == 2
        chi = shows[0]
        assert chi.city == "Chicago"
        assert chi.region == "IL"
        assert chi.venue == "Zanies"
        assert chi.date_label == "Jul 12, 2026"
        assert chi.ticket_url == "https://tix.example/chicago"

    def test_prefers_tickets_offer_over_other(self):
        shows = sc._parse_bandsintown(SAMPLE_EVENTS)
        assert shows[1].ticket_url == "https://tix.example/ny"

    def test_handles_empty_and_malformed(self):
        assert sc._parse_bandsintown([]) == []
        assert sc._parse_bandsintown([None, "x", 5]) == []

    def test_missing_offers_yields_empty_url(self):
        shows = sc._parse_bandsintown([{"datetime": "2026-07-12T20:00:00",
                                        "venue": {"city": "Boston"}}])
        assert shows[0].ticket_url == ""
        assert shows[0].city == "Boston"


# ---------------------------------------------------------------------------
# Config fallback parsing
# ---------------------------------------------------------------------------

class TestShowsFromConfig:
    def test_reads_upcoming_shows(self):
        cfg = _cfg(shows=[
            {"city": "Austin", "state": "TX", "venue": "Cap City", "date": "Jun 25, 2026",
             "ticket_url": "https://t/austin"},
        ])
        shows = sc._shows_from_config(cfg)
        assert len(shows) == 1
        assert shows[0].city == "Austin"
        assert shows[0].region == "TX"
        assert shows[0].date_label == "Jun 25, 2026"
        assert shows[0].ticket_url == "https://t/austin"

    def test_empty_when_no_shows(self):
        assert sc._shows_from_config(_cfg()) == []


# ---------------------------------------------------------------------------
# City matching
# ---------------------------------------------------------------------------

class TestMatchCity:
    def _shows(self):
        return sc._parse_bandsintown(SAMPLE_EVENTS)

    def test_matches_city_substring(self):
        m = sc._match_city("when are you coming to chicago?", self._shows())
        assert m and m.city == "Chicago"

    def test_no_match_returns_none(self):
        assert sc._match_city("are you touring in seattle?", self._shows()) is None

    def test_empty_message_returns_none(self):
        assert sc._match_city("", self._shows()) is None

    def test_region_fallback(self):
        shows = [sc.Show(city="Smalltown", region="Vermont", venue="V", date_label="x")]
        assert sc._match_city("anything in vermont soon?", shows).city == "Smalltown"

    def test_two_letter_region_not_matched(self):
        # Two-letter regions (IL, NY) are too noisy to match as substrings.
        shows = [sc.Show(city="Nowhere", region="IL", venue="V", date_label="x")]
        assert sc._match_city("will it rain", shows) is None


# ---------------------------------------------------------------------------
# Directive building
# ---------------------------------------------------------------------------

class TestBuildDirective:
    def test_none_config(self):
        assert sc.build_show_directive("chicago?", None) is None

    def test_empty_calendar_returns_none(self):
        assert sc.build_show_directive("chicago?", _cfg()) is None

    def test_upcoming_match_uses_show_link_and_names_date(self):
        # Relative future date so the show stays "upcoming" as time passes
        # (a hardcoded date silently lapses into the past and breaks this).
        label = _label(datetime.now(timezone.utc).date() + timedelta(days=20))
        cfg = _cfg(shows=[
            {"city": "Austin", "venue": "Cap City", "date": label,
             "ticket_url": "https://t/austin"},
        ])
        d = sc.build_show_directive("are you coming to austin?", cfg)
        assert d is not None
        assert d.ticket_url == "https://t/austin"
        assert "Austin" in d.instruction
        assert label in d.instruction
        assert "Cap City" in d.instruction

    def test_upcoming_match_without_show_link_falls_back_to_generic(self):
        cfg = _cfg(shows=[{"city": "Austin", "venue": "Cap City", "date": "Jun 25, 2026"}],
                   tickets="https://generic/tickets")
        d = sc.build_show_directive("austin?", cfg)
        assert d.ticket_url == "https://generic/tickets"

    def test_no_city_match_lists_upcoming(self):
        # Relative future dates so both shows stay upcoming regardless of when
        # the suite runs.
        today = datetime.now(timezone.utc).date()
        cfg = _cfg(shows=[
            {"city": "Austin", "venue": "Cap City", "date": _label(today + timedelta(days=15))},
            {"city": "Chicago", "venue": "Zanies", "date": _label(today + timedelta(days=25))},
        ], tickets="https://generic/tickets")
        d = sc.build_show_directive("are you touring anywhere?", cfg)
        assert d is not None
        assert d.ticket_url == "https://generic/tickets"
        assert "Austin" in d.instruction and "Chicago" in d.instruction

    def test_recent_past_match_says_just_there(self, monkeypatch):
        monkeypatch.setenv("SHOW_CALENDAR_BANDSINTOWN", "1")
        recent = (datetime.now(timezone.utc).date() - timedelta(days=10)).isoformat()

        def fake_fetch(artist, past=False):
            if past:
                return [{"datetime": f"{recent}T20:00:00",
                         "venue": {"name": "Mimi Ohio Theatre", "city": "Cleveland", "region": "OH"},
                         "offers": []}]
            return []  # no upcoming

        monkeypatch.setattr(sc, "_fetch_bandsintown", fake_fetch)
        cfg = _cfg(artist="zarna garg", tickets="https://generic/tickets")
        d = sc.build_show_directive("coming back to cleveland?", cfg)
        assert d is not None
        assert "just" in d.instruction.lower()
        assert "Cleveland" in d.instruction
        # Past show's link is stale — must use the generic page.
        assert d.ticket_url == "https://generic/tickets"

    def test_old_past_show_filtered_out(self, monkeypatch):
        old = (datetime.now(timezone.utc).date() - timedelta(days=200)).isoformat()

        def fake_fetch(artist, past=False):
            if past:
                return [{"datetime": f"{old}T20:00:00",
                         "venue": {"name": "Old Venue", "city": "Denver"}, "offers": []}]
            return []

        monkeypatch.setattr(sc, "_fetch_bandsintown", fake_fetch)
        cfg = _cfg(artist="zarna garg")
        # No upcoming, past too old -> empty calendar -> no directive.
        assert sc.build_show_directive("denver?", cfg) is None


# ---------------------------------------------------------------------------
# Source priority + cache
# ---------------------------------------------------------------------------

class TestPriorityAndCache:
    def test_config_preferred_by_default_bandsintown_disabled(self, monkeypatch):
        # Bandsintown is OFF by default: the config upcoming_shows list is the
        # source of truth (kept in sync with the official tickets page), so even
        # a live-looking feed must be ignored without the env flag.
        def fake_fetch(artist, past=False):
            raise AssertionError("Bandsintown must not be fetched when disabled")

        monkeypatch.delenv("SHOW_CALENDAR_BANDSINTOWN", raising=False)
        monkeypatch.setattr(sc, "_fetch_bandsintown", fake_fetch)
        label = _label(datetime.now(timezone.utc).date() + timedelta(days=20))
        cfg = _cfg(slug="prio-off", artist="zarna garg",
                   shows=[{"city": "Austin", "venue": "Cap City", "date": label}])
        cal = sc.get_calendar(cfg)
        assert [s.city for s in cal.upcoming] == ["Austin"]

    def test_bandsintown_preferred_when_flag_enabled(self, monkeypatch):
        monkeypatch.setenv("SHOW_CALENDAR_BANDSINTOWN", "1")

        def fake_fetch(artist, past=False):
            if past:
                return []
            return SAMPLE_EVENTS

        monkeypatch.setattr(sc, "_fetch_bandsintown", fake_fetch)
        cfg = _cfg(slug="prio", artist="zarna garg",
                   shows=[{"city": "Austin", "venue": "Cap City", "date": "Jun 25, 2026"}])
        cal = sc.get_calendar(cfg)
        cities = {s.city for s in cal.upcoming}
        assert "Chicago" in cities  # came from Bandsintown, not config's Austin
        assert "Austin" not in cities

    def test_falls_back_to_config_when_api_empty(self, monkeypatch):
        monkeypatch.setattr(sc, "_fetch_bandsintown", lambda artist, past=False: [])
        label = _label(datetime.now(timezone.utc).date() + timedelta(days=20))
        cfg = _cfg(slug="fallback", artist="zarna garg",
                   shows=[{"city": "Austin", "venue": "Cap City", "date": label}])
        cal = sc.get_calendar(cfg)
        assert [s.city for s in cal.upcoming] == ["Austin"]

    def test_calendar_is_cached(self, monkeypatch):
        monkeypatch.setenv("SHOW_CALENDAR_BANDSINTOWN", "1")
        calls = {"n": 0}

        def fake_fetch(artist, past=False):
            calls["n"] += 1
            return SAMPLE_EVENTS if not past else []

        monkeypatch.setattr(sc, "_fetch_bandsintown", fake_fetch)
        cfg = _cfg(slug="cached", artist="zarna garg")
        sc.get_calendar(cfg)
        first = calls["n"]
        sc.get_calendar(cfg)
        assert calls["n"] == first  # second call served from cache

    def test_clear_cache_forces_refetch(self, monkeypatch):
        monkeypatch.setenv("SHOW_CALENDAR_BANDSINTOWN", "1")
        calls = {"n": 0}

        def fake_fetch(artist, past=False):
            calls["n"] += 1
            return SAMPLE_EVENTS if not past else []

        monkeypatch.setattr(sc, "_fetch_bandsintown", fake_fetch)
        cfg = _cfg(slug="clear", artist="zarna garg")
        sc.get_calendar(cfg)
        n1 = calls["n"]
        sc.clear_cache()
        sc.get_calendar(cfg)
        assert calls["n"] > n1


# ---------------------------------------------------------------------------
# Generator SHOW-prompt integration
# ---------------------------------------------------------------------------

class TestGeneratorIntegration:
    def _prompt(self, show_directive=None, cfg=None):
        return _build_prompt(
            Intent.SHOW,
            "when are you coming to chicago?",
            chunks=[],
            history=[],
            creator_config=cfg or _cfg(tickets="https://example.com/tickets/"),
            show_directive=show_directive,
        )

    def test_without_directive_uses_generic_link(self):
        prompt = self._prompt()
        assert "https://example.com/tickets/" in prompt
        # No directive block (the guardrails legitimately mention "Show guidance").
        assert "Show guidance (follow this precisely)" not in prompt

    def test_with_directive_injects_instruction_and_show_link(self):
        d = sc.ShowDirective(
            instruction="Zarna HAS an upcoming show: Zanies in Chicago on Jul 12, 2026.",
            ticket_url="https://tix.example/chicago",
        )
        prompt = self._prompt(show_directive=d)
        assert "Show guidance" in prompt
        assert "Zanies in Chicago on Jul 12, 2026" in prompt
        assert "https://tix.example/chicago" in prompt

    def test_directive_without_url_keeps_generic_link(self):
        d = sc.ShowDirective(instruction="We were just in Cleveland!", ticket_url="")
        prompt = self._prompt(show_directive=d)
        assert "We were just in Cleveland!" in prompt
        assert "https://example.com/tickets/" in prompt

    def test_show_prompt_includes_todays_date(self):
        # The bot must know what "today" is so a fan asking about "tonight"
        # isn't told a show happening today is in the future.
        from datetime import datetime
        try:
            from zoneinfo import ZoneInfo
            today = datetime.now(ZoneInfo("America/New_York"))
        except Exception:
            today = datetime.now()
        expected = today.strftime("%A, %B %d, %Y").replace(" 0", " ")
        prompt = self._prompt()
        assert "Today's date is" in prompt
        assert expected in prompt

    def test_conversational_prompts_include_todays_date(self):
        # Date awareness applies to ordinary chat too, not only SHOW replies.
        cfg = _cfg(tickets="https://example.com/tickets/")
        for intent in (Intent.GENERAL, Intent.QUESTION, Intent.GREETING,
                       Intent.PERSONAL, Intent.FEEDBACK):
            prompt = _build_prompt(
                intent, "hey what's up?", chunks=[], history=[], creator_config=cfg,
            )
            assert "Today's date is" in prompt, f"missing date line for {intent}"


def test_current_date_line_respects_config_timezone():
    # A creator in a different timezone gets that timezone's local date.
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
    except Exception:
        import pytest
        pytest.skip("zoneinfo unavailable")
    from app.brain.generator import _current_date_line
    cfg = CreatorConfig(slug="tz", name="TZ", timezone="Asia/Tokyo")
    line = _current_date_line(cfg)
    expected = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%A, %B %d, %Y").replace(" 0", " ")
    assert expected in line


def test_today_for_config_uses_timezone():
    cfg = CreatorConfig(slug="tz", name="TZ", timezone="America/New_York")
    # Should not raise and should return a date object.
    from datetime import date
    assert isinstance(sc._today_for_config(cfg), date)


# ---------------------------------------------------------------------------
# Config date-label parsing
# ---------------------------------------------------------------------------

def _label(d):
    """Format a date the way the config labels are written ('Jun 5, 2026')."""
    return d.strftime("%b %d, %Y").replace(" 0", " ")


class TestParseLabelDates:
    def test_single_day(self):
        assert sc._parse_label_dates("Jun 9, 2026") == (
            sc.date(2026, 6, 9), sc.date(2026, 6, 9))

    def test_day_range(self):
        assert sc._parse_label_dates("Jun 5-6, 2026") == (
            sc.date(2026, 6, 5), sc.date(2026, 6, 6))

    def test_full_month_name(self):
        assert sc._parse_label_dates("December 31, 2026") == (
            sc.date(2026, 12, 31), sc.date(2026, 12, 31))

    def test_unparseable(self):
        assert sc._parse_label_dates("sometime soon") == (None, None)
        assert sc._parse_label_dates("") == (None, None)


# ---------------------------------------------------------------------------
# State-name region matching (Kentucky -> KY)
# ---------------------------------------------------------------------------

class TestRegionMatching:
    def test_full_state_name_matches_abbrev_region(self):
        shows = [sc.Show(city="Lexington", region="KY", venue="Comedy Off Broadway",
                         date_label="Jun 5, 2026")]
        m = sc._match_city("are you coming to kentucky?", shows)
        assert m and m.city == "Lexington"

    def test_abbrev_in_message_matches(self):
        shows = [sc.Show(city="Austin", region="TX", venue="Cap City", date_label="x")]
        assert sc._match_city("any shows in TX?", shows).city == "Austin"

    def test_city_still_takes_priority(self):
        shows = [sc.Show(city="Lexington", region="KY", venue="V", date_label="x")]
        assert sc._match_city("coming to lexington?", shows).city == "Lexington"

    def test_two_letter_code_not_substring_matched(self):
        # "ky" must not match inside an unrelated word.
        shows = [sc.Show(city="Nowhere", region="KY", venue="V", date_label="x")]
        assert sc._match_city("is it rocky out there", shows) is None


# ---------------------------------------------------------------------------
# Continent / multi-country region matching ("any shows in Europe?")
# ---------------------------------------------------------------------------

class TestContinentMatching:
    def _intl_shows(self):
        return [
            sc.Show(city="London", region="UK", venue="Leicester Square Theatre", date_label="Aug 7-8, 2026"),
            sc.Show(city="Dublin", region="Ireland", venue="The Ambassador Theatre", date_label="Aug 5, 2026"),
            sc.Show(city="Berlin", region="Germany", venue="PUNCH L!NE", date_label="Aug 21, 2026"),
            sc.Show(city="Stockholm", region="Sweden", venue="Bio Skandia", date_label="Aug 25, 2026"),
            sc.Show(city="Singapore", region="Singapore", venue="MES Theatre", date_label="Aug 1, 2026"),
            sc.Show(city="Austin", region="TX", venue="Cap City", date_label="Jun 25, 2026"),
        ]

    def test_europe_matches_all_european_shows(self):
        key, matches = sc._match_region("any shows in europe?", self._intl_shows())
        assert key == "europe"
        cities = {s.city for s in matches}
        assert {"London", "Dublin", "Berlin", "Stockholm"} <= cities
        assert "Singapore" not in cities and "Austin" not in cities

    def test_asia_matches_singapore(self):
        key, matches = sc._match_region("are you coming to asia?", self._intl_shows())
        assert key == "asia"
        assert [s.city for s in matches] == ["Singapore"]

    def test_no_region_word_returns_empty(self):
        key, matches = sc._match_region("are you coming to austin?", self._intl_shows())
        assert key is None and matches == []


class TestCityAliases:
    def _shows(self):
        return [
            sc.Show(city="New York", region="NY", venue="Beacon Theatre", date_label="Dec 31, 2026"),
            sc.Show(city="San Francisco", region="CA", venue="Palace of Fine Arts", date_label="Oct 10, 2026"),
        ]

    def test_nyc_matches_new_york(self):
        assert sc._match_city("when are you coming to nyc?", self._shows()).city == "New York"

    def test_sf_matches_san_francisco(self):
        assert sc._match_city("any shows in SF?", self._shows()).city == "San Francisco"

    def test_short_alias_requires_word_boundary(self):
        # "sf" must not match inside an unrelated word like "useful".
        shows = [sc.Show(city="San Francisco", region="CA", venue="V", date_label="x")]
        assert sc._match_city("is this useful", shows) is None

    def test_alias_only_matches_when_show_exists(self):
        # No NYC show present -> alias must not fabricate a match.
        shows = [sc.Show(city="Austin", region="TX", venue="V", date_label="x")]
        assert sc._match_city("coming to nyc?", shows) is None


# ---------------------------------------------------------------------------
# Config calendar: chronological sort + past filtering + recent_past
# ---------------------------------------------------------------------------

class TestConfigCalendar:
    def test_sorts_upcoming_and_drops_past(self):
        today = datetime.now(timezone.utc).date()
        soon = today + timedelta(days=10)
        later = today + timedelta(days=40)
        old = today - timedelta(days=120)
        cfg = _cfg(slug="cfgcal", shows=[
            {"city": "Later", "venue": "L", "date": _label(later)},
            {"city": "Soon", "venue": "S", "date": _label(soon)},
            {"city": "Old", "venue": "O", "date": _label(old)},
        ])
        cal = sc.get_calendar(cfg)
        cities = [s.city for s in cal.upcoming]
        assert cities == ["Soon", "Later"]   # sorted, "Old" filtered out

    def test_recent_past_surfaced_for_just_there(self):
        today = datetime.now(timezone.utc).date()
        recent = today - timedelta(days=12)
        cfg = _cfg(slug="cfgpast", shows=[
            {"city": "Cleveland", "venue": "Mimi Ohio", "date": _label(recent)},
        ])
        cal = sc.get_calendar(cfg)
        assert [s.city for s in cal.upcoming] == []
        assert [s.city for s in cal.recent_past] == ["Cleveland"]

    def test_just_there_directive_from_config(self):
        today = datetime.now(timezone.utc).date()
        recent = today - timedelta(days=12)
        cfg = _cfg(slug="cfgjust", shows=[
            {"city": "Cleveland", "venue": "Mimi Ohio", "date": _label(recent)},
        ], tickets="https://generic/tickets")
        d = sc.build_show_directive("coming back to cleveland?", cfg)
        assert d is not None
        assert "just" in d.instruction.lower()
        assert d.ticket_url == "https://generic/tickets"


class TestDateMatching:
    """Fan-typed dates must resolve to the real show — never a denial."""

    def _cfg_with_dated_shows(self):
        today = datetime.now(timezone.utc).date()
        d1 = today + timedelta(days=30)
        d2 = today + timedelta(days=60)
        self.d1, self.d2 = d1, d2
        return _cfg(slug="dated", shows=[
            {"city": "Portland", "state": "ME", "venue": "State Theatre", "date": _label(d1)},
            {"city": "Boston", "state": "MA", "venue": "Wilbur", "date": _label(d2)},
        ], tickets="https://generic/tickets")

    def test_month_day_resolves_to_show(self):
        cfg = self._cfg_with_dated_shows()
        msg = f"I don't see tickets for {self.d1.strftime('%b')}{self.d1.day} {self.d1.year}"
        d = sc.build_show_directive(msg, cfg)
        assert d is not None
        assert "Portland" in d.instruction
        assert "never deny this show exists" in d.instruction

    def test_numeric_date_resolves_to_show(self):
        cfg = self._cfg_with_dated_shows()
        d = sc.build_show_directive(f"is there a show on {self.d2.month}/{self.d2.day}?", cfg)
        assert d is not None
        assert "Boston" in d.instruction

    def test_date_with_no_show_gets_honest_no(self):
        cfg = self._cfg_with_dated_shows()
        # Pick a date 10 days out — guaranteed not to collide with d1/d2.
        miss = datetime.now(timezone.utc).date() + timedelta(days=10)
        d = sc.build_show_directive(f"anything on {miss.strftime('%b')} {miss.day}?", cfg)
        assert d is not None
        assert "NO show on that exact date" in d.instruction
        assert "Do not invent any other date" in d.instruction

    def test_no_date_no_city_falls_through_to_generic(self):
        cfg = self._cfg_with_dated_shows()
        d = sc.build_show_directive("where can I buy tickets?", cfg)
        assert d is not None
        assert "NEXT show" in d.instruction


class TestSameCityDisambiguation:
    """Portland OR (just passed) vs Portland ME (upcoming) must never mix."""

    def _cfg_two_portlands(self):
        today = datetime.now(timezone.utc).date()
        past = today - timedelta(days=5)
        future = today + timedelta(days=90)
        self.future_label = _label(future)
        return _cfg(slug="portlands", shows=[
            {"city": "Portland", "state": "OR", "venue": "Helium", "date": _label(past)},
            {"city": "Portland", "state": "ME", "venue": "State Theatre", "date": _label(future)},
        ], tickets="https://generic/tickets")

    def test_bare_portland_gets_upcoming_show(self):
        d = sc.build_show_directive("are you coming to portland?", self._cfg_two_portlands())
        assert d is not None
        assert "State Theatre" in d.instruction
        assert self.future_label in d.instruction

    def test_portland_maine_gets_upcoming_show(self):
        d = sc.build_show_directive("coming to Portland Maine?", self._cfg_two_portlands())
        assert "State Theatre" in d.instruction

    def test_portland_oregon_gets_just_there(self):
        d = sc.build_show_directive("coming back to portland oregon?", self._cfg_two_portlands())
        assert d is not None
        assert "JUST there" in d.instruction
        assert "Helium" in d.instruction

    def test_upcoming_city_and_state_disambiguates(self):
        today = datetime.now(timezone.utc).date()
        cfg = _cfg(slug="twoup", shows=[
            {"city": "Portland", "state": "OR", "venue": "Helium", "date": _label(today + timedelta(days=20))},
            {"city": "Portland", "state": "ME", "venue": "State Theatre", "date": _label(today + timedelta(days=90))},
        ])
        d = sc.build_show_directive("any shows in portland maine?", cfg)
        assert "State Theatre" in d.instruction
        d = sc.build_show_directive("any shows in portland oregon?", cfg)
        assert "Helium" in d.instruction


class TestAmbiguousStateCodes:
    """2-letter codes that are English words (ME, OR, IN, OK…) must only match
    when the fan typed them in caps — 'any shows near me?' must NOT sell Maine."""

    def _cfg_maine(self):
        today = datetime.now(timezone.utc).date()
        self.label = _label(today + timedelta(days=60))
        return _cfg(slug="maine", shows=[
            {"city": "Portland", "state": "ME", "venue": "State Theatre", "date": self.label},
        ], tickets="https://generic/tickets")

    def test_near_me_does_not_match_maine(self):
        d = sc.build_show_directive("any shows near me?", self._cfg_maine())
        assert d is not None
        # Falls to the generic fallback, never claims to know the fan is in ME.
        assert "State Theatre in Portland" not in d.instruction.split("NEXT show")[0]
        assert "NEXT show" in d.instruction

    def test_uppercase_code_still_matches(self):
        d = sc.build_show_directive("are you coming to Portland ME?", self._cfg_maine())
        assert "State Theatre" in d.instruction

    def test_or_conjunction_does_not_match_oregon(self):
        today = datetime.now(timezone.utc).date()
        cfg = _cfg(slug="oregon", shows=[
            {"city": "Bend", "state": "OR", "venue": "Tower", "date": _label(today + timedelta(days=30))},
        ])
        d = sc.build_show_directive("boston or chicago please?", cfg)
        assert d is not None
        assert "The fan asked about Bend" not in d.instruction

    def test_full_state_name_still_matches(self):
        d = sc.build_show_directive("anything in maine?", self._cfg_maine())
        assert "State Theatre" in d.instruction


class TestNextShowDirective:
    def test_no_city_names_the_soonest_show(self):
        today = datetime.now(timezone.utc).date()
        soon = today + timedelta(days=5)
        later = today + timedelta(days=30)
        cfg = _cfg(slug="nextshow", shows=[
            {"city": "Later City", "venue": "LV", "date": _label(later)},
            {"city": "Soon City", "venue": "SV", "date": _label(soon)},
        ])
        d = sc.build_show_directive("what's your next show?", cfg)
        assert d is not None
        # Names the soonest (Soon City), not the array-order first.
        assert "Soon City" in d.instruction
        assert _label(soon) in d.instruction
        assert "NEXT show" in d.instruction


# ---------------------------------------------------------------------------
# Intent routing for tour/location questions (fast path, no LLM)
# ---------------------------------------------------------------------------

class TestShowIntentRouting:
    def test_are_you_coming_to_place_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("Are you coming to Kentucky?") == Intent.SHOW

    def test_next_show_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("What's your next show?") == Intent.SHOW

    def test_do_you_have_shows_in_place_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("Do you have any shows in Kentucky?") == Intent.SHOW

    def test_playing_in_city_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("are you playing in Chicago?") == Intent.SHOW

    def test_having_tickets_still_not_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("I already have my tickets!") == Intent.FEEDBACK

    def test_watch_your_shows_not_forced_to_show(self):
        # "do your kids watch your shows" must not hit the SHOW fast path
        # ("your shows" is not "shows in") — leave it to the LLM classifier.
        from app.brain.intent import _fast_classify
        assert _fast_classify("do your kids watch your shows") is not Intent.SHOW

    def test_coming_back_to_city_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("are you coming back to Lexington?") == Intent.SHOW
        assert _fast_classify("when will you come back to philly") == Intent.SHOW


class TestDateDisputeRouting:
    """A fan disputing a date the bot just gave ("the website doesnt show august 6")
    must route to SHOW so the calendar directive reaffirms the real show — left as
    QUESTION the LLM freestyles and can concede a real show doesn't exist."""

    def test_website_dispute_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("u sure? the website doesnt show august 6") == Intent.SHOW

    def test_numeric_date_dispute_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("I looked and there's nothing on 12/31?!") == Intent.SHOW

    def test_cant_find_tickets_for_date_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("I can't find tickets for march 3?") == Intent.SHOW

    def test_date_without_dispute_word_not_forced(self):
        # "my birthday is august 6" — date but no dispute/lookup word.
        from app.brain.intent import _fast_classify
        assert _fast_classify("my birthday is august 6") is not Intent.SHOW

    def test_possession_still_wins(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify("I already have my tickets for dec 31!") == Intent.FEEDBACK


class TestPraiseDoesNotMaskRequest:
    """A compliment must not short-circuit a real request to FEEDBACK.

    Before the fix, a leading praise phrase ("you're so funny", "great show")
    returned FEEDBACK from the fast path and the message never reached the
    SHOW keyword check or the Gemini classifier.
    """

    def test_praise_plus_show_keyword_is_show(self):
        # "when are you" is a SHOW keyword; the praise must not pre-empt it.
        from app.brain.intent import _fast_classify
        assert _fast_classify("you're so funny, when are you in London?") == Intent.SHOW

    def test_praise_plus_coming_back_is_show(self):
        from app.brain.intent import _fast_classify
        assert _fast_classify(
            "great show last night! when are you coming back to philly?"
        ) == Intent.SHOW

    def test_praise_plus_vague_request_defers_to_llm(self):
        # No SHOW keyword here ("see you live" is not one), so the fast path must
        # NOT label it FEEDBACK — it returns None to let the Gemini classifier
        # read the whole message (which classifies it as SHOW live).
        from app.brain.intent import _fast_classify
        result = _fast_classify("ur so funny, where can i actually see u live")
        assert result is None

    def test_pure_praise_still_fast_paths_to_feedback(self):
        # Regression: plain compliments must keep skipping the LLM.
        from app.brain.intent import _fast_classify
        assert _fast_classify("great show!") == Intent.FEEDBACK
        assert _fast_classify("you're hilarious") == Intent.FEEDBACK
        assert _fast_classify("loved it tonight, you killed it") == Intent.FEEDBACK

    def test_having_tickets_still_feedback(self):
        # Regression: possession guard unchanged.
        from app.brain.intent import _fast_classify
        assert _fast_classify("I already have my tickets!") == Intent.FEEDBACK
