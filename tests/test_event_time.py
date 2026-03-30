"""Live show event timezone helpers."""

from datetime import datetime, timezone

import pytest

from app.live_shows.event_time import (
    effective_event_timezone,
    format_window_human,
    parse_local_datetime,
    timezone_select_value_from_show,
    utc_to_datetime_local_value,
)


def test_effective_event_timezone_legacy_null():
    assert effective_event_timezone(None) == "UTC"
    assert effective_event_timezone("") == "UTC"


def test_parse_nyc_3pm_to_utc_edt():
    # Mar 15 2025 is EDT — 3pm Eastern = 19:00 UTC
    utc_dt = parse_local_datetime("2025-03-15T15:00", "America/New_York")
    assert utc_dt is not None
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.hour == 19
    assert utc_dt.day == 15


def test_roundtrip_datetime_local_value():
    utc_in = datetime(2025, 3, 15, 19, 0, tzinfo=timezone.utc)
    local_str = utc_to_datetime_local_value(utc_in, "America/New_York")
    assert local_str == "2025-03-15T15:00"
    back = parse_local_datetime(local_str, "America/New_York")
    assert back == utc_in


def test_format_window_human_empty():
    h = format_window_human(None, None, None)
    assert "No window set" in h
    assert "UTC" in h


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, "UTC"),
        ("", "UTC"),
        ("America/Chicago", "America/Chicago"),
    ],
)
def test_timezone_select_value_from_show(raw, expected):
    assert timezone_select_value_from_show(raw) == expected
