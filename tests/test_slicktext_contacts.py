import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.messaging.slicktext_contacts import (
    DEFAULT_TEXTWORDS,
    classify_sync_action,
    fetch_unique_contacts,
    parse_slicktext_subscribed_date,
    parse_textword_config,
)


class _FakeResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, pages):
        self._pages = pages

    def get(self, _url, params, auth, timeout):
        key = params["textword"], params["offset"]
        return _FakeResponse(self._pages[key])


def test_parse_textword_config_defaults():
    assert parse_textword_config("") == list(DEFAULT_TEXTWORDS)


def test_parse_textword_config_custom_labels():
    assert parse_textword_config("123:vip,456") == [
        (123, "vip"),
        (456, "textword-456"),
    ]


def test_parse_slicktext_subscribed_date_rejects_bad_values():
    assert parse_slicktext_subscribed_date("2026-04-12 10:30:00") == "2026-04-12 10:30:00"
    assert parse_slicktext_subscribed_date("not-a-date") is None


def test_classify_sync_action():
    assert classify_sync_action(None, "2026-04-01 09:00:00") == "insert"
    assert (
        classify_sync_action(
            datetime(2026, 4, 1, tzinfo=timezone.utc),
            "2026-03-01 09:00:00",
        )
        == "update"
    )
    assert (
        classify_sync_action(
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            "2026-03-01 09:00:00",
        )
        == "skip"
    )
    assert (
        classify_sync_action(
            datetime(2026, 4, 1, 9, 0, 0, tzinfo=timezone.utc),
            "2026-04-01 09:00:00",
        )
        == "skip"
    )
    assert classify_sync_action(datetime(2026, 4, 1, tzinfo=timezone.utc), None) == "skip"


def test_fetch_unique_contacts_dedupes_across_textwords():
    client = _FakeClient(
        {
            (111, 0): {
                "meta": {"total": 2},
                "contacts": [
                    {"number": "+15550000001", "subscribedDate": "2026-04-01 09:00:00"},
                    {"number": "+15550000002", "subscribedDate": None},
                ],
            },
            (222, 0): {
                "meta": {"total": 2},
                "contacts": [
                    {"number": "+15550000002", "subscribedDate": "2026-04-02 09:00:00"},
                    {"number": "+15550000003", "subscribedDate": "2026-04-03 09:00:00"},
                ],
            },
        }
    )

    contacts, stats = fetch_unique_contacts(
        public_key="pub",
        private_key="priv",
        textwords=[(111, "one"), (222, "two")],
        client=client,
    )

    assert contacts == [
        ("+15550000001", "2026-04-01 09:00:00"),
        ("+15550000002", None),
        ("+15550000003", "2026-04-03 09:00:00"),
    ]
    assert stats.fetched == 4
    assert stats.unique == 3
    assert stats.with_dates == 2
