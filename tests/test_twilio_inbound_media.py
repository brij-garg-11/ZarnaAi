"""
Tests for inbound Twilio handling of the three message shapes we want the
212-number migration to survive: long messages, photos (MMS), and many
messages in quick succession.

These exercise the pure parsing/filtering layer in TwilioAdapter plus the
per-phone rate limiter, so they run without a live Twilio client, DB, or brain.
"""

import importlib

import pytest

from app.messaging.twilio_adapter import (
    INBOUND_PHOTO_PLACEHOLDER,
    TwilioAdapter,
)


def _main():
    """Import the app entry module the same way the existing suite does —
    skip cleanly if the twilio SDK isn't installed in this environment."""
    pytest.importorskip("twilio")
    return importlib.import_module("main")


def _adapter():
    return TwilioAdapter(
        account_sid="ACfake",
        auth_token="tokenfake",
        from_number="+12125550000",
        messaging_service_sid="MGfake",
    )


# ---------------------------------------------------------------------------
# Photos / MMS
# ---------------------------------------------------------------------------


def test_caption_less_photo_gets_placeholder_body():
    """A photo with no text must NOT be dropped — it gets a placeholder so the
    fan still receives an in-character reply."""
    a = _adapter()
    form = {"From": "+12125551234", "To": "+12125550000", "Body": "", "NumMedia": "1"}
    body = a.normalize_inbound_body(form, form["Body"])
    assert body == INBOUND_PHOTO_PLACEHOLDER

    phone, message = a.filter_inbound_for_ai(form["From"], body)
    assert phone == "+12125551234"
    assert message == INBOUND_PHOTO_PLACEHOLDER


def test_photo_with_caption_keeps_caption():
    a = _adapter()
    form = {"From": "+12125551234", "Body": "look at this!", "NumMedia": "2"}
    body = a.normalize_inbound_body(form, form["Body"])
    assert body == "look at this!"


def test_plain_text_with_no_media_is_unchanged():
    a = _adapter()
    form = {"From": "+12125551234", "Body": "hey there", "NumMedia": "0"}
    assert a.normalize_inbound_body(form, form["Body"]) == "hey there"


def test_empty_message_no_media_stays_empty():
    """No body and no media → still nothing to reply to (filtered as before)."""
    a = _adapter()
    form = {"From": "+12125551234", "Body": "", "NumMedia": "0"}
    # Body is returned unchanged (falsy) so filter_inbound_for_ai drops it.
    assert not a.normalize_inbound_body(form, form["Body"])
    phone, message = a.filter_inbound_for_ai(form["From"], a.normalize_inbound_body(form, form["Body"]))
    assert phone is None and message is None


def test_count_inbound_media_handles_garbage():
    a = _adapter()
    assert a.count_inbound_media({"NumMedia": "3"}) == 3
    assert a.count_inbound_media({"NumMedia": ""}) == 0
    assert a.count_inbound_media({"NumMedia": "abc"}) == 0
    assert a.count_inbound_media({}) == 0
    assert a.count_inbound_media({"NumMedia": None}) == 0


def test_whatsapp_image_is_treated_as_photo():
    a = _adapter()
    form = {"From": "whatsapp:+12125551234", "Body": "", "NumMedia": "1"}
    assert a.normalize_inbound_body(form, form["Body"]) == INBOUND_PHOTO_PLACEHOLDER


# ---------------------------------------------------------------------------
# Long messages
# ---------------------------------------------------------------------------


def test_long_inbound_body_passes_through_unfiltered():
    """A long multi-segment SMS (Twilio concatenates segments into one Body)
    must reach the AI intact — we never truncate inbound text."""
    a = _adapter()
    long_text = "I have a really long story to tell you. " * 50  # ~2000 chars
    form = {"From": "+12125551234", "Body": long_text, "NumMedia": "0"}
    body = a.normalize_inbound_body(form, form["Body"])
    phone, message = a.filter_inbound_for_ai(form["From"], body)
    assert phone == "+12125551234"
    assert message == long_text
    assert len(message) > 1000


# ---------------------------------------------------------------------------
# Many messages — per-phone rate limiter behavior
# ---------------------------------------------------------------------------


def test_rate_limiter_allows_burst_then_blocks():
    """Documents the current 3-per-60s limit: the 4th rapid message from the
    same phone is dropped. If this test fails, the prod rate limit changed."""
    main = _main()
    phone = "+12125559999"
    # Clean slate for this phone.
    with main._rate_lock:
        main._rate_data.pop(phone, None)

    allowed = [not main._is_rate_limited(phone) for _ in range(main._RATE_MAX + 2)]
    assert allowed[: main._RATE_MAX] == [True] * main._RATE_MAX
    assert allowed[main._RATE_MAX] is False  # first message over the limit


def test_rate_limiter_is_per_phone():
    main = _main()
    a, b = "+12125550001", "+12125550002"
    with main._rate_lock:
        main._rate_data.pop(a, None)
        main._rate_data.pop(b, None)
    for _ in range(main._RATE_MAX):
        main._is_rate_limited(a)
    # Phone A is now exhausted, but phone B is unaffected.
    assert main._is_rate_limited(a) is True
    assert main._is_rate_limited(b) is False


def test_duplicate_message_sid_is_deduped():
    """Twilio retries deliver the same MessageSid — we process it once."""
    main = _main()
    sid = "SMtest-dedup-0001"
    with main._seen_lock:
        main._seen_message_ids.pop(sid, None)
    assert main._already_processed(sid) is False
    assert main._already_processed(sid) is True
