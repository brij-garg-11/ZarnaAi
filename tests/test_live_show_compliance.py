"""Live-show keyword joins must carry the A2P/CTIA disclosure.

Keyword-only joins short-circuit the webhook (suppress_ai) before the normal
AI/welcome path that adds the compliance footer, so the disclosure has to ride
on the join confirmation itself for brand-new fans. These tests cover the
main.py wiring helpers directly (adapters faked — no Twilio/network calls).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import pytest

# main.py imports the twilio SDK at module load; skip cleanly if unavailable.
pytest.importorskip("twilio")

import main as app_module
from app.messaging.contact_card import COMPLIANCE_FOOTER


@pytest.fixture
def captured(monkeypatch):
    """Capture confirmation sends and run the pool submission inline."""
    calls = []

    def fake_slick(phone, body, *a, **kw):
        calls.append({"channel": "slicktext", "phone": phone, "body": body})

    def fake_twilio(phone, body, *a, **kw):
        calls.append({"channel": "twilio", "phone": phone, "body": body})

    monkeypatch.setattr(app_module.slicktext, "send_reply", fake_slick)
    monkeypatch.setattr(app_module.twilio, "send_reply", fake_twilio)
    # Run the queued job synchronously so assertions don't race the pool.
    monkeypatch.setattr(app_module._confirm_pool, "submit", lambda fn, *a, **kw: fn())
    return calls


class TestJoinConfirmationCompliance:
    def test_appends_footer_for_new_fan(self, captured):
        app_module._send_join_confirmation_async(
            "+15551234567", "slicktext", "You're IN — see you tonight!",
            append_compliance=True,
        )
        assert len(captured) == 1
        body = captured[0]["body"]
        assert body.startswith("You're IN — see you tonight!")
        assert COMPLIANCE_FOOTER in body

    def test_no_footer_for_returning_fan(self, captured):
        app_module._send_join_confirmation_async(
            "+15551234567", "slicktext", "You're IN — see you tonight!",
            append_compliance=False,
        )
        assert len(captured) == 1
        assert captured[0]["body"] == "You're IN — see you tonight!"
        assert COMPLIANCE_FOOTER not in captured[0]["body"]

    def test_default_does_not_append(self, captured):
        # Backward-compatible: existing callers that don't pass the flag are
        # unchanged (no disclosure appended).
        app_module._send_join_confirmation_async("+15551234567", "twilio", "hi")
        assert captured[0]["body"] == "hi"

    def test_footer_not_double_appended(self, captured):
        body_with_footer = f"You're IN!\n\n{COMPLIANCE_FOOTER}"
        app_module._send_join_confirmation_async(
            "+15551234567", "slicktext", body_with_footer, append_compliance=True,
        )
        assert captured[0]["body"].count(COMPLIANCE_FOOTER) == 1

    def test_minimal_opt_in_gets_footer(self, captured):
        # The no-themed-copy case ("other" category) sends the minimal opt-in
        # with the disclosure appended.
        app_module._send_join_confirmation_async(
            "+15551234567", "twilio", app_module.MINIMAL_OPT_IN_CONFIRMATION,
            append_compliance=True,
        )
        body = captured[0]["body"]
        assert app_module.MINIMAL_OPT_IN_CONFIRMATION in body
        assert COMPLIANCE_FOOTER in body

    def test_minimal_opt_in_constant_is_nonempty(self):
        assert app_module.MINIMAL_OPT_IN_CONFIRMATION.strip()


class TestIsNewFan:
    def test_true_when_first_message(self, monkeypatch):
        monkeypatch.setattr(app_module.brain.storage, "is_first_message", lambda p: True)
        assert app_module._live_show_is_new_fan("+15551234567") is True

    def test_false_when_returning(self, monkeypatch):
        monkeypatch.setattr(app_module.brain.storage, "is_first_message", lambda p: False)
        assert app_module._live_show_is_new_fan("+15551234567") is False

    def test_false_on_empty_phone(self):
        assert app_module._live_show_is_new_fan("") is False

    def test_false_and_swallows_errors(self, monkeypatch):
        def boom(p):
            raise RuntimeError("db down")

        monkeypatch.setattr(app_module.brain.storage, "is_first_message", boom)
        # Never raises; defaults to not-new so we don't block or double-send.
        assert app_module._live_show_is_new_fan("+15551234567") is False
