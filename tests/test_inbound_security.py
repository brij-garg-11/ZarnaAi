"""Tests for inbound security helpers (no Flask context required)."""

import os
from unittest.mock import MagicMock, patch

from app.inbound_security import running_in_production, timing_safe_equal


def test_timing_safe_equal():
    assert timing_safe_equal("abc", "abc") is True
    assert timing_safe_equal("abc", "abz") is False
    assert timing_safe_equal("", "a") is False
    assert timing_safe_equal("a", "") is False


def test_running_in_production_monkeypatch(monkeypatch):
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("PRODUCTION", raising=False)
    assert running_in_production() is False

    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    assert running_in_production() is True

    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "production")
    assert running_in_production() is True


# ---------------------------------------------------------------------------
# Twilio validator fail-safe (C3)
# ---------------------------------------------------------------------------

def test_twilio_validate_signature_no_auth_token_returns_false():
    """When TWILIO_AUTH_TOKEN is missing, validate_signature must return False (fail-safe)."""
    from app.messaging.twilio_adapter import TwilioAdapter
    adapter = TwilioAdapter(account_sid="", auth_token="", from_number="")
    result = adapter.validate_signature("https://example.com/twilio/webhook", {}, "fake-sig")
    assert result is False, "validate_signature must return False when no auth token is configured"


def test_twilio_validate_signature_with_valid_token():
    """When auth token is present, validation is delegated to the RequestValidator."""
    from app.messaging.twilio_adapter import TwilioAdapter
    adapter = TwilioAdapter(account_sid="ACtest", auth_token="some_token", from_number="")
    # Patch the internal validator to return True
    adapter._validator = MagicMock()
    adapter._validator.validate.return_value = True
    result = adapter.validate_signature("https://example.com/twilio/webhook", {}, "sig")
    assert result is True
    adapter._validator.validate.assert_called_once()


# ---------------------------------------------------------------------------
# SlickText secret (C4)
# ---------------------------------------------------------------------------

def test_verify_slicktext_secret_mismatch_returns_false(monkeypatch):
    """Wrong X-Zarna-Webhook-Secret header must be rejected."""
    monkeypatch.setenv("SLICKTEXT_WEBHOOK_SECRET", "correct-secret")
    from flask import Flask
    app = Flask(__name__)
    with app.test_request_context(
        "/slicktext/webhook",
        method="POST",
        headers={"X-Zarna-Webhook-Secret": "wrong-secret"},
    ):
        from app.inbound_security import verify_slicktext_webhook_secret
        assert verify_slicktext_webhook_secret() is False


def test_verify_slicktext_secret_match_returns_true(monkeypatch):
    """Correct X-Zarna-Webhook-Secret header must be accepted."""
    monkeypatch.setenv("SLICKTEXT_WEBHOOK_SECRET", "correct-secret")
    from flask import Flask
    app = Flask(__name__)
    with app.test_request_context(
        "/slicktext/webhook",
        method="POST",
        headers={"X-Zarna-Webhook-Secret": "correct-secret"},
    ):
        from app.inbound_security import verify_slicktext_webhook_secret
        assert verify_slicktext_webhook_secret() is True


def test_verify_slicktext_no_secret_returns_true(monkeypatch):
    """When no secret is configured, backward-compatible open behavior is preserved."""
    monkeypatch.delenv("SLICKTEXT_WEBHOOK_SECRET", raising=False)
    from flask import Flask
    app = Flask(__name__)
    with app.test_request_context("/slicktext/webhook", method="POST"):
        from app.inbound_security import verify_slicktext_webhook_secret
        assert verify_slicktext_webhook_secret() is True


# ---------------------------------------------------------------------------
# PII in SlickText logs (H9)
# ---------------------------------------------------------------------------

def test_slicktext_filter_inbound_logs_last4_not_full_phone():
    """Reserved keyword log must not contain the full phone number."""
    import logging
    from app.messaging.slicktext_adapter import SlickTextAdapter
    adapter = SlickTextAdapter.__new__(SlickTextAdapter)
    adapter._version = "v1"

    with patch("app.messaging.slicktext_adapter.logger") as mock_logger:
        adapter.filter_inbound_for_ai("+15551234567", "STOP")
        # All log calls should use last-4 masking, not the full number
        for call in mock_logger.info.call_args_list:
            args = str(call)
            assert "+15551234567" not in args, f"Full phone number leaked in log: {args}"

