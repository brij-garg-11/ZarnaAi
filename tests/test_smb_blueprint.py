"""
Tests for app/smb/blueprint.py

Tests the Flask routes using the app's test client.
Twilio signature validation is disabled via env var for all tests.
The SMB brain and Twilio adapter are mocked so no real AI or SMS calls happen.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TWILIO_VALIDATE_SIGNATURE", "false")

# Stub twilio package so tests run without it installed (it lives on Railway only)
from unittest.mock import MagicMock as _MagicMock
for _mod in (
    "twilio", "twilio.base", "twilio.base.exceptions",
    "twilio.request_validator", "twilio.rest",
):
    sys.modules.setdefault(_mod, _MagicMock())

from unittest.mock import patch, MagicMock
import threading
import time


# ---------------------------------------------------------------------------
# Flask test client setup
# ---------------------------------------------------------------------------

def _make_test_app():
    """Build a minimal Flask app with only the SMB blueprint registered."""
    from flask import Flask
    from app.smb.blueprint import smb_bp
    app = Flask(__name__)
    app.register_blueprint(smb_bp)
    app.config["TESTING"] = True
    return app


# ---------------------------------------------------------------------------
# GET /smb/health
# ---------------------------------------------------------------------------

def test_health_returns_ok():
    app = _make_test_app()
    with app.test_client() as client:
        resp = client.get("/smb/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["service"] == "smb"
    print("✓ GET /smb/health returns 200 ok")


# ---------------------------------------------------------------------------
# POST /smb/twilio/webhook — basic routing
# ---------------------------------------------------------------------------

def _post_webhook(client, from_number="+15550003333", to_number="+15550002222", body="hello"):
    return client.post(
        "/smb/twilio/webhook",
        data={"From": from_number, "To": to_number, "Body": body},
        content_type="application/x-www-form-urlencoded",
    )


def test_webhook_returns_204():
    app = _make_test_app()
    with app.test_client() as client:
        with patch("app.smb.blueprint.threading.Thread") as mock_thread:
            mock_thread.return_value = MagicMock()
            mock_thread.return_value.start = lambda: None
            resp = _post_webhook(client)
    assert resp.status_code == 204
    print("✓ POST /smb/twilio/webhook returns 204 immediately")


def test_webhook_missing_from_returns_204_no_thread():
    """Missing From field should be silently ignored, no thread spawned."""
    app = _make_test_app()
    with app.test_client() as client:
        with patch("app.smb.blueprint.threading.Thread") as mock_thread:
            resp = client.post(
                "/smb/twilio/webhook",
                data={"To": "+15550002222", "Body": "hello"},
                content_type="application/x-www-form-urlencoded",
            )
    assert resp.status_code == 204
    mock_thread.assert_not_called()
    print("✓ missing From field silently ignored, no thread spawned")


def test_webhook_empty_body_returns_204_no_thread():
    """Empty Body should be silently ignored."""
    app = _make_test_app()
    with app.test_client() as client:
        with patch("app.smb.blueprint.threading.Thread") as mock_thread:
            resp = client.post(
                "/smb/twilio/webhook",
                data={"From": "+15550003333", "To": "+15550002222", "Body": "   "},
                content_type="application/x-www-form-urlencoded",
            )
    assert resp.status_code == 204
    mock_thread.assert_not_called()
    print("✓ whitespace-only Body silently ignored, no thread spawned")


def test_webhook_spawns_daemon_thread():
    """Valid request spawns a daemon thread with correct args."""
    app = _make_test_app()
    captured = {}

    def capture(**kwargs):
        captured.update(kwargs)
        m = MagicMock()
        m.start = lambda: None
        return m

    with app.test_client() as client:
        with patch("app.smb.blueprint.threading.Thread", side_effect=capture):
            _post_webhook(client, from_number="+15550003333", to_number="+15550002222", body="Opening tonight!")

    assert captured.get("daemon") is True
    assert captured.get("args") == ("+15550003333", "+15550002222", "Opening tonight!")
    print("✓ valid webhook spawns daemon thread with correct from/to/body args")


# ---------------------------------------------------------------------------
# _process_smb_message — async worker
# ---------------------------------------------------------------------------

def test_process_smb_message_sends_reply_when_brain_returns_text():
    from app.smb.blueprint import _process_smb_message

    with patch("app.smb.blueprint._brain") as mock_brain:
        with patch("app.smb.blueprint._twilio") as mock_twilio:
            mock_brain.handle_message.return_value = "Opening tonight 8pm!"
            _process_smb_message("+15550003333", "+15550002222", "COMEDY")

    mock_brain.handle_message.assert_called_once_with("+15550003333", "+15550002222", "COMEDY")
    mock_twilio.send_reply.assert_called_once_with("+15550003333", "Opening tonight 8pm!")
    print("✓ _process_smb_message sends reply when brain returns text")


def test_process_smb_message_no_send_when_brain_returns_none():
    from app.smb.blueprint import _process_smb_message

    with patch("app.smb.blueprint._brain") as mock_brain:
        with patch("app.smb.blueprint._twilio") as mock_twilio:
            mock_brain.handle_message.return_value = None
            _process_smb_message("+15550003333", "+15550002222", "random text")

    mock_twilio.send_reply.assert_not_called()
    print("✓ _process_smb_message skips send when brain returns None")


def test_process_smb_message_no_send_when_brain_returns_empty():
    from app.smb.blueprint import _process_smb_message

    with patch("app.smb.blueprint._brain") as mock_brain:
        with patch("app.smb.blueprint._twilio") as mock_twilio:
            mock_brain.handle_message.return_value = "   "
            _process_smb_message("+15550003333", "+15550002222", "hey")

    mock_twilio.send_reply.assert_not_called()
    print("✓ _process_smb_message skips send for whitespace-only reply")


def test_process_smb_message_handles_brain_exception_gracefully():
    from app.smb.blueprint import _process_smb_message

    with patch("app.smb.blueprint._brain") as mock_brain:
        with patch("app.smb.blueprint._twilio") as mock_twilio:
            mock_brain.handle_message.side_effect = Exception("AI exploded")
            _process_smb_message("+15550003333", "+15550002222", "hey")

    mock_twilio.send_reply.assert_not_called()
    print("✓ _process_smb_message handles brain exception gracefully, no crash")


# ---------------------------------------------------------------------------
# main.py registration check
# ---------------------------------------------------------------------------

def test_smb_blueprint_registered_in_main():
    """Verify smb_bp is imported and registered in main.py source."""
    main_path = os.path.join(os.path.dirname(__file__), "..", "main.py")
    with open(main_path) as f:
        source = f.read()
    assert "from app.smb.blueprint import smb_bp" in source, \
        "smb_bp not imported in main.py"
    assert "app.register_blueprint(smb_bp)" in source, \
        "smb_bp not registered in main.py"
    print("✓ smb_bp imported and registered in main.py")
