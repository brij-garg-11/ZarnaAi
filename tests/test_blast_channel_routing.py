"""
Unit tests for per-fan channel routing in operator.app.blast_sender.

Fans who signed up via WhatsApp must receive blasts on WhatsApp (from the
registered TWILIO_WHATSAPP_NUMBER sender), while SMS fans keep the existing
Twilio SMS / A2P messaging-service path. Also covers deduping fans who appear
in an audience under both bare and whatsapp:-prefixed phone keys.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# operator/app/ shares its package name ("app") with the main Flask app at
# the repo root, so we can't just import it — load the module directly from
# its file path instead (same pattern as test_business_blast.py).
ROOT = Path(__file__).resolve().parent.parent
_BS_PATH = ROOT / "operator" / "app" / "blast_sender.py"
_spec = importlib.util.spec_from_file_location("operator_blast_sender", _BS_PATH)
bs = importlib.util.module_from_spec(_spec)
sys.modules["operator_blast_sender"] = bs
_spec.loader.exec_module(bs)


# ---------------------------------------------------------------------------
# _normalize_bare / _wa_from
# ---------------------------------------------------------------------------

def test_normalize_bare_strips_prefix():
    assert bs._normalize_bare("whatsapp:+447700900123") == "+447700900123"
    assert bs._normalize_bare("WhatsApp:+15551234567") == "+15551234567"
    assert bs._normalize_bare("+15551234567") == "+15551234567"
    assert bs._normalize_bare("  whatsapp:+44 ") == "+44"
    assert bs._normalize_bare("") == ""
    assert bs._normalize_bare(None) == ""


def test_wa_from_unset(monkeypatch):
    monkeypatch.delenv("TWILIO_WHATSAPP_NUMBER", raising=False)
    assert bs._wa_from() == ""


def test_wa_from_adds_prefix(monkeypatch):
    monkeypatch.setenv("TWILIO_WHATSAPP_NUMBER", "+18556081717")
    assert bs._wa_from() == "whatsapp:+18556081717"


def test_wa_from_keeps_existing_prefix(monkeypatch):
    monkeypatch.setenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+18556081717")
    assert bs._wa_from() == "whatsapp:+18556081717"


# ---------------------------------------------------------------------------
# _partition_audience
# ---------------------------------------------------------------------------

def test_partition_routes_show_whatsapp_signups():
    phones = ["+447700900001", "+15550000001", "+447700900002"]
    wa_bare = {"+447700900001", "+447700900002"}
    assert bs._partition_audience(phones, wa_bare) == [
        ("+447700900001", True),
        ("+15550000001", False),
        ("+447700900002", True),
    ]


def test_partition_routes_prefixed_contacts_without_lookup():
    phones = ["whatsapp:+447700900001", "+15550000001"]
    assert bs._partition_audience(phones, set()) == [
        ("+447700900001", True),
        ("+15550000001", False),
    ]


def test_partition_dedupes_bare_and_prefixed_rows():
    # Same fan appears under both keys (signup row + brain contact row):
    # one send, via WhatsApp, regardless of which form comes first.
    for phones in (
        ["+447700900001", "whatsapp:+447700900001"],
        ["whatsapp:+447700900001", "+447700900001"],
    ):
        out = bs._partition_audience(phones, set())
        assert out == [("+447700900001", True)]


def test_partition_skips_empty_entries_and_preserves_order():
    phones = ["", None, "+15550000001", "+15550000002"]
    assert bs._partition_audience(phones, set()) == [
        ("+15550000001", False),
        ("+15550000002", False),
    ]


# ---------------------------------------------------------------------------
# _wa_signup_bare_set — non-DB paths
# ---------------------------------------------------------------------------

def test_wa_signup_set_empty_for_non_show_audience():
    assert bs._wa_signup_bare_set({"audience_type": "tag", "audience_filter": "vip"}, "zarna") == set()


def test_wa_signup_set_empty_for_bad_show_filter():
    assert bs._wa_signup_bare_set({"audience_type": "show", "audience_filter": "abc"}, "zarna") == set()
    assert bs._wa_signup_bare_set({"audience_type": "show", "audience_filter": ""}, "zarna") == set()


# ---------------------------------------------------------------------------
# _send_twilio — WhatsApp kwargs
# ---------------------------------------------------------------------------

def _twilio_env(monkeypatch, wa_number="whatsapp:+18556081717"):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtest")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "token")
    monkeypatch.setenv("TWILIO_PHONE_NUMBER", "+18556081717")
    monkeypatch.setenv("TWILIO_MESSAGING_SERVICE_SID", "MGtest")
    if wa_number is None:
        monkeypatch.delenv("TWILIO_WHATSAPP_NUMBER", raising=False)
    else:
        monkeypatch.setenv("TWILIO_WHATSAPP_NUMBER", wa_number)


def test_send_twilio_whatsapp_uses_explicit_sender(monkeypatch):
    _twilio_env(monkeypatch)
    fake_client = MagicMock()
    fake_client.messages.create.return_value = MagicMock(sid="SMxxx")
    with patch("twilio.rest.Client", return_value=fake_client):
        ok = bs._send_twilio("+447700900123", "hi", whatsapp=True)
    assert ok is True
    kwargs = fake_client.messages.create.call_args.kwargs
    assert kwargs["to"] == "whatsapp:+447700900123"
    assert kwargs["from_"] == "whatsapp:+18556081717"
    # WhatsApp must never route through the SMS A2P messaging service.
    assert "messaging_service_sid" not in kwargs


def test_send_twilio_whatsapp_fails_without_sender(monkeypatch):
    _twilio_env(monkeypatch, wa_number=None)
    fake_client = MagicMock()
    with patch("twilio.rest.Client", return_value=fake_client):
        ok = bs._send_twilio("+447700900123", "hi", whatsapp=True)
    assert ok is False
    fake_client.messages.create.assert_not_called()


def test_send_twilio_sms_path_unchanged(monkeypatch):
    _twilio_env(monkeypatch)
    fake_client = MagicMock()
    fake_client.messages.create.return_value = MagicMock(sid="SMxxx")
    with patch("twilio.rest.Client", return_value=fake_client):
        ok = bs._send_twilio("+15550000001", "hi")
    assert ok is True
    kwargs = fake_client.messages.create.call_args.kwargs
    assert kwargs["to"] == "+15550000001"
    assert kwargs["messaging_service_sid"] == "MGtest"
    assert "from_" not in kwargs
