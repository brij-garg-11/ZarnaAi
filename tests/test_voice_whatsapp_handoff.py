"""
Tests for WhatsApp Business Calling handoff to ElevenLabs Agents.

Regression coverage for the failed WhatsApp call (Twilio error 64112: voice_id
not found). PSTN calls to the business number go straight to ElevenLabs Agents
via the number's voice_url, but WhatsApp calls must route through the TwiML app
to this service — and ConversationRelay can't use the private cloned voice in
the creator's ElevenLabs account. The fix re-POSTs the webhook to ElevenLabs
with bare E.164 numbers and returns its TwiML, so WhatsApp callers get the
exact same agent and voice as phone callers.
"""
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

from app.voice import server as voice_server


ELEVEN_TWIML = (
    '<?xml version="1.0" encoding="UTF-8"?><Response><Connect>'
    '<Stream track="inbound_track" url="wss://api.elevenlabs.io/v1/convai/conversation">'
    '<Parameter name="conversation_id" value="conv_test123" /></Stream>'
    '</Connect></Response>'
)


class TestStripWa:
    def test_strips_prefix(self):
        assert voice_server._strip_wa("whatsapp:+18556081717") == "+18556081717"

    def test_case_insensitive(self):
        assert voice_server._strip_wa("WhatsApp:+18556081717") == "+18556081717"

    def test_bare_number_unchanged(self):
        assert voice_server._strip_wa("+18556081717") == "+18556081717"

    def test_empty(self):
        assert voice_server._strip_wa("") == ""
        assert voice_server._strip_wa(None) == ""


class TestForwardWhatsappToElevenlabs:
    def _form(self):
        return {
            "To": "whatsapp:+18556081717",
            "From": "whatsapp:+16465551234",
            "CallSid": "CAxxxx",
            "AccountSid": "ACxxxx",
        }

    def test_posts_bare_numbers_and_returns_twiml(self):
        mock_resp = MagicMock(status_code=200, text=ELEVEN_TWIML)
        with patch("requests.post", return_value=mock_resp) as mock_post:
            out = voice_server._forward_whatsapp_to_elevenlabs(self._form())
        assert out == ELEVEN_TWIML
        payload = mock_post.call_args.kwargs["data"]
        assert payload["To"] == "+18556081717"
        assert payload["From"] == "+16465551234"
        assert payload["CallSid"] == "CAxxxx"

    def test_original_form_not_mutated(self):
        form = self._form()
        mock_resp = MagicMock(status_code=200, text=ELEVEN_TWIML)
        with patch("requests.post", return_value=mock_resp):
            voice_server._forward_whatsapp_to_elevenlabs(form)
        assert form["To"] == "whatsapp:+18556081717"

    def test_non_200_returns_none(self):
        mock_resp = MagicMock(status_code=404, text="Error in setting up Twilio inbound call.")
        with patch("requests.post", return_value=mock_resp):
            assert voice_server._forward_whatsapp_to_elevenlabs(self._form()) is None

    def test_200_without_twiml_returns_none(self):
        mock_resp = MagicMock(status_code=200, text='{"ok": true}')
        with patch("requests.post", return_value=mock_resp):
            assert voice_server._forward_whatsapp_to_elevenlabs(self._form()) is None

    def test_network_error_returns_none(self):
        with patch("requests.post", side_effect=ConnectionError("boom")):
            assert voice_server._forward_whatsapp_to_elevenlabs(self._form()) is None


class TestTwilioVoiceRoute:
    """End-to-end through the FastAPI route with signature validation disabled."""

    def _client(self):
        from fastapi.testclient import TestClient
        return TestClient(voice_server.app)

    def _post(self, client, to):
        return client.post(
            "/twilio/voice",
            data={"To": to, "From": "whatsapp:+16465551234", "CallSid": "CAxxxx"},
        )

    def test_whatsapp_call_returns_elevenlabs_twiml(self, monkeypatch):
        monkeypatch.setenv("TWILIO_VALIDATE_SIGNATURE", "false")
        mock_resp = MagicMock(status_code=200, text=ELEVEN_TWIML)
        with patch("requests.post", return_value=mock_resp):
            r = self._post(self._client(), "whatsapp:+18556081717")
        assert r.status_code == 200
        assert "api.elevenlabs.io/v1/convai/conversation" in r.text
        assert "ConversationRelay" not in r.text

    def test_whatsapp_call_falls_back_to_apology_on_handoff_failure(self, monkeypatch):
        monkeypatch.setenv("TWILIO_VALIDATE_SIGNATURE", "false")
        mock_resp = MagicMock(status_code=404, text="nope")
        with patch("requests.post", return_value=mock_resp):
            r = self._post(self._client(), "whatsapp:+18556081717")
        assert r.status_code == 200
        assert "<Say>" in r.text and "<Hangup/>" in r.text

    def test_pstn_call_keeps_conversation_relay_path(self, monkeypatch):
        monkeypatch.setenv("TWILIO_VALIDATE_SIGNATURE", "false")

        class _Voice:
            enabled = True
            voice_id = "EwBdsqLgp91P4QdrChX8"
            greeting = "hi"
            language = "en-US"
            provider = "elevenlabs"

        with patch.object(voice_server, "_voice_settings", return_value=_Voice()):
            r = self._post(self._client(), "+18556081717")
        assert r.status_code == 200
        assert "ConversationRelay" in r.text
