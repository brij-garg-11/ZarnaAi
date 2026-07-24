"""Tests for the CALL gate (app/brain/call_gate.py + handler integration).

The matcher must catch the bare "CALL" keyword and unambiguous call-asks,
must NOT fire on naming questions ("what should I call you?"), and the
handler must reply channel-aware: SMS fans get a "yes, call this number",
WhatsApp fans get the not-supported-yet reply. Creators without the voice
feature keep pure-LLM behaviour.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import pytest

from app.brain.call_gate import (
    SMS_CALL_REPLY,
    WHATSAPP_CALL_REPLY,
    is_call_request,
    try_call_gate,
)


class TestCallMatcher:
    @pytest.mark.parametrize("msg", [
        # The bare keyword — the most common form fans send
        "CALL",
        "call",
        "Call!",
        "call?",
        "CALL ME",
        "call now",
        "call you?",
        "phone call",
        # Unambiguous asks
        "can I call you?",
        "Can i call u",
        "could we call",
        "may I please call you",
        "how do I call you?",
        "how can I call",
        "I want to call you",
        "wanna call you so bad",
        "would love to call you sometime",
        "can I give you a call?",
        "can we talk on the phone?",
        "does the call feature work?",
        "is calling available?",
        "is the call thing working",
        "I heard about the call feature",
    ])
    def test_call_language_matches(self, msg):
        assert is_call_request(msg), f"expected call match for: {msg!r}"

    @pytest.mark.parametrize("msg", [
        # Naming questions — about names, not phone calls
        "what should I call you?",
        "what do I call you",
        "can I call you aunty?",
        "should I call you Zarna or Mrs. Garg?",
        # "call" in ordinary conversation
        "they call me the funny one at work",
        "my mom calls every day and complains",
        "I got a call from school about my kid",
        "last call for the show tickets?",
        "don't call me crazy but I love your MIL bits",
        # Benign / unrelated
        "when are you coming to Portland?",
        "tell me a joke",
        "",
    ])
    def test_benign_language_does_not_match(self, msg):
        assert not is_call_request(msg), f"false positive on: {msg!r}"

    def test_never_raises(self):
        assert is_call_request(None) is False  # type: ignore[arg-type]


class _VoiceOn:
    class voice:  # noqa: D106 — minimal stand-in for CreatorConfig.voice
        enabled = True


class _VoiceOff:
    class voice:
        enabled = False


class TestTryCallGate:
    def test_sms_fan_gets_yes_reply(self):
        assert try_call_gate("+15550001111", "CALL", _VoiceOn()) == SMS_CALL_REPLY

    def test_whatsapp_fan_gets_not_supported_reply(self):
        reply = try_call_gate("whatsapp:+15550001111", "can I call you?", _VoiceOn())
        assert reply == WHATSAPP_CALL_REPLY

    def test_voice_disabled_creator_never_fires(self):
        assert try_call_gate("+15550001111", "CALL", _VoiceOff()) is None

    def test_no_config_never_fires(self):
        assert try_call_gate("+15550001111", "CALL", None) is None

    def test_non_call_message_falls_through(self):
        assert try_call_gate("+15550001111", "tell me a joke", _VoiceOn()) is None


class TestHandlerGate:
    def _brain(self):
        from app.brain.handler import ZarnaBrain
        from app.retrieval.base import BaseRetriever
        from app.storage.memory import InMemoryStorage

        class NullRetriever(BaseRetriever):
            def get_relevant_chunks(self, message, top_k=None):
                return []

        # Zarna's live config has voice.enabled=True, which is exactly the
        # production path this gate exists for.
        return ZarnaBrain(storage=InMemoryStorage(), retriever=NullRetriever(), slug="zarna")

    def test_call_keyword_gets_fixed_response_without_llm(self, monkeypatch):
        brain = self._brain()

        def no_llm(*a, **k):
            raise AssertionError("LLM must not be called for CALL keyword")
        monkeypatch.setattr("app.brain.handler.generate_zarna_reply", no_llm)
        monkeypatch.setattr("app.brain.handler.classify_intent", no_llm)

        reply = brain.handle_incoming_message("+15550001111", "CALL")
        assert reply == SMS_CALL_REPLY

    def test_whatsapp_call_gets_not_supported_reply(self, monkeypatch):
        brain = self._brain()

        def no_llm(*a, **k):
            raise AssertionError("LLM must not be called for CALL keyword")
        monkeypatch.setattr("app.brain.handler.generate_zarna_reply", no_llm)
        monkeypatch.setattr("app.brain.handler.classify_intent", no_llm)

        reply = brain.handle_incoming_message("whatsapp:+15550001111", "can I call you?")
        assert reply == WHATSAPP_CALL_REPLY

    def test_call_reply_saved_to_history(self):
        brain = self._brain()
        brain.handle_incoming_message("+15550002222", "CALL")
        history = brain.storage.get_conversation_history("+15550002222", limit=10)
        roles = [(m.role, m.text) for m in history]
        assert ("assistant", SMS_CALL_REPLY) in roles

    def test_crisis_gate_still_wins_over_call_gate(self, monkeypatch):
        # A message that somehow matched both must get the crisis script.
        from app.brain.crisis import CRISIS_RESPONSE
        brain = self._brain()
        monkeypatch.setattr("app.brain.crisis._get_conn", lambda: None)
        reply = brain.handle_incoming_message(
            "+15550003333", "I want to kill myself, can I call you?"
        )
        assert reply == CRISIS_RESPONSE
