"""Tests for the crisis gate (app/brain/crisis.py + handler integration).

The matcher must catch real crisis language and must NOT fire on comedy-club
reactions ("dying laughing", "you kill me") — a false positive turns a joke
into a jarring crisis script.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import pytest

from app.brain.crisis import CRISIS_RESPONSE, check_crisis


class TestCrisisMatcher:
    @pytest.mark.parametrize("msg,label", [
        ("I want to kill myself", "kill_myself"),
        ("i've been thinking about killing myself", "kill_myself"),
        ("I'm feeling suicidal", "suicide"),
        ("having suicidal thoughts again", "suicide"),
        ("thinking about suicide", "suicide"),
        ("I want to end my life", "end_my_life"),
        ("sometimes I think about ending my life", "end_my_life"),
        ("I'm going to take my own life", "take_my_life"),
        ("I just want to die", "want_to_die"),
        ("i wanna die", "want_to_die"),
        ("I wish I was dead", "want_to_die"),
        ("I don't want to live anymore", "not_want_live"),
        ("i dont want to be alive", "not_want_live"),
        ("I've been self harming", "self_harm"),
        ("I keep cutting myself", "self_harm"),
        ("I want to harm myself", "self_harm"),
        ("I'm ready to end it all", "end_it_all"),
        ("i've been thinking about ending it all", "end_it_all"),
        ("everyone would be better off without me", "better_off_dead"),
        ("I'm better off dead", "better_off_dead"),
        ("there's no reason to live", "no_reason_live"),
        ("I have nothing to live for", "no_reason_live"),
    ])
    def test_crisis_language_matches(self, msg, label):
        m = check_crisis(msg)
        assert m is not None, f"expected crisis match for: {msg!r}"
        assert m.label == label

    @pytest.mark.parametrize("msg", [
        # Comedy reactions — the fan base's normal language
        "omg I'm dying laughing",
        "I died laughing at that MIL bit",
        "you kill me Zarna 😂",
        "that joke killed it",
        "you're killing it girl",
        "I'm dead 💀",
        "lol I want to die 😂 that was so funny",
        "this show is to die for",
        # Ordinary sadness — LLM handles with sensitive_care, not the 988 script
        "I'm feeling really sad today",
        "I've been so anxious lately",
        "my mother passed away last month and I miss her",
        "I'm going through a divorce and it hurts",
        "work is killing me lately",
        # Random benign
        "when are you coming to Portland?",
        "tell me a joke",
        "",
    ])
    def test_benign_language_does_not_match(self, msg):
        assert check_crisis(msg) is None, f"false positive on: {msg!r}"

    def test_explicit_phrase_fires_even_with_laugh_emoji(self):
        # "haha I'm gonna kill myself" is NOT exempt — explicit phrases always fire.
        assert check_crisis("lol I'm going to kill myself 😂") is not None

    def test_never_raises(self):
        assert check_crisis(None) is None  # type: ignore[arg-type]


class TestHandlerGate:
    def _brain(self):
        from app.brain.handler import ZarnaBrain
        from app.retrieval.base import BaseRetriever
        from app.storage.memory import InMemoryStorage

        class NullRetriever(BaseRetriever):
            def get_relevant_chunks(self, message, top_k=None):
                return []

        return ZarnaBrain(storage=InMemoryStorage(), retriever=NullRetriever(), slug="zarna")

    def test_crisis_message_gets_fixed_response_without_llm(self, monkeypatch):
        brain = self._brain()

        # Any LLM call would prove the gate failed.
        def no_llm(*a, **k):
            raise AssertionError("LLM must not be called for crisis messages")
        monkeypatch.setattr("app.brain.handler.generate_zarna_reply", no_llm)
        monkeypatch.setattr("app.brain.handler.classify_intent", no_llm)

        # Don't write to any DB from the async flag recorder during tests.
        monkeypatch.setattr("app.brain.crisis._get_conn", lambda: None)

        reply = brain.handle_incoming_message("+15550001111", "I want to kill myself")
        assert reply == CRISIS_RESPONSE
        assert "988" in reply

    def test_crisis_reply_saved_to_history(self, monkeypatch):
        brain = self._brain()
        monkeypatch.setattr("app.brain.crisis._get_conn", lambda: None)
        brain.handle_incoming_message("+15550002222", "I don't want to live anymore")
        history = brain.storage.get_conversation_history("+15550002222", limit=10)
        roles = [(m.role, m.text) for m in history]
        assert ("assistant", CRISIS_RESPONSE) in roles

    def test_crisis_records_safety_flag(self, monkeypatch):
        brain = self._brain()
        recorded = {}

        def fake_record(phone, text, pattern, slug, response):
            recorded.update(phone=phone, text=text, pattern=pattern, slug=slug)

        monkeypatch.setattr("app.brain.crisis.record_safety_flag", fake_record)
        brain.handle_incoming_message("+15550003333", "I'm feeling suicidal")
        # record_safety_flag runs on the shared executor — wait for it.
        import app.brain.handler as handler_mod
        handler_mod._executor.submit(lambda: None).result(timeout=5)
        assert recorded.get("pattern") == "suicide"
        assert recorded.get("phone") == "+15550003333"

    def test_non_crisis_message_untouched(self, monkeypatch):
        brain = self._brain()
        monkeypatch.setattr("app.brain.handler.classify_intent",
                            lambda *a, **k: __import__("app.brain.intent", fromlist=["Intent"]).Intent.GENERAL)
        monkeypatch.setattr("app.brain.handler.generate_zarna_reply",
                            lambda **k: "normal reply")
        monkeypatch.setattr("app.brain.handler.classify_routing_tier", lambda *a, **k: "low")
        reply = brain.handle_incoming_message("+15550004444", "I love your MIL jokes")
        assert reply == "normal reply"
