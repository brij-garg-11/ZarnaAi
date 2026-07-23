"""
Tests for bounded Gemini "thinking" on the SMS text path.

Regression coverage for the latency incident where ``gemini-flash-latest``
rolled forward to ``gemini-3.6-flash`` — a reasoning model that defaults to
high/dynamic thinking. The SMS path sent no thinking config, so every reply
paid seconds of hidden reasoning (prod gen_ms jumped from ~1-2s to 4-5s).

Guarantees under test:
  1. build_thinking_config produces a bounded-thinking config, never asks a
     Gemini 3.x model to fully disable thinking (budget 0), and honors the
     "off" escape hatch.
  2. generate_with_thinking retries exactly once without the config if the
     model rejects it, so an alias roll-forward can never kill replies.
  3. The three call sites (SMS generation, intent classify, complexity router)
     all send the bounded config and survive its rejection.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import app.brain.generator as gen
import app.brain.intent as intent_mod
import app.brain.routing as routing_mod
from app.brain.intent import Intent
from app.brain.thinking import build_thinking_config, generate_with_thinking


# ---------------------------------------------------------------------------
# 1. build_thinking_config
# ---------------------------------------------------------------------------

class TestBuildThinkingConfig:
    @pytest.mark.parametrize("level", ["minimal", "low", "medium", "high"])
    def test_valid_levels_produce_bounded_config(self, level):
        config = build_thinking_config(level)
        if config is None:  # SDK too old to expose ThinkingConfig
            return
        tc = config.thinking_config
        got_level = getattr(tc, "thinking_level", None)
        got_budget = getattr(tc, "thinking_budget", None)
        # Either the modern level knob, or a positive legacy budget — never 0.
        assert got_budget != 0
        assert got_level is not None or (got_budget is not None and got_budget > 0)
        if got_level is not None:
            assert str(got_level).lower().endswith(level)

    @pytest.mark.parametrize("level", ["off", "none", "default", "", None])
    def test_off_values_return_none(self, level):
        assert build_thinking_config(level) is None

    def test_unknown_level_falls_back_to_low_not_none(self):
        config = build_thinking_config("turbo")
        known = build_thinking_config("low")
        # Whatever the SDK supports, unknown input must behave exactly like "low".
        if known is None:
            assert config is None
        else:
            assert config is not None
            assert getattr(config.thinking_config, "thinking_level", None) == getattr(
                known.thinking_config, "thinking_level", None
            )

    def test_never_produces_budget_zero_even_with_zero_fallback(self):
        config = build_thinking_config("low", fallback_budget=0)
        if config is None:
            return
        assert getattr(config.thinking_config, "thinking_budget", None) != 0


# ---------------------------------------------------------------------------
# 2. generate_with_thinking retry semantics
# ---------------------------------------------------------------------------

class _FakeModels:
    def __init__(self, fail_with_config=False, fail_always=False):
        self.calls = []  # list of bools: config sent?
        self.fail_with_config = fail_with_config
        self.fail_always = fail_always

    def generate_content(self, **kwargs):
        has_config = "config" in kwargs
        self.calls.append(has_config)
        if self.fail_always or (self.fail_with_config and has_config):
            raise Exception("400 INVALID_ARGUMENT")
        return f"ok(config={has_config})"


class _FakeClient:
    def __init__(self, **kwargs):
        self.models = _FakeModels(**kwargs)


class TestGenerateWithThinking:
    def test_sends_config_when_present(self):
        client = _FakeClient()
        out = generate_with_thinking(client, "m", "p", object())
        assert client.models.calls == [True]
        assert out == "ok(config=True)"

    def test_skips_config_entirely_when_none(self):
        client = _FakeClient()
        out = generate_with_thinking(client, "m", "p", None)
        assert client.models.calls == [False]
        assert out == "ok(config=False)"

    def test_retries_once_without_config_on_rejection(self):
        client = _FakeClient(fail_with_config=True)
        out = generate_with_thinking(client, "m", "p", object())
        assert client.models.calls == [True, False]
        assert out == "ok(config=False)"

    def test_propagates_when_configless_attempt_also_fails(self):
        client = _FakeClient(fail_always=True)
        with pytest.raises(Exception):
            generate_with_thinking(client, "m", "p", object())
        assert client.models.calls == [True, False]  # exactly one retry, no loop


# ---------------------------------------------------------------------------
# 3. Call sites: SMS generation, intent classify, router
# ---------------------------------------------------------------------------

class TestSmsGeneration:
    def test_generation_sends_bounded_config_and_survives_rejection(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "Namaste! Ready for some chaos?"
            usage_metadata = None

        def fake_generate_content(**kwargs):
            has_config = "config" in kwargs
            seen.append(has_config)
            if has_config:
                raise Exception("400 INVALID_ARGUMENT")
            return _Resp()

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content", fake_generate_content
        )

        out = gen._generate_gemini_raw("PROMPT")
        assert seen == [True, False]
        assert "chaos" in out.lower()

    def test_generation_single_call_when_config_accepted(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "Beta, I'm busy."
            usage_metadata = None

        def fake_generate_content(**kwargs):
            seen.append("config" in kwargs)
            return _Resp()

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content", fake_generate_content
        )

        out = gen._generate_gemini_raw("PROMPT")
        assert seen == [True]  # no wasteful second call on success
        assert out == "Beta, I'm busy."


class TestIntentClassification:
    def test_intent_sends_config_and_survives_rejection(self, monkeypatch):
        # Force the LLM path — bypass the regex fast-classifier.
        monkeypatch.setattr(intent_mod, "_fast_classify", lambda m: None)
        monkeypatch.setattr(intent_mod, "_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "question"

        def fake_generate_content(**kwargs):
            has_config = "config" in kwargs
            seen.append(has_config)
            if has_config:
                raise Exception("400 INVALID_ARGUMENT")
            return _Resp()

        monkeypatch.setattr(
            intent_mod._client.models, "generate_content", fake_generate_content
        )

        result = intent_mod.classify_intent("how long have you done comedy for?")
        assert seen == [True, False]
        assert result == Intent.QUESTION

    def test_intent_still_falls_back_to_general_on_total_failure(self, monkeypatch):
        monkeypatch.setattr(intent_mod, "_fast_classify", lambda m: None)
        monkeypatch.setattr(intent_mod, "_THINKING_CONFIG", object())

        def always_fail(**kwargs):
            raise Exception("boom")

        monkeypatch.setattr(
            intent_mod._client.models, "generate_content", always_fail
        )

        assert intent_mod.classify_intent("anything") == Intent.GENERAL


class TestRouterClassification:
    # Long enough + question mark so neither the skip fast-path nor the
    # heuristic floor short-circuits before the router model call.
    _MESSAGE = "Can you give me some detailed advice about handling my in-laws?"

    def test_router_sends_config_and_survives_rejection(self, monkeypatch):
        monkeypatch.setattr(routing_mod, "_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = '{"tier":"medium","confidence":0.9,"reason":"advice"}'

        def fake_generate_content(**kwargs):
            has_config = "config" in kwargs
            seen.append(has_config)
            if has_config:
                raise Exception("400 INVALID_ARGUMENT")
            return _Resp()

        monkeypatch.setattr(
            routing_mod._client.models, "generate_content", fake_generate_content
        )

        tier = routing_mod.classify_routing_tier(self._MESSAGE, history=[])
        assert seen == [True, False]
        assert tier == "medium"

    def test_router_still_falls_back_to_medium_on_total_failure(self, monkeypatch):
        monkeypatch.setattr(routing_mod, "_THINKING_CONFIG", object())

        def always_fail(**kwargs):
            raise Exception("boom")

        monkeypatch.setattr(
            routing_mod._client.models, "generate_content", always_fail
        )

        assert routing_mod.classify_routing_tier(self._MESSAGE, history=[]) == "medium"


# ---------------------------------------------------------------------------
# 4. Voice path still gets a bounded config after the shared-helper refactor
# ---------------------------------------------------------------------------

class TestVoiceDelegation:
    def test_voice_stream_config_matches_shared_low_config(self):
        voice = gen._voice_stream_config()
        shared = build_thinking_config("low", fallback_budget=128)
        if shared is None:
            assert voice is None
            return
        assert voice is not None
        assert getattr(voice.thinking_config, "thinking_level", None) == getattr(
            shared.thinking_config, "thinking_level", None
        )
        assert getattr(voice.thinking_config, "thinking_budget", None) != 0
