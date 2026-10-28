"""
Tests for bounded Gemini "thinking" on the SMS text path.

Regression coverage for two incidents:
  - Jul 2026: ``gemini-flash-latest`` rolled to ``gemini-3.6-flash`` and the
    un-configured SMS path paid seconds of hidden reasoning per reply (#78).
  - Oct 2026: the alias rolled to ``gemini-3.8-flash``, which rejects
    ``thinking_level="minimal"``. The old fallback (config-less retry on every
    call) silently restored full dynamic thinking — billed but invisible to
    cost tracking — and tripled daily Gemini spend.

Guarantees under test:
  1. build_thinking_config produces a bounded-thinking config, never asks a
     Gemini 3.x model to fully disable thinking (budget 0), and honors the
     "off" escape hatch.
  2. generate_with_thinking downgrades a rejected config to "low" before going
     config-less, caches the outcome so no later call repeats the doomed
     attempt, and keeps the plain single-retry behaviour for transient errors.
  3. The call sites (SMS generation, intent classify, complexity router,
     fan-memory extraction) all send the bounded config and survive its
     rejection.
  4. Thinking/thought tokens are counted in recorded completion-token usage.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import app.brain.generator as gen
import app.brain.intent as intent_mod
import app.brain.memory as memory_mod
import app.brain.routing as routing_mod
import app.brain.thinking as thinking_mod
from app.brain.intent import Intent
from app.brain.thinking import (
    build_thinking_config,
    clear_thinking_caches,
    generate_with_thinking,
)


@pytest.fixture(autouse=True)
def _fresh_thinking_caches():
    """Cached downgrades must never leak between test cases."""
    clear_thinking_caches()
    yield
    clear_thinking_caches()


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
# 2. generate_with_thinking retry/downgrade semantics
# ---------------------------------------------------------------------------

_REJECT_MSG = (
    "400 INVALID_ARGUMENT. Thinking level MINIMAL is not supported for this "
    "model. Please retry with other thinking level."
)


class _FakeModels:
    """Records each call as (config object or None); programmable failures.

    ``reject`` — configs (by identity) the fake rejects with a Gemini-style
    400 INVALID_ARGUMENT. ``reject_all_configs`` rejects any config.
    ``transient_fail_first`` raises a non-config error on the first call only.
    ``fail_always`` rejects everything, config or not.
    """

    def __init__(self, reject=(), reject_all_configs=False,
                 transient_fail_first=False, fail_always=False):
        self.calls = []
        self.reject = list(reject)
        self.reject_all_configs = reject_all_configs
        self.transient_fail_first = transient_fail_first
        self.fail_always = fail_always
        self._n = 0

    def generate_content(self, **kwargs):
        self._n += 1
        config = kwargs.get("config")
        self.calls.append(config)
        if self.fail_always:
            raise Exception(_REJECT_MSG)
        if self.transient_fail_first and self._n == 1:
            raise Exception("boom: connection reset")
        if config is not None and (
            self.reject_all_configs or any(config is r for r in self.reject)
        ):
            raise Exception(_REJECT_MSG)
        return f"ok(config={config is not None})"


class _FakeClient:
    def __init__(self, **kwargs):
        self.models = _FakeModels(**kwargs)


class TestGenerateWithThinking:
    def test_sends_config_when_present(self):
        client = _FakeClient()
        cfg = object()
        out = generate_with_thinking(client, "m", "p", cfg)
        assert client.models.calls == [cfg]
        assert out == "ok(config=True)"

    def test_skips_config_entirely_when_none(self):
        client = _FakeClient()
        out = generate_with_thinking(client, "m", "p", None)
        assert client.models.calls == [None]
        assert out == "ok(config=False)"

    def test_rejected_config_downgrades_to_low_and_caches(self):
        cfg = build_thinking_config("minimal")
        if cfg is None:
            pytest.skip("SDK too old for ThinkingConfig")
        client = _FakeClient(reject=[cfg])
        out = generate_with_thinking(client, "m", "p", cfg)
        # minimal rejected -> low accepted
        assert len(client.models.calls) == 2
        assert client.models.calls[0] is cfg
        low = client.models.calls[1]
        assert low is not None and low is not cfg
        assert str(getattr(low.thinking_config, "thinking_level", "")).lower().endswith("low")
        assert out == "ok(config=True)"

        # Second call goes straight to the cached low config — no doomed attempt.
        out2 = generate_with_thinking(client, "m", "p", cfg)
        assert client.models.calls[2] is low
        assert len(client.models.calls) == 3
        assert out2 == "ok(config=True)"

    def test_all_configs_rejected_falls_back_configless_and_caches(self):
        cfg = build_thinking_config("minimal")
        if cfg is None:
            pytest.skip("SDK too old for ThinkingConfig")
        client = _FakeClient(reject_all_configs=True)
        out = generate_with_thinking(client, "m", "p", cfg)
        # minimal rejected -> low rejected -> config-less
        assert [c is not None for c in client.models.calls] == [True, True, False]
        assert out == "ok(config=False)"

        # Cached: next call is config-less immediately.
        generate_with_thinking(client, "m", "p", cfg)
        assert [c is not None for c in client.models.calls] == [True, True, False, False]

    def test_already_low_config_skips_downgrade(self):
        cfg = build_thinking_config("low")
        if cfg is None:
            pytest.skip("SDK too old for ThinkingConfig")
        client = _FakeClient(reject_all_configs=True)
        out = generate_with_thinking(client, "m", "p", cfg)
        # low rejected -> straight to config-less (no pointless second low try)
        assert [c is not None for c in client.models.calls] == [True, False]
        assert out == "ok(config=False)"

    def test_transient_error_retries_configless_without_caching(self):
        cfg = object()
        client = _FakeClient(transient_fail_first=True)
        out = generate_with_thinking(client, "m", "p", cfg)
        assert [c is not None for c in client.models.calls] == [True, False]
        assert out == "ok(config=False)"

        # Nothing cached — next call sends the config again (and succeeds).
        out2 = generate_with_thinking(client, "m", "p", cfg)
        assert client.models.calls[-1] is cfg
        assert out2 == "ok(config=True)"

    def test_propagates_when_configless_attempt_also_fails(self):
        cfg = build_thinking_config("minimal")
        if cfg is None:
            pytest.skip("SDK too old for ThinkingConfig")
        client = _FakeClient(fail_always=True)
        with pytest.raises(Exception):
            generate_with_thinking(client, "m", "p", cfg)
        # minimal -> low -> config-less, then the failure propagates; no loop.
        assert [c is not None for c in client.models.calls] == [True, True, False]


# ---------------------------------------------------------------------------
# 3. Call sites: SMS generation, intent classify, router, memory extraction
# ---------------------------------------------------------------------------

def _reject_configs_fake(seen, response):
    """generate_content stub rejecting any config with a Gemini-style 400."""

    def fake_generate_content(**kwargs):
        has_config = kwargs.get("config") is not None
        seen.append(has_config)
        if has_config:
            raise Exception(_REJECT_MSG)
        return response

    return fake_generate_content


class TestSmsGeneration:
    def test_generation_sends_bounded_config_and_survives_rejection(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "Namaste! Ready for some chaos?"
            usage_metadata = None

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content",
            _reject_configs_fake(seen, _Resp()),
        )

        out = gen._generate_gemini_raw("PROMPT")
        # original config rejected -> "low" downgrade rejected -> config-less
        assert seen == [True, True, False]
        assert "chaos" in out.lower()

    def test_generation_single_call_when_config_accepted(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "Beta, I'm busy."
            usage_metadata = None

        def fake_generate_content(**kwargs):
            seen.append(kwargs.get("config") is not None)
            return _Resp()

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content", fake_generate_content
        )

        out = gen._generate_gemini_raw("PROMPT")
        assert seen == [True]  # no wasteful second call on success
        assert out == "Beta, I'm busy."

    def test_generation_skips_doomed_config_once_rejection_is_cached(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "Chaos, always."
            usage_metadata = None

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content",
            _reject_configs_fake(seen, _Resp()),
        )

        gen._generate_gemini_raw("PROMPT")   # learns: everything rejected
        gen._generate_gemini_raw("PROMPT")   # must go straight config-less
        assert seen == [True, True, False, False]


class TestIntentClassification:
    def test_intent_sends_config_and_survives_rejection(self, monkeypatch):
        # Force the LLM path — bypass the regex fast-classifier.
        monkeypatch.setattr(intent_mod, "_fast_classify", lambda m: None)
        monkeypatch.setattr(intent_mod, "_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = "question"

        monkeypatch.setattr(
            intent_mod._client.models, "generate_content",
            _reject_configs_fake(seen, _Resp()),
        )

        result = intent_mod.classify_intent("how long have you done comedy for?")
        assert seen == [True, True, False]
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

        monkeypatch.setattr(
            routing_mod._client.models, "generate_content",
            _reject_configs_fake(seen, _Resp()),
        )

        tier = routing_mod.classify_routing_tier(self._MESSAGE, history=[])
        assert seen == [True, True, False]
        assert tier == "medium"

    def test_router_still_falls_back_to_medium_on_total_failure(self, monkeypatch):
        monkeypatch.setattr(routing_mod, "_THINKING_CONFIG", object())

        def always_fail(**kwargs):
            raise Exception("boom")

        monkeypatch.setattr(
            routing_mod._client.models, "generate_content", always_fail
        )

        assert routing_mod.classify_routing_tier(self._MESSAGE, history=[]) == "medium"


class TestMemoryExtraction:
    """memory.py was the call site missed in #78 — it must now be bounded."""

    def test_memory_module_builds_a_config(self):
        # If the SDK supports ThinkingConfig at all, memory must use it.
        if build_thinking_config("low") is not None:
            assert memory_mod._THINKING_CONFIG is not None

    def test_memory_sends_config_and_survives_rejection(self, monkeypatch):
        monkeypatch.setattr(memory_mod, "_THINKING_CONFIG", object())
        seen = []

        class _Resp:
            text = (
                '{"memory": "Fan is from Austin.", "name": "Sarah", '
                '"location": "Austin", "tags": [], "minor": false}'
            )

        monkeypatch.setattr(
            memory_mod._client.models, "generate_content",
            _reject_configs_fake(seen, _Resp()),
        )

        memory, tags, location, minor, name = memory_mod.extract_memory(
            "", "hi I'm Sarah from Austin, loved the show!"
        )
        assert seen == [True, True, False]
        assert minor is False
        assert location == "Austin"


# ---------------------------------------------------------------------------
# 4. Thought tokens are counted in recorded usage (cost visibility)
# ---------------------------------------------------------------------------

class TestUsageCountsThoughtTokens:
    def test_completion_tokens_include_thoughts(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", None)

        class _Usage:
            prompt_token_count = 5500
            candidates_token_count = 54
            thoughts_token_count = 1200

        class _Resp:
            text = "Namaste!"
            usage_metadata = _Usage()

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content", lambda **kw: _Resp()
        )

        gen._generate_gemini_raw("PROMPT")
        provider, prompt_tokens, completion_tokens = gen.get_last_usage()
        assert provider == "gemini"
        assert prompt_tokens == 5500
        assert completion_tokens == 54 + 1200  # thoughts are billed output

    def test_missing_thoughts_field_counts_as_zero(self, monkeypatch):
        monkeypatch.setattr(gen, "_SMS_THINKING_CONFIG", None)

        class _Usage:
            prompt_token_count = 100
            candidates_token_count = 20
            # no thoughts_token_count attribute at all (pre-3.x models)

        class _Resp:
            text = "Beta."
            usage_metadata = _Usage()

        monkeypatch.setattr(
            gen._CLIENT.models, "generate_content", lambda **kw: _Resp()
        )

        gen._generate_gemini_raw("PROMPT")
        _, _, completion_tokens = gen.get_last_usage()
        assert completion_tokens == 20


# ---------------------------------------------------------------------------
# 5. Voice path still gets a bounded config after the shared-helper refactor
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
