"""
Tests for the voice streaming generation config and its resilience.

Regression coverage for the outage where ``gemini-flash-latest`` rolled forward to
a Gemini 3.x model that rejects ``thinking_budget=0`` (400 INVALID_ARGUMENT). Every
voice turn then failed generation and the caller heard the fallback line
("Sorry, you cut out there for a second — say that again?") on a loop.

Two guarantees:
  1. _voice_stream_config never asks Gemini to fully disable thinking (budget 0).
  2. generate_zarna_reply_stream retries without the config if the model rejects it,
     so a live call is never dropped over a config incompatibility.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import app.brain.generator as gen
from app.brain.intent import Intent


class _Chunk:
    def __init__(self, text: str) -> None:
        self.text = text


# ---------------------------------------------------------------------------
# 1. Config never disables thinking outright
# ---------------------------------------------------------------------------

class TestVoiceStreamConfig:
    def test_never_sets_thinking_budget_zero(self):
        config = gen._voice_stream_config()
        if config is None:  # SDK too old to expose ThinkingConfig — nothing to assert
            return
        tc = config.thinking_config
        # Either a low thinking_level, or a positive budget — never 0.
        assert getattr(tc, "thinking_budget", None) != 0
        level = getattr(tc, "thinking_level", None)
        budget = getattr(tc, "thinking_budget", None)
        assert level is not None or (budget is not None and budget > 0)


# ---------------------------------------------------------------------------
# 2. Streaming falls back to a config-less request on rejection
# ---------------------------------------------------------------------------

class TestStreamRetriesWithoutConfig:
    def test_retries_without_config_when_model_rejects_it(self, monkeypatch):
        monkeypatch.setattr(gen, "_build_prompt", lambda *a, **k: "PROMPT")
        # Force the "config present" path regardless of installed SDK.
        monkeypatch.setattr(gen, "_voice_stream_config", lambda: object())

        seen_config_flags = []

        def fake_stream(**kwargs):
            has_config = "config" in kwargs
            seen_config_flags.append(has_config)
            if has_config:
                raise Exception("400 INVALID_ARGUMENT")
            return iter([_Chunk("Hey there. "), _Chunk("How are you doing?")])

        monkeypatch.setattr(gen._CLIENT.models, "generate_content_stream", fake_stream)

        out = list(
            gen.generate_zarna_reply_stream(Intent.GENERAL, "hi", [], history=[])
        )

        # First attempt sent the config (and failed); second dropped it (and worked).
        assert seen_config_flags == [True, False]
        joined = " ".join(out).strip()
        assert joined  # real text produced, not the empty-fallback path
        assert "how are you" in joined.lower()

    def test_no_duplicate_output_when_error_after_first_sentence(self, monkeypatch):
        monkeypatch.setattr(gen, "_build_prompt", lambda *a, **k: "PROMPT")
        monkeypatch.setattr(gen, "_voice_stream_config", lambda: object())

        def fake_stream(**kwargs):
            def _gen():
                yield _Chunk("First sentence here. ")
                raise Exception("mid-stream boom")

            return _gen()

        monkeypatch.setattr(gen._CLIENT.models, "generate_content_stream", fake_stream)

        out = list(
            gen.generate_zarna_reply_stream(Intent.GENERAL, "hi", [], history=[])
        )

        # We already emitted the first sentence, so a mid-stream failure must not
        # restart and re-emit it.
        assert out.count("First sentence here.") <= 1
        assert len([s for s in out if "First sentence" in s]) == 1
