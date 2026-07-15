"""
Tests for known-caller recognition on the voice path.

Covers the three pieces of the feature:
  1. Config — voice.known_callers loads from creator_config/<slug>.json
  2. Lookup — known_caller_note matches numbers loosely and misses strangers
  3. Transport — the caller id is parsed out of the ElevenLabs system prompt
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

from app.brain.creator_config import load_creator
from app.voice.openai_llm import _split_messages
from app.voice.voice_brain import known_caller_note


BRIJ_NUMBER = "+16466406086"


def _zarna_config():
    cfg = load_creator("zarna")
    assert cfg is not None
    return cfg


# ---------------------------------------------------------------------------
# 1. Config loading
# ---------------------------------------------------------------------------

class TestKnownCallersConfig:
    def test_zarna_known_callers_loaded(self):
        cfg = _zarna_config()
        assert BRIJ_NUMBER in cfg.voice.known_callers
        assert "Brij" in cfg.voice.known_callers[BRIJ_NUMBER]

    def test_missing_block_defaults_empty(self):
        cfg = load_creator("test_creator")
        assert cfg is not None
        assert cfg.voice.known_callers == {}


# ---------------------------------------------------------------------------
# 2. Lookup semantics
# ---------------------------------------------------------------------------

class TestKnownCallerNote:
    def test_exact_e164_match(self):
        note = known_caller_note(_zarna_config(), BRIJ_NUMBER)
        assert "Brij" in note

    def test_matches_without_country_code(self):
        note = known_caller_note(_zarna_config(), "6466406086")
        assert "Brij" in note

    def test_matches_formatted_number(self):
        note = known_caller_note(_zarna_config(), "+1 (646) 640-6086")
        assert "Brij" in note

    def test_unknown_caller_gets_no_note(self):
        assert known_caller_note(_zarna_config(), "+12125551234") == ""

    def test_empty_caller_gets_no_note(self):
        assert known_caller_note(_zarna_config(), "") == ""

    def test_none_config_gets_no_note(self):
        assert known_caller_note(None, BRIJ_NUMBER) == ""


# ---------------------------------------------------------------------------
# 3. Caller id extraction from the ElevenLabs message array
# ---------------------------------------------------------------------------

class TestCallerIdExtraction:
    def test_caller_id_parsed_from_system_message(self):
        messages = [
            {"role": "system", "content": "Persona label.\ncaller_id: +16466406086"},
            {"role": "assistant", "content": "Hey! What's on your mind?"},
            {"role": "user", "content": "Mom, I want to be an artist when I grow up."},
        ]
        user_text, history, caller_id = _split_messages(messages)
        assert user_text == "Mom, I want to be an artist when I grow up."
        assert history == [{"role": "assistant", "text": "Hey! What's on your mind?"}]
        assert caller_id == "+16466406086"

    def test_no_system_message_yields_empty_caller(self):
        messages = [{"role": "user", "content": "hi"}]
        user_text, history, caller_id = _split_messages(messages)
        assert user_text == "hi"
        assert caller_id == ""

    def test_unsubstituted_placeholder_yields_empty_caller(self):
        # Non-phone contexts (dashboard test widget) may leave the variable empty
        # or unsubstituted — either way no number should be extracted.
        messages = [
            {"role": "system", "content": "Persona.\ncaller_id: {{system__caller_id}}"},
            {"role": "user", "content": "hello"},
        ]
        _, _, caller_id = _split_messages(messages)
        assert caller_id == ""

    def test_system_message_still_dropped_from_history(self):
        messages = [
            {"role": "system", "content": "caller_id: +16466406086"},
            {"role": "user", "content": "hello"},
        ]
        user_text, history, _ = _split_messages(messages)
        assert user_text == "hello"
        assert history == []
