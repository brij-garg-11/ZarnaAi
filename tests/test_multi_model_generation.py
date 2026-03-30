"""Multi-provider reply generation (mocked external APIs)."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.brain.intent import Intent


@patch("app.brain.generator._generate_openai_raw")
@patch("app.brain.generator._generate_anthropic_raw")
@patch("app.brain.generator._generate_gemini_raw")
def test_show_intent_uses_only_gemini_even_when_multi(mock_gemini, mock_anthropic, mock_openai):
    mock_gemini.return_value = "Get tickets here.\nhttps://zarnagarg.com/tickets/"

    from app.brain.generator import generate_zarna_reply

    out = generate_zarna_reply(
        Intent.SHOW,
        "when is the next show",
        [],
        [],
        "",
        routing_tier="high",
    )
    assert "zarnagarg.com" in out
    mock_gemini.assert_called_once()
    mock_openai.assert_not_called()
    mock_anthropic.assert_not_called()


@patch("app.brain.generator._generate_openai_raw")
@patch("app.brain.generator._generate_anthropic_raw")
@patch("app.brain.generator._generate_gemini_raw")
def test_medium_tier_calls_openai_when_enabled(mock_gemini, mock_anthropic, mock_openai):
    mock_openai.return_value = "OpenAI says hello in three sentences max."

    from app.brain.generator import generate_zarna_reply

    with patch("app.brain.generator._multi_model_enabled", return_value=True), patch.multiple(
        "app.brain.generator",
        OPENAI_API_KEY="sk-test",
        ANTHROPIC_API_KEY="ak-test",
    ):
        out = generate_zarna_reply(
            Intent.GENERAL,
            "How are you?",
            ["fact one"],
            [{"role": "user", "text": "hi"}],
            "",
            routing_tier="medium",
        )
    assert mock_openai.call_count == 1
    mock_anthropic.assert_not_called()
    assert "OpenAI" in out or "hello" in out.lower()


@patch("app.brain.generator._generate_openai_raw")
@patch("app.brain.generator._generate_anthropic_raw")
@patch("app.brain.generator._generate_gemini_raw")
def test_high_tier_calls_anthropic_first(mock_gemini, mock_anthropic, mock_openai):
    mock_anthropic.return_value = "Claude gives careful advice in one calm reply."

    from app.brain.generator import generate_zarna_reply

    with patch("app.brain.generator._multi_model_enabled", return_value=True), patch.multiple(
        "app.brain.generator",
        OPENAI_API_KEY="sk-test",
        ANTHROPIC_API_KEY="ak-test",
    ):
        out = generate_zarna_reply(
            Intent.GENERAL,
            "I feel lost about my career and family pressure",
            [],
            [],
            "",
            routing_tier="high",
        )
    mock_anthropic.assert_called_once()
    mock_openai.assert_not_called()
    assert "Claude" in out or "careful" in out.lower()


@patch("app.brain.generator._generate_openai_raw")
@patch("app.brain.generator._generate_anthropic_raw")
@patch("app.brain.generator._generate_gemini_raw")
def test_high_falls_back_openai_when_anthropic_fails(mock_gemini, mock_anthropic, mock_openai):
    mock_anthropic.side_effect = RuntimeError("api down")
    mock_openai.return_value = "Backup from OpenAI."

    from app.brain.generator import generate_zarna_reply

    with patch("app.brain.generator._multi_model_enabled", return_value=True), patch.multiple(
        "app.brain.generator",
        OPENAI_API_KEY="sk-test",
        ANTHROPIC_API_KEY="ak-test",
    ):
        out = generate_zarna_reply(
            Intent.GENERAL,
            "long question " * 20,
            [],
            [],
            "",
            routing_tier="high",
        )
    mock_anthropic.assert_called_once()
    mock_openai.assert_called_once()
    assert "Backup" in out


@patch("app.brain.generator._generate_gemini_raw")
def test_low_tier_uses_gemini(mock_gemini):
    mock_gemini.return_value = "Short Gemini reply."

    from app.brain.generator import generate_zarna_reply

    with patch("app.brain.generator._multi_model_enabled", return_value=True), patch.multiple(
        "app.brain.generator",
        OPENAI_API_KEY="sk-test",
        ANTHROPIC_API_KEY="ak-test",
    ):
        out = generate_zarna_reply(
            Intent.JOKE,
            "quick joke",
            [],
            [],
            "",
            routing_tier="low",
        )
    mock_gemini.assert_called_once()
    assert "Gemini" in out or "reply" in out.lower()


@patch("app.brain.generator._multi_model_enabled", return_value=False)
@patch("app.brain.generator._generate_gemini_raw")
def test_multi_disabled_always_gemini(mock_gemini, _mc):
    mock_gemini.return_value = "Only Gemini."

    from app.brain.generator import generate_zarna_reply

    out = generate_zarna_reply(
        Intent.GENERAL,
        "anything",
        [],
        [],
        "",
        routing_tier="high",
    )
    mock_gemini.assert_called_once()
    assert "Only Gemini" in out
