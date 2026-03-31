"""Routing tier classification (heuristics + mocked Gemini)."""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_heuristic_long_message_forces_high():
    from app.brain import routing as r

    msg = "x" * 950
    assert r._heuristic_floor(msg) == "high"


def test_heuristic_many_questions_forces_medium():
    from app.brain import routing as r

    assert r._heuristic_floor("a? b? c? d?") == "medium"


def test_heuristic_sensitive_forces_high():
    from app.brain import routing as r

    assert r._heuristic_floor("I want legal advice about my visa") == "high"


def _msg_needs_router_api() -> str:
    """Skips router fast-path (needs ? or length); keeps tests on the Gemini path."""
    return (
        "I have been thinking about comedy and family dynamics for a while — "
        "what is your honest take on setting boundaries without losing the laugh?"
    )


def test_try_router_skip_safe_short_banter():
    from app.brain import routing as r

    assert r.try_router_skip_safe("hi") is True
    assert r.try_router_skip_safe("thanks so much") is True
    assert r.try_router_skip_safe("ok cool") is True


def test_try_router_skip_safe_blocked_by_question():
    from app.brain import routing as r

    assert r.try_router_skip_safe("how are you?") is False


def test_classify_routing_uses_json_from_gemini():
    from app.brain import routing

    mock_resp = MagicMock()
    mock_resp.text = '{"tier":"low","confidence":0.9,"reason":"hi"}'

    with patch.object(routing._client.models, "generate_content", return_value=mock_resp):
        assert routing.classify_routing_tier(_msg_needs_router_api(), [], "") == "low"


def test_classify_low_confidence_bumped_to_medium():
    from app.brain import routing

    mock_resp = MagicMock()
    mock_resp.text = '{"tier":"low","confidence":0.5,"reason":"unsure"}'

    with patch.object(routing._client.models, "generate_content", return_value=mock_resp):
        assert routing.classify_routing_tier(_msg_needs_router_api(), [], "") == "medium"


def test_classify_invalid_json_falls_back_medium():
    from app.brain import routing

    mock_resp = MagicMock()
    mock_resp.text = "not json"

    with patch.object(routing._client.models, "generate_content", return_value=mock_resp):
        assert routing.classify_routing_tier(_msg_needs_router_api(), [], "") == "medium"


def test_heuristic_high_before_gemini():
    from app.brain import routing

    long_msg = "y" * 1000
    with patch.object(routing._client.models, "generate_content") as gen:
        assert routing.classify_routing_tier(long_msg, [], "") == "high"
        gen.assert_not_called()


def test_skip_router_returns_low_without_gemini():
    from app.brain import routing

    with patch.object(routing._client.models, "generate_content") as gen:
        assert routing.classify_routing_tier("lol thanks", [], "") == "low"
        gen.assert_not_called()
