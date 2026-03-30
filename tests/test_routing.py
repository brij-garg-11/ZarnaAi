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


def test_classify_routing_uses_json_from_gemini():
    from app.brain import routing

    mock_resp = MagicMock()
    mock_resp.text = '{"tier":"low","confidence":0.9,"reason":"hi"}'

    with patch.object(routing._client.models, "generate_content", return_value=mock_resp):
        assert routing.classify_routing_tier("hi", [], "") == "low"


def test_classify_low_confidence_bumped_to_medium():
    from app.brain import routing

    mock_resp = MagicMock()
    mock_resp.text = '{"tier":"low","confidence":0.5,"reason":"unsure"}'

    with patch.object(routing._client.models, "generate_content", return_value=mock_resp):
        assert routing.classify_routing_tier("whatever", [], "") == "medium"


def test_classify_invalid_json_falls_back_medium():
    from app.brain import routing

    mock_resp = MagicMock()
    mock_resp.text = "not json"

    with patch.object(routing._client.models, "generate_content", return_value=mock_resp):
        assert routing.classify_routing_tier("hello", [], "") == "medium"


def test_heuristic_high_before_gemini():
    from app.brain import routing

    long_msg = "y" * 1000
    with patch.object(routing._client.models, "generate_content") as gen:
        assert routing.classify_routing_tier(long_msg, [], "") == "high"
        gen.assert_not_called()
