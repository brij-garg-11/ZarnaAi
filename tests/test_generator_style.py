"""Regression: voice rules in generator must stay aligned with product direction."""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.brain.generator as gen


def test_style_rules_include_question_and_my_friend_guidance():
    rules = gen._STYLE_RULES
    ex = gen._TONE_EXAMPLES
    assert "Sadness" in rules
    assert "my friend" in rules.lower()
    assert "echo-mock" in rules or "echo" in rules.lower()
    assert "bad day?" in ex.lower()
    assert "never two" in rules.lower() or "at most one question" in rules.lower()
    assert "asterisk" in rules.lower() or "emphasis" in rules.lower()
    assert "ok and?" in ex.lower()
    assert "default **no**" in rules or "default no" in rules.replace("**", "").lower()
