"""Regression: voice rules in generator must stay aligned with product direction."""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.brain.generator as gen


def test_style_rules_include_question_and_my_friend_guidance():
    rules = gen._STYLE_RULES
    assert "Default: land the joke" in rules or "No question is the norm" in rules
    assert "every three or four" in rules.lower() or "three or four fan messages" in rules
    assert "my friend" in rules.lower()
    assert "Never stack two questions" in rules or "At most one question per reply" in rules
    assert "rhetorical" in rules.lower()
    assert "Do not mirror the fan's words back" in rules or "mirror the fan" in rules.lower()
    assert "direct question" in rules.lower()
