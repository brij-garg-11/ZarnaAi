"""Tests for *emphasis* throttle and distress stripping."""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.brain.emphasis import (
    recent_assistant_replies_used_emphasis,
    should_suppress_all_emphasis,
    strip_all_emphasis,
    user_signals_distress,
)
from app.brain.intent import Intent


def test_user_signals_distress_phrases():
    assert user_signals_distress("I'm feeling very sad today")
    assert user_signals_distress("i have anxiety about work")
    assert user_signals_distress("This is hopeless")
    assert user_signals_distress("My day is going bad")
    assert user_signals_distress("having a rough day")


def test_user_signals_distress_not_false_on_negation():
    assert not user_signals_distress("I am not sad at all, just tired")


def test_user_signals_distress_bad_without_day_context():
    assert not user_signals_distress("that was a bad joke")


def test_user_signals_distress_word_sad():
    assert user_signals_distress("So sad right now")


def test_recent_assistant_replies_used_emphasis():
    assert not recent_assistant_replies_used_emphasis([], k=3)
    assert not recent_assistant_replies_used_emphasis(["plain text", "also plain"], k=3)
    assert recent_assistant_replies_used_emphasis(["plain", "one *hit* word"], k=3)
    assert recent_assistant_replies_used_emphasis(
        ["a", "b", "c", "last has *x*"], k=3
    )


def test_should_suppress_distress_overrides_joke():
    assert should_suppress_all_emphasis(
        "I'm so sad", Intent.JOKE, ["previous *punch*"]
    )


def test_should_suppress_joke_bypasses_throttle():
    assert not should_suppress_all_emphasis(
        "tell me another joke", Intent.JOKE, ["had a *word* here"]
    )


def test_should_suppress_general_when_recent_had_emphasis():
    assert should_suppress_all_emphasis(
        "lol ok", Intent.GENERAL, ["line one", "two *boom*", "three"]
    )


def test_should_not_suppress_general_when_recent_plain():
    assert not should_suppress_all_emphasis(
        "lol ok", Intent.GENERAL, ["plain", "also plain", "still plain"]
    )


def test_strip_all_emphasis():
    assert strip_all_emphasis("no stars") == "no stars"
    assert strip_all_emphasis("one *word* here") == "one word here"
    assert strip_all_emphasis("*a* and *b*") == "a and b"
    assert strip_all_emphasis("**bold** and *x*") == "bold and x"
