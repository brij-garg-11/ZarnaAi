"""Unit tests for live show keyword matching (no DB)."""

from unittest.mock import patch

import pytest

from app.live_shows import keyword_match as km
from app.live_shows import signup as ls


@pytest.mark.parametrize(
    "body,kw,expect",
    [
        ("blue", "blue", True),
        ("Blue", "blue", True),
        ("BLUE", "blue", True),
        ("bleu", "blue", True),
        ("bule", "blue", True),
        ("blue!", "blue", True),
        ("blue party", "blue", True),
        ("glue", "blue", False),
        ("bl", "blue", False),
        ("no", "go", False),
    ],
)
def test_body_matches_keyword(body, kw, expect):
    assert km.body_matches_keyword(body, kw) is expect


@pytest.mark.parametrize(
    "body,kw,expect",
    [
        ("blue", "blue", True),
        ("Bleu", "blue", True),
        ("blue!", "blue", True),
        ("blue party", "blue", False),
        ("hello", "blue", False),
    ],
)
def test_is_keyword_only_join(body, kw, expect):
    assert km.is_keyword_only_join(body, kw) is expect


def test_try_live_show_signup_returns_suppress_when_keyword_only():
    show = {
        "id": 1,
        "keyword": "chicago",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        assert ls.try_live_show_signup("+15551234567", "chicago", "slicktext") is True


def test_try_live_show_signup_no_suppress_when_extra_words():
    show = {
        "id": 1,
        "keyword": "chicago",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        assert ls.try_live_show_signup("+15551234567", "chicago what time", "slicktext") is False


def test_try_live_show_signup_no_shows():
    with patch.object(ls.repo, "active_live_shows", return_value=[]):
        assert ls.try_live_show_signup("+15551234567", "blue", "slicktext") is False
