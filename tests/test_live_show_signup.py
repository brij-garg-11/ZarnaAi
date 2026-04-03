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
        "event_category": "other",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        r = ls.try_live_show_signup("+15551234567", "chicago", "slicktext")
        assert r.suppress_ai is True
        assert r.join_confirmation_sms is None


def test_comedy_sends_confirmation_on_new_signup():
    show = {
        "id": 1,
        "keyword": "chicago",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "comedy",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        r = ls.try_live_show_signup("+15551234567", "chicago", "slicktext")
        assert r.suppress_ai is True
        assert r.join_confirmation_sms
        assert len(r.join_confirmation_sms) > 20
        assert r.confirmation_phone == "+15551234567"
        assert r.confirmation_channel == "slicktext"


def test_comedy_repeat_still_gets_short_confirmation():
    show = {
        "id": 1,
        "keyword": "chicago",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "comedy",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=False
    ):
        r = ls.try_live_show_signup("+15551234567", "chicago", "slicktext")
        assert r.suppress_ai is True
        assert r.join_confirmation_sms
        low = r.join_confirmation_sms.lower()
        assert "list" in low or "still" in low or "already" in low or "got" in low


def test_live_stream_sends_confirmation_on_new_signup():
    show = {
        "id": 1,
        "keyword": "watch",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "live_stream",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        r = ls.try_live_show_signup("+15551234567", "watch", "slicktext")
        assert r.suppress_ai is True
        assert r.join_confirmation_sms
        low = r.join_confirmation_sms.lower()
        assert "live" in low or "stream" in low
        assert r.confirmation_phone == "+15551234567"


def test_live_stream_repeat_gets_confirmation():
    show = {
        "id": 1,
        "keyword": "watch",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "live_stream",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=False
    ):
        r = ls.try_live_show_signup("+15551234567", "watch", "slicktext")
        assert r.suppress_ai is True
        assert r.join_confirmation_sms
        assert "live" in r.join_confirmation_sms.lower() or "stream" in r.join_confirmation_sms.lower()


def test_event_category_livestream_alias():
    show = {
        "id": 1,
        "keyword": "watch",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "livestream",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        r = ls.try_live_show_signup("+15551234567", "watch", "slicktext")
        assert r.suppress_ai is True
        assert r.join_confirmation_sms


def test_try_live_show_signup_no_suppress_when_extra_words():
    show = {
        "id": 1,
        "keyword": "chicago",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "other",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), patch.object(
        ls.repo, "add_signup", return_value=True
    ):
        r = ls.try_live_show_signup("+15551234567", "chicago what time", "slicktext")
        assert r.suppress_ai is False


def test_try_live_show_signup_no_shows():
    with patch.object(ls.repo, "active_live_shows", return_value=[]):
        r = ls.try_live_show_signup("+15551234567", "blue", "slicktext")
        assert r.suppress_ai is False


def test_confirmation_uses_pool_not_bare_thread():
    """Signup confirmations must be submitted to the bounded pool, not raw threads."""
    pytest.importorskip("twilio")
    import main as app_module
    from concurrent.futures import ThreadPoolExecutor

    assert isinstance(app_module._confirm_pool, ThreadPoolExecutor), \
        "_confirm_pool should be a ThreadPoolExecutor"

    submitted = []
    original_submit = app_module._confirm_pool.submit

    show = {
        "id": 1,
        "keyword": "chicago",
        "use_keyword_only": True,
        "window_start": None,
        "window_end": None,
        "event_category": "comedy",
    }
    with patch.object(ls.repo, "active_live_shows", return_value=[show]), \
         patch.object(ls.repo, "add_signup", return_value=True), \
         patch.object(app_module._confirm_pool, "submit", side_effect=lambda fn, *a, **kw: submitted.append(fn) or original_submit(fn, *a, **kw)):
        r = ls.try_live_show_signup("+15551234567", "chicago", "slicktext")
        app_module._send_join_confirmation_async(
            r.confirmation_phone, r.confirmation_channel, r.join_confirmation_sms
        )

    assert len(submitted) == 1, "Expected exactly one pool submission for the confirmation"
