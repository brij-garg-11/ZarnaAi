"""
Website link tracking is opt-in (TRACK_BOT_WEBSITE_LINKS). While off (default),
the bot must send the real creator URL untouched so the SMS preview shows the
branded domain (e.g. zarnagarg.com) instead of a tracked redirect host.
"""
import importlib
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _reload_tracker(monkeypatch, flag_value):
    if flag_value is None:
        monkeypatch.delenv("TRACK_BOT_WEBSITE_LINKS", raising=False)
    else:
        monkeypatch.setenv("TRACK_BOT_WEBSITE_LINKS", flag_value)
    monkeypatch.setenv("MAIN_APP_BASE_URL", "https://api.zar.bot")
    import app.link_tracker as lt
    return importlib.reload(lt)


def test_website_link_untouched_when_tracking_off_by_default(monkeypatch):
    lt = _reload_tracker(monkeypatch, None)
    reply = "I'd love to see you there! https://zarnagarg.com/tickets/"
    assert lt.rewrite_bot_reply(reply, phone_number="+15551234567") == reply


def test_website_link_untouched_when_tracking_explicitly_off(monkeypatch):
    lt = _reload_tracker(monkeypatch, "0")
    reply = "Tickets at https://zarnagarg.com/shows are live."
    assert lt.rewrite_bot_reply(reply) == reply
    assert "/t/bot-website" not in lt.rewrite_bot_reply(reply)


def test_no_urls_returns_unchanged(monkeypatch):
    lt = _reload_tracker(monkeypatch, "1")
    reply = "See you soon!"
    assert lt.rewrite_bot_reply(reply) == reply


def teardown_module(module):
    # Restore the module to its env-default state for any later imports.
    import app.link_tracker as lt
    importlib.reload(lt)
