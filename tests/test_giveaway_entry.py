"""
Tests for giveaway keyword matching + entry recording (app/giveaway/entry.py).

The DB layer is monkeypatched, so these run without a database: we verify the
pure keyword predicate and the decision logic in try_giveaway_entry (when to
return a confirmation vs. let the AI reply proceed).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.giveaway.entry as entry
from app.giveaway.entry import keyword_matches, try_giveaway_entry


# ── keyword_matches (pure) ──────────────────────────────────────────────────

def test_keyword_matches_exact_and_case_insensitive():
    assert keyword_matches("FREE", "free")
    assert keyword_matches("free", "FREE")
    assert keyword_matches("Free", "FrEe")


def test_keyword_matches_contains():
    assert keyword_matches("FREE", "I want free stuff please")
    assert keyword_matches("FREE", "free!")
    assert keyword_matches("CALL", "please CALL me")


def test_keyword_matches_no_match():
    assert not keyword_matches("FREE", "hello there")
    assert not keyword_matches("CALL", "i love zarna")


def test_keyword_matches_empty_inputs():
    assert not keyword_matches("", "free")
    assert not keyword_matches("FREE", "")
    assert not keyword_matches("", "")


# ── try_giveaway_entry (logic, DB monkeypatched) ────────────────────────────

def _stub_campaigns(monkeypatch, campaigns):
    monkeypatch.setattr(entry, "_active_campaigns", lambda slug: campaigns)


def test_new_entry_returns_confirmation(monkeypatch):
    _stub_campaigns(monkeypatch, [{"id": 1, "keyword": "FREE", "confirmation": "You win!"}])
    monkeypatch.setattr(entry, "_insert_entry", lambda *a, **k: True)  # new row
    reply = try_giveaway_entry("+15551234567", "FREE", message_id=99, slug="zarna")
    assert reply == "You win!"


def test_already_entered_returns_none(monkeypatch):
    _stub_campaigns(monkeypatch, [{"id": 1, "keyword": "FREE", "confirmation": "You win!"}])
    monkeypatch.setattr(entry, "_insert_entry", lambda *a, **k: False)  # dedup: no new row
    reply = try_giveaway_entry("+15551234567", "FREE", slug="zarna")
    assert reply is None


def test_no_active_campaign_returns_none(monkeypatch):
    _stub_campaigns(monkeypatch, [])
    reply = try_giveaway_entry("+15551234567", "FREE", slug="zarna")
    assert reply is None


def test_keyword_not_matched_returns_none(monkeypatch):
    _stub_campaigns(monkeypatch, [{"id": 1, "keyword": "FREE", "confirmation": "You win!"}])
    called = {"insert": False}

    def _insert(*a, **k):
        called["insert"] = True
        return True

    monkeypatch.setattr(entry, "_insert_entry", _insert)
    reply = try_giveaway_entry("+15551234567", "just saying hi", slug="zarna")
    assert reply is None
    assert called["insert"] is False  # never even attempted


def test_blank_confirmation_falls_back_to_default(monkeypatch):
    _stub_campaigns(monkeypatch, [{"id": 1, "keyword": "FREE", "confirmation": ""}])
    monkeypatch.setattr(entry, "_insert_entry", lambda *a, **k: True)
    reply = try_giveaway_entry("+15551234567", "FREE", slug="zarna")
    assert reply == entry._DEFAULT_CONFIRMATION


def test_empty_message_returns_none(monkeypatch):
    _stub_campaigns(monkeypatch, [{"id": 1, "keyword": "FREE", "confirmation": "x"}])
    assert try_giveaway_entry("+15551234567", "", slug="zarna") is None
    assert try_giveaway_entry("", "FREE", slug="zarna") is None
