"""
Tests for app/smb/onboarding.py

Covers the simplified passive-preference onboarding model:
  - Any first message from an unknown number → subscriber created, welcome returned
  - All subsequent messages → None (brain handles them)
  - If step 0 and message looks like an answer → preference save attempted in background
  - _welcome_and_question formatting
  - _looks_like_question_or_request detection
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sqlite3
from unittest.mock import patch, MagicMock

from app.smb.onboarding import (
    get_onboarding_reply,
    _welcome_and_question,
    _looks_like_question_or_request,
    _looks_like_opt_in,
    _ai_thinks_opt_in,
)
from app.smb.tenants import BusinessTenant


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tenant(keyword="COMEDY"):
    return BusinessTenant(
        slug="west_side_comedy",
        display_name="West Side Comedy Club",
        business_type="comedy_club",
        sms_number=None,
        owner_phone=None,
        keyword=keyword,
        tone="fun and casual",
        welcome_message="Thanks for joining West Side Comedy Club! Really glad you're here.",
        signup_question="Who's a comedian you love, or what kind of comedy are you into?",
        value_content_topics=["comedy tips"],
        blast_triggers=["opening", "deal"],
        segments=[
            {"name": "STANDUP", "question_key": "0", "answers": ["STANDUP", "BOTH"],
             "description": "Standup comedy fans"},
            {"name": "IMPROV", "question_key": "0", "answers": ["IMPROV", "BOTH"],
             "description": "Improv fans"},
        ],
    )


def _make_sqlite_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE smb_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number TEXT NOT NULL,
            tenant_slug  TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'active',
            onboarding_step INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now')),
            updated_at   TEXT DEFAULT (datetime('now')),
            UNIQUE (phone_number, tenant_slug)
        )
    """)
    conn.execute("""
        CREATE TABLE smb_preferences (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_id INTEGER NOT NULL REFERENCES smb_subscribers(id),
            question_key  TEXT NOT NULL,
            answer        TEXT NOT NULL DEFAULT '',
            answered_at   TEXT DEFAULT (datetime('now')),
            UNIQUE (subscriber_id, question_key)
        )
    """)
    conn.commit()
    return conn


class _SQLiteConnWrapper:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _SQLiteCursorWrapper(self._conn.cursor())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        return False

    def commit(self):
        self._conn.commit()

    def close(self):
        pass  # keep open for assertions


class _SQLiteCursorWrapper:
    def __init__(self, cur):
        self._cur = cur
        self.rowcount = 0

    def execute(self, sql, params=()):
        sql = sql.replace("%s", "?").replace("NOW()", "datetime('now')")
        if "ON CONFLICT (subscriber_id, question_key)" in sql:
            sql = (
                "INSERT OR REPLACE INTO smb_preferences (subscriber_id, question_key, answer) "
                "VALUES (?, ?, ?)"
            )
        if "ON CONFLICT (phone_number, tenant_slug) DO NOTHING" in sql:
            sql = sql.replace("ON CONFLICT (phone_number, tenant_slug) DO NOTHING", "")
            sql = sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        self._cur.execute(sql, params)
        self.rowcount = self._cur.rowcount

    def fetchone(self):
        row = self._cur.fetchone()
        return tuple(row) if row is not None else None

    def fetchall(self):
        return [tuple(r) for r in self._cur.fetchall()]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _make_wrapped_conn():
    return _SQLiteConnWrapper(_make_sqlite_conn())


def _patch_db(conn_wrapper):
    return patch("app.smb.onboarding.get_db_connection", return_value=conn_wrapper)


# ---------------------------------------------------------------------------
# Pure logic: _welcome_and_question
# ---------------------------------------------------------------------------

def test_welcome_includes_signup_question():
    tenant = _make_tenant()
    reply = _welcome_and_question(tenant)
    assert tenant.welcome_message in reply
    assert tenant.signup_question in reply
    print("✓ welcome reply contains both welcome message and signup question")


def test_welcome_includes_stop_line():
    tenant = _make_tenant()
    reply = _welcome_and_question(tenant)
    assert "STOP" in reply
    print("✓ welcome reply always contains STOP opt-out line")


def test_welcome_fallback_includes_stop():
    tenant = _make_tenant()
    tenant.signup_question = ""
    reply = _welcome_and_question(tenant)
    assert "West Side Comedy Club" in reply or "Welcome" in reply
    assert "STOP" in reply
    print("✓ welcome fallback also includes STOP opt-out line")


# ---------------------------------------------------------------------------
# Pure logic: _looks_like_opt_in
# ---------------------------------------------------------------------------

def test_opt_in_keyword_match():
    assert _looks_like_opt_in("COMEDY", keyword="COMEDY") is True
    assert _looks_like_opt_in("comedy", keyword="COMEDY") is True
    print("✓ signup keyword matches as opt-in")


def test_opt_in_yes_variants():
    for word in ["yes", "YES", "yeah", "yep", "sure", "ok", "okay", "in", "join", "i'm in", "im in"]:
        assert _looks_like_opt_in(word) is True, f"'{word}' should be opt-in"
    print("✓ yes/yeah/sure/ok/join all detected as opt-in")


def test_non_opt_in_messages():
    for msg in ["hey", "what time is the show?", "hi there", "hello", "bill burr"]:
        assert _looks_like_opt_in(msg) is False, f"'{msg}' should NOT be opt-in"
    print("✓ random messages not misclassified as opt-in")


def test_ai_fallback_opt_in():
    """Ambiguous short reply that AI says YES to → opt-in."""
    with patch("app.smb.onboarding._ai_thinks_opt_in", return_value=True):
        assert _looks_like_opt_in("sounds good!") is True
    print("✓ ambiguous reply delegates to AI and accepts YES")


def test_ai_fallback_not_opt_in():
    """Ambiguous short reply that AI says NO to → not opt-in."""
    with patch("app.smb.onboarding._ai_thinks_opt_in", return_value=False):
        assert _looks_like_opt_in("maybe later") is False
    print("✓ ambiguous reply delegates to AI and rejects NO")


def test_long_message_skips_ai():
    """Messages over 80 chars skip the AI check entirely."""
    long_msg = "I would really love to join your comedy club text list please add me thanks so much!"
    with patch("app.smb.onboarding._ai_thinks_opt_in") as mock_ai:
        result = _looks_like_opt_in(long_msg)
    mock_ai.assert_not_called()
    print("✓ long messages skip AI check")


# ---------------------------------------------------------------------------
# Pure logic: _looks_like_question_or_request
# ---------------------------------------------------------------------------

def test_question_mark_detected():
    assert _looks_like_question_or_request("What time is the show?") is True
    print("✓ trailing ? detected as question")


def test_question_word_detected():
    assert _looks_like_question_or_request("what time do you open") is True
    assert _looks_like_question_or_request("Is there parking nearby") is True
    assert _looks_like_question_or_request("Do you have a kids menu") is True
    print("✓ question-starting words detected")


def test_preference_answer_not_detected_as_question():
    assert _looks_like_question_or_request("standup comedy") is False
    assert _looks_like_question_or_request("I love improv") is False
    assert _looks_like_question_or_request("Bill Burr") is False
    print("✓ preference answers not misclassified as questions")


# ---------------------------------------------------------------------------
# Integration: new subscriber flow
# ---------------------------------------------------------------------------

def test_new_subscriber_gets_welcome_on_yes():
    """Opt-in reply (YES) from unknown number → subscriber created, welcome returned."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        with patch("app.smb.onboarding.threading.Thread"):
            with patch("app.smb.onboarding.tagging.tag_geo"):
                reply = get_onboarding_reply("+15550001111", "yes", tenant)
    assert reply is not None
    assert "West Side Comedy Club" in reply or "comedy" in reply.lower()
    assert "STOP" in reply
    print("✓ new subscriber gets welcome message with STOP on opt-in")


def test_new_subscriber_gets_welcome_on_keyword():
    """Signup keyword from unknown number → subscriber created, welcome returned."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        with patch("app.smb.onboarding.threading.Thread"):
            with patch("app.smb.onboarding.tagging.tag_geo"):
                reply = get_onboarding_reply("+15550001111", "COMEDY", tenant)
    assert reply is not None
    assert "STOP" in reply
    print("✓ new subscriber gets welcome message on keyword")


def test_non_optin_unknown_sender_returns_none():
    """Unknown sender who doesn't opt in → None so brain sends the invite nudge."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "hey there", tenant)
    assert reply is None
    sub = wrapped._conn.execute(
        "SELECT id FROM smb_subscribers WHERE phone_number=?", ("+15550001111",)
    ).fetchone()
    assert sub is None
    print("✓ non-opt-in unknown sender gets None (not subscribed)")


def test_new_subscriber_created_in_db():
    """Opt-in keyword creates subscriber row with status=active, step=0."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        with patch("app.smb.onboarding.threading.Thread"):
            with patch("app.smb.onboarding.tagging.tag_geo"):
                get_onboarding_reply("+15550001111", "COMEDY", tenant)
    sub = wrapped._conn.execute(
        "SELECT status, onboarding_step FROM smb_subscribers WHERE phone_number=?",
        ("+15550001111",)
    ).fetchone()
    assert sub is not None
    assert sub[0] == "active"
    assert sub[1] == 0
    print("✓ new subscriber created with status=active, step=0")


def test_existing_subscriber_step_0_returns_none():
    """Step-0 subscriber (hasn't answered yet) → None so brain takes over."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'active', 0)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "I love standup", tenant)
    assert reply is None
    print("✓ step-0 subscriber returns None (brain handles reply)")


def test_existing_active_subscriber_returns_none():
    """Active subscriber past onboarding → None so brain takes over."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'active', 1)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "what time is the show?", tenant)
    assert reply is None
    print("✓ active subscriber (step > 0) returns None")


def test_no_db_connection_returns_none():
    """DB failure on new subscriber → None, no crash."""
    tenant = _make_tenant()
    with patch("app.smb.onboarding.get_db_connection", return_value=None):
        reply = get_onboarding_reply("+15550001111", "COMEDY", tenant)
    assert reply is None
    print("✓ DB failure returns None gracefully")


def test_geo_tagged_at_subscriber_creation():
    """New subscriber signup triggers tag_geo with their phone number."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        with patch("app.smb.onboarding.threading.Thread"):
            with patch("app.smb.onboarding.tagging.tag_geo") as mock_tag:
                get_onboarding_reply("+12125550001", "YES", tenant)
    mock_tag.assert_called_once()
    call_args = mock_tag.call_args
    assert call_args[0][2] == "+12125550001"  # phone_number is 3rd positional arg
    print("✓ tag_geo called with correct phone number at signup")


def test_step_0_question_does_not_trigger_passive_save():
    """If step-0 subscriber sends a question, preference save thread is NOT started."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'active', 0)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        with patch("app.smb.onboarding.threading.Thread") as mock_thread:
            get_onboarding_reply("+15550001111", "what shows are on this weekend?", tenant)
    mock_thread.assert_not_called()
    print("✓ question from step-0 subscriber does not trigger preference save thread")


def test_step_0_answer_triggers_passive_save_thread():
    """If step-0 subscriber sends an answer (not a question), save thread IS started."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'active', 0)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        with patch("app.smb.onboarding.threading.Thread") as mock_thread:
            get_onboarding_reply("+15550001111", "I love standup comedy", tenant)
    mock_thread.assert_called_once()
    print("✓ answer from step-0 subscriber triggers background preference save")
