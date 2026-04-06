"""
Tests for app/smb/onboarding.py and app/smb/storage.py

Pure logic tests run without any DB.
Integration tests mock get_db_connection() with an in-memory SQLite
database that mirrors the smb_subscribers / smb_preferences schema.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sqlite3
import unittest
from unittest.mock import patch, MagicMock
from dataclasses import dataclass, field
from typing import Optional

from app.smb.onboarding import (
    is_signup_keyword,
    get_onboarding_reply,
    _ask_question,
    _completion_message,
)
from app.smb.tenants import BusinessTenant


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tenant(keyword="COMEDY", questions=None):
    return BusinessTenant(
        slug="west_side_comedy",
        display_name="West Side Comedy Club",
        business_type="comedy_club",
        sms_number=None,
        owner_phone=None,
        keyword=keyword,
        tone="fun and casual",
        value_content_topics=["comedy tips"],
        signup_questions=questions or [
            "What kind of comedy do you love? Reply: STANDUP, IMPROV, or BOTH",
            "How often do you want to hear from us? Reply: WEEKLY or DEALS ONLY",
        ],
        blast_triggers=["opening", "deal"],
    )


def _make_sqlite_conn():
    """
    In-memory SQLite connection with the SMB schema.
    Uses ? placeholders (SQLite) so storage functions need adapting —
    we use a thin cursor wrapper to translate %s → ? for tests.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE smb_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number TEXT NOT NULL,
            tenant_slug  TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'onboarding',
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
    """
    Wraps a SQLite connection so it works with our storage functions.
    Translates psycopg2-style %s placeholders → SQLite ? placeholders,
    and exposes a context manager for transaction blocks.
    """
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
        self.lastrowid = None

    def execute(self, sql, params=()):
        sql = sql.replace("%s", "?").replace("NOW()", "datetime('now')")
        # Rewrite Postgres UPSERT for smb_preferences → SQLite INSERT OR REPLACE
        if "ON CONFLICT (subscriber_id, question_key)" in sql:
            sql = (
                "INSERT OR REPLACE INTO smb_preferences (subscriber_id, question_key, answer) "
                "VALUES (?, ?, ?)"
            )
        # Rewrite Postgres ON CONFLICT ... DO NOTHING → SQLite INSERT OR IGNORE INTO
        if "ON CONFLICT (phone_number, tenant_slug) DO NOTHING" in sql:
            sql = sql.replace("ON CONFLICT (phone_number, tenant_slug) DO NOTHING", "")
            sql = sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        self._cur.execute(sql, params)
        self.lastrowid = self._cur.lastrowid

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return tuple(row)

    def fetchall(self):
        return [tuple(r) for r in self._cur.fetchall()]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# Pure logic tests (no DB)
# ---------------------------------------------------------------------------

def test_is_signup_keyword_match():
    tenant = _make_tenant(keyword="COMEDY")
    assert is_signup_keyword("COMEDY", tenant) is True
    assert is_signup_keyword("comedy", tenant) is True
    assert is_signup_keyword("  Comedy  ", tenant) is True
    print("✓ is_signup_keyword matches case-insensitively")


def test_is_signup_keyword_no_match():
    tenant = _make_tenant(keyword="COMEDY")
    assert is_signup_keyword("hello", tenant) is False
    assert is_signup_keyword("COMEDY CLUB", tenant) is False
    assert is_signup_keyword("", tenant) is False
    print("✓ is_signup_keyword rejects non-keyword messages")


def test_is_signup_keyword_no_keyword_set():
    tenant = _make_tenant(keyword=None)
    assert is_signup_keyword("COMEDY", tenant) is False
    print("✓ is_signup_keyword returns False when tenant has no keyword")


def test_ask_question_returns_first_question():
    tenant = _make_tenant()
    reply = _ask_question(tenant, 0)
    assert "STANDUP" in reply or "comedy" in reply.lower()
    print("✓ _ask_question returns correct question for step 0")


def test_ask_question_returns_second_question():
    tenant = _make_tenant()
    reply = _ask_question(tenant, 1)
    assert "WEEKLY" in reply or "often" in reply.lower()
    print("✓ _ask_question returns correct question for step 1")


def test_ask_question_past_end_returns_completion():
    tenant = _make_tenant()
    reply = _ask_question(tenant, 99)
    assert "all set" in reply.lower() or "welcome" in reply.lower()
    print("✓ _ask_question returns completion message when past last question")


def test_completion_message_contains_business_name():
    tenant = _make_tenant()
    msg = _completion_message(tenant)
    assert "West Side Comedy Club" in msg
    assert "STOP" in msg
    print("✓ completion message contains business name and STOP instruction")


# ---------------------------------------------------------------------------
# Integration tests — mock DB with SQLite wrapper
# ---------------------------------------------------------------------------

def _make_wrapped_conn():
    return _SQLiteConnWrapper(_make_sqlite_conn())


def _patch_db(conn_wrapper):
    """Return a context manager that patches get_db_connection to return our wrapper."""
    return patch("app.smb.onboarding.get_db_connection", return_value=conn_wrapper)


def test_signup_creates_subscriber_and_asks_first_question():
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "COMEDY", tenant)
    assert reply is not None
    assert "STANDUP" in reply or "comedy" in reply.lower()
    # Verify subscriber was created in DB
    sub = wrapped._conn.execute(
        "SELECT status, onboarding_step FROM smb_subscribers WHERE phone_number=?",
        ("+15550001111",)
    ).fetchone()
    assert sub is not None
    assert sub[0] == "onboarding"
    assert sub[1] == 0
    print("✓ signup keyword creates subscriber and returns first question")


def test_already_subscribed_returns_friendly_message():
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    # Pre-insert an active subscriber
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'active', 2)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "COMEDY", tenant)
    assert reply is not None
    assert "already subscribed" in reply.lower()
    print("✓ already-active subscriber gets friendly 'already subscribed' message")


def test_first_answer_saves_preference_and_asks_second_question():
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    # Pre-insert subscriber at step 0 (onboarding)
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'onboarding', 0)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "STANDUP", tenant)
    assert reply is not None
    assert "WEEKLY" in reply or "often" in reply.lower()
    # Preference saved
    pref = wrapped._conn.execute(
        "SELECT answer FROM smb_preferences WHERE question_key=?", ("0",)
    ).fetchone()
    assert pref is not None
    assert pref[0] == "STANDUP"
    # Step advanced
    sub = wrapped._conn.execute(
        "SELECT onboarding_step, status FROM smb_subscribers WHERE phone_number=?",
        ("+15550001111",)
    ).fetchone()
    assert sub[0] == 1
    assert sub[1] == "onboarding"
    print("✓ first answer saved and second question returned")


def test_final_answer_completes_onboarding():
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    # Pre-insert subscriber at step 1 (last question)
    wrapped._conn.execute(
        "INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step) "
        "VALUES (?, ?, 'onboarding', 1)",
        ("+15550001111", "west_side_comedy")
    )
    wrapped._conn.commit()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "WEEKLY", tenant)
    assert reply is not None
    assert "all set" in reply.lower() or "welcome" in reply.lower()
    # Status is now active
    sub = wrapped._conn.execute(
        "SELECT status FROM smb_subscribers WHERE phone_number=?",
        ("+15550001111",)
    ).fetchone()
    assert sub[0] == "active"
    print("✓ final answer marks subscriber active and returns completion message")


def test_non_subscriber_non_keyword_returns_none():
    """Regular message from unknown number → not an onboarding message."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()
    with _patch_db(wrapped):
        reply = get_onboarding_reply("+15550001111", "what time do you open?", tenant)
    assert reply is None
    print("✓ non-subscriber non-keyword returns None (routes to main brain)")


def test_full_onboarding_happy_path():
    """Walk through the complete onboarding: keyword → q1 → q2 → done."""
    tenant = _make_tenant()
    wrapped = _make_wrapped_conn()

    with _patch_db(wrapped):
        r1 = get_onboarding_reply("+15550009999", "COMEDY", tenant)
    assert r1 is not None and ("STANDUP" in r1 or "comedy" in r1.lower())

    with _patch_db(wrapped):
        r2 = get_onboarding_reply("+15550009999", "BOTH", tenant)
    assert r2 is not None and ("WEEKLY" in r2 or "often" in r2.lower())

    with _patch_db(wrapped):
        r3 = get_onboarding_reply("+15550009999", "WEEKLY", tenant)
    assert r3 is not None and ("all set" in r3.lower() or "welcome" in r3.lower())

    sub = wrapped._conn.execute(
        "SELECT status, onboarding_step FROM smb_subscribers WHERE phone_number=?",
        ("+15550009999",)
    ).fetchone()
    assert sub[0] == "active"
    assert sub[1] == 2

    prefs = wrapped._conn.execute(
        "SELECT question_key, answer FROM smb_preferences ORDER BY question_key"
    ).fetchall()
    assert len(prefs) == 2
    assert prefs[0][1] == "BOTH"
    assert prefs[1][1] == "WEEKLY"

    print("✓ full happy path: keyword → q1 → q2 → active, both preferences saved")
