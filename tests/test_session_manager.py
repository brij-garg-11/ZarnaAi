"""
Phase 3 session manager tests.

Tests the pure logic of session_manager without a real database.
We patch _get_conn() to return a mock connection with a mock cursor.
"""

import sys
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Stub psycopg2 if not installed
if "psycopg2" not in sys.modules:
    _stub = MagicMock()
    _stub.extras = MagicMock()
    _stub.extras.DictCursor = None
    sys.modules["psycopg2"] = _stub
    sys.modules["psycopg2.extras"] = _stub.extras

from app.analytics.session_manager import (
    SESSION_GAP_HOURS,
    get_or_create_session,
    close_stale_sessions,
    backfill_came_back_within_7d,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cursor(fetchone_result=None, fetchall_result=None, rowcount=0):
    cur = MagicMock()
    cur.__enter__ = lambda s: cur
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = fetchone_result
    cur.fetchall.return_value = fetchall_result or []
    cur.rowcount = rowcount
    return cur


def _make_conn(cur):
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.close = MagicMock()
    return conn


# ---------------------------------------------------------------------------
# get_or_create_session — no DB
# ---------------------------------------------------------------------------

def test_get_or_create_session_returns_none_when_no_db():
    with patch("app.analytics.session_manager._get_conn", return_value=None):
        result = get_or_create_session("+10000000001", "user")
    assert result is None
    print("✓ get_or_create_session returns None when no DB")


# ---------------------------------------------------------------------------
# get_or_create_session — creates new session
# ---------------------------------------------------------------------------

def test_creates_new_session_when_none_exists():
    cur = _make_cursor(
        fetchone_result=(42,),  # RETURNING id from INSERT
    )
    # First SELECT returns nothing (no existing session)
    cur.fetchone.side_effect = [None, (42,)]
    conn = _make_conn(cur)

    with patch("app.analytics.session_manager._get_conn", return_value=conn):
        session_id = get_or_create_session("+10000000002", "user")

    assert session_id == 42
    conn.close.assert_called_once()
    print("✓ get_or_create_session creates a new session when none exists")


# ---------------------------------------------------------------------------
# get_or_create_session — resumes active session
# ---------------------------------------------------------------------------

def test_resumes_active_session_within_gap():
    now = datetime.now(timezone.utc)
    recent = now - timedelta(hours=1)  # well within SESSION_GAP_HOURS

    cur = _make_cursor()
    cur.fetchone.side_effect = [(99, recent)]  # existing session, recent activity
    conn = _make_conn(cur)

    with patch("app.analytics.session_manager._get_conn", return_value=conn):
        session_id = get_or_create_session("+10000000003", "user")

    assert session_id == 99
    # Should have called UPDATE (bump counter), not INSERT
    executed_sqls = [str(c.args[0]).strip().upper() for c in cur.execute.call_args_list]
    assert any("UPDATE" in sql for sql in executed_sqls)
    assert not any("INSERT" in sql for sql in executed_sqls)
    print("✓ get_or_create_session resumes active session within gap")


# ---------------------------------------------------------------------------
# get_or_create_session — gap exceeded → closes old, creates new
# ---------------------------------------------------------------------------

def test_closes_old_session_and_creates_new_after_gap():
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(hours=SESSION_GAP_HOURS + 2)

    cur = _make_cursor()
    # First fetchone: old stale session; Second fetchone: new session id from INSERT
    cur.fetchone.side_effect = [(55, stale_time), (88,)]
    conn = _make_conn(cur)

    with patch("app.analytics.session_manager._get_conn", return_value=conn):
        session_id = get_or_create_session("+10000000004", "user")

    assert session_id == 88
    executed_sqls = [str(c.args[0]) for c in cur.execute.call_args_list]
    # Should UPDATE (close old), then INSERT (create new)
    assert any("user_silence" in sql for sql in executed_sqls)
    assert any("INSERT" in sql.upper() for sql in executed_sqls)
    print("✓ get_or_create_session closes stale session and creates new after gap")


# ---------------------------------------------------------------------------
# get_or_create_session — exception handling
# ---------------------------------------------------------------------------

def test_get_or_create_session_swallows_exceptions():
    cur = _make_cursor()
    cur.execute.side_effect = RuntimeError("DB exploded")
    conn = _make_conn(cur)

    with patch("app.analytics.session_manager._get_conn", return_value=conn):
        result = get_or_create_session("+10000000005", "user")

    assert result is None  # exception swallowed, not propagated
    conn.close.assert_called_once()
    print("✓ get_or_create_session swallows exceptions and returns None")


# ---------------------------------------------------------------------------
# close_stale_sessions
# ---------------------------------------------------------------------------

def test_close_stale_sessions_returns_rowcount():
    cur = _make_cursor(rowcount=7)
    conn = _make_conn(cur)

    with patch("app.analytics.session_manager._get_conn", return_value=conn):
        count = close_stale_sessions(silence_hours=24)

    assert count == 7
    # Should have run an UPDATE
    executed_sqls = [str(c.args[0]) for c in cur.execute.call_args_list]
    assert any("user_silence" in sql for sql in executed_sqls)
    print("✓ close_stale_sessions returns correct rowcount")


def test_close_stale_sessions_returns_zero_when_no_db():
    with patch("app.analytics.session_manager._get_conn", return_value=None):
        count = close_stale_sessions()
    assert count == 0
    print("✓ close_stale_sessions returns 0 when no DB")


# ---------------------------------------------------------------------------
# backfill_came_back_within_7d
# ---------------------------------------------------------------------------

def test_backfill_came_back_within_7d_returns_rowcount():
    cur = _make_cursor(rowcount=3)
    conn = _make_conn(cur)

    with patch("app.analytics.session_manager._get_conn", return_value=conn):
        count = backfill_came_back_within_7d()

    assert count == 3
    print("✓ backfill_came_back_within_7d returns correct rowcount")


def test_backfill_came_back_within_7d_returns_zero_when_no_db():
    with patch("app.analytics.session_manager._get_conn", return_value=None):
        count = backfill_came_back_within_7d()
    assert count == 0
    print("✓ backfill_came_back_within_7d returns 0 when no DB")


# ---------------------------------------------------------------------------
# handler integration — verify session calls are submitted to executor
# ---------------------------------------------------------------------------

def test_handler_submits_session_tracking():
    """Verify that handle_incoming_message submits session calls to the executor."""
    from tests.gemini_test_util import ensure_placeholder_key_for_import
    ensure_placeholder_key_for_import()

    from app.storage.memory import InMemoryStorage
    from app.brain.handler import ZarnaBrain
    from app.brain.intent import Intent

    mock_retriever = MagicMock()
    mock_retriever.get_relevant_chunks.return_value = []

    storage = InMemoryStorage()
    storage.save_contact("+10000000006")

    submitted_fns = []

    def fake_submit(fn, *args, **kwargs):
        submitted_fns.append((fn, args, kwargs))
        f = MagicMock()
        # Return sensible defaults so the handler doesn't crash
        name = getattr(fn, "__name__", "")
        if name == "classify_intent":
            f.result.return_value = Intent.GENERAL
        elif name == "get_relevant_chunks":
            f.result.return_value = []
        elif name == "classify_routing_tier":
            f.result.return_value = "low"
        else:
            f.result.return_value = None
        return f

    with patch("app.brain.handler._executor") as mock_exec, \
         patch("app.brain.handler.classify_tone_mode", return_value="playful"), \
         patch("app.brain.handler.generate_zarna_reply", return_value="Hey!"), \
         patch("app.brain.handler.try_router_skip_safe", return_value=True), \
         patch("app.brain.handler._fast_classify", return_value=None):

        mock_exec.submit.side_effect = fake_submit

        brain = ZarnaBrain(storage=storage, retriever=mock_retriever)
        brain.handle_incoming_message("+10000000006", "hello there")

    # get_or_create_session should have been submitted at least twice (user + assistant)
    session_submits = [
        (fn, args) for fn, args, _ in submitted_fns
        if getattr(fn, "__name__", "") == "get_or_create_session"
    ]
    assert len(session_submits) >= 2, f"Expected >=2 session submits, got {len(session_submits)}"
    roles = [args[1] for _, args in session_submits]
    assert "user" in roles
    assert "assistant" in roles
    print("✓ handler submits get_or_create_session for both user and assistant turns")


if __name__ == "__main__":
    test_get_or_create_session_returns_none_when_no_db()
    test_creates_new_session_when_none_exists()
    test_resumes_active_session_within_gap()
    test_closes_old_session_and_creates_new_after_gap()
    test_get_or_create_session_swallows_exceptions()
    test_close_stale_sessions_returns_rowcount()
    test_close_stale_sessions_returns_zero_when_no_db()
    test_backfill_came_back_within_7d_returns_rowcount()
    test_backfill_came_back_within_7d_returns_zero_when_no_db()
    test_handler_submits_session_tracking()
    print("\n✅ All Phase 3 session manager tests passed.")
