"""Tests for cross-worker inbound dedup (app/inbound_dedup.py).

The in-memory LRU path is tested without a database. The Postgres claim path
is exercised with a fake connection so no DATABASE_URL is required, plus a
simulated two-worker race.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

import app.inbound_dedup as dedup


@pytest.fixture(autouse=True)
def _reset():
    dedup.reset_for_tests()
    yield
    dedup.reset_for_tests()


class TestInMemoryPath:
    def test_empty_id_never_processed(self, monkeypatch):
        monkeypatch.setattr(dedup, "_get_conn", lambda: None)
        assert dedup.already_processed("") is False
        assert dedup.already_processed(None) is False

    def test_first_seen_false_second_true(self, monkeypatch):
        monkeypatch.setattr(dedup, "_get_conn", lambda: None)
        assert dedup.already_processed("SM123") is False
        assert dedup.already_processed("SM123") is True

    def test_lru_bounded(self, monkeypatch):
        monkeypatch.setattr(dedup, "_get_conn", lambda: None)
        for i in range(dedup._MAX_SEEN + 10):
            dedup.already_processed(f"id-{i}")
        assert len(dedup._seen_message_ids) <= dedup._MAX_SEEN

    def test_db_error_fails_open(self, monkeypatch):
        def boom():
            raise RuntimeError("db down")
        monkeypatch.setattr(dedup, "_get_conn", boom)
        # First sight: LRU records it, DB check explodes -> still processed once.
        assert dedup.already_processed("SM999") is False
        # Second sight caught by LRU without touching the DB.
        assert dedup.already_processed("SM999") is True


class _FakeCursor:
    """Minimal cursor emulating INSERT ... ON CONFLICT DO NOTHING semantics."""

    def __init__(self, store):
        self.store = store
        self.rowcount = 0

    def execute(self, sql, params=None):
        if sql.startswith("INSERT"):
            mid = params[0]
            if mid in self.store:
                self.rowcount = 0
            else:
                self.store.add(mid)
                self.rowcount = 1
        else:  # DDL / DELETE — no-op
            self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, store):
        self.store = store

    def cursor(self):
        return _FakeCursor(self.store)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class TestCrossWorkerPath:
    def test_two_workers_one_claim(self, monkeypatch):
        """Simulate two gunicorn workers: separate LRUs, shared claim table."""
        shared_db = set()
        monkeypatch.setattr(dedup, "_get_conn", lambda: _FakeConn(shared_db))
        monkeypatch.setattr(dedup, "_CLEANUP_PROBABILITY", 0)

        # Worker A processes the message.
        assert dedup.already_processed("SM_race") is False

        # Worker B has its own empty LRU (simulate by clearing this process's).
        dedup.reset_for_tests()
        # The shared DB still has the claim -> worker B must skip it.
        assert dedup.already_processed("SM_race") is True

    def test_distinct_ids_both_process(self, monkeypatch):
        shared_db = set()
        monkeypatch.setattr(dedup, "_get_conn", lambda: _FakeConn(shared_db))
        monkeypatch.setattr(dedup, "_CLEANUP_PROBABILITY", 0)
        assert dedup.already_processed("SM_a") is False
        assert dedup.already_processed("SM_b") is False


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="needs live DATABASE_URL")
class TestLiveDatabase:
    def test_claim_roundtrip(self):
        import uuid
        mid = f"test-dedup-{uuid.uuid4()}"
        assert dedup.already_processed(mid) is False
        dedup.reset_for_tests()  # wipe LRU: forces the DB path
        assert dedup.already_processed(mid) is True
        # Clean up the test row.
        conn = dedup._get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM processed_messages WHERE message_id = %s", (mid,))
        conn.close()
