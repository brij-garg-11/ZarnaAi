"""
Tests for the concurrent Twilio blast runner (app/blast_sender).

Twilio is stubbed (see conftest sys.modules stubs) — these tests verify the
pacing, bookkeeping, and cancellation semantics of the thread-pool runner,
not the Twilio API itself.
"""

import sys
import threading
import time
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def fake_twilio(monkeypatch):
    """Install a fake twilio.rest.Client that records send timestamps."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtest")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "token")
    monkeypatch.setenv("TWILIO_MESSAGING_SERVICE_SID", "MGtest")

    sent_log = []
    lock = threading.Lock()

    def fake_create(**kwargs):
        with lock:
            sent_log.append((time.monotonic(), kwargs["to"]))
        m = MagicMock()
        m.sid = "SMtest"
        return m

    client = MagicMock()
    client.messages.create.side_effect = fake_create
    sys.modules["twilio.rest"].Client = MagicMock(return_value=client)
    return sent_log


def _run(phones, *, should_stop=None, mps="25", monkeypatch=None, hooks=None):
    from app.blast_sender import _run_concurrent_twilio_blast

    hooks = hooks if hooks is not None else {"success": [], "progress": []}
    return _run_concurrent_twilio_blast(
        draft_id=1,
        phones=phones,
        build_fan_body=lambda p: f"hello {p}",
        media_url="",
        after_success=lambda p, b: hooks["success"].append(p),
        should_stop=should_stop or (lambda s, f: False),
        mark_progress=lambda d, s, f: hooks["progress"].append((s, f)),
    ), hooks


def test_concurrent_blast_sends_all_at_capped_rate(fake_twilio, monkeypatch):
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "25")
    phones = [f"+1555000{i:04d}" for i in range(100)]

    t0 = time.monotonic()
    (sent, failed, sent_phones), hooks = _run(phones)
    elapsed = time.monotonic() - t0

    assert sent == 100
    assert failed == 0
    assert sorted(sent_phones) == sorted(phones)
    assert len(hooks["success"]) == 100

    # 100 msgs at 25 MPS should take ~4s. Generous upper bound for slow CI.
    assert elapsed >= 3.5, f"finished too fast ({elapsed:.2f}s) — rate cap not applied"
    assert elapsed <= 10.0, f"too slow ({elapsed:.2f}s)"

    # No 1-second window may exceed the MPS cap (±2 jitter from thread timing).
    times = sorted(t for t, _ in fake_twilio)
    peak = max(sum(1 for t in times if w <= t < w + 1.0) for w in times)
    assert peak <= 27, f"peak rate {peak} exceeded 25 MPS cap"


def test_concurrent_blast_stops_at_checkpoint_on_cancel(fake_twilio, monkeypatch):
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "100")
    phones = [f"+1555000{i:04d}" for i in range(200)]

    # should_stop fires at the first checkpoint (50 done) — remaining
    # recipients must be skipped.
    (sent, failed, sent_phones), _ = _run(phones, should_stop=lambda s, f: True)

    assert sent < 200
    assert sent >= 50  # everything before the checkpoint was already in flight


def test_concurrent_blast_counts_failures(fake_twilio, monkeypatch):
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "100")

    client = MagicMock()
    client.messages.create.side_effect = Exception("boom")
    sys.modules["twilio.rest"].Client = MagicMock(return_value=client)

    phones = [f"+1555000{i:04d}" for i in range(10)]
    (sent, failed, sent_phones), hooks = _run(phones)

    assert sent == 0
    assert failed == 10
    assert sent_phones == []
    assert hooks["success"] == []
