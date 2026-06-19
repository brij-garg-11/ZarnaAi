"""
Tests for live-show blast provider defaulting and Twilio high-throughput sending.

Covers:
  - resolve_broadcast_provider(): auto now prefers Twilio, falls back to SlickText
    only when Twilio is unconfigured; explicit overrides still win.
  - _broadcast_mps(): env parsing + clamping.
  - _RateLimiter: paces grants to the target rate across threads.
  - run_loop_broadcast(provider="twilio"): concurrent sends, per-success hook,
    accurate counts — without touching the real Twilio API.
"""
import time

import app.messaging.broadcast as bc
from app.messaging.broadcast import (
    _RateLimiter,
    _broadcast_mps,
    resolve_broadcast_provider,
    run_loop_broadcast,
)


# ---------------------------------------------------------------------------
# Provider defaulting
# ---------------------------------------------------------------------------

def _set_twilio(monkeypatch, sid="AC123", token="tok", phone="+18556081717", svc=""):
    monkeypatch.setattr("app.config.TWILIO_ACCOUNT_SID", sid, raising=False)
    monkeypatch.setattr("app.config.TWILIO_AUTH_TOKEN", token, raising=False)
    monkeypatch.setattr("app.config.TWILIO_PHONE_NUMBER", phone, raising=False)
    monkeypatch.setattr("app.config.TWILIO_MESSAGING_SERVICE_SID", svc, raising=False)


def _set_slicktext(monkeypatch, on=True):
    pub = "pub" if on else ""
    priv = "priv" if on else ""
    monkeypatch.setattr("app.config.SLICKTEXT_PUBLIC_KEY", pub, raising=False)
    monkeypatch.setattr("app.config.SLICKTEXT_PRIVATE_KEY", priv, raising=False)
    monkeypatch.setattr("app.config.SLICKTEXT_API_KEY", "", raising=False)
    monkeypatch.setattr("app.config.SLICKTEXT_BRAND_ID", "", raising=False)


def test_auto_prefers_twilio_when_configured(monkeypatch):
    monkeypatch.delenv("LIVE_SHOW_BROADCAST_PROVIDER", raising=False)
    _set_twilio(monkeypatch)
    _set_slicktext(monkeypatch, on=True)  # even with SlickText present, Twilio wins
    assert resolve_broadcast_provider() == "twilio"


def test_auto_falls_back_to_slicktext_when_twilio_unconfigured(monkeypatch):
    monkeypatch.delenv("LIVE_SHOW_BROADCAST_PROVIDER", raising=False)
    _set_twilio(monkeypatch, sid="", token="", phone="", svc="")
    _set_slicktext(monkeypatch, on=True)
    assert resolve_broadcast_provider() == "slicktext"


def test_explicit_slicktext_override_wins(monkeypatch):
    monkeypatch.setenv("LIVE_SHOW_BROADCAST_PROVIDER", "slicktext")
    _set_twilio(monkeypatch)
    assert resolve_broadcast_provider() == "slicktext"


def test_explicit_twilio_override_wins(monkeypatch):
    monkeypatch.setenv("LIVE_SHOW_BROADCAST_PROVIDER", "twilio")
    _set_twilio(monkeypatch, sid="", token="")
    assert resolve_broadcast_provider() == "twilio"


# ---------------------------------------------------------------------------
# MPS config + rate limiter
# ---------------------------------------------------------------------------

def test_broadcast_mps_default(monkeypatch):
    monkeypatch.delenv("TWILIO_BROADCAST_MPS", raising=False)
    assert _broadcast_mps() == 25


def test_broadcast_mps_parses_and_clamps(monkeypatch):
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "10")
    assert _broadcast_mps() == 10
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "0")
    assert _broadcast_mps() == 1
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "99999")
    assert _broadcast_mps() == 100
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "garbage")
    assert _broadcast_mps() == 25


def test_rate_limiter_paces_grants():
    rate = 200.0  # 5ms spacing — keeps the test fast but measurable
    limiter = _RateLimiter(rate)
    start = time.monotonic()
    for _ in range(10):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 10 grants spaced by 1/200s => at least ~9 intervals of wait.
    assert elapsed >= 9 / rate * 0.8


def test_rate_limiter_zero_rate_is_noop():
    limiter = _RateLimiter(0)
    start = time.monotonic()
    for _ in range(100):
        limiter.acquire()
    assert (time.monotonic() - start) < 0.1


# ---------------------------------------------------------------------------
# Concurrent Twilio broadcast
# ---------------------------------------------------------------------------

def test_twilio_broadcast_sends_all_and_hooks_each_success(monkeypatch):
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "50")
    monkeypatch.setattr(bc, "_twilio_client_or_none", lambda: object())

    sent_to = []
    import threading
    lock = threading.Lock()

    def fake_send(_client, phone, _body, _wa):
        with lock:
            sent_to.append(phone)
        return True

    monkeypatch.setattr(bc, "_twilio_send_with_client", fake_send)

    phones = [f"+1555000{i:04d}" for i in range(20)]
    succeeded_hook = []
    res = run_loop_broadcast(
        phones=phones,
        body="Live now!",
        provider="twilio",
        deliver_whatsapp=False,
        slicktext_send=lambda *_: True,
        on_success=lambda p: succeeded_hook.append(p),
    )
    assert res.attempted == 20
    assert res.succeeded == 20
    assert res.failed == 0
    assert sorted(sent_to) == sorted(phones)
    assert sorted(succeeded_hook) == sorted(phones)


def test_twilio_broadcast_counts_failures(monkeypatch):
    monkeypatch.setenv("TWILIO_BROADCAST_MPS", "50")
    monkeypatch.setattr(bc, "_twilio_client_or_none", lambda: object())

    def fake_send(_client, phone, _body, _wa):
        # Fail the odd-indexed numbers.
        return phone.endswith(("0", "2", "4", "6", "8"))

    monkeypatch.setattr(bc, "_twilio_send_with_client", fake_send)

    phones = [f"+1555000000{i}" for i in range(10)]
    hooked = []
    res = run_loop_broadcast(
        phones=phones,
        body="hi",
        provider="twilio",
        deliver_whatsapp=False,
        slicktext_send=lambda *_: True,
        on_success=lambda p: hooked.append(p),
    )
    assert res.attempted == 10
    assert res.succeeded == 5
    assert res.failed == 5
    assert len(hooked) == 5  # hook only fires for successes


def test_twilio_broadcast_without_client_marks_all_failed(monkeypatch):
    monkeypatch.setattr(bc, "_twilio_client_or_none", lambda: None)
    res = run_loop_broadcast(
        phones=["+15551112222"],
        body="hi",
        provider="twilio",
        deliver_whatsapp=False,
        slicktext_send=lambda *_: True,
    )
    assert res.succeeded == 0
    assert res.failed == 1
    assert res.errors
