"""Tests for the /verify/signup endpoint (app/verify.py).

Covers phone normalization, the auth contract, input validation, rate limiting,
and the subscribed/unsubscribed query paths — all without a live database.
"""

from contextlib import contextmanager

import pytest
from flask import Flask

from app import verify


# --------------------------------------------------------------------------- #
# Phone normalization
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "raw,expected",
    [
        # US / Canada bare-number convenience (no country code supplied)
        ("6466406086", "+16466406086"),
        ("(646) 640-6086", "+16466406086"),
        ("646-640-6086", "+16466406086"),
        ("646.640.6086", "+16466406086"),
        ("16466406086", "+16466406086"),
        ("+1 (646) 640-6086", "+16466406086"),
        ("  6466406086  ", "+16466406086"),
        # International — leading "+" (what the VIP country picker always sends)
        ("+44 20 7946 0958", "+442079460958"),
        ("+91 98765 43210", "+919876543210"),
        ("+61 2 1234 5678", "+61212345678"),
        # International — "00" access prefix
        ("0044 20 7946 0958", "+442079460958"),
    ],
)
def test_to_e164_valid(raw, expected):
    assert verify._to_e164(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "", "   ", "abc", "12345",
        "011234567890",   # bare 12 digits, no + / 00 → not a NANP number
        "+123",           # too short to be a real E.164 number
        "+1234567890123456",  # 16 digits → longer than E.164 allows
    ],
)
def test_to_e164_invalid(raw):
    assert verify._to_e164(raw) is None


# --------------------------------------------------------------------------- #
# Fakes + fixtures
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, subscribed: bool):
        self._subscribed = subscribed

    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return (1,) if self._subscribed else None


class _FakeConn:
    def __init__(self, subscribed: bool):
        self._subscribed = subscribed
        self.closed = False

    @contextmanager
    def cursor(self, *_args, **_kwargs):
        yield _FakeCursor(self._subscribed)

    def close(self):
        self.closed = True


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(verify, "_VERIFY_SECRET", "test-secret")
    verify._volume_hits.clear()
    verify._last_alert_at = 0.0
    app = Flask(__name__)
    app.register_blueprint(verify.verify_bp)
    return app.test_client()


def _post(client, phone="6466406086", key="test-secret"):
    headers = {"X-Api-Key": key} if key is not None else {}
    return client.post("/verify/signup", json={"phone": phone}, headers=headers)


# --------------------------------------------------------------------------- #
# Endpoint behavior
# --------------------------------------------------------------------------- #
def test_missing_secret_returns_503(client, monkeypatch):
    monkeypatch.setattr(verify, "_VERIFY_SECRET", "")
    resp = _post(client)
    assert resp.status_code == 503
    assert resp.get_json()["error"] == "Misconfigured"


def test_wrong_key_returns_403(client):
    resp = _post(client, key="nope")
    assert resp.status_code == 403


def test_missing_key_returns_403(client):
    resp = _post(client, key=None)
    assert resp.status_code == 403


def test_missing_phone_returns_400(client):
    resp = client.post("/verify/signup", json={}, headers={"X-Api-Key": "test-secret"})
    assert resp.status_code == 400


def test_phone_not_in_query_string(client, monkeypatch):
    conn = _FakeConn(subscribed=True)
    monkeypatch.setattr(verify, "get_db_connection", lambda: conn)
    resp = client.post(
        "/verify/signup?phone=6466406086", headers={"X-Api-Key": "test-secret"}
    )
    assert resp.status_code == 400


def test_invalid_phone_returns_400(client):
    resp = _post(client, phone="12345")
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "invalid phone"


def test_no_database_returns_503(client, monkeypatch):
    monkeypatch.setattr(verify, "get_db_connection", lambda: None)
    resp = _post(client)
    assert resp.status_code == 503


def test_subscribed_true(client, monkeypatch):
    conn = _FakeConn(subscribed=True)
    monkeypatch.setattr(verify, "get_db_connection", lambda: conn)
    resp = _post(client)
    assert resp.status_code == 200
    assert resp.get_json() == {"subscribed": True}
    assert conn.closed is True


def test_subscribed_false(client, monkeypatch):
    conn = _FakeConn(subscribed=False)
    monkeypatch.setattr(verify, "get_db_connection", lambda: conn)
    resp = _post(client)
    assert resp.status_code == 200
    assert resp.get_json() == {"subscribed": False}


def test_high_volume_never_blocks_and_warns(client, monkeypatch):
    monkeypatch.setattr(verify, "get_db_connection", lambda: _FakeConn(subscribed=False))
    monkeypatch.setattr(verify, "_VOLUME_ALERT_PER_MIN", 5)
    warnings: list = []
    monkeypatch.setattr(verify.logger, "warning", lambda *a, **k: warnings.append(a))
    statuses = [_post(client).status_code for _ in range(verify._VOLUME_ALERT_PER_MIN + 3)]
    assert all(s == 200 for s in statuses)  # never blocked
    assert len(warnings) >= 1  # spike was flagged
