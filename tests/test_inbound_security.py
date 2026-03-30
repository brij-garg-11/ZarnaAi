"""Tests for inbound security helpers (no Flask context required)."""

from app.inbound_security import running_in_production, timing_safe_equal


def test_timing_safe_equal():
    assert timing_safe_equal("abc", "abc") is True
    assert timing_safe_equal("abc", "abz") is False
    assert timing_safe_equal("", "a") is False
    assert timing_safe_equal("a", "") is False


def test_running_in_production_monkeypatch(monkeypatch):
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("PRODUCTION", raising=False)
    assert running_in_production() is False

    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    assert running_in_production() is True

    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "production")
    assert running_in_production() is True
