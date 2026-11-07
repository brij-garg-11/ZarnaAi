"""Tests for the global AI spend kill-switch (app.brain.budget)."""

import pytest

from app.brain import budget
from app.brain.budget import AIBudgetExceeded, guard_ai_call, reset_budget_guard


@pytest.fixture(autouse=True)
def _clean_guard(monkeypatch):
    # Disable the DB-backed dollar budget by default; tests opt in explicitly.
    monkeypatch.setenv("AI_DAILY_BUDGET_USD", "0")
    monkeypatch.setenv("AI_MAX_CALLS_PER_HOUR", "0")
    monkeypatch.setenv("AI_MAX_CALLS_PER_DAY", "0")
    reset_budget_guard()
    yield
    reset_budget_guard()


def test_unlimited_when_all_layers_disabled():
    for _ in range(50):
        guard_ai_call("generate")  # must not raise


def test_hourly_cap_trips(monkeypatch):
    monkeypatch.setenv("AI_MAX_CALLS_PER_HOUR", "5")
    for _ in range(5):
        guard_ai_call("generate")
    with pytest.raises(AIBudgetExceeded, match="hourly"):
        guard_ai_call("generate")


def test_daily_cap_trips(monkeypatch):
    monkeypatch.setenv("AI_MAX_CALLS_PER_DAY", "3")
    for _ in range(3):
        guard_ai_call("embedding")
    with pytest.raises(AIBudgetExceeded, match="daily AI call limit"):
        guard_ai_call("embedding")


def test_recovers_when_window_clears(monkeypatch):
    monkeypatch.setenv("AI_MAX_CALLS_PER_HOUR", "2")
    guard_ai_call("generate")
    guard_ai_call("generate")
    with pytest.raises(AIBudgetExceeded):
        guard_ai_call("generate")
    # Simulate the hour passing by aging the recorded timestamps.
    with budget._GUARD._lock:
        aged = [t - 3601 for t in budget._GUARD._calls_hour]
        budget._GUARD._calls_hour.clear()
        budget._GUARD._calls_hour.extend(aged)
    guard_ai_call("generate")  # must not raise


def test_dollar_budget_trips(monkeypatch):
    monkeypatch.setenv("AI_DAILY_BUDGET_USD", "30")
    monkeypatch.setattr(budget._GUARD, "_todays_logged_cost", lambda: 31.50)
    with pytest.raises(AIBudgetExceeded, match=r"daily AI budget"):
        guard_ai_call("generate")


def test_dollar_budget_under_limit_allows(monkeypatch):
    monkeypatch.setenv("AI_DAILY_BUDGET_USD", "30")
    monkeypatch.setattr(budget._GUARD, "_todays_logged_cost", lambda: 4.20)
    guard_ai_call("generate")  # must not raise


def test_db_failure_fails_open(monkeypatch):
    monkeypatch.setenv("AI_DAILY_BUDGET_USD", "30")
    monkeypatch.setenv("OPERATOR_DATABASE_URL", "postgresql://nope:1/x")
    budget._GUARD._cost_cache = None
    guard_ai_call("generate")  # lookup fails -> $0 assumed -> must not raise


def test_trip_writes_one_alert(monkeypatch):
    calls = []
    monkeypatch.setenv("AI_MAX_CALLS_PER_HOUR", "1")
    import app.alert_writer as aw

    monkeypatch.setattr(aw, "write_alert", lambda **kw: calls.append(kw))
    guard_ai_call("generate")
    for _ in range(3):
        with pytest.raises(AIBudgetExceeded):
            guard_ai_call("generate")
    assert len(calls) == 1
    assert calls[0]["alert_type"] == "ai_budget_exceeded"


def test_generate_with_thinking_blocks_before_api_call(monkeypatch):
    monkeypatch.setenv("AI_MAX_CALLS_PER_HOUR", "1")
    from app.brain.thinking import generate_with_thinking

    class _Boom:
        class models:
            @staticmethod
            def generate_content(**kw):
                raise AssertionError("client must not be called when capped")

    guard_ai_call("generate")  # consume the single allowed call
    with pytest.raises(AIBudgetExceeded):
        generate_with_thinking(_Boom(), "gemini-flash-latest", "hi", None)


def test_stream_yields_nothing_when_capped(monkeypatch):
    monkeypatch.setenv("AI_MAX_CALLS_PER_HOUR", "1")
    from app.brain.generator import generate_zarna_reply_stream
    from app.brain.intent import Intent

    guard_ai_call("generate")  # consume the single allowed call
    out = list(
        generate_zarna_reply_stream(
            intent=Intent.GENERAL, user_message="hello", chunks=[]
        )
    )
    assert out == []
