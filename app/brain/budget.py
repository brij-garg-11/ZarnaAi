"""
Global AI spend kill-switch.

Born from the Nov 2026 incident: zombie ElevenLabs voice sessions called the
brain around the clock for ~2 weeks and ran up ~$8,000 of Gemini usage that
internal cost tracking couldn't see. Every paid AI call now passes through
``guard_ai_call()`` first, which blocks the call once any of three independent
limits is hit:

  1. ``AI_MAX_CALLS_PER_HOUR``  — in-process sliding window (default 1200).
     Catches runaway loops fast, with no DB dependency.
  2. ``AI_MAX_CALLS_PER_DAY``   — in-process sliding window (default 8000).
     Catches slower leaks that stay under the hourly radar.
  3. ``AI_DAILY_BUDGET_USD``    — today's total from the shared ai_cost_log
     table in the operator DB (default $30, cached 60s, fail-open on DB
     errors). Catches aggregate spend across services where cost is logged.

Set any limit to 0 to disable that layer. Defaults are ~5-10x normal daily
traffic (so legit blast days never trip) and far below runaway rates.

When a limit trips: every subsequent AI call raises ``AIBudgetExceeded``
(callers' existing error handling turns that into a safe fallback / no reply),
a CRITICAL log line is emitted, and one alert row is written to the operator
DB so it shows up in the dashboard. Fans getting silence is the accepted
trade against another four-figure surprise bill.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque

log = logging.getLogger(__name__)

_WINDOW_HOUR = 3600.0
_WINDOW_DAY = 86400.0
_COST_CACHE_TTL = 60.0


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        log.warning("[AI_BUDGET] invalid %s — using default %s", name, default)
        return default


class AIBudgetExceeded(RuntimeError):
    """Raised instead of making a paid AI call once a spend limit is hit."""


class _Guard:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls_hour: deque[float] = deque()
        self._calls_day: deque[float] = deque()
        self._cost_cache: tuple[float, float] | None = None  # (checked_at, usd)
        self._tripped_reason: str | None = None

    # -- limits are read per call so tests (and env flips) take effect live --
    @property
    def max_per_hour(self) -> float:
        return _env_float("AI_MAX_CALLS_PER_HOUR", 1200)

    @property
    def max_per_day(self) -> float:
        return _env_float("AI_MAX_CALLS_PER_DAY", 8000)

    @property
    def daily_budget_usd(self) -> float:
        return _env_float("AI_DAILY_BUDGET_USD", 30.0)

    def reset(self) -> None:
        """Test hook — clears windows, caches, and the tripped flag."""
        with self._lock:
            self._calls_hour.clear()
            self._calls_day.clear()
            self._cost_cache = None
            self._tripped_reason = None

    def _todays_logged_cost(self) -> float:
        """Today's summed cost_usd from ai_cost_log (cached, fail-open)."""
        now = time.monotonic()
        if self._cost_cache and now - self._cost_cache[0] < _COST_CACHE_TTL:
            return self._cost_cache[1]
        dsn = (os.getenv("OPERATOR_DATABASE_URL") or "").strip()
        if not dsn:
            self._cost_cache = (now, 0.0)
            return 0.0
        try:
            import psycopg2

            dsn = dsn.replace("postgres://", "postgresql://", 1)
            with psycopg2.connect(dsn, connect_timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(SUM(cost_usd), 0) FROM ai_cost_log"
                        " WHERE log_date = CURRENT_DATE"
                    )
                    usd = float(cur.fetchone()[0] or 0.0)
        except Exception as exc:  # fail-open: never kill replies over a DB blip
            log.warning("[AI_BUDGET] cost lookup failed (%s) — assuming $0", exc)
            usd = 0.0
        self._cost_cache = (now, usd)
        return usd

    def _blocked_reason(self) -> str | None:
        now = time.monotonic()
        while self._calls_hour and now - self._calls_hour[0] > _WINDOW_HOUR:
            self._calls_hour.popleft()
        while self._calls_day and now - self._calls_day[0] > _WINDOW_DAY:
            self._calls_day.popleft()

        if self.max_per_hour > 0 and len(self._calls_hour) >= self.max_per_hour:
            return (
                f"hourly AI call limit reached ({len(self._calls_hour)}"
                f"/{self.max_per_hour:g} calls in the last hour)"
            )
        if self.max_per_day > 0 and len(self._calls_day) >= self.max_per_day:
            return (
                f"daily AI call limit reached ({len(self._calls_day)}"
                f"/{self.max_per_day:g} calls in the last 24h)"
            )
        return None

    def check(self, kind: str) -> None:
        """Record one intended AI call; raise AIBudgetExceeded when over limit."""
        with self._lock:
            reason = self._blocked_reason()
            if reason is None and self.daily_budget_usd > 0:
                usd = self._todays_logged_cost()
                if usd >= self.daily_budget_usd:
                    reason = (
                        f"daily AI budget reached (${usd:.2f}"
                        f"/${self.daily_budget_usd:g} logged today)"
                    )
            if reason is None:
                now = time.monotonic()
                self._calls_hour.append(now)
                self._calls_day.append(now)
                if self._tripped_reason is not None:
                    log.warning("[AI_BUDGET] limits back under threshold — resuming")
                    self._tripped_reason = None
                return

            first_trip = self._tripped_reason is None
            self._tripped_reason = reason

        # Outside the lock: logging + alert I/O must never serialize callers.
        if first_trip:
            log.critical(
                "[AI_BUDGET] KILL-SWITCH TRIPPED — blocking all AI calls: %s "
                "(raise the AI_* env limits or wait for the window to clear)",
                reason,
            )
            try:
                from app.alert_writer import write_alert

                write_alert(
                    creator_slug=(os.getenv("CREATOR_SLUG") or "zarna"),
                    alert_type="ai_budget_exceeded",
                    severity="error",
                    title="AI replies paused — usage limit reached",
                    summary=(
                        "AI usage hit the configured safety limit, so replies "
                        "are paused to prevent a runaway bill. They resume "
                        "automatically when usage drops back under the limit."
                    ),
                    detail=reason,
                )
            except Exception:
                log.exception("[AI_BUDGET] failed to write budget alert")
        else:
            log.warning("[AI_BUDGET] blocked %s call: %s", kind, reason)
        raise AIBudgetExceeded(reason)


_GUARD = _Guard()


def guard_ai_call(kind: str = "generate") -> None:
    """Call before any paid AI request. Raises AIBudgetExceeded when capped."""
    _GUARD.check(kind)


def reset_budget_guard() -> None:
    """Test hook — never needed in production."""
    _GUARD.reset()
