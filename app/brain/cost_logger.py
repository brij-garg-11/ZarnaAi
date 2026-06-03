"""
Fire-and-forget AI cost logger.

Writes one row per LLM call to the operator DB (OPERATOR_DATABASE_URL) so
the per-client P&L view has accurate AI spend data. Never raises — a DB
failure here must never affect the SMS reply pipeline.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import date

log = logging.getLogger(__name__)

_OPERATOR_DB_URL = os.getenv("OPERATOR_DATABASE_URL", "")


def log_ai_cost(
    creator_slug: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
) -> None:
    """Queue a cost-log insert in a background thread. Returns immediately."""
    if not _OPERATOR_DB_URL:
        log.debug("[AI_COST] OPERATOR_DATABASE_URL not set — skipping cost log")
        return
    threading.Thread(
        target=_log_ai_cost_sync,
        args=(creator_slug, model, input_tokens, output_tokens, cost_usd),
        daemon=True,
    ).start()


def _log_ai_cost_sync(
    creator_slug: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
) -> None:
    try:
        import psycopg2

        dsn = _OPERATOR_DB_URL.replace("postgres://", "postgresql://", 1)
        today = date.today().isoformat()
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_cost_log
                        (creator_slug, log_date, model, input_tokens, output_tokens, cost_usd)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (creator_slug, today, model, input_tokens, output_tokens, cost_usd),
                )
        log.debug(
            "[AI_COST] logged %.6f USD for slug=%s model=%s in=%d out=%d",
            cost_usd, creator_slug, model, input_tokens, output_tokens,
        )
    except Exception as exc:
        log.warning("[AI_COST] failed to log AI cost: %s", exc)
