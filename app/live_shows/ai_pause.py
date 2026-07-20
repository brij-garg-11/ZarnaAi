"""
AI pause (human takeover) — lets the operator hold a real conversation with a
fan without the bot replying on top of them.

When the operator pauses a fan (explicitly, or automatically by sending a manual
message from the inbox), a row is written to ai_paused_fans. On every inbound
fan message the webhook calls is_ai_paused(): if paused, the inbound is still
logged to the inbox but NO AI reply is generated. Pauses have no TTL — they stay
until the operator hits Resume (deletes the row).

Multi-tenant note: every row is scoped by creator_slug, so pausing a Zarna fan
never affects another creator's fan with the same number. Callers MUST pass the
resolved creator_slug.

This module is the main app's READ side. The operator service owns the write
side (operator/app/ai_pause.py). Both share DATABASE_URL.
"""

from __future__ import annotations

import logging
import os

_logger = logging.getLogger(__name__)


def _conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def is_ai_paused(phone_number: str, creator_slug: str) -> bool:
    """
    True if the operator has paused AI replies for this fan.

    Fails open (returns False → AI replies as normal) on any error, so a DB
    hiccup can never silently take the bot offline for every fan.
    """
    if not phone_number or not creator_slug:
        return False
    c = _conn()
    if not c:
        return False
    try:
        with c.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM ai_paused_fans "
                "WHERE phone_number = %s AND creator_slug = %s LIMIT 1",
                (phone_number, creator_slug),
            )
            return cur.fetchone() is not None
    except Exception:
        _logger.exception("is_ai_paused failed — treating fan as not paused")
        return False
    finally:
        c.close()
