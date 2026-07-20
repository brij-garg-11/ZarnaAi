"""
AI pause (human takeover) — operator write side.

Pausing a fan writes a row to ai_paused_fans; the main app's inbound webhook
reads it and, while present, logs the fan's messages to the inbox but skips the
AI reply so the operator can hold a real conversation. Pauses have no TTL — they
stay until resume_ai() deletes the row.

Every row is scoped by creator_slug so pausing one creator's fan can never
affect another creator's fan with the same number.

The table itself is created by init_db() in operator/app/db.py (and mirrored in
the main app's migrations).
"""

import logging

from .db import get_conn

logger = logging.getLogger(__name__)


def pause_ai(phone_number: str, creator_slug: str, paused_by: str = "") -> None:
    """Pause AI replies for a fan (idempotent)."""
    if not phone_number or not creator_slug:
        return
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_paused_fans (phone_number, creator_slug, paused_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (phone_number, creator_slug) DO NOTHING
                    """,
                    (phone_number, creator_slug, paused_by or ""),
                )
    finally:
        conn.close()


def resume_ai(phone_number: str, creator_slug: str) -> None:
    """Resume AI replies for a fan (idempotent)."""
    if not phone_number or not creator_slug:
        return
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ai_paused_fans "
                    "WHERE phone_number = %s AND creator_slug = %s",
                    (phone_number, creator_slug),
                )
    finally:
        conn.close()


def is_ai_paused(phone_number: str, creator_slug: str) -> bool:
    """True if AI replies are currently paused for this fan."""
    if not phone_number or not creator_slug:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM ai_paused_fans "
                "WHERE phone_number = %s AND creator_slug = %s LIMIT 1",
                (phone_number, creator_slug),
            )
            return cur.fetchone() is not None
    except Exception:
        logger.exception("is_ai_paused failed for slug=%s", creator_slug)
        return False
    finally:
        conn.close()
