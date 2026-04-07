"""
Blast context sessions — soft AI context injection for post-blast fan replies.

When an operator sends a blast with a context note, a blast_context_sessions row
is created in the operator DB. On every inbound fan message within the 24h window,
get_active_blast_context() returns the most recent active note so it can be injected
softly into the AI prompt as background framing (not an override — the AI just knows
what the blast was about and can respond more intelligently).
"""

from __future__ import annotations

import logging
import os
from typing import Optional

_logger = logging.getLogger(__name__)


def _conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def get_active_blast_context() -> Optional[str]:
    """
    Return the context_note from the most recent active blast_context_sessions row, or None.

    Active = not yet expired (or no expiry set).
    """
    c = _conn()
    if not c:
        return None
    try:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT context_note
                FROM   blast_context_sessions
                WHERE  (expires_at IS NULL OR expires_at > NOW())
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        _logger.exception("get_active_blast_context failed")
        return None
    finally:
        c.close()


def build_blast_context_prompt(context_note: str) -> str:
    """
    Build the soft context block injected into the AI prompt when a fan replies
    after a blast was sent with an operator context note.

    This is background framing only — it does not override intent routing.
    """
    return (
        f"BLAST CONTEXT — background only, do not reference that a blast was sent:\n"
        f"{context_note.strip()}\n"
        f"If the fan's message seems related to this topic, factor it in naturally. "
        f"Do not mention 'blast', 'mass message', or 'text campaign'. "
        f"Just respond as Zarna would, informed by this context.\n"
    )
