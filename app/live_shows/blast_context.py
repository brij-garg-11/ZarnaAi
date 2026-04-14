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
    Build the context block injected into the AI prompt when a fan replies
    after a blast was sent with an operator context note.

    This is high-priority framing — the AI should treat the fan's reply as
    being about the blast topic and respond accordingly.
    """
    return (
        f"BLAST CONTEXT — HIGH PRIORITY. The fan just received a text about this topic "
        f"and their reply is almost certainly related to it. Use this context to guide your response:\n"
        f"{context_note.strip()}\n"
        f"Treat the fan's message as being about this topic. If they ask about voting, "
        f"provide the specific voting instructions from the context above — do not give generic "
        f"or unrelated answers. Do not mention 'blast', 'mass message', or 'text campaign'. "
        f"Respond as Zarna would, but stay anchored to this context until the conversation "
        f"clearly moves on to a different topic.\n"
    )
