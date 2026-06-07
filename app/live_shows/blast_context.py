"""
Blast context sessions — soft AI context injection for post-blast fan replies.

When an operator sends a blast with a context note, a blast_context_sessions row
is created in the operator DB. On every inbound fan message within the 24h window,
get_active_blast_context() returns the most recent active note so it can be injected
softly into the AI prompt as background framing (not an override — the AI just knows
what the blast was about and can respond more intelligently).

Multi-tenant note
-----------------
Every active row is tagged with the creator_slug of the tenant that sent the
blast. Callers MUST pass creator_slug so a WSCC fan's reply cannot pick up
Zarna's context (and vice-versa). The unscoped path is kept for backward
compat but emits a WARNING so stray callers show up in Railway logs.
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


def get_active_blast_context(creator_slug: Optional[str] = None) -> Optional[str]:
    """
    Return the context_note from the most recent active blast_context_sessions row.

    Active = not yet expired (or no expiry set).

    creator_slug: when provided, restrict the lookup to that tenant. This is
    the correct call path for any production webhook. The unscoped branch is
    only kept so older code paths don't crash — it is NEVER safe in a
    multi-tenant deployment because it returns whichever row is globally
    most recent.
    """
    c = _conn()
    if not c:
        return None
    try:
        with c.cursor() as cur:
            # "Active" = explicit expiry still in the future, OR (no expiry set)
            # created within the documented 24h window. The created_at cap is the
            # safety net: a row written with a NULL expires_at must NOT stay active
            # forever — otherwise a single blast note keeps getting injected into
            # every fan's reply for days (it is creator-wide, not per-fan).
            _active = (
                "((expires_at IS NOT NULL AND expires_at > NOW()) "
                "OR (expires_at IS NULL AND created_at > NOW() - INTERVAL '24 hours'))"
            )
            if creator_slug:
                cur.execute(
                    f"""
                    SELECT context_note
                    FROM   blast_context_sessions
                    WHERE  {_active}
                      AND  creator_slug = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (creator_slug,),
                )
            else:
                _logger.warning(
                    "get_active_blast_context called without creator_slug — "
                    "this is cross-tenant unsafe and should only happen during "
                    "legacy code paths. Fix the caller to pass a slug."
                )
                cur.execute(
                    f"""
                    SELECT context_note
                    FROM   blast_context_sessions
                    WHERE  {_active}
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

    This is *background* framing, NOT an override. It helps the AI answer a fan
    who is reacting to the blast (e.g. a voting blast → give voting steps), but it
    must never hijack a message that is clearly about something else. Otherwise a
    stale/active blast note (it applies creator-wide, not per-fan) makes the bot
    drag the blast topic into every unrelated reply.
    """
    return (
        "BLAST CONTEXT — background only. The fan may have recently received a text about "
        "the topic below. ONLY if their current message is plausibly a reaction to it, use "
        "these details to answer specifically and helpfully:\n"
        f"{context_note.strip()}\n"
        "If their message is clearly about something else — a different question, tour/ticket "
        "dates, a personal share, or small talk — just answer what they actually said and do "
        "NOT bring up this topic, quote it, or imply you are 'watching' them. Never mention "
        "'blast', 'mass message', or 'text campaign'. Do not force this topic into the reply or "
        "keep circling back to it.\n"
    )
