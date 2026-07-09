"""
Crisis detection and deterministic response.

When a fan's message signals suicidal ideation or self-harm, the reply must
NOT depend on whatever an LLM decides to generate that day. This module gives
the pipeline a hard gate:

  1. ``check_crisis(message)`` — high-precision matcher. Deliberately narrow:
     it must never fire on comedy-club language ("dying laughing", "you kill
     me", "I'm dead 💀"), because a false positive turns a joke into a jarring
     crisis script. Broader emotional support (sad/anxious/grief) stays with
     the LLM under the ``sensitive_care`` tone mode.
  2. ``CRISIS_RESPONSE`` — the fixed reply sent verbatim (988 Suicide & Crisis
     Lifeline). No model call, no variation.
  3. ``record_safety_flag(...)`` — writes a row to the ``safety_flags`` table
     so the operator can review these conversations on zar.bot (the operator
     service reads the same client DB). Standalone connection, same pattern as
     app/inbound_dedup.py, so app/storage/postgres.py stays migration-only.

Never raises to callers: a bug here must not stop the fan from getting the
crisis reply (matcher errors fall back to "no match", flag-write errors are
logged and swallowed).
"""
from __future__ import annotations

import logging
import os
import re
import threading
from dataclasses import dataclass
from typing import Optional

_logger = logging.getLogger(__name__)

# The reply sent for every crisis match — verbatim, no LLM. Honest about being
# an AI, warm but not jokey, and puts 988 front and center.
CRISIS_RESPONSE = (
    "I'm really glad you told me. I'm an AI, so I can't be there for you the way "
    "a real person can — and you deserve real support right now. Please call or "
    "text 988 (the Suicide & Crisis Lifeline) to talk to someone who can help, "
    "any time, day or night. You matter."
)

# High-precision crisis signals. Each pattern should be unambiguous on its own;
# ambiguous words ("die", "kill", "dead") only count inside explicit phrases.
_CRISIS_PATTERNS: tuple[tuple[str, re.Pattern], ...] = tuple(
    (label, re.compile(rx, re.IGNORECASE))
    for label, rx in [
        ("suicide",       r"\bsuicid(e|al)\b"),
        ("kill_myself",   r"\bkill(ing)?\s+myself\b"),
        ("end_my_life",   r"\bend(ing)?\s+my\s+(own\s+)?life\b"),
        ("take_my_life",  r"\btak(e|ing)\s+my\s+own\s+life\b"),
        ("want_to_die",   r"\b(want|wanna|wish)\s+to\s+die\b|\bwanna\s+die\b|\bwish\s+i\s+(was|were)\s+dead\b"),
        ("not_want_live", r"\b(don'?t|do\s+not|no\s+longer)\s+want\s+to\s+(live|be\s+alive)\b"),
        ("self_harm",     r"\bself[- ]?harm(ing|ed)?\b|\bharm(ing)?\s+myself\b|\bhurt(ing)?\s+myself\s+on\s+purpose\b|\bcut(ting)?\s+myself\b"),
        ("end_it_all",    r"\bend(ing)?\s+it\s+all\b"),
        ("better_off_dead", r"\bbetter\s+off\s+dead\b|\bbetter\s+off\s+without\s+me\b"),
        ("no_reason_live",  r"\bno\s+reason\s+to\s+(live|keep\s+going)\b|\bnothing\s+to\s+live\s+for\b"),
    ]
)

# Comedy-context guard: these mean the fan is joking/reacting, not in crisis.
# Only applied to the softer phrases — explicit ones ("kill myself", "suicidal")
# fire regardless, because "haha I want to kill myself" still warrants 988.
_JOKE_CONTEXT_RE = re.compile(
    r"(dying laughing|died laughing|dead 💀|💀|😂|🤣|\blol\b|\blmao\b|laugh so hard|"
    r"so funny|hilarious|you kill me|killed it|killing it)",
    re.IGNORECASE,
)
_JOKE_EXEMPT_LABELS = frozenset({"want_to_die", "end_it_all"})


@dataclass
class CrisisMatch:
    label: str  # which pattern fired (stored with the safety flag)


def check_crisis(message: str) -> Optional[CrisisMatch]:
    """Return a CrisisMatch if the message signals crisis, else None. Never raises."""
    try:
        text = (message or "").strip()
        if not text:
            return None
        for label, pattern in _CRISIS_PATTERNS:
            if pattern.search(text):
                if label in _JOKE_EXEMPT_LABELS and _JOKE_CONTEXT_RE.search(text):
                    continue
                return CrisisMatch(label=label)
        return None
    except Exception:
        _logger.exception("[ZARNA] crisis matcher failed — treating as no match")
        return None


# ---------------------------------------------------------------------------
# safety_flags persistence (reviewed by the operator on zar.bot)
# ---------------------------------------------------------------------------

_DDL = (
    """
    CREATE TABLE IF NOT EXISTS safety_flags (
        id              BIGSERIAL   PRIMARY KEY,
        phone_number    TEXT        NOT NULL,
        message_text    TEXT        NOT NULL,
        matched_pattern TEXT        NOT NULL DEFAULT '',
        bot_response    TEXT        NOT NULL DEFAULT '',
        creator_slug    TEXT        NOT NULL DEFAULT 'zarna',
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reviewed        BOOLEAN     NOT NULL DEFAULT FALSE,
        reviewed_at     TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_safety_flags_review ON safety_flags (reviewed, created_at DESC)",
)

_table_ready = False
_table_lock = threading.Lock()


def _get_conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    import psycopg2
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def _ensure_table(conn) -> None:
    global _table_ready
    if _table_ready:
        return
    with _table_lock:
        if _table_ready:
            return
        with conn:
            with conn.cursor() as cur:
                for sql in _DDL:
                    cur.execute(sql)
        _table_ready = True


def record_safety_flag(
    phone_number: str,
    message_text: str,
    matched_pattern: str,
    creator_slug: str = "zarna",
    bot_response: str = "",
) -> None:
    """Persist a crisis flag for operator review. Logged + swallowed on failure."""
    conn = None
    try:
        conn = _get_conn()
        if conn is None:
            _logger.warning(
                "[ZARNA] safety flag NOT persisted (no DATABASE_URL) phone=...%s pattern=%s",
                phone_number[-4:] if phone_number else "?", matched_pattern,
            )
            return
        _ensure_table(conn)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO safety_flags
                        (phone_number, message_text, matched_pattern, bot_response, creator_slug)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (phone_number, message_text, matched_pattern, bot_response, creator_slug),
                )
        _logger.info(
            "[ZARNA] safety flag recorded phone=...%s pattern=%s",
            phone_number[-4:] if phone_number else "?", matched_pattern,
        )
    except Exception:
        _logger.exception(
            "[ZARNA] failed to record safety flag phone=...%s",
            phone_number[-4:] if phone_number else "?",
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def reset_for_tests() -> None:
    global _table_ready
    _table_ready = False
