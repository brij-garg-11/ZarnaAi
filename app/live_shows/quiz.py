"""
Live show pop quiz — session management and AI context builder.

Flow:
  1. Operator sends a quiz blast (is_quiz=True) to a show audience.
  2. blast_sender creates a quiz_sessions row with the question + correct answer.
  3. On every inbound message, get_active_quiz_for_fan() checks for an unanswered
     active quiz for that fan.
  4. If one exists, record_quiz_response() marks them as answered so they can't
     be quizzed again, and build_quiz_context() returns a prompt block injected
     into the AI generator — giving Zarna full context to react in character.
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


def get_active_quiz_for_fan(phone_number: str) -> Optional[dict]:
    """
    Return the active, unanswered quiz session for this fan, or None.

    Active = not yet expired (or no expiry set) AND fan hasn't answered yet.
    Returns the most recently created qualifying session.
    """
    c = _conn()
    if not c:
        return None
    try:
        import psycopg2.extras
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT qs.id, qs.question_text, qs.correct_answer
                FROM   quiz_sessions qs
                WHERE  (qs.expires_at IS NULL OR qs.expires_at > NOW())
                  AND  NOT EXISTS (
                      SELECT 1 FROM quiz_responses qr
                      WHERE  qr.quiz_id      = qs.id
                        AND  qr.phone_number = %s
                  )
                ORDER BY qs.created_at DESC
                LIMIT 1
                """,
                (phone_number,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception:
        _logger.exception(
            "get_active_quiz_for_fan failed for ...%s",
            phone_number[-4:] if phone_number else "?",
        )
        return None
    finally:
        c.close()


def record_quiz_response(quiz_id: int, phone_number: str, fan_answer: str) -> None:
    """
    Record that this fan has answered the quiz.
    Uses ON CONFLICT DO NOTHING — safe to call more than once.
    """
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO quiz_responses (quiz_id, phone_number, fan_answer)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (quiz_id, phone_number) DO NOTHING
                    """,
                    (quiz_id, phone_number, (fan_answer or "")[:500]),
                )
        _logger.info(
            "quiz_response recorded: quiz_id=%s fan=...%s",
            quiz_id,
            phone_number[-4:] if phone_number else "?",
        )
    except Exception:
        _logger.exception(
            "record_quiz_response failed quiz_id=%s ...%s",
            quiz_id,
            phone_number[-4:] if phone_number else "?",
        )
    finally:
        c.close()


def build_quiz_context(question: str, correct_answer: str, fan_reply: str) -> str:
    """
    Build the context block injected into the AI prompt when a fan is answering a quiz.

    The generator treats this as high-priority framing: the AI knows the question,
    the right answer, and the fan's reply — and generates a funny Zarna-voice reaction.
    """
    return (
        f"QUIZ SITUATION — override normal conversation mode for this reply only:\n"
        f"You sent this fan a pop quiz question via blast: \"{question}\"\n"
        f"The correct answer is: \"{correct_answer}\"\n"
        f"The fan just replied: \"{fan_reply}\"\n\n"
        f"Decide if they got it right — be generous, partial or phonetically close answers count. "
        f"React entirely in Zarna's voice: funny, high-energy, warm. "
        f"If correct: celebrate it with Zarna-style excitement (keep love under the joke). "
        f"If wrong: playfully roast them and drop the correct answer naturally. "
        f"2 sentences max. No lists. No 'Correct!' or 'Incorrect!' as a standalone opener — "
        f"weave the verdict into a real Zarna line."
    )
