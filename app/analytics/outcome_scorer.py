"""
Engagement outcome scoring — fire-and-forget helpers called from the brain handler.

score_previous_bot_reply_async
  Called at the top of every inbound message.  Retroactively scores the
  most recent bot reply by recording that the fan did reply and how long
  they took.  Runs in the background so it never delays reply generation.

save_reply_context_async
  Called immediately after the bot reply is generated and saved.  Writes
  the context metadata (intent, tone, tier, length, etc.) onto that message
  row so we can later correlate it with engagement outcomes.
"""

import logging
import re
from concurrent.futures import Executor
from typing import Optional

from app.storage.base import BaseStorage

_logger = logging.getLogger(__name__)

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


def score_previous_bot_reply_async(
    executor: Executor,
    storage: BaseStorage,
    phone_number: str,
) -> None:
    """Submit background task to score the previous bot reply for this fan."""

    def _run():
        try:
            storage.score_previous_bot_reply(phone_number)
        except Exception:
            _logger.exception(
                "outcome_scorer: score_previous_bot_reply failed for ...%s",
                phone_number[-4:] if phone_number else "?",
            )

    executor.submit(_run)


def save_reply_context_async(
    executor: Executor,
    storage: BaseStorage,
    message_id: Optional[int],
    reply_text: str,
    intent: Optional[str],
    tone_mode: Optional[str],
    routing_tier: Optional[str],
    gen_ms: float,
    conversation_turn: int,
) -> None:
    """Submit background task to write context metadata onto the saved reply row."""
    if message_id is None:
        return

    has_link = bool(_URL_RE.search(reply_text or ""))
    reply_length = len(reply_text or "")

    def _run():
        try:
            storage.save_reply_context(
                message_id=message_id,
                intent=intent,
                tone_mode=tone_mode,
                routing_tier=routing_tier,
                reply_length_chars=reply_length,
                has_link=has_link,
                conversation_turn=conversation_turn,
                gen_ms=gen_ms,
            )
        except Exception:
            _logger.exception(
                "outcome_scorer: save_reply_context failed for message_id=%s", message_id
            )

    executor.submit(_run)
