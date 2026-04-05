import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from app.analytics.outcome_scorer import (
    save_reply_context_async,
    score_previous_bot_reply_async,
)
from app.analytics.session_manager import get_or_create_session
from app.brain.conversation_end import is_conversation_ender
from app.brain.emphasis import should_suppress_all_emphasis
from app.brain.generator import generate_zarna_reply, infer_reply_provider
from app.brain.intent import Intent, _fast_classify, classify_intent
from app.brain.memory import extract_memory
from app.brain.routing import classify_routing_tier, try_router_skip_safe
from app.brain.tone import classify_tone_mode
from app.config import CONVERSATION_HISTORY_LIMIT, LOG_REPLY_METRICS
from app.retrieval.base import BaseRetriever
from app.storage.base import BaseStorage

# Shared thread pool — reused across requests so we don't pay thread-spawn
# cost on every message. 32 threads handles 100+ simultaneous AI calls
# without queuing (each call is mostly I/O-bound waiting on Gemini).
_executor = ThreadPoolExecutor(max_workers=32)
_logger = logging.getLogger(__name__)

# Routing uses Gemini-only for these; parallel router work is skipped when fast intent matches.
_STRUCTURED_ROUTE_INTENTS = frozenset(
    {Intent.CLIP, Intent.SHOW, Intent.BOOK, Intent.PODCAST},
)

_ROAST_FAMILY_HINTS = re.compile(
    r"\b(shalabh|husband|mother[- ]in[- ]law|mil|baba\s*ramdev)\b",
    re.IGNORECASE,
)
_VULNERABLE_HINTS = re.compile(
    r"\b(sad|anxious|anxiety|depress|grief|grieving|panic|hurt|heartbroken|"
    r"loss|cancer|illness|scared|not okay|not okay)\b",
    re.IGNORECASE,
)


class ZarnaBrain:
    """
    Central handler. Owns no state of its own — all persistence goes through
    storage, all content retrieval goes through retriever. Swap either without
    touching this class.
    """

    def __init__(self, storage: BaseStorage, retriever: BaseRetriever):
        self.storage = storage
        self.retriever = retriever

    def handle_incoming_message(self, phone_number: str, message_text: str, quiz_context: Optional[str] = None) -> str:
        # 1. Ensure contact exists
        self.storage.save_contact(phone_number)

        # 1b. Score the previous bot reply now that the fan has replied —
        #     fire-and-forget so it never adds latency to this reply.
        score_previous_bot_reply_async(_executor, self.storage, phone_number)

        # 1c. Track conversation session — fire-and-forget
        _executor.submit(get_or_create_session, phone_number, "user")

        # 2. Persist the user's message
        self.storage.save_message(phone_number, "user", message_text)

        # 2b. Conversation closers (lol, thanks, ok) — no reply expected
        if is_conversation_ender(message_text):
            return ""

        # 3. Pull prior conversation (excluding the message we just saved)
        raw_history = self.storage.get_conversation_history(
            phone_number, limit=CONVERSATION_HISTORY_LIMIT + 1
        )
        history = [{"role": m.role, "text": m.text} for m in raw_history[:-1]]

        # 4. Load existing fan memory for personalization
        fan_memory = self.storage.get_memory(phone_number)

        # 5 + 6. Classify intent AND retrieve chunks in parallel; start routing in parallel
        # when safe (no wasted router call for structured fast-path intents or skip-low).
        skip_router_api = try_router_skip_safe(message_text)
        fast_intent = _fast_classify(message_text)
        structured_fast = fast_intent in _STRUCTURED_ROUTE_INTENTS if fast_intent else False
        start_route_parallel = not skip_router_api and not structured_fast

        t_parallel = time.perf_counter()
        future_intent = _executor.submit(classify_intent, message_text)
        future_chunks = _executor.submit(self.retriever.get_relevant_chunks, message_text)
        future_route = None
        if start_route_parallel:
            future_route = _executor.submit(
                classify_routing_tier, message_text, history, fan_memory
            )

        intent = future_intent.result()
        chunks = future_chunks.result()
        intent_chunks_ms = (time.perf_counter() - t_parallel) * 1000

        # Recent assistant bodies for *emphasis* throttle (exclude this turn)
        history_for_emphasis = self.storage.get_conversation_history(
            phone_number, limit=24
        )
        assistant_texts = [m.text for m in history_for_emphasis if m.role == "assistant"]
        emphasis_suppress_all = should_suppress_all_emphasis(
            message_text, intent, assistant_texts
        )

        # 7. Route complexity for GENERAL/JOKE; structured intents stay Gemini-only.
        t_route = time.perf_counter()
        route_source = "structured"
        family_roast_override = bool(_ROAST_FAMILY_HINTS.search(message_text)) and not bool(
            _VULNERABLE_HINTS.search(message_text)
        )
        if intent in _STRUCTURED_ROUTE_INTENTS:
            routing_tier = None
            if future_route is not None:
                future_route.result()  # drain parallel work we don't need
            route_ms = (time.perf_counter() - t_route) * 1000
        elif family_roast_override:
            routing_tier = "low"
            route_source = "family_roast_force_low"
            route_ms = 0.0
        elif skip_router_api:
            routing_tier = "low"
            route_source = "skip"
            route_ms = 0.0
        else:
            if future_route is not None:
                routing_tier = future_route.result()
                route_source = "parallel"
            else:
                routing_tier = classify_routing_tier(message_text, history, fan_memory)
                route_source = "sync"
            route_ms = (time.perf_counter() - t_route) * 1000

        t_gen = time.perf_counter()
        tone_mode = classify_tone_mode(message_text, intent, history)

        # Fetch high-engagement examples for this intent+tone combo (cached, never blocks).
        # Only used for conversational intents — structured ones (show/clip/book/podcast) skip.
        winning_examples = None
        _LEARNING_INTENTS = frozenset({
            "greeting", "feedback", "question", "personal", "general", "joke",
        })
        if intent and intent.value in _LEARNING_INTENTS:
            try:
                winning_examples = self.storage.get_top_performing_replies(
                    intent.value, str(tone_mode) if tone_mode else ""
                ) or None
            except Exception:
                pass  # learning is best-effort, never block a reply

        reply = generate_zarna_reply(
            intent=intent,
            user_message=message_text,
            chunks=chunks,
            history=history,
            fan_memory=fan_memory,
            emphasis_suppress_all=emphasis_suppress_all,
            routing_tier=routing_tier,
            tone_mode=tone_mode,
            quiz_context=quiz_context,
            winning_examples=winning_examples,
        )
        gen_ms = (time.perf_counter() - t_gen) * 1000

        # Silently rewrite known URLs (website, podcast) to tracked /t/<slug> links
        try:
            from app.link_tracker import rewrite_bot_reply
            reply = rewrite_bot_reply(reply)
        except Exception:
            pass  # never block a reply over tracking

        if LOG_REPLY_METRICS:
            provider = infer_reply_provider(intent, routing_tier)
            _logger.info(
                "reply_metrics intent=%s tier=%s route_src=%s provider=%s "
                "tone=%s intent_chunks_ms=%.1f route_ms=%.1f gen_ms=%.1f phone_last4=%s",
                intent.value,
                routing_tier if routing_tier is not None else "none",
                route_source,
                provider,
                tone_mode,
                intent_chunks_ms,
                route_ms,
                gen_ms,
                phone_number[-4:] if len(phone_number) >= 4 else "****",
            )

        # 8. Persist the assistant's reply (returns the row id for analytics)
        saved_reply = self.storage.save_message(phone_number, "assistant", reply)

        # 8b. Track bot turn in session
        _executor.submit(get_or_create_session, phone_number, "assistant")

        # 8c. Write engagement context onto that row in the background
        save_reply_context_async(
            executor=_executor,
            storage=self.storage,
            message_id=saved_reply.id,
            reply_text=reply,
            intent=intent.value if intent else None,
            tone_mode=str(tone_mode) if tone_mode is not None else None,
            routing_tier=routing_tier,
            gen_ms=gen_ms,
            conversation_turn=len(history) // 2 + 1,
        )

        # 9. Update fan memory in the background — no latency impact on reply
        _executor.submit(self._update_memory, phone_number, message_text, fan_memory)

        return reply

    def _update_memory(self, phone_number: str, message_text: str, current_memory: str) -> None:
        try:
            new_memory, new_tags, location, minor_detected = extract_memory(current_memory, message_text)

            if minor_detected:
                # COPPA / privacy: clear any existing profile for this number
                if current_memory:
                    self.storage.update_memory(phone_number, "", [], "")
                    import logging
                    logging.getLogger(__name__).info(
                        "Cleared fan profile for %s — minor signal detected", phone_number[-4:]
                    )
                return  # Never store data for minors

            if new_memory != current_memory or new_tags or location:
                self.storage.update_memory(phone_number, new_memory, new_tags, location)
        except Exception:
            pass  # Memory update is best-effort; never block a reply


def create_brain() -> ZarnaBrain:
    """
    Factory that wires up the default production dependencies.
    Uses PostgresStorage when DATABASE_URL is set (production on Railway),
    falls back to InMemoryStorage for local dev without a database.
    """
    from app.retrieval.embedding import EmbeddingRetriever

    database_url = os.getenv("DATABASE_URL", "")
    if database_url:
        from app.storage.postgres import PostgresStorage
        # Railway injects postgres:// but psycopg2 requires postgresql://
        dsn = database_url.replace("postgres://", "postgresql://", 1)
        storage = PostgresStorage(dsn=dsn)
    else:
        from app.storage.memory import InMemoryStorage
        storage = InMemoryStorage()

    return ZarnaBrain(
        storage=storage,
        retriever=EmbeddingRetriever(),
    )
