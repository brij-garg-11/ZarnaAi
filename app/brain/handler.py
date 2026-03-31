import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

from app.brain.conversation_end import is_conversation_ender
from app.brain.emphasis import should_suppress_all_emphasis
from app.brain.generator import generate_zarna_reply, infer_reply_provider
from app.brain.intent import Intent, _fast_classify, classify_intent
from app.brain.memory import extract_memory
from app.brain.routing import classify_routing_tier, try_router_skip_safe
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


class ZarnaBrain:
    """
    Central handler. Owns no state of its own — all persistence goes through
    storage, all content retrieval goes through retriever. Swap either without
    touching this class.
    """

    def __init__(self, storage: BaseStorage, retriever: BaseRetriever):
        self.storage = storage
        self.retriever = retriever

    def handle_incoming_message(self, phone_number: str, message_text: str) -> str:
        # 1. Ensure contact exists
        self.storage.save_contact(phone_number)

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
        if intent in _STRUCTURED_ROUTE_INTENTS:
            routing_tier = None
            if future_route is not None:
                future_route.result()  # drain parallel work we don't need
            route_ms = (time.perf_counter() - t_route) * 1000
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
        reply = generate_zarna_reply(
            intent=intent,
            user_message=message_text,
            chunks=chunks,
            history=history,
            fan_memory=fan_memory,
            emphasis_suppress_all=emphasis_suppress_all,
            routing_tier=routing_tier,
        )
        gen_ms = (time.perf_counter() - t_gen) * 1000

        if LOG_REPLY_METRICS:
            provider = infer_reply_provider(intent, routing_tier)
            _logger.info(
                "reply_metrics intent=%s tier=%s route_src=%s provider=%s "
                "intent_chunks_ms=%.1f route_ms=%.1f gen_ms=%.1f phone_last4=%s",
                intent.value,
                routing_tier if routing_tier is not None else "none",
                route_source,
                provider,
                intent_chunks_ms,
                route_ms,
                gen_ms,
                phone_number[-4:] if len(phone_number) >= 4 else "****",
            )

        # 8. Persist the assistant's reply
        self.storage.save_message(phone_number, "assistant", reply)

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
