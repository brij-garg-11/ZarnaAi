import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from app.analytics.outcome_scorer import (
    save_reply_context_async,
    score_previous_bot_reply_async,
)
from app.analytics.session_manager import get_or_create_session
from app.brain.conversation_end import is_conversation_ender
from app.brain.creator_config import CreatorConfig, load_creator
from app.brain.cost_logger import log_ai_cost as _log_ai_cost
from app.brain.emphasis import should_suppress_all_emphasis
import app.brain.generator as _generator_mod
from app.brain.generator import generate_zarna_reply, infer_reply_provider
from app.brain.intent import Intent, _fast_classify, classify_intent
from app.brain.memory import extract_memory
from app.brain.routing import classify_routing_tier, try_router_skip_safe
from app.brain.tone import classify_tone_mode
from app.config import CONVERSATION_HISTORY_LIMIT, CREATOR_SLUG, LOG_REPLY_METRICS
from app.retrieval.base import BaseRetriever
from app.storage.base import BaseStorage

# Shared thread pool — reused across requests so we don't pay thread-spawn
# cost on every message. 32 threads handles 100+ simultaneous AI calls
# without queuing (each call is mostly I/O-bound waiting on Gemini).
_executor = ThreadPoolExecutor(max_workers=32)
_logger = logging.getLogger(__name__)

# Routing uses Gemini-only for these; parallel router work is skipped when fast intent matches.
_STRUCTURED_ROUTE_INTENTS = frozenset(
    {Intent.CLIP, Intent.SHOW, Intent.BOOK, Intent.PODCAST, Intent.MERCH},
)

# Intents that involve selling — eligible for per-fan sell context + A/B variant.
_SELL_INTENTS = frozenset({Intent.SHOW, Intent.MERCH})

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

    def __init__(
        self,
        storage: BaseStorage,
        retriever: BaseRetriever,
        slug: str | None = None,
    ):
        self.storage = storage
        self.retriever = retriever
        # Resolve the effective slug: explicit arg (multi-tenant caller),
        # otherwise fall back to the process-wide CREATOR_SLUG env var so
        # Zarna's singleton brain in main.py keeps working unchanged.
        self.slug: str = (slug or CREATOR_SLUG or "zarna").strip().lower()
        # Load creator config for THIS brain's slug — critical for multi-tenant:
        # without this, every non-Zarna brain would inherit Zarna's prompt
        # blocks (style/voice/tone/guardrails) even though its retriever was
        # correctly pointed at the right slug's embeddings.
        self.creator_config: CreatorConfig | None = load_creator(self.slug)
        if self.creator_config:
            _logger.info(
                "ZarnaBrain: loaded CreatorConfig slug=%s name=%r",
                self.creator_config.slug,
                self.creator_config.name,
            )
        else:
            _logger.info(
                "ZarnaBrain: no CreatorConfig loaded for slug=%r — using all hardcoded defaults",
                self.slug,
            )

    def handle_incoming_message(self, phone_number: str, message_text: str, quiz_context: Optional[str] = None, blast_context: Optional[str] = None) -> str:
        # 1. Ensure contact exists
        self.storage.save_contact(phone_number)

        # 1b. Score the previous bot reply now that the fan has replied —
        #     fire-and-forget so it never adds latency to this reply.
        score_previous_bot_reply_async(_executor, self.storage, phone_number)

        # 1c. Track conversation session — fire-and-forget
        _executor.submit(get_or_create_session, phone_number, "user")

        # 2. Persist the user's message
        user_msg = self.storage.save_message(phone_number, "user", message_text)

        # 2a. CRISIS GATE — suicidal/self-harm signals get a fixed, deterministic
        #     988 response. No LLM involved: the right reply here can never be
        #     left to whatever the model generates that day. The message is
        #     flagged to safety_flags for operator review on zar.bot.
        from app.brain.crisis import CRISIS_RESPONSE, check_crisis, record_safety_flag
        crisis = check_crisis(message_text)
        if crisis is not None:
            _logger.info(
                "[ZARNA] crisis gate fired phone=...%s pattern=%s",
                phone_number[-4:] if phone_number else "?", crisis.label,
            )
            _executor.submit(
                record_safety_flag,
                phone_number, message_text, crisis.label, self.slug, CRISIS_RESPONSE,
            )
            self.storage.save_message(phone_number, "assistant", CRISIS_RESPONSE)
            return CRISIS_RESPONSE

        # 2b. GIVEAWAY GATE — if this text enters the fan into an active giveaway
        #     campaign (keyword match), record the entry and reply with the fixed
        #     confirmation instead of an AI reply. Only fires on a brand-new
        #     entry, so an already-entered fan keeps chatting with the AI. Fully
        #     guarded: any error here must never block a fan's reply.
        try:
            from app.giveaway.entry import try_giveaway_entry
            giveaway_reply = try_giveaway_entry(
                phone_number,
                message_text,
                message_id=getattr(user_msg, "id", None),
                slug=self.slug,
            )
        except Exception:
            _logger.warning("[ZARNA] giveaway gate error", exc_info=True)
            giveaway_reply = None
        if giveaway_reply:
            self.storage.save_message(phone_number, "assistant", giveaway_reply)
            return giveaway_reply

        # 2c. CALL gate — fans regularly text "CALL" to ask if they can phone
        #     the bot. The LLM knows nothing about the voice feature and used
        #     to deny it exists, so answer deterministically: SMS fans get
        #     "yes, call this number", WhatsApp fans get "texting works, but
        #     WhatsApp calling isn't supported yet". Only fires when this
        #     creator's voice feature is enabled. Never blocks a fan's reply.
        try:
            from app.brain.call_gate import try_call_gate
            call_reply = try_call_gate(phone_number, message_text, self.creator_config)
        except Exception:
            _logger.warning("[ZARNA] call gate error", exc_info=True)
            call_reply = None
        if call_reply:
            self.storage.save_message(phone_number, "assistant", call_reply)
            return call_reply

        # 2d. Conversation closers (lol, thanks, ok) — no reply expected
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
        future_intent = _executor.submit(classify_intent, message_text, self.creator_config)
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
        tone_mode = classify_tone_mode(message_text, intent, history, self.creator_config)

        # Fetch high-engagement examples for this intent+tone combo (cached, never blocks).
        # Only used for conversational intents — structured ones (show/clip/book/podcast/merch) skip.
        winning_examples = None
        _LEARNING_INTENTS = frozenset({
            "greeting", "feedback", "question", "personal", "general", "joke",
        })
        if intent and intent.value in _LEARNING_INTENTS:
            try:
                # Scope to THIS brain's slug — without this, Zarna's
                # high-engagement replies (Shalabh / chai / MIL lines) get
                # injected as few-shot examples into every other creator's
                # prompt, polluting their voice. PostgresStorage already
                # supports the kwarg; default still 'zarna' for legacy callers.
                winning_examples = self.storage.get_top_performing_replies(
                    intent.value,
                    str(tone_mode) if tone_mode else "",
                    creator_slug=self.slug,
                ) or None
            except TypeError:
                # InMemoryStorage / older shims don't take creator_slug — fall
                # back so we don't crash; multi-tenant correctness still holds
                # because those backends don't have shared engagement data.
                try:
                    winning_examples = self.storage.get_top_performing_replies(
                        intent.value, str(tone_mode) if tone_mode else ""
                    ) or None
                except Exception:
                    _logger.debug(
                        "winning_examples legacy fallback also failed for intent=%s",
                        intent.value,
                    )
            except Exception:
                # Learning is best-effort but failures must be visible — silent
                # exceptions previously hid real DB schema drift in production.
                _logger.exception(
                    "winning_examples lookup failed for intent=%s slug=%s",
                    intent.value, self.slug,
                )

        # Per-fan sell context (Step 5) and A/B variant (Step 7) — sell intents only.
        # sell_context: fan's most recent show attendance + their stored location.
        # sell_variant: randomly assigned "A" or "B" so copy variations can be tracked.
        sell_context: Optional[str] = None
        sell_variant: Optional[str] = None
        if intent in _SELL_INTENTS:
            sell_variant = random.choice(["A", "B"])
            try:
                show_ctx = self.storage.get_fan_show_context(phone_number)
                location = self.storage.get_fan_location(phone_number)
                parts = []
                if show_ctx:
                    parts.append(show_ctx)
                if location:
                    parts.append(f"Fan is from {location}.")
                sell_context = " ".join(parts) if parts else None
            except Exception:
                _logger.exception(
                    "sell_context enrichment failed for phone=...%s slug=%s",
                    phone_number[-4:] if phone_number else "?", self.slug,
                )

        # Show directive (Item 1): for SHOW intent, match the fan's city against the
        # tour calendar so the reply names the specific upcoming show + date and link.
        # Never fatal — a failure here just falls back to the generic ticket reply.
        show_directive = None
        if intent == Intent.SHOW:
            try:
                from app.brain.show_calendar import build_show_directive
                show_directive = build_show_directive(message_text, self.creator_config)
            except Exception:
                _logger.exception("show_directive build failed for slug=%s", self.slug)

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
            blast_context=blast_context,
            winning_examples=winning_examples,
            sell_context=sell_context,
            sell_variant=sell_variant,
            creator_config=self.creator_config,
            show_directive=show_directive,
        )
        gen_ms = (time.perf_counter() - t_gen) * 1000
        ai_provider, ai_prompt_tokens, ai_completion_tokens = _generator_mod.get_last_usage()
        ai_cost = _generator_mod.calc_ai_cost(ai_provider, ai_prompt_tokens, ai_completion_tokens)

        # Log AI spend to operator DB for per-client P&L (internal only, non-blocking).
        _log_ai_cost(self.slug, ai_provider, ai_prompt_tokens, ai_completion_tokens, ai_cost)

        # Silently rewrite known URLs (website, podcast) to tracked /t/<slug> links
        # Phone number is embedded as ?f=<token> so clicks can be attributed to this fan.
        try:
            from app.link_tracker import rewrite_bot_reply
            reply = rewrite_bot_reply(reply, phone_number=phone_number)
        except Exception:
            _logger.exception(
                "link_tracker.rewrite_bot_reply failed for phone=...%s",
                phone_number[-4:] if phone_number else "?",
            )

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

        # 8c. Write engagement context + AI cost onto that row in the background
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
            sell_variant=sell_variant,
            provider=ai_provider,
            prompt_tokens=ai_prompt_tokens,
            completion_tokens=ai_completion_tokens,
            ai_cost_usd=ai_cost,
        )

        # 9. Update fan memory in the background — no latency impact on reply
        _executor.submit(self._update_memory, phone_number, message_text, fan_memory)

        return reply

    def _update_memory(self, phone_number: str, message_text: str, current_memory: str) -> None:
        try:
            new_memory, new_tags, location, minor_detected, name = extract_memory(current_memory, message_text)

            if minor_detected:
                # COPPA / privacy: clear any existing profile for this number
                if current_memory:
                    self.storage.update_memory(phone_number, "", [], "")
                    import logging
                    logging.getLogger(__name__).info(
                        "Cleared fan profile for %s — minor signal detected", phone_number[-4:]
                    )
                return  # Never store data for minors

            if new_memory != current_memory or new_tags or location or name:
                self.storage.update_memory(phone_number, new_memory, new_tags, location, name)
        except Exception:
            _logger.exception(
                "_update_memory failed for phone=...%s slug=%s",
                phone_number[-4:] if phone_number else "?", self.slug,
            )


def create_brain(slug: Optional[str] = None) -> ZarnaBrain:
    """
    Factory that wires up the default production dependencies.
    Uses PostgresStorage when DATABASE_URL is set (production on Railway),
    falls back to InMemoryStorage for local dev without a database.

    Retrieval selection:
      - If `slug` is a NEW creator (not 'zarna'): always use PgRetriever(slug).
        These creators' chunks only exist in the creator_embeddings table.
      - If `slug` is 'zarna' OR None: default to the legacy EmbeddingRetriever
        (file-backed) so Zarna's reply quality is unchanged from production.
        Flip PG_RETRIEVER_FOR_ZARNA=1 in the environment to exercise
        PgRetriever('zarna') with Zarna's source weights — the Phase 5
        migration (scripts/migrate_zarna_to_pg.py) must have been applied
        first, and quality verified via the Phase 5 comparison test.
    """
    database_url = os.getenv("DATABASE_URL", "")
    if database_url:
        from app.storage.postgres import PostgresStorage
        # Railway injects postgres:// but psycopg2 requires postgresql://
        dsn = database_url.replace("postgres://", "postgresql://", 1)
        # Pass slug so save_contact() stamps new fans with the correct
        # creator_slug — without this, Marcus's fans would be tagged
        # 'zarna' via the module-global fallback, poisoning Zarna's
        # engagement pool and starving Marcus's winning_examples.
        storage = PostgresStorage(dsn=dsn, creator_slug=slug)
    else:
        from app.storage.memory import InMemoryStorage
        storage = InMemoryStorage()

    _use_pg_for_zarna = os.getenv("PG_RETRIEVER_FOR_ZARNA", "0").strip().lower() in ("1", "true", "on", "yes")
    _is_zarna_path = (slug is None) or (slug == "zarna")

    if _is_zarna_path and not _use_pg_for_zarna:
        # Legacy path — unchanged behaviour for Zarna in production.
        from app.retrieval.embedding import EmbeddingRetriever
        retriever = EmbeddingRetriever()
    elif _is_zarna_path and _use_pg_for_zarna:
        # PgRetriever('zarna') WITH Zarna's hand-tuned source weights so
        # reply quality matches EmbeddingRetriever's ranking.
        from app.retrieval.pg_retriever import PgRetriever
        from app.retrieval.source_weights import (
            load_podcast_transcript_ids,
            zarna_weight_fn,
        )
        weight_fn = zarna_weight_fn(
            podcast_transcript_ids=load_podcast_transcript_ids(),
            podcast_mode=os.getenv("PODCAST_TRANSCRIPTS_MODE", "exclude"),
            monday_mode=os.getenv("MONDAY_MOTIVATION_MODE", "include"),
        )
        retriever = PgRetriever("zarna", weight_fn=weight_fn)
    else:
        # New creator — Postgres is the only source of truth. No
        # hand-tuned weights yet (they're creator-specific); raw pgvector
        # ordering is fine until we have engagement data to rank with.
        from app.retrieval.pg_retriever import PgRetriever
        retriever = PgRetriever(slug)

    return ZarnaBrain(
        storage=storage,
        retriever=retriever,
        slug=slug,  # CRITICAL: drives load_creator() so multi-tenant brains
                    # use their own personality config, not Zarna's defaults.
    )


# ---------------------------------------------------------------------------
# Per-slug brain cache (multi-tenant "apartment building")
# ---------------------------------------------------------------------------
# One main-app process serves many creators. Building a brain is non-trivial
# (loads personality config + a retriever), so we memoise one brain per slug
# and reuse it across inbound messages. Bounded so a flood of signups can't
# grow memory without limit — least-recently-built brains are evicted (an
# evicted brain stays alive until its in-flight requests release it; the next
# message simply rebuilds it).
_BRAIN_CACHE: "dict[str, ZarnaBrain]" = {}
_BRAIN_CACHE_LOCK = threading.Lock()
_BRAIN_CACHE_MAX = int(os.getenv("BRAIN_CACHE_MAX", "256"))


def get_brain(slug: Optional[str]) -> ZarnaBrain:
    """
    Return a cached brain for `slug`, building it on first use.

    Thread-safe. The (possibly slow) brain construction happens outside the
    lock; `setdefault` collapses any concurrent first-builds for the same slug.
    """
    key = (slug or "").strip().lower() or "zarna"

    with _BRAIN_CACHE_LOCK:
        cached = _BRAIN_CACHE.get(key)
        if cached is not None:
            return cached

    brain = create_brain(key)

    with _BRAIN_CACHE_LOCK:
        if key not in _BRAIN_CACHE and len(_BRAIN_CACHE) >= _BRAIN_CACHE_MAX:
            # Evict the oldest-inserted brain to bound memory.
            oldest = next(iter(_BRAIN_CACHE))
            _BRAIN_CACHE.pop(oldest, None)
            _logger.info("brain cache full — evicted %s to make room for %s", oldest, key)
        return _BRAIN_CACHE.setdefault(key, brain)


def reset_brain_cache() -> None:
    """Clear the per-slug brain cache (used by tests and after config changes)."""
    with _BRAIN_CACHE_LOCK:
        _BRAIN_CACHE.clear()
