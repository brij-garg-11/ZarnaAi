"""
Voice reply generation — a thin wrapper over the existing AI brain.

Reuses the same retrieval, intent classification, tone, personality, and
guardrails as SMS, but calls generate_zarna_reply(channel="voice") so the
output is spoken-phone friendly (short sentences, no asterisks, no URLs read
aloud). It deliberately does NOT reuse ZarnaBrain.handle_incoming_message,
because that path is SMS-shaped (it returns "" on conversation enders, applies
link tracking, and assumes an async SMS lifecycle).
"""
from __future__ import annotations

import logging
import re

from app.brain.generator import generate_zarna_reply, generate_zarna_reply_stream
from app.brain.intent import classify_intent
from app.brain.tone import classify_tone_mode

_logger = logging.getLogger(__name__)

# Spoken fallback if the model returns nothing — stays in character, no formatting.
_VOICE_FALLBACK = "Sorry, you cut out there for a second — say that again?"

# Routing tier for voice. "low" keeps generation on Gemini for the lowest latency,
# which matters far more on a live call than the marginal quality of a higher tier.
_VOICE_ROUTING_TIER = "low"


def _normalize_phone(number: str) -> str:
    """Reduce a phone number to its last 10 digits so formatting and country
    code never matter ('+1 (646) 640-6086' == '6466406086')."""
    digits = re.sub(r"\D", "", number or "")
    return digits[-10:] if len(digits) >= 10 else digits


def known_caller_note(creator_config, caller_id: str) -> str:
    """Return the persona note for a recognized caller, or "" for everyone else.

    Looks up voice.known_callers from the creator config. This is our own
    recognition layer — the telephony provider only supplies the raw number.
    """
    caller = _normalize_phone(caller_id)
    if not caller:
        return ""
    voice = getattr(creator_config, "voice", None)
    for number, note in (getattr(voice, "known_callers", None) or {}).items():
        if _normalize_phone(number) == caller and (note or "").strip():
            return note.strip()
    return ""


def generate_voice_reply(brain, user_text: str, history: list[dict], caller_id: str = "") -> str:
    """Produce a spoken reply for one call turn.

    Args:
        brain: a ZarnaBrain (gives us retriever + creator_config for this slug).
        user_text: the caller's transcribed utterance for this turn.
        history: prior turns this call, as [{"role": "user"|"assistant", "text": ...}].
        caller_id: the caller's phone number, if known — enables known-caller personas.
    """
    user_text = (user_text or "").strip()
    if not user_text:
        return _VOICE_FALLBACK

    cc = getattr(brain, "creator_config", None)
    try:
        intent = classify_intent(user_text, cc)
        chunks = brain.retriever.get_relevant_chunks(user_text)
        tone_mode = classify_tone_mode(user_text, intent, history, cc)
        reply = generate_zarna_reply(
            intent=intent,
            user_message=user_text,
            chunks=chunks,
            history=history,
            fan_memory=known_caller_note(cc, caller_id),
            routing_tier=_VOICE_ROUTING_TIER,
            tone_mode=tone_mode,
            creator_config=cc,
            channel="voice",
        )
    except Exception:
        _logger.exception("[ZARNA] voice reply generation failed")
        return _VOICE_FALLBACK

    reply = (reply or "").strip()
    return reply or _VOICE_FALLBACK


def generate_voice_reply_stream(brain, user_text: str, history: list[dict], caller_id: str = ""):
    """Stream a spoken reply for one call turn, sentence by sentence.

    Same brain pipeline as generate_voice_reply, but yields each sentence as soon
    as it's ready so the caller (the ElevenLabs custom-LLM endpoint) can forward it
    immediately and TTS can start on the first sentence. Intent skips the Gemini
    fallback (allow_llm=False) to shave a round-trip on the live path. Always yields
    at least the fallback line if generation produces nothing.
    """
    user_text = (user_text or "").strip()
    if not user_text:
        yield _VOICE_FALLBACK
        return

    cc = getattr(brain, "creator_config", None)
    try:
        intent = classify_intent(user_text, cc, allow_llm=False)
        chunks = brain.retriever.get_relevant_chunks(user_text)
        tone_mode = classify_tone_mode(user_text, intent, history, cc)
    except Exception:
        _logger.exception("[ZARNA] voice reply (stream) setup failed")
        yield _VOICE_FALLBACK
        return

    produced = False
    try:
        for piece in generate_zarna_reply_stream(
            intent=intent,
            user_message=user_text,
            chunks=chunks,
            history=history,
            fan_memory=known_caller_note(cc, caller_id),
            tone_mode=tone_mode,
            creator_config=cc,
        ):
            if piece:
                produced = True
                yield piece
    except Exception:
        _logger.exception("[ZARNA] voice reply (stream) generation failed")

    if not produced:
        yield _VOICE_FALLBACK
