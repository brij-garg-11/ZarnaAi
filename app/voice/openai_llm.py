"""
OpenAI-compatible Chat Completions endpoint for ElevenLabs Agents (custom LLM).

ElevenLabs Conversational AI handles the call itself — telephony (via its native
Twilio integration), speech-to-text, her cloned voice (TTS), and turn-taking. For
every turn it calls an OpenAI-style ``POST /v1/chat/completions`` endpoint to decide
what to say. This module is that endpoint: it adapts the request into a single
ZarnaBrain voice turn and streams the reply back as Server-Sent Events in OpenAI's
chunk format.

Why this exists (instead of Twilio ConversationRelay): ConversationRelay's native
ElevenLabs TTS can only resolve voices in ElevenLabs' *public* library, so Zarna's
*private* clone returns Twilio error 64112 ("voice not found"). Running the agent on
ElevenLabs keeps the clone private (it never leaves your ElevenLabs account) and uses
the lowest-latency path for an ElevenLabs voice, while ZarnaBrain still decides every
word via this endpoint.

Auth: requests must present ``Authorization: Bearer <VOICE_LLM_API_KEY>`` matching the
secret stored in the ElevenLabs agent config. If the env var is unset (local dev) the
check is skipped with a loud warning.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.brain.handler import get_brain
from app.voice.voice_brain import generate_voice_reply, generate_voice_reply_stream

_logger = logging.getLogger("zarna.voice.llm")

router = APIRouter()

_MODEL_NAME = "zarna-brain"


def _slug() -> str:
    return (os.getenv("CREATOR_SLUG") or "zarna").strip().lower()


def _authorized(authorization: str | None) -> bool:
    """Validate the shared bearer secret (fail-open only when no secret is configured)."""
    secret = (os.getenv("VOICE_LLM_API_KEY") or "").strip()
    if not secret:
        _logger.warning("[ZARNA] VOICE_LLM_API_KEY unset — skipping LLM endpoint auth")
        return True
    if not authorization:
        return False
    token = authorization.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token == secret


# Matches the "caller_id: +16466406086" line the ElevenLabs agent prompt carries
# (populated from its {{system__caller_id}} dynamic variable on phone calls).
_CALLER_ID_RE = re.compile(r"caller_id:\s*(\+?[\d\s().-]{7,20})")


def _split_messages(messages: list[dict]) -> tuple[str, list[dict], str]:
    """Return (latest_user_text, prior_history, caller_id) from OpenAI-style messages.

    History is mapped to ZarnaBrain's shape: [{"role": "user"|"assistant", "text": ...}].
    System messages are dropped — ZarnaBrain supplies its own persona/system prompt —
    except that the caller's phone number is first parsed out of them, since the
    agent's system prompt is the only place ElevenLabs surfaces the caller ID.
    """
    turns: list[dict] = []
    caller_id = ""
    for m in messages or []:
        role = (m.get("role") or "").lower()
        content = m.get("content")
        if isinstance(content, list):
            # Some clients send content as a list of parts; concatenate text parts.
            content = "".join(
                part.get("text", "") for part in content if isinstance(part, dict)
            )
        content = (content or "").strip()
        if role == "system" and content and not caller_id:
            match = _CALLER_ID_RE.search(content)
            if match:
                caller_id = match.group(1).strip()
        if not content or role not in ("user", "assistant"):
            continue
        turns.append({"role": role, "text": content})

    user_text = ""
    if turns and turns[-1]["role"] == "user":
        user_text = turns[-1]["text"]
        history = turns[:-1]
    else:
        history = turns
    return user_text, history, caller_id


def _greeting() -> str:
    """First-turn fallback: the configured spoken greeting, if any."""
    try:
        brain = get_brain(_slug())
        voice = getattr(getattr(brain, "creator_config", None), "voice", None)
        return (getattr(voice, "greeting", "") or "").strip()
    except Exception:
        return ""


def _reply_for(messages: list[dict]) -> str:
    user_text, history, caller_id = _split_messages(messages)
    if not user_text:
        # ElevenLabs called us with no caller turn (e.g. to open the call).
        return _greeting() or "Hey, it's Zarna. What's on your mind?"
    brain = get_brain(_slug())
    return generate_voice_reply(brain, user_text, history, caller_id=caller_id)


async def _sentence_stream(messages: list[dict]):
    """Async generator of spoken sentences, bridged off the blocking brain stream.

    Runs the synchronous voice-brain generator in a worker thread and hands each
    sentence back to the event loop the instant it's produced, so the first sentence
    reaches ElevenLabs (and starts TTS) without waiting for the full reply.
    """
    user_text, history, caller_id = _split_messages(messages)
    if not user_text:
        yield _greeting() or "Hey, it's Zarna. What's on your mind?"
        return

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    sentinel = object()

    def worker():
        try:
            brain = get_brain(_slug())
            for piece in generate_voice_reply_stream(brain, user_text, history, caller_id=caller_id):
                if piece:
                    loop.call_soon_threadsafe(queue.put_nowait, piece)
        except Exception:
            _logger.exception("[ZARNA] voice-llm stream worker failed")
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, sentinel)

    loop.run_in_executor(None, worker)

    while True:
        item = await queue.get()
        if item is sentinel:
            break
        yield item


def _chunk(delta: dict, *, cid: str, created: int, finish: str | None = None) -> str:
    payload = {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": created,
        "model": _MODEL_NAME,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    return f"data: {json.dumps(payload)}\n\n"


# ElevenLabs' custom-LLM client is inconsistent about whether it treats the configured
# URL as a base (and appends "/chat/completions") or as the full endpoint. With the URL
# set to ".../v1/chat/completions" we've seen it hit ".../v1/chat/completions/chat/completions"
# and 404 on a burst of retries, which makes the agent hang up mid-call. Registering the
# same handler on every path form it might send makes the endpoint resilient to both.
@router.post("/v1/chat/completions")
@router.post("/chat/completions")
@router.post("/v1/chat/completions/chat/completions")
async def chat_completions(request: Request, authorization: str | None = Header(default=None)):
    if not _authorized(authorization):
        _logger.warning("[ZARNA] rejected LLM request — bad/missing bearer token")
        return JSONResponse({"error": {"message": "unauthorized", "type": "invalid_request_error"}}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": {"message": "invalid JSON body"}}, status_code=400)

    messages = body.get("messages") or []
    stream = bool(body.get("stream", True))
    cid = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())

    if not stream:
        # Non-streaming clients get the full reply at once (blocking generation
        # kept off the event loop).
        reply = await asyncio.to_thread(_reply_for, messages)
        _logger.info("[ZARNA] voice-llm reply (non-stream): %s", reply)
        return JSONResponse(
            {
                "id": cid,
                "object": "chat.completion",
                "created": created,
                "model": _MODEL_NAME,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": reply},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            }
        )

    async def event_stream():
        yield _chunk({"role": "assistant", "content": ""}, cid=cid, created=created)
        spoken: list[str] = []
        async for sentence in _sentence_stream(messages):
            spoken.append(sentence)
            # Trailing space so sentences don't run together when concatenated.
            yield _chunk({"content": sentence + " "}, cid=cid, created=created)
        _logger.info("[ZARNA] voice-llm reply: %s", " ".join(spoken))
        yield _chunk({}, cid=cid, created=created, finish="stop")
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
