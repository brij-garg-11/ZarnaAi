"""
FastAPI app for the inbound phone-voice service.

Routes:
    GET  /health         -> liveness probe
    POST /twilio/voice   -> returns TwiML that hands the call to ConversationRelay
    WS   /voice/relay    -> ConversationRelay session (text in, reply text out)

This service is deployed separately from the main Flask app (see voice_main.py
and railway.voice.toml) and shares only the app.brain code, never the SMS
webhook handlers in main.py.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect

from app.brain.handler import get_brain
from app.voice.openai_llm import router as llm_router
from app.voice.voice_brain import generate_voice_reply

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("zarna.voice")

app = FastAPI(title="Zarna Voice")

# OpenAI-compatible custom-LLM endpoint for ElevenLabs Agents (/v1/chat/completions).
app.include_router(llm_router)


def _slug() -> str:
    return (os.getenv("CREATOR_SLUG") or "zarna").strip().lower()


def _voice_settings():
    """Return the VoiceSettings for the active creator (or None if unavailable)."""
    brain = get_brain(_slug())
    cc = getattr(brain, "creator_config", None)
    return getattr(cc, "voice", None)


def _wss_url(request: Request) -> str:
    """Public wss:// URL ConversationRelay should connect back to."""
    override = (os.getenv("VOICE_WSS_URL") or "").strip()
    if override:
        return override
    host = request.headers.get("x-forwarded-host") or request.url.netloc
    return f"wss://{host}/voice/relay"


def _validate_twilio_signature(request: Request, form: dict) -> bool:
    """Validate the Twilio voice webhook signature (fail-closed when a token is set)."""
    if (os.getenv("TWILIO_VALIDATE_SIGNATURE", "true").lower()) == "false":
        return True
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not token:
        # No token configured (typical for local dev) — allow but warn loudly.
        _logger.warning("[ZARNA] TWILIO_AUTH_TOKEN unset — skipping voice signature validation")
        return True
    try:
        from twilio.request_validator import RequestValidator
    except ImportError:
        _logger.error("[ZARNA] twilio package missing — cannot validate signature")
        return False
    validator = RequestValidator(token)
    sig = request.headers.get("X-Twilio-Signature", "")
    host = request.headers.get("x-forwarded-host") or request.url.netloc
    url = f"https://{host}{request.url.path}"
    if request.url.query:
        url += f"?{request.url.query}"
    return validator.validate(url, form, sig)


def _twiml(body: str) -> Response:
    xml = f'<?xml version="1.0" encoding="UTF-8"?>\n<Response>{body}</Response>'
    return Response(content=xml, media_type="text/xml")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "voice", "slug": _slug()}


@app.api_route("/twilio/voice", methods=["POST", "GET"])
async def twilio_voice(request: Request):
    """Answer an inbound call: hand it to ConversationRelay with the clone voice."""
    form = dict((await request.form())) if request.method == "POST" else {}

    if not _validate_twilio_signature(request, form):
        _logger.warning("[ZARNA] rejected voice webhook — bad Twilio signature")
        return Response(content="Forbidden", status_code=403)

    voice = _voice_settings()
    if not voice or not voice.enabled or not voice.voice_id:
        _logger.warning("[ZARNA] voice not enabled/configured for slug=%s — declining call", _slug())
        return _twiml(
            "<Say>Sorry, the voice line isn't available right now. "
            "Please send a text message instead.</Say><Hangup/>"
        )

    wss = _wss_url(request)
    greeting = html.escape(voice.greeting or "", quote=True)
    voice_id = html.escape(voice.voice_id, quote=True)
    language = html.escape(voice.language or "en-US", quote=True)
    provider = "ElevenLabs" if (voice.provider or "").lower() == "elevenlabs" else (voice.provider or "ElevenLabs")
    provider = html.escape(provider, quote=True)
    slug = html.escape(_slug(), quote=True)

    cr = (
        f'<Connect>'
        f'<ConversationRelay url="{html.escape(wss, quote=True)}" '
        f'welcomeGreeting="{greeting}" '
        f'ttsProvider="{provider}" '
        f'voice="{voice_id}" '
        f'language="{language}" '
        f'interruptible="true">'
        f'<Parameter name="slug" value="{slug}"/>'
        f'</ConversationRelay>'
        f'</Connect>'
    )
    _logger.info("[ZARNA] answering call slug=%s relay=%s", _slug(), wss)
    return _twiml(cr)


class _CallSession:
    """In-memory state for one live call (history powers multi-turn context)."""

    def __init__(self) -> None:
        self.call_sid: str = ""
        self.caller: str = ""
        self.slug: str = _slug()
        self.history: list[dict] = []

    def add(self, role: str, text: str) -> None:
        if text:
            self.history.append({"role": role, "text": text})
        # keep the context window bounded (last 16 turns is plenty for a call)
        if len(self.history) > 16:
            self.history = self.history[-16:]


@app.websocket("/voice/relay")
async def voice_relay(ws: WebSocket):
    """Handle one ConversationRelay session: text prompts in, reply text out."""
    await ws.accept()
    session = _CallSession()
    brain = get_brain(session.slug)
    _logger.info("[ZARNA] voice relay connected")

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                _logger.warning("[ZARNA] non-JSON relay message ignored")
                continue

            mtype = msg.get("type")

            if mtype == "setup":
                session.call_sid = msg.get("callSid", "") or ""
                session.caller = msg.get("from", "") or ""
                params = msg.get("customParameters") or {}
                slug = (params.get("slug") or "").strip().lower()
                if slug and slug != session.slug:
                    session.slug = slug
                    brain = get_brain(slug)
                _logger.info(
                    "[ZARNA] call setup sid=%s from=...%s slug=%s",
                    session.call_sid, (session.caller or "")[-4:], session.slug,
                )

            elif mtype == "prompt":
                user_text = (msg.get("voicePrompt") or "").strip()
                if not user_text:
                    continue
                _logger.info("[ZARNA] caller: %s", user_text)
                # Generation is blocking (LLM call) — run off the event loop.
                reply = await asyncio.to_thread(
                    generate_voice_reply, brain, user_text, session.history, session.caller
                )
                session.add("user", user_text)
                session.add("assistant", reply)
                _logger.info("[ZARNA] zarna: %s", reply)
                await ws.send_text(json.dumps({"type": "text", "token": reply, "last": True}))

            elif mtype == "interrupt":
                # Caller talked over the TTS — nothing to do for the non-streaming
                # v1 (we already finished sending). Logged for visibility.
                _logger.info("[ZARNA] caller interrupted")

            elif mtype == "error":
                _logger.warning("[ZARNA] relay error: %s", msg.get("description"))

            # other message types (dtmf, etc.) are ignored in v1

    except WebSocketDisconnect:
        _logger.info("[ZARNA] voice relay disconnected sid=%s", session.call_sid)
    except Exception:
        _logger.exception("[ZARNA] voice relay crashed")
        try:
            await ws.close()
        except Exception:
            pass
