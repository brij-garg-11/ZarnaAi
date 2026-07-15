#!/usr/bin/env python3
"""
Provision (or update) an ElevenLabs Conversational AI agent that runs on a
creator's private voice clone and is driven by our own ZarnaBrain via the
OpenAI-compatible custom-LLM endpoint (app/voice/openai_llm.py).

Why this exists: Twilio ConversationRelay's native ElevenLabs TTS can only resolve
voices in ElevenLabs' *public* library, so a *private* clone returns Twilio error
64112. Running the agent on ElevenLabs keeps the clone private and gives the
lowest-latency path for an ElevenLabs voice, while ZarnaBrain still decides every
word through our endpoint.

The script is idempotent and slug-based, so it works for any future client:
  1. Stores VOICE_LLM_API_KEY as a workspace secret (reuses one if already present).
  2. Finds the agent by id (--agent-id) or by name, else creates it, then PATCHes its
     TTS (voice clone), first message (greeting), and custom-LLM config.
  3. Imports the Twilio number into ElevenLabs (if not already imported) and assigns
     the agent so inbound calls route to it.

Reads everything from env + creator_config/<slug>.json so no secrets live in code.

Required env:
  ELEVENLABS_API_KEY   ElevenLabs API key (workspace admin).
  VOICE_LLM_API_KEY    Shared bearer secret the voice service expects.
  VOICE_PUBLIC_URL     Public https base URL of the deployed voice service
                       (e.g. https://zarnavoice-production.up.railway.app).
Optional env:
  CREATOR_SLUG         Creator config slug (default: zarna).
  TWILIO_ACCOUNT_SID   Twilio Account SID  (needed only to import the number).
  TWILIO_AUTH_TOKEN    Twilio Auth Token   (needed only to import the number).
  TWILIO_PHONE_NUMBER  E.164 number to import/assign (e.g. +18556081717).
  AGENT_ID             Existing agent id to update (skips name lookup/create).

Usage:
  python scripts/provision_elevenlabs_voice_agent.py
  python scripts/provision_elevenlabs_voice_agent.py --dry-run
  python scripts/provision_elevenlabs_voice_agent.py --skip-phone
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

BASE = "https://api.elevenlabs.io"

# ElevenLabs TTS model: flash v2 is the lowest-latency option (~75ms). English-locked
# agents reject the multilingual *_v2_5 models ("English Agents must use turbo or flash
# v2"), so flash v2 is the correct low-latency choice here.
TTS_MODEL = "eleven_flash_v2"
SECRET_NAME = "voice_llm_api_key"
AGENT_DISPLAY_NAME = "Zarna"


def _die(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _api(method: str, path: str, key: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"xi-api-key": key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        detail = e.read().decode()
        raise RuntimeError(f"{method} {path} -> {e.code}: {detail[:600]}") from None


def _load_voice_config(slug: str) -> dict:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "creator_config", f"{slug}.json")
    if not os.path.exists(path):
        _die(f"creator config not found: {path}")
    with open(path) as f:
        cfg = json.load(f)
    voice = cfg.get("voice") or {}
    if not voice.get("voice_id"):
        _die(f"creator_config/{slug}.json has no voice.voice_id")
    return voice


def _ensure_secret(key: str, value: str, dry_run: bool) -> str:
    """Return a secret_id for the bearer token, reusing an existing one by name."""
    _, listing = _api("GET", "/v1/convai/secrets", key)
    for s in listing.get("secrets", []):
        if s.get("name") == SECRET_NAME:
            print(f"  secret '{SECRET_NAME}' already exists -> {s['secret_id']}")
            return s["secret_id"]
    if dry_run:
        print(f"  [dry-run] would create secret '{SECRET_NAME}'")
        return "SECRET_ID_PLACEHOLDER"
    _, resp = _api(
        "POST",
        "/v1/convai/secrets",
        key,
        {"type": "new", "name": SECRET_NAME, "value": value},
    )
    print(f"  created secret '{SECRET_NAME}' -> {resp['secret_id']}")
    return resp["secret_id"]


def _find_agent_id(key: str) -> str | None:
    if os.getenv("AGENT_ID"):
        return os.environ["AGENT_ID"].strip()
    _, listing = _api("GET", "/v1/convai/agents", key)
    for a in listing.get("agents", []):
        if a.get("name") == AGENT_DISPLAY_NAME:
            return a["agent_id"]
    return None


def _agent_config(voice: dict, llm_url: str, secret_id: str) -> dict:
    greeting = (voice.get("greeting") or "").strip()
    style = (voice.get("style_rules_text") or "").strip()
    language = (voice.get("language") or "en-US").split("-")[0]  # ElevenLabs wants ISO-639-1
    # ZarnaBrain supplies its own full persona/RAG and our endpoint drops the system
    # message, so this prompt is mostly a human-readable label. The custom_llm block is
    # what actually routes generation to our service. The caller_id line is the one
    # functional part: ElevenLabs substitutes {{system__caller_id}} with the caller's
    # phone number, and our endpoint parses it back out (app/voice/openai_llm.py) to
    # power known-caller personas (voice.known_callers in the creator config).
    system_prompt = (
        "Spoken phone persona for the creator. All replies are produced by the "
        "creator's own brain service via the custom LLM endpoint. " + style
        + "\ncaller_id: {{system__caller_id}}"
    )
    return {
        "conversation_config": {
            "agent": {
                "first_message": greeting,
                "language": language,
                "prompt": {
                    "prompt": system_prompt,
                    "llm": "custom-llm",
                    "custom_llm": {
                        "url": llm_url,
                        "model_id": "zarna-brain",
                        "api_key": {"secret_id": secret_id},
                        "api_type": "chat_completions",
                    },
                },
            },
            "tts": {
                "voice_id": voice["voice_id"],
                "model_id": TTS_MODEL,
                "optimize_streaming_latency": 3,
            },
            "turn": {"turn_eagerness": "normal", "turn_timeout": 7},
        }
    }


def _import_and_assign_phone(key: str, agent_id: str, dry_run: bool):
    number = (os.getenv("TWILIO_PHONE_NUMBER") or "").strip()
    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not (number and sid and token):
        print("  skipping phone import (TWILIO_PHONE_NUMBER/SID/AUTH_TOKEN not all set)")
        return

    _, existing = _api("GET", "/v1/convai/phone-numbers", key)
    rows = existing if isinstance(existing, list) else existing.get("phone_numbers", [])
    for row in rows:
        if row.get("phone_number") == number:
            pid = row.get("phone_number_id")
            print(f"  number {number} already imported -> {pid}")
            if dry_run:
                print(f"  [dry-run] would assign agent {agent_id} to {pid}")
                return
            _api("PATCH", f"/v1/convai/phone-numbers/{pid}", key, {"agent_id": agent_id})
            print(f"  assigned agent to {number}")
            return

    if dry_run:
        print(f"  [dry-run] would import {number} from Twilio and assign agent {agent_id}")
        return
    _, resp = _api(
        "POST",
        "/v1/convai/phone-numbers",
        key,
        {
            "phone_number": number,
            "label": f"{AGENT_DISPLAY_NAME} voice line",
            "provider": "twilio",
            "sid": sid,
            "token": token,
            "agent_id": agent_id,
        },
    )
    print(f"  imported {number} and assigned agent -> {resp.get('phone_number_id')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print actions without mutating.")
    ap.add_argument("--skip-phone", action="store_true", help="Don't import/assign the number.")
    args = ap.parse_args()

    key = (os.getenv("ELEVENLABS_API_KEY") or "").strip()
    bearer = (os.getenv("VOICE_LLM_API_KEY") or "").strip()
    public = (os.getenv("VOICE_PUBLIC_URL") or "").strip().rstrip("/")
    slug = (os.getenv("CREATOR_SLUG") or "zarna").strip().lower()
    if not key:
        _die("ELEVENLABS_API_KEY is not set")
    if not bearer:
        _die("VOICE_LLM_API_KEY is not set")
    if not public:
        _die("VOICE_PUBLIC_URL is not set (e.g. https://zarnavoice-production.up.railway.app)")

    llm_url = f"{public}/v1/chat/completions"
    voice = _load_voice_config(slug)

    print(f"Provisioning ElevenLabs voice agent for slug='{slug}'")
    print(f"  voice_id   = {voice['voice_id']}")
    print(f"  llm_url    = {llm_url}")

    print("Secret:")
    secret_id = _ensure_secret(key, bearer, args.dry_run)

    print("Agent:")
    agent_id = _find_agent_id(key)
    cfg = _agent_config(voice, llm_url, secret_id)
    if agent_id:
        print(f"  updating existing agent {agent_id}")
        if not args.dry_run:
            _api("PATCH", f"/v1/convai/agents/{agent_id}", key, cfg)
            print("  agent updated")
        else:
            print("  [dry-run] would PATCH agent config")
    else:
        print("  no agent found; creating new one")
        if not args.dry_run:
            body = {"name": AGENT_DISPLAY_NAME, **cfg}
            _, resp = _api("POST", "/v1/convai/agents/create", key, body)
            agent_id = resp.get("agent_id")
            print(f"  created agent {agent_id}")
        else:
            print("  [dry-run] would create agent")
            agent_id = "AGENT_ID_PLACEHOLDER"

    if not args.skip_phone:
        print("Phone:")
        _import_and_assign_phone(key, agent_id, args.dry_run)

    print("\nDone. Agent id:", agent_id)


if __name__ == "__main__":
    main()
