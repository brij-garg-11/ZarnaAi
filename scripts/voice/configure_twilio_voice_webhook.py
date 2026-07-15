"""
Point a Twilio number's Voice webhook at the voice service.

This automates the one go-live step that has a clean API: setting the number's
"A call comes in" Voice URL to the deployed voice service's /twilio/voice
endpoint. (Deploying the service, adding the ElevenLabs key in the Twilio
Console, and placing a real test call still have to be done by a human.)

Reads TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN from the environment (.env loaded
automatically). It looks up the number resource and updates voice_url +
voice_method, leaving SMS config untouched.

Usage:
    # set the voice webhook for Zarna's number
    python scripts/voice/configure_twilio_voice_webhook.py \
        --voice-url https://<voice-host>/twilio/voice

    # explicit number (defaults to TWILIO_PHONE_NUMBER, else +18556081717)
    python scripts/voice/configure_twilio_voice_webhook.py \
        --number +18556081717 \
        --voice-url https://<voice-host>/twilio/voice

    # preview without changing anything
    python scripts/voice/configure_twilio_voice_webhook.py \
        --voice-url https://<voice-host>/twilio/voice --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv()

_DEFAULT_NUMBER = "+18556081717"  # Zarna's voice-capable Twilio toll-free


def _client():
    from twilio.rest import Client

    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not sid or not token:
        print("error: TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN not set (add them to .env)", file=sys.stderr)
        raise SystemExit(2)
    return Client(sid, token)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--voice-url", required=True, help="public https URL of the voice service /twilio/voice endpoint")
    parser.add_argument("--number", default=os.getenv("TWILIO_PHONE_NUMBER") or _DEFAULT_NUMBER, help="E.164 number to configure")
    parser.add_argument("--dry-run", action="store_true", help="show what would change without updating")
    args = parser.parse_args()

    if not args.voice_url.startswith("https://"):
        print("error: --voice-url must be a public https:// URL (Twilio requires TLS)", file=sys.stderr)
        return 2

    client = _client()

    matches = client.incoming_phone_numbers.list(phone_number=args.number, limit=5)
    if not matches:
        print(f"error: number {args.number} not found on this Twilio account", file=sys.stderr)
        return 1
    number = matches[0]

    print(f"number:        {number.phone_number} (sid={number.sid})")
    print(f"current voice: {number.voice_url or '(none)'} [{number.voice_method}]")
    print(f"new voice:     {args.voice_url} [POST]")

    if args.dry_run:
        print("dry-run: no changes made")
        return 0

    number.update(voice_url=args.voice_url, voice_method="POST")
    print("updated: voice webhook is now live on this number")
    print("reminder: also add your ElevenLabs API key in the Twilio Console (ConversationRelay TTS) before calling.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
