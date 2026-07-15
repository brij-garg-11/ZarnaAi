"""
Entry point for the inbound phone-voice service (separate Railway deployment).

Run locally:
    uvicorn app.voice.server:app --host 0.0.0.0 --port 8080
or:
    python voice_main.py

This is intentionally separate from main.py (the Flask SMS app) so the voice
WebSocket server runs on an ASGI stack (uvicorn) and the live SMS pipeline is
never affected by voice changes.
"""
from __future__ import annotations

import os

from app.voice.server import app  # noqa: F401  (re-exported for `uvicorn voice_main:app`)

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("app.voice.server:app", host="0.0.0.0", port=port, log_level="info")
