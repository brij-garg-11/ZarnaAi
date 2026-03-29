"""Shared GEMINI_API_KEY handling for tests (import-safe placeholder vs live API)."""
import os

PLACEHOLDER_KEY = "placeholder-key-for-test-import-only"


def ensure_placeholder_key_for_import() -> None:
    if not (os.environ.get("GEMINI_API_KEY") or "").strip():
        os.environ["GEMINI_API_KEY"] = PLACEHOLDER_KEY


def live_gemini_configured() -> bool:
    key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    return bool(key) and key != PLACEHOLDER_KEY
