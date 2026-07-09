"""
Google GenAI Client() rejects an empty GEMINI_API_KEY at import time.
"""
import os

from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

# Webhook tests post synthetic payloads with deterministic dedup keys. The
# cross-worker dedup would claim those keys in whatever real database
# DATABASE_URL points at (and the claims persist, breaking reruns), so the DB
# half of dedup is off in tests — the in-memory LRU path still runs. Tests
# that exercise the DB path re-enable it or stub the connection explicitly.
os.environ.setdefault("INBOUND_DEDUP_DB", "off")
