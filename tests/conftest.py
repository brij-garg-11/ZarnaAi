"""
Google GenAI Client() rejects an empty GEMINI_API_KEY at import time.
"""
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()
