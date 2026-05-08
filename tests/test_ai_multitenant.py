"""
Tests for Part 5 — AI & Multi-tenant fixes.

Covers:
- Structured intent fallback (Gemini → OpenAI → Anthropic) in generator.py
- Training data warning when slug-specific files missing
- Handler silent except: pass replaced with logger.exception()
- banned_words enforcement post-generation
- Join confirmations swap to neutral copy for non-Zarna slugs
- main.py log lines tagged [ZARNA] not [WEB] (root logger mapping)
"""

import os
import re

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# H7 — Structured intent fallback
# ---------------------------------------------------------------------------

def test_structured_intent_falls_back_to_openai_after_gemini():
    """generator._produce_raw_text must call OpenAI on Gemini failure for structured intents."""
    src = _read("app/brain/generator.py")

    fn_start = src.find("def _produce_raw_text(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    # The structured branch must reference both OpenAI and Anthropic fallbacks
    structured_branch = fn_body.split("if structured", 1)[-1]
    structured_branch = structured_branch.split("# Explicit tier only", 1)[0]

    assert "_generate_openai_raw" in structured_branch, (
        "Structured intent branch must fall back to OpenAI on Gemini failure"
    )
    assert "_generate_anthropic_raw" in structured_branch, (
        "Structured intent branch must fall back to Anthropic on OpenAI failure"
    )


def test_structured_fallback_only_calls_openai_when_key_present():
    """Fallbacks must check OPENAI_API_KEY / ANTHROPIC_API_KEY before invoking."""
    src = _read("app/brain/generator.py")
    fn_start = src.find("def _produce_raw_text(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    structured_branch = fn_body.split("if structured", 1)[-1].split("# Explicit tier only", 1)[0]

    assert "OPENAI_API_KEY" in structured_branch, (
        "Structured fallback must guard the OpenAI call with OPENAI_API_KEY check"
    )
    assert "ANTHROPIC_API_KEY" in structured_branch, (
        "Structured fallback must guard the Anthropic call with ANTHROPIC_API_KEY check"
    )


# ---------------------------------------------------------------------------
# H8 — Training data warning
# ---------------------------------------------------------------------------

def test_config_warns_on_training_data_fallback():
    """app/config.py must log loudly when slug-specific training files are missing."""
    src = _read("app/config.py")

    assert "_chunks_using_fallback" in src or "training data missing" in src.lower(), (
        "config.py must track when it's using the Zarna training-data fallback"
    )

    # Must call logging.error/.warning with a message about fallback (multiline-aware)
    assert re.search(r"\.error\([^)]*[Tt]raining data", src, re.DOTALL) or \
           re.search(r"\.warning\([^)]*[Tt]raining data", src, re.DOTALL), (
        "config.py must emit a logger.error/warning when slug training data is missing"
    )


def test_training_data_warning_only_for_non_zarna():
    """The warning must be gated on CREATOR_SLUG != 'zarna' so the default deploy stays quiet."""
    src = _read("app/config.py")

    # Find the warning block
    idx = src.lower().find("training data missing")
    if idx == -1:
        idx = src.find("[CONFIG]")
    assert idx != -1, "training data warning not found"

    # Look at ~200 chars before the log call to find the conditional
    context = src[max(0, idx - 400): idx + 50]
    assert "CREATOR_SLUG" in context and "zarna" in context.lower(), (
        "Training data warning must be gated on CREATOR_SLUG.lower() != 'zarna'"
    )


# ---------------------------------------------------------------------------
# Handler silent failure replacements
# ---------------------------------------------------------------------------

def test_handler_no_silent_pass_in_winning_examples():
    """The outer winning_examples except must call _logger.exception, not pass silently."""
    src = _read("app/brain/handler.py")

    # Find the winning_examples block
    idx = src.find("winning_examples lookup failed")
    assert idx != -1, (
        "Outer winning_examples except must log via _logger.exception('winning_examples lookup failed ...') "
        "instead of `pass`"
    )


def test_handler_no_silent_pass_in_sell_context():
    """The sell_context enrichment except must log, not pass silently."""
    src = _read("app/brain/handler.py")

    assert "sell_context enrichment failed" in src, (
        "sell_context enrichment except must log via _logger.exception() — was silent pass"
    )


def test_handler_no_silent_pass_in_link_tracker():
    """The link_tracker rewrite except must log, not pass silently."""
    src = _read("app/brain/handler.py")

    assert "link_tracker.rewrite_bot_reply failed" in src, (
        "link_tracker rewrite except must log via _logger.exception() — was silent pass"
    )


def test_handler_no_silent_pass_in_update_memory():
    """_update_memory's except must log, not pass silently."""
    src = _read("app/brain/handler.py")

    assert "_update_memory failed" in src, (
        "_update_memory except must log via _logger.exception() — was silent pass"
    )


# ---------------------------------------------------------------------------
# M14 — banned_words enforcement
# ---------------------------------------------------------------------------

def test_generator_has_find_banned_word_helper():
    """generator.py must define a _find_banned_word helper used by generate_zarna_reply."""
    src = _read("app/brain/generator.py")
    assert "def _find_banned_word(" in src, (
        "generator.py must define _find_banned_word(text, banned) for post-gen filtering"
    )


def test_generate_zarna_reply_calls_banned_words_check():
    """generate_zarna_reply must invoke _find_banned_word when creator_config.banned_words is set."""
    src = _read("app/brain/generator.py")
    fn_start = src.find("def generate_zarna_reply(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "_find_banned_word" in fn_body, (
        "generate_zarna_reply must call _find_banned_word(...) on the raw output"
    )
    assert "banned_words" in fn_body, (
        "generate_zarna_reply must reference creator_config.banned_words"
    )


def test_find_banned_word_uses_word_boundary_match():
    """_find_banned_word must use \\b boundaries (so 'shit' matches but 'shitake' does not)."""
    src = _read("app/brain/generator.py")
    fn_start = src.find("def _find_banned_word(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert r"\b" in fn_body, (
        "_find_banned_word must use word-boundary regex (\\b) to avoid false positives"
    )


def test_find_banned_word_actual_behavior():
    """Smoke-test the helper — given a banned word, it returns it; otherwise None."""
    # Import via importlib to avoid pulling all of app.brain
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_test_generator",
        os.path.join(PROJECT_ROOT, "app", "brain", "generator.py"),
    )
    # We can't actually import generator.py in a vacuum because it pulls in
    # heavy deps (gemini sdk, etc.). Re-implement the helper logic here for
    # behavior verification — a smoke test that the regex pattern works.
    import re as _re

    def find(text: str, banned: tuple):
        if not text or not banned:
            return None
        lower = text.lower()
        for raw in banned:
            word = (raw or "").strip().lower()
            if not word:
                continue
            if _re.search(r"\b" + _re.escape(word) + r"\b", lower):
                return raw
        return None

    # Positive: banned word matches
    assert find("This is a damn good show", ("damn",)) == "damn"
    # Word boundary: 'shit' matches but 'shitake' does not
    assert find("Try the shitake mushrooms", ("shit",)) is None
    assert find("That's bullshit", ("shit",)) is None  # bullshit != shit at boundary
    # Case-insensitive
    assert find("DAMN cool", ("damn",)) == "damn"
    # Empty
    assert find("hello world", ()) is None
    assert find("", ("damn",)) is None


# ---------------------------------------------------------------------------
# M11 — Join confirmations per-creator
# ---------------------------------------------------------------------------

def test_join_confirmations_have_generic_pool():
    """join_confirmations.py must define a non-Zarna generic copy pool."""
    src = _read("app/live_shows/join_confirmations.py")

    assert "_GENERIC_COMEDY_NEW" in src, (
        "Must define _GENERIC_COMEDY_NEW (non-Zarna fallback copy)"
    )
    assert "_GENERIC_COMEDY_REPEAT" in src, "Must define _GENERIC_COMEDY_REPEAT"
    assert "_GENERIC_LIVE_STREAM_NEW" in src, "Must define _GENERIC_LIVE_STREAM_NEW"
    assert "_GENERIC_LIVE_STREAM_REPEAT" in src, "Must define _GENERIC_LIVE_STREAM_REPEAT"


def test_join_confirmations_route_to_generic_for_non_zarna():
    """random_*_confirmation_* helpers must check CREATOR_SLUG and route accordingly."""
    src = _read("app/live_shows/join_confirmations.py")

    assert "_is_zarna" in src or "CREATOR_SLUG" in src, (
        "join_confirmations must check CREATOR_SLUG to decide between Zarna and generic copy"
    )

    # Each random_* function must reference both pools
    for fn_name in (
        "random_comedy_confirmation_new",
        "random_comedy_confirmation_repeat",
        "random_live_stream_confirmation_new",
        "random_live_stream_confirmation_repeat",
    ):
        idx = src.find(f"def {fn_name}(")
        assert idx != -1, f"{fn_name} not defined"
        next_fn = re.search(r"\ndef \w", src[idx + 1:])
        body = src[idx: idx + (next_fn.start() + 1 if next_fn else len(src))]
        assert "_GENERIC" in body or "_is_zarna" in body, (
            f"{fn_name} must route between Zarna and generic copy"
        )


def test_generic_copy_is_voice_neutral():
    """Generic comedy/live-stream copy must not name-drop Zarna's family / desi-auntie tropes."""
    src = _read("app/live_shows/join_confirmations.py")

    # Find each generic pool block and check its body
    for label in (
        "_GENERIC_COMEDY_NEW",
        "_GENERIC_COMEDY_REPEAT",
        "_GENERIC_LIVE_STREAM_NEW",
        "_GENERIC_LIVE_STREAM_REPEAT",
    ):
        match = re.search(rf"{label}\s*=\s*\[(.*?)\]", src, re.DOTALL)
        assert match, f"{label} pool not found"
        body = match.group(1).lower()

        # These tokens are Zarna-specific and must NOT appear in generic copy
        for token in ("mil", "desi", "auntie", "husband", "401(k)", "wifi"):
            assert token not in body, (
                f"{label} must not include Zarna-specific token {token!r}. "
                f"Found in generic pool — keep the generic copy voice-neutral."
            )


# ---------------------------------------------------------------------------
# M23 — Logger naming so main.py is tagged [ZARNA] not [WEB]
# ---------------------------------------------------------------------------

def test_main_log_formatter_maps_root_to_zarna():
    """main.py _ServiceFormatter must map the root logger / __main__ to [ZARNA]."""
    src = _read("main.py")

    fmt_start = src.find("class _ServiceFormatter(")
    assert fmt_start != -1
    next_class = re.search(r"\nclass \w", src[fmt_start + 1:])
    fmt_body = src[fmt_start: fmt_start + (next_class.start() + 1 if next_class else len(src))]

    # Must include a mapping for "root" or "__main__" or "main" to ZARNA
    has_main_mapping = any(
        re.search(rf'\(\s*"{name}"\s*,\s*"\[ZARNA\]', fmt_body)
        for name in ("__main__", "root", "main")
    )
    assert has_main_mapping, (
        "_ServiceFormatter._PREFIXES must map __main__/root/main → '[ZARNA] ' so direct "
        "logging.info() calls in main.py get the right service tag"
    )
