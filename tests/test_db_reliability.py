"""
Tests for Part 2 — DB Reliability fixes.

Covers:
- messages.creator_slug migration is registered in main app (was operator-only)
- ensure_session_tables() failure is logged, not silently swallowed
- live show signup propagates creator_slug into contacts insert
"""

import os
import re

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# H1 — messages.creator_slug migration in main app (not just operator)
# ---------------------------------------------------------------------------

def test_messages_creator_slug_migration_in_main_app():
    """app/storage/postgres.py must include an ALTER TABLE messages ADD creator_slug.

    Without this, a fresh deploy of a new client will crash on save_message
    because operator/app/db.py owns the equivalent migration but only runs in
    the operator service, not the main app pod.
    """
    src = _read("app/storage/postgres.py")

    pattern = re.compile(
        r"ALTER\s+TABLE\s+messages\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+creator_slug",
        re.IGNORECASE,
    )
    assert pattern.search(src), (
        "app/storage/postgres.py must include "
        "'ALTER TABLE messages ADD COLUMN IF NOT EXISTS creator_slug ...'"
    )


def test_messages_creator_slug_has_default_zarna():
    """The migration must default existing rows to 'zarna' so single-tenant data is preserved."""
    src = _read("app/storage/postgres.py")
    # Find the messages.creator_slug ALTER and check its DEFAULT
    match = re.search(
        r"ALTER\s+TABLE\s+messages\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+creator_slug[^,\"]*",
        src,
        re.IGNORECASE,
    )
    assert match, "messages.creator_slug ALTER not found"
    snippet = match.group(0).lower()
    assert "default" in snippet and "'zarna'" in snippet, (
        f"messages.creator_slug must DEFAULT 'zarna' for backward compatibility. Got: {snippet}"
    )


# ---------------------------------------------------------------------------
# H2 — ensure_session_tables failure is logged, not swallowed
# ---------------------------------------------------------------------------

def test_ensure_session_tables_failure_is_logged():
    """_ensure_tables must log (not silently swallow) ensure_session_tables failures."""
    src = _read("app/storage/postgres.py")

    # Find the try block that calls ensure_session_tables
    idx = src.find("ensure_session_tables()")
    assert idx != -1, "ensure_session_tables call not found"

    # Look at the next ~400 chars (covers the except block)
    snippet = src[idx: idx + 600]

    # Must NOT be `except Exception: pass`
    assert not re.search(r"except\s+Exception\s*:\s*\n\s*pass", snippet), (
        "ensure_session_tables failure must be logged, not swallowed with `pass`. "
        f"Snippet:\n{snippet}"
    )

    # Must contain logger.exception or logging.exception in the except block
    assert ".exception(" in snippet, (
        "ensure_session_tables except block must call logger.exception() to surface failures"
    )


# ---------------------------------------------------------------------------
# H3 — live show signup tags contacts with the right creator_slug
# ---------------------------------------------------------------------------

def test_add_signup_signature_accepts_creator_slug():
    """repository.add_signup must accept a creator_slug parameter."""
    from app.live_shows.repository import add_signup
    import inspect

    sig = inspect.signature(add_signup)
    assert "creator_slug" in sig.parameters, (
        "add_signup must accept a creator_slug parameter so multi-tenant deployments "
        "can tag new contacts with the correct creator"
    )

    # Default must be backward-compatible (None or 'zarna' or env-driven)
    default = sig.parameters["creator_slug"].default
    assert default is None or default == "zarna", (
        f"creator_slug default must be None (resolve at call time) or 'zarna'. Got: {default!r}"
    )


def test_add_signup_inserts_creator_slug_into_contacts():
    """add_signup's contacts INSERT must include creator_slug column and parameter."""
    src = _read("app/live_shows/repository.py")

    fn_start = src.find("def add_signup(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    # The contacts INSERT must include creator_slug
    contacts_insert_match = re.search(
        r"INSERT\s+INTO\s+contacts\s*\([^)]+\)",
        fn_body,
        re.IGNORECASE,
    )
    assert contacts_insert_match, "add_signup must INSERT INTO contacts"
    cols = contacts_insert_match.group(0).lower()
    assert "creator_slug" in cols, (
        f"contacts INSERT must include creator_slug column. Got: {cols}"
    )


def test_try_live_show_signup_signature_accepts_creator_slug():
    """signup.try_live_show_signup must accept and forward creator_slug."""
    from app.live_shows.signup import try_live_show_signup
    import inspect

    sig = inspect.signature(try_live_show_signup)
    assert "creator_slug" in sig.parameters, (
        "try_live_show_signup must accept creator_slug for multi-tenant tagging"
    )


def test_try_live_show_signup_forwards_creator_slug_to_add_signup():
    """try_live_show_signup must pass creator_slug through to repo.add_signup."""
    src = _read("app/live_shows/signup.py")
    fn_start = src.find("def try_live_show_signup(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert re.search(
        r"add_signup\([^)]*creator_slug\s*=\s*creator_slug",
        fn_body,
        re.DOTALL,
    ), "try_live_show_signup must forward creator_slug=... to repo.add_signup"


def test_main_safe_signup_passes_brain_slug():
    """main._safe_try_live_show_signup must pass brain.slug to try_live_show_signup."""
    src = _read("main.py")
    fn_start = src.find("def _safe_try_live_show_signup(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert re.search(
        r"try_live_show_signup\([^)]*creator_slug\s*=",
        fn_body,
        re.DOTALL,
    ), (
        "_safe_try_live_show_signup must pass creator_slug= to try_live_show_signup"
    )
    assert "brain" in fn_body and "slug" in fn_body, (
        "_safe_try_live_show_signup must read the slug from the brain (getattr(brain, 'slug'))"
    )
