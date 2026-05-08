"""
Tests for Part 6 — Cleanup.

Covers:
- Dead code removed: portal_interactive.py / reporting.py / content.py gone
- Stale comment fixed: dedup window says "1000" not "200"
- main.py docstring still references operator portal (canonical implementation)
- postgres.py migration list smoke tests (validates table coverage and structure)
"""

import os
import re

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# L1, L2, L3 — Dead code removed
# ---------------------------------------------------------------------------

def test_portal_interactive_deleted():
    """app/smb/portal_interactive.py was a 600+ line unregistered blueprint — must be gone."""
    path = os.path.join(PROJECT_ROOT, "app", "smb", "portal_interactive.py")
    assert not os.path.exists(path), (
        f"{path} should be deleted — duplicate of operator/app/routes/smb_portal.py"
    )


def test_smb_reporting_stub_deleted():
    """app/smb/reporting.py was a 2-line stub — must be gone."""
    path = os.path.join(PROJECT_ROOT, "app", "smb", "reporting.py")
    assert not os.path.exists(path), f"{path} should be deleted — empty stub"


def test_smb_content_stub_deleted():
    """app/smb/content.py was a 1-line stub — must be gone."""
    path = os.path.join(PROJECT_ROOT, "app", "smb", "content.py")
    assert not os.path.exists(path), f"{path} should be deleted — empty stub"


def test_main_py_no_longer_imports_portal_interactive():
    """main.py must not import portal_interactive_bp (the file is gone)."""
    src = _read("main.py")
    assert "portal_interactive_bp" not in src, (
        "main.py should not reference portal_interactive_bp — file deleted in cleanup"
    )
    assert "from app.smb.portal_interactive" not in src, (
        "main.py should not import from app.smb.portal_interactive (deleted)"
    )


def test_main_py_documents_canonical_portal_location():
    """main.py should still document where the canonical interactive portal lives."""
    src = _read("main.py")
    # Helpful comment for future maintainers
    assert "operator/app/routes/smb_portal.py" in src, (
        "main.py should keep a comment pointing to the canonical interactive portal "
        "(operator/app/routes/smb_portal.py) so future contributors know where it lives"
    )


# ---------------------------------------------------------------------------
# Stale comment fix
# ---------------------------------------------------------------------------

def test_dedup_comment_matches_actual_buffer_size():
    """main.py dedup comment must say '1000' to match _MAX_SEEN, not the stale '200'."""
    src = _read("main.py")

    # _MAX_SEEN should be 1000 (current value)
    match = re.search(r"_MAX_SEEN\s*=\s*(\d+)", src)
    assert match, "_MAX_SEEN constant not found"
    actual = int(match.group(1))
    assert actual == 1000, f"_MAX_SEEN expected 1000, got {actual}"

    # The header comment block must reference the same number, not '200'
    idx = src.find("Deduplication:")
    assert idx != -1, "Deduplication comment header not found"
    header = src[idx: idx + 300]
    assert "1000" in header, (
        f"Deduplication header comment must mention {actual} message IDs (was the stale '200')"
    )
    assert "200 message IDs" not in header, (
        "Deduplication header still references stale '200' — should be '1000'"
    )


# ---------------------------------------------------------------------------
# H14 — postgres.py migration smoke tests
# ---------------------------------------------------------------------------

def test_postgres_migrations_cover_all_known_tables():
    """app/storage/postgres.py migrations must reference every table the code reads/writes."""
    src = _read("app/storage/postgres.py")

    # Tables that the main app reads or writes — every one must have a CREATE TABLE migration
    required_tables = (
        "contacts",
        "messages",
        "live_shows",
        "live_show_signups",
        "live_broadcast_jobs",
        "quiz_sessions",
        "quiz_responses",
        "blast_context_sessions",
        "winning_examples_corpus",
        "sms_cost_log",
    )
    for table in required_tables:
        pattern = rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{table}\b"
        assert re.search(pattern, src, re.IGNORECASE), (
            f"app/storage/postgres.py must include CREATE TABLE IF NOT EXISTS {table}"
        )


def test_postgres_migration_tuples_are_well_formed():
    """The migration tuples must be tuples (not lists) and contain only string SQL statements."""
    src = _read("app/storage/postgres.py")

    # All known migration constants
    constants = (
        "_LIVE_SHOW_MIGRATIONS",
        "_LIVE_SHOW_ADDITIVE_MIGRATIONS",
        "_ENGAGEMENT_ANALYTICS_MIGRATIONS",
        "_QUIZ_MIGRATIONS",
        "_QUALITY_DIGEST_MIGRATIONS",
        "_WINNING_EXAMPLES_MIGRATIONS",
        "_SMB_MIGRATIONS",
        "_SMB_SHOW_MIGRATIONS",
        "_SMB_OUTREACH_MIGRATIONS",
    )
    for name in constants:
        # Must be assigned with `_NAME = (`
        assert re.search(rf"^{name}\s*=\s*\(", src, re.MULTILINE), (
            f"{name} must be defined as a tuple (note the trailing comma is fine)"
        )


def test_messages_migrations_include_engagement_columns():
    """Engagement analytics columns must all be ALTER TABLE messages additions.

    Note: creator_slug is asserted separately in test_db_reliability.py because
    that migration ships in the Part 2 branch (fix/db-reliability).
    """
    src = _read("app/storage/postgres.py")

    required_columns = (
        "intent",
        "tone_mode",
        "routing_tier",
        "ai_cost_usd",
        "sms_segments",
        "did_user_reply",
    )
    for col in required_columns:
        pattern = rf"ALTER\s+TABLE\s+messages\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+{col}\b"
        assert re.search(pattern, src, re.IGNORECASE), (
            f"app/storage/postgres.py must include ALTER TABLE messages ADD COLUMN IF NOT EXISTS {col}"
        )


def test_postgres_uses_advisory_lock_for_migration_serialization():
    """Migrations must run inside pg_advisory_xact_lock to serialize across gunicorn workers."""
    src = _read("app/storage/postgres.py")

    # The original deadlock fix — ensure the lock is still in place
    assert "pg_advisory_xact_lock" in src, (
        "_ensure_tables must use pg_advisory_xact_lock to serialize concurrent worker startup; "
        "without it, parallel ALTER TABLE deadlocks return"
    )


def test_postgres_creator_slug_is_normalized():
    """PostgresStorage._creator_slug must be lowercased + stripped at init."""
    src = _read("app/storage/postgres.py")

    # The init logic should call .strip().lower()
    init_match = re.search(
        r"self\._creator_slug\s*=\s*\([^)]*\)\.strip\(\)\.lower\(\)",
        src,
    )
    assert init_match, (
        "PostgresStorage.__init__ must normalize creator_slug via .strip().lower() to prevent "
        "case-sensitive duplication of contacts/messages rows across casing variants"
    )
