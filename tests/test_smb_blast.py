"""
Tests for app/smb/blast.py

Pure logic tests run without any mocking.
Integration tests mock the broadcast infrastructure and DB.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import threading
import time
from unittest.mock import patch, MagicMock, call

from app.smb.blast import (
    is_blast_command,
    handle_owner_blast,
    _format_blast,
    _run_blast_async,
)
from app.smb.tenants import BusinessTenant


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_TRIGGERS = [
    "opening", "seats available", "tonight",
    "last minute", "discount", "deal", "special", "show",
]

def _make_tenant(blast_triggers=None):
    triggers = _DEFAULT_TRIGGERS if blast_triggers is None else blast_triggers
    return BusinessTenant(
        slug="west_side_comedy",
        display_name="West Side Comedy Club",
        business_type="comedy_club",
        sms_number=None,
        owner_phone="+15550001111",
        keyword="COMEDY",
        tone="fun and casual",
        value_content_topics=[],
        signup_questions=[],
        blast_triggers=triggers,
    )


# ---------------------------------------------------------------------------
# is_blast_command — pure logic
# ---------------------------------------------------------------------------

def test_is_blast_command_detects_trigger_words():
    tenant = _make_tenant()
    assert is_blast_command("Opening tonight at 8pm!", tenant) is True
    assert is_blast_command("Last minute deal — 20% off", tenant) is True
    assert is_blast_command("Seats available right now", tenant) is True
    assert is_blast_command("Great show tonight", tenant) is True
    print("✓ is_blast_command detects trigger words case-insensitively")


def test_is_blast_command_rejects_non_triggers():
    tenant = _make_tenant()
    assert is_blast_command("Hey how are you", tenant) is False
    assert is_blast_command("What time do you close?", tenant) is False
    assert is_blast_command("", tenant) is False
    print("✓ is_blast_command correctly rejects non-trigger messages")


def test_is_blast_command_no_triggers_configured():
    tenant = _make_tenant(blast_triggers=[])
    assert is_blast_command("opening tonight", tenant) is False
    print("✓ is_blast_command returns False when no triggers configured")


def test_is_blast_command_partial_word_match():
    """Trigger 'deal' should match 'great deal tonight' but not mis-match unrelated words."""
    tenant = _make_tenant()
    assert is_blast_command("great deal tonight!", tenant) is True
    assert is_blast_command("I idealized this place", tenant) is False
    print("✓ is_blast_command partial word matching works correctly")


# ---------------------------------------------------------------------------
# _format_blast — pure logic
# ---------------------------------------------------------------------------

def test_format_blast_prepends_business_name_when_absent():
    tenant = _make_tenant()
    result = _format_blast("Opening tonight 8pm — 20% off", tenant)
    assert result.startswith("West Side Comedy Club:")
    assert "Opening tonight" in result
    print("✓ _format_blast prepends business name when not already in message")


def test_format_blast_does_not_double_prepend():
    tenant = _make_tenant()
    msg = "West Side Comedy Club has openings tonight!"
    result = _format_blast(msg, tenant)
    assert result.count("West Side Comedy Club") == 1
    print("✓ _format_blast does not prepend if business name already present")


def test_format_blast_preserves_original_message():
    tenant = _make_tenant()
    original = "Seats available at 9pm — grab them fast!"
    result = _format_blast(original, tenant)
    assert original in result
    print("✓ _format_blast preserves original message content")


# ---------------------------------------------------------------------------
# handle_owner_blast — non-blast returns help text
# ---------------------------------------------------------------------------

def test_handle_owner_blast_non_command_returns_help():
    tenant = _make_tenant()
    reply = handle_owner_blast("+15550001111", "How are you?", tenant)
    assert reply is not None
    assert "opening" in reply.lower() or "trigger" in reply.lower() or "example" in reply.lower()
    print("✓ non-blast owner message returns helpful guidance")


def test_handle_owner_blast_help_shows_trigger_examples():
    tenant = _make_tenant()
    reply = handle_owner_blast("+15550001111", "just checking in", tenant)
    # Should include at least one trigger word as an example
    assert any(t in reply.lower() for t in tenant.blast_triggers)
    print("✓ help reply includes trigger word examples")


# ---------------------------------------------------------------------------
# handle_owner_blast — blast command fires async thread
# ---------------------------------------------------------------------------

def test_handle_owner_blast_returns_confirmation_immediately():
    tenant = _make_tenant()
    with patch("app.smb.blast._run_blast_async") as mock_run:
        # Patch threading.Thread so it doesn't actually spawn
        with patch("app.smb.blast.threading.Thread") as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            reply = handle_owner_blast(
                "+15550001111", "Opening tonight 8pm — 20% off!", tenant
            )

    assert "blast queued" in reply.lower() or "sending" in reply.lower()
    mock_thread.assert_called_once()
    mock_thread_instance.start.assert_called_once()
    print("✓ blast command returns instant confirmation and spawns thread")


def test_handle_owner_blast_thread_receives_correct_args():
    tenant = _make_tenant()
    captured = {}

    def capture_thread(**kwargs):
        captured.update(kwargs)
        m = MagicMock()
        m.start = lambda: None
        return m

    with patch("app.smb.blast.threading.Thread", side_effect=capture_thread):
        handle_owner_blast("+15550001111", "Opening tonight 8pm!", tenant)

    assert captured.get("target") == _run_blast_async
    assert captured.get("args") == ("Opening tonight 8pm!", tenant)
    assert captured.get("daemon") is True
    print("✓ blast thread spawned with correct target, args, and daemon=True")


# ---------------------------------------------------------------------------
# _run_blast_async — end-to-end with mocked broadcast + DB
# ---------------------------------------------------------------------------

def test_run_blast_async_sends_to_all_active_subscribers():
    tenant = _make_tenant()

    fake_subscribers = [
        {"id": 1, "phone_number": "+15550002222"},
        {"id": 2, "phone_number": "+15550003333"},
    ]

    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_result = MagicMock()
    mock_result.attempted = 2
    mock_result.succeeded = 2
    mock_result.failed = 0

    with patch("app.smb.blast.get_db_connection", return_value=mock_conn):
        with patch("app.smb.storage.get_active_subscribers", return_value=fake_subscribers):
            with patch("app.smb.blast.run_loop_broadcast", return_value=mock_result) as mock_broadcast:
                with patch("app.smb.blast._record_blast") as mock_record:
                    with patch("app.smb.blast.resolve_broadcast_provider", return_value="twilio"):
                        _run_blast_async("Opening tonight!", tenant)

    mock_broadcast.assert_called_once()
    call_kwargs = mock_broadcast.call_args.kwargs
    assert set(call_kwargs["phones"]) == {"+15550002222", "+15550003333"}
    assert "West Side Comedy Club" in call_kwargs["body"]
    assert call_kwargs["provider"] == "twilio"
    mock_record.assert_called_once()
    print("✓ _run_blast_async sends to all active subscribers with formatted body")


def test_run_blast_async_skips_when_no_subscribers():
    tenant = _make_tenant()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch("app.smb.blast.get_db_connection", return_value=mock_conn):
        with patch("app.smb.storage.get_active_subscribers", return_value=[]):
            with patch("app.smb.blast.run_loop_broadcast") as mock_broadcast:
                _run_blast_async("Opening tonight!", tenant)

    mock_broadcast.assert_not_called()
    print("✓ _run_blast_async skips broadcast when no active subscribers")


def test_run_blast_async_skips_when_no_db():
    tenant = _make_tenant()
    with patch("app.smb.blast.get_db_connection", return_value=None):
        with patch("app.smb.blast.run_loop_broadcast") as mock_broadcast:
            _run_blast_async("Opening tonight!", tenant)

    mock_broadcast.assert_not_called()
    print("✓ _run_blast_async exits cleanly when DB is unavailable")
