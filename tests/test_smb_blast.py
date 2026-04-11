"""
Tests for app/smb/blast.py

The blast flow is now fully AI-driven using conversation history:
  1. Owner sends a blast message  →  AI returns CLARIFY (ask who to send to)
  2. Owner picks an audience      →  AI returns SEND_BLAST (fire the blast)

All AI calls are mocked so tests run fast and offline.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import threading
from unittest.mock import patch, MagicMock

from app.smb.blast import (
    _ai_decide_owner_action,
    handle_owner_blast,
    _run_blast_async,
    _seg_display_name,
)
from app.smb.tenants import BusinessTenant


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tenant(segments=None):
    segs = segments if segments is not None else [
        {
            "name": "STANDUP",
            "question_key": "0",
            "answers": ["standup", "stand-up"],
            "description": "Standup comedy fans",
        },
        {
            "name": "IMPROV",
            "question_key": "0",
            "answers": ["improv"],
            "description": "Improv fans",
        },
    ]
    return BusinessTenant(
        slug="west_side_comedy",
        display_name="West Side Comedy Club",
        business_type="comedy_club",
        sms_number="+18557689537",
        owner_phone="+15550001111",
        keyword="COMEDY",
        tone="fun and casual",
        blast_triggers=["tonight", "deal", "show", "opening", "seats available"],
        segments=segs,
    )


def _ai_returns(action, blast_message="", segment="ALL", reply=""):
    """Return a mock for smb_ai.generate that yields the given JSON decision."""
    payload = json.dumps({
        "action": action,
        "blast_message": blast_message,
        "segment": segment,
        "reply": reply,
    })
    return patch("app.smb.blast.smb_ai.generate", return_value=payload)


# ---------------------------------------------------------------------------
# _ai_decide_owner_action — unit tests with mocked AI
# ---------------------------------------------------------------------------

def test_decide_returns_clarify_for_new_blast():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "25% off standup tonight 8pm"}]
    with _ai_returns("CLARIFY", reply="Got it! Who should get this? Everyone or just standup fans?"):
        decision = _ai_decide_owner_action("25% off standup tonight 8pm", history, tenant)
    assert decision["action"] == "CLARIFY"
    assert decision["reply"]
    print("✓ CLARIFY returned for new blast message")


def test_decide_returns_send_blast_after_clarify():
    tenant = _make_tenant()
    history = [
        {"role": "user", "body": "25% off standup tonight 8pm"},
        {"role": "assistant", "body": "Got it! Who should get this?\n• Standup fans ← suggested\n• Improv fans\nReply with your choice."},
        {"role": "user", "body": "standup comedy fans"},
    ]
    with _ai_returns("SEND_BLAST", blast_message="25% off standup tonight 8pm", segment="STANDUP"):
        decision = _ai_decide_owner_action("standup comedy fans", history, tenant)
    assert decision["action"] == "SEND_BLAST"
    assert decision["blast_message"] == "25% off standup tonight 8pm"
    assert decision["segment"] == "STANDUP"
    print("✓ SEND_BLAST returned after CLARIFY with correct blast_message extracted")


def test_decide_returns_stats_for_subscriber_query():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "how many standup fans do we have?"}]
    with _ai_returns("STATS", reply=""):
        decision = _ai_decide_owner_action("how many standup fans do we have?", history, tenant)
    assert decision["action"] == "STATS"
    print("✓ STATS returned for subscriber count query")


def test_decide_returns_cancel():
    tenant = _make_tenant()
    history = [
        {"role": "user", "body": "30% off tonight"},
        {"role": "assistant", "body": "Who should get this blast?"},
        {"role": "user", "body": "cancel"},
    ]
    with _ai_returns("CANCEL", reply="Got it, nothing sent."):
        decision = _ai_decide_owner_action("cancel", history, tenant)
    assert decision["action"] == "CANCEL"
    print("✓ CANCEL returned for cancel message")


def test_decide_returns_help_for_unrecognised():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "hey how are you"}]
    with _ai_returns("HELP", reply="Here's what I can do..."):
        decision = _ai_decide_owner_action("hey how are you", history, tenant)
    assert decision["action"] == "HELP"
    print("✓ HELP returned for non-blast message")


def test_decide_handles_json_parse_failure():
    """If AI returns garbage, gracefully fall back to HELP."""
    tenant = _make_tenant()
    history = [{"role": "user", "body": "30% off tonight"}]
    with patch("app.smb.blast.smb_ai.generate", return_value="not valid json at all"):
        decision = _ai_decide_owner_action("30% off tonight", history, tenant)
    assert decision["action"] == "HELP"
    print("✓ JSON parse failure falls back to HELP action")


def test_decide_strips_markdown_code_fences():
    """AI sometimes wraps JSON in ```json ... ``` — should still parse."""
    tenant = _make_tenant()
    history = [{"role": "user", "body": "30% off tonight"}]
    payload = '```json\n{"action": "CLARIFY", "blast_message": "", "segment": "ALL", "reply": "Who to send to?"}\n```'
    with patch("app.smb.blast.smb_ai.generate", return_value=payload):
        decision = _ai_decide_owner_action("30% off tonight", history, tenant)
    assert decision["action"] == "CLARIFY"
    print("✓ Markdown code fences stripped before JSON parse")


# ---------------------------------------------------------------------------
# handle_owner_blast — integration with mocked AI + threading
# ---------------------------------------------------------------------------

def test_handle_returns_clarify_reply_for_new_blast():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "25% off standup tonight"}]
    clarify_text = "Got it! Who should get this?\n• Standup fans\n• Improv fans"
    with _ai_returns("CLARIFY", reply=clarify_text):
        reply = handle_owner_blast("+15550001111", "25% off standup tonight", history, tenant)
    assert reply == clarify_text
    print("✓ handle_owner_blast returns CLARIFY reply for new blast")


def test_handle_fires_blast_thread_after_audience_selection():
    tenant = _make_tenant()
    history = [
        {"role": "user", "body": "25% off standup tonight 8pm"},
        {"role": "assistant", "body": "Who should get this blast?"},
        {"role": "user", "body": "standup fans"},
    ]
    with _ai_returns("SEND_BLAST", blast_message="25% off standup tonight 8pm", segment="STANDUP"):
        with patch("app.smb.blast.threading.Thread") as mock_thread:
            mock_instance = MagicMock()
            mock_thread.return_value = mock_instance
            reply = handle_owner_blast("+15550001111", "standup fans", history, tenant)

    assert "sending" in reply.lower()
    mock_thread.assert_called_once()
    mock_instance.start.assert_called_once()
    # Verify the blast_message passed to the thread is the original, not the audience reply
    args = mock_thread.call_args.kwargs["args"]
    assert args[0] == "25% off standup tonight 8pm"
    print("✓ SEND_BLAST fires thread with original blast message, not audience reply")


def test_handle_send_blast_resolves_correct_segment():
    tenant = _make_tenant()
    history = [
        {"role": "user", "body": "improv night tomorrow"},
        {"role": "assistant", "body": "Who should get this?"},
        {"role": "user", "body": "improv people"},
    ]
    captured = {}
    def capture_thread(**kwargs):
        captured.update(kwargs)
        m = MagicMock()
        m.start = lambda: None
        return m

    with _ai_returns("SEND_BLAST", blast_message="improv night tomorrow", segment="IMPROV"):
        with patch("app.smb.blast.threading.Thread", side_effect=capture_thread):
            handle_owner_blast("+15550001111", "improv people", history, tenant)

    blast_args = captured["args"]
    assert blast_args[0] == "improv night tomorrow"
    assert blast_args[2] is not None                      # segment is set
    assert blast_args[2]["name"] == "IMPROV"
    print("✓ SEND_BLAST resolves IMPROV segment correctly")


def test_handle_send_blast_all_when_segment_unknown():
    """If AI returns an unrecognised segment name, fall back to ALL."""
    tenant = _make_tenant()
    history = [
        {"role": "user", "body": "big show tonight"},
        {"role": "assistant", "body": "Who to send to?"},
        {"role": "user", "body": "everyone"},
    ]
    captured = {}
    def capture_thread(**kwargs):
        captured.update(kwargs)
        m = MagicMock()
        m.start = lambda: None
        return m

    with _ai_returns("SEND_BLAST", blast_message="big show tonight", segment="NONEXISTENT"):
        with patch("app.smb.blast.threading.Thread", side_effect=capture_thread):
            handle_owner_blast("+15550001111", "everyone", history, tenant)

    blast_args = captured["args"]
    assert blast_args[2] is None   # no segment = send to all
    print("✓ Unknown segment falls back to ALL")


def test_handle_missing_blast_message_returns_error():
    """If AI decides SEND_BLAST but can't find the message, tell the owner."""
    tenant = _make_tenant()
    history = [{"role": "user", "body": "something"}]
    with _ai_returns("SEND_BLAST", blast_message="", segment="ALL"):
        with patch("app.smb.blast.threading.Thread") as mock_thread:
            reply = handle_owner_blast("+15550001111", "everyone", history, tenant)
    mock_thread.assert_not_called()
    assert "again" in reply.lower() or "lost" in reply.lower()
    print("✓ Missing blast_message in SEND_BLAST returns recoverable error, no thread spawned")


def test_handle_stats_calls_get_audience_stats():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "how many subscribers?"}]
    with _ai_returns("STATS"):
        with patch("app.smb.blast._get_audience_stats", return_value="You have 8 fans!") as mock_stats:
            reply = handle_owner_blast("+15550001111", "how many subscribers?", history, tenant)
    mock_stats.assert_called_once_with(tenant)
    assert reply == "You have 8 fans!"
    print("✓ STATS action calls _get_audience_stats with correct tenant")


def test_handle_cancel_returns_reply():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "cancel"}]
    with _ai_returns("CANCEL", reply="Got it, nothing sent."):
        reply = handle_owner_blast("+15550001111", "cancel", history, tenant)
    assert reply == "Got it, nothing sent."
    print("✓ CANCEL returns AI reply directly")


def test_handle_help_returns_reply():
    tenant = _make_tenant()
    history = [{"role": "user", "body": "hey"}]
    with _ai_returns("HELP", reply="Here's what I can do: send blasts, check stats."):
        reply = handle_owner_blast("+15550001111", "hey", history, tenant)
    assert "blast" in reply.lower() or "can do" in reply.lower()
    print("✓ HELP returns AI reply")


# ---------------------------------------------------------------------------
# Two-step confirmation is enforced by the AI prompt (contract test)
# ---------------------------------------------------------------------------

def test_two_step_enforced_prompt_contains_rule():
    """
    The CLARIFY-before-SEND_BLAST rule must appear in the AI prompt so the
    model always follows it. This test captures the prompt and checks the
    key constraint text is present.
    """
    tenant = _make_tenant()
    history = [{"role": "user", "body": "25% off tonight"}]
    captured_prompt = {}

    def capture(prompt):
        captured_prompt["text"] = prompt
        return json.dumps({"action": "CLARIFY", "blast_message": "", "segment": "ALL", "reply": "ok"})

    with patch("app.smb.blast.smb_ai.generate", side_effect=capture):
        _ai_decide_owner_action("25% off tonight", history, tenant)

    prompt_text = captured_prompt.get("text", "")
    assert "CLARIFY" in prompt_text
    assert "SEND_BLAST" in prompt_text
    # The critical constraint must be explicit
    assert "never skip" in prompt_text.lower() or "always use clarify before send_blast" in prompt_text.lower()
    print("✓ AI prompt contains the CLARIFY-before-SEND_BLAST enforcement rule")


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

    with patch("app.smb.blast.get_db_connection", return_value=mock_conn):
        with patch("app.smb.storage.get_active_subscribers", return_value=fake_subscribers):
            with patch("app.smb.blast._twilio_send_smb", return_value=True) as mock_send:
                with patch("app.smb.blast._ai_enhance_blast", return_value="25% off tonight!"):
                    with patch("app.smb.blast._record_blast") as mock_record:
                        _run_blast_async("25% off tonight!", tenant)

    assert mock_send.call_count == 2
    sent_to = {c.args[0] for c in mock_send.call_args_list}
    assert sent_to == {"+15550002222", "+15550003333"}
    mock_record.assert_called_once()
    print("✓ _run_blast_async sends to all active subscribers")


def test_run_blast_async_skips_when_no_subscribers():
    tenant = _make_tenant()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch("app.smb.blast.get_db_connection", return_value=mock_conn):
        with patch("app.smb.storage.get_active_subscribers", return_value=[]):
            with patch("app.smb.blast._twilio_send_smb") as mock_send:
                _run_blast_async("25% off tonight!", tenant)

    mock_send.assert_not_called()
    print("✓ _run_blast_async skips when no active subscribers")


def test_run_blast_async_skips_when_no_db():
    tenant = _make_tenant()
    with patch("app.smb.blast.get_db_connection", return_value=None):
        with patch("app.smb.blast._twilio_send_smb") as mock_send:
            _run_blast_async("25% off tonight!", tenant)

    mock_send.assert_not_called()
    print("✓ _run_blast_async exits cleanly when DB is unavailable")


# ---------------------------------------------------------------------------
# _seg_display_name helper
# ---------------------------------------------------------------------------

def test_seg_display_name_truncates_long_descriptions():
    seg = {"name": "STANDUP", "description": "Standup comedy fans who love live shows, touring acts, and open mics"}
    result = _seg_display_name(seg)
    assert len(result) <= 35
    print("✓ _seg_display_name truncates long descriptions")


def test_seg_display_name_uses_description_when_short():
    seg = {"name": "STANDUP", "description": "Standup fans"}
    result = _seg_display_name(seg)
    assert result == "Standup fans"
    print("✓ _seg_display_name uses description when short enough")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"✗ {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
