"""
Tests for the SlickText → Twilio number-migration responder
(app/messaging/slicktext_migration.py) and its wiring into the SlickText webhook.

The migration module is intentionally standalone (no heavy import chain), so we
unit-test its behaviour directly. The webhook wiring in main.py is verified via
source inspection — main.py has a heavy import chain that blocks unit loading,
matching the strategy used in tests/test_blast_optout.py.
"""

import os
import re

from app.messaging import slicktext_migration as m

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, rowcount=1):
        self._cur = FakeCursor(rowcount)
        self.closed = False

    def cursor(self):
        return self._cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        self.closed = True


class FakeAdapter:
    def __init__(self):
        self.calls = []

    def send_reply(self, to_number, body, *args, **kwargs):
        self.calls.append((to_number, body))
        return True


# ---------------------------------------------------------------------------
# migration_enabled — off by default, toggled by env
# ---------------------------------------------------------------------------

def test_migration_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SLICKTEXT_MIGRATION_MODE", raising=False)
    assert m.migration_enabled() is False


def test_migration_enabled_truthy_values(monkeypatch):
    for val in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv("SLICKTEXT_MIGRATION_MODE", val)
        assert m.migration_enabled() is True
    for val in ("0", "false", "no", ""):
        monkeypatch.setenv("SLICKTEXT_MIGRATION_MODE", val)
        assert m.migration_enabled() is False


# ---------------------------------------------------------------------------
# format_us_number
# ---------------------------------------------------------------------------

def test_format_us_number_e164():
    assert m.format_us_number("+18556081717") == "(855) 608-1717"


def test_format_us_number_ten_digits():
    assert m.format_us_number("8556081717") == "(855) 608-1717"


def test_format_us_number_passthrough_when_unparseable():
    assert m.format_us_number("not-a-number") == "not-a-number"
    assert m.format_us_number("") == ""


# ---------------------------------------------------------------------------
# Message bodies
# ---------------------------------------------------------------------------

def test_twilio_opener_includes_compliance_footer():
    opener = m.twilio_opener_text()
    assert "STOP" in opener  # A2P disclosure appended via first_message_with_footer


def test_slicktext_bridge_includes_formatted_new_number():
    body = m.slicktext_bridge_text("+18556081717")
    assert "(855) 608-1717" in body


def test_message_templates_are_env_overridable(monkeypatch):
    monkeypatch.setenv("SLICKTEXT_MIGRATION_REPLY", "go to {new}")
    assert m.slicktext_bridge_text("+18556081717") == "go to (855) 608-1717"


# ---------------------------------------------------------------------------
# opener_cooldown_seconds — env-driven, sane default
# ---------------------------------------------------------------------------

def test_opener_cooldown_defaults_to_24h(monkeypatch):
    monkeypatch.delenv("SLICKTEXT_MIGRATION_OPENER_COOLDOWN_HOURS", raising=False)
    assert m.opener_cooldown_seconds() == 24 * 3600


def test_opener_cooldown_honours_env(monkeypatch):
    monkeypatch.setenv("SLICKTEXT_MIGRATION_OPENER_COOLDOWN_HOURS", "0")
    assert m.opener_cooldown_seconds() == 0
    monkeypatch.setenv("SLICKTEXT_MIGRATION_OPENER_COOLDOWN_HOURS", "1.5")
    assert m.opener_cooldown_seconds() == 5400


def test_opener_cooldown_falls_back_on_garbage(monkeypatch):
    monkeypatch.setenv("SLICKTEXT_MIGRATION_OPENER_COOLDOWN_HOURS", "nope")
    assert m.opener_cooldown_seconds() == 24 * 3600


# ---------------------------------------------------------------------------
# claim_migration_send — send when fresh/past-cooldown, skip within cooldown
# ---------------------------------------------------------------------------

def test_claim_returns_true_when_row_inserted_or_refreshed():
    conn = FakeConn(rowcount=1)
    assert m.claim_migration_send("+15551234567", lambda: conn) is True
    assert conn.closed is True


def test_claim_returns_false_within_cooldown():
    conn = FakeConn(rowcount=0)
    assert m.claim_migration_send("+15551234567", lambda: conn) is False


def test_claim_passes_cooldown_to_query():
    conn = FakeConn(rowcount=1)
    m.claim_migration_send("+15551234567", lambda: conn, cooldown_seconds=42)
    upsert = conn._cur.executed[-1]
    assert "make_interval" in upsert[0]
    assert upsert[1] == ("+15551234567", 42)


def test_claim_returns_false_without_phone_or_conn():
    assert m.claim_migration_send("", lambda: FakeConn()) is False
    assert m.claim_migration_send("+15551234567", lambda: None) is False


def test_claim_swallows_db_errors():
    def boom():
        raise RuntimeError("db down")

    assert m.claim_migration_send("+15551234567", boom) is False


# ---------------------------------------------------------------------------
# handle_migration — opener when past cooldown, bridge best-effort every time
# ---------------------------------------------------------------------------

def test_handle_migration_sends_opener_and_bridge_when_due(monkeypatch):
    monkeypatch.setattr(m, "claim_migration_send", lambda phone, get_conn: True)
    twilio, slick = FakeAdapter(), FakeAdapter()

    m.handle_migration("+15551234567", twilio, slick, lambda: None, new_number="+18556081717")

    assert len(twilio.calls) == 1
    assert "STOP" in twilio.calls[0][1]
    assert len(slick.calls) == 1
    assert "(855) 608-1717" in slick.calls[0][1]


def test_handle_migration_silent_within_cooldown(monkeypatch):
    """Within the cooldown window we send nothing on either channel — the fan was
    redirected recently, and this keeps us inside the SlickText credit cap."""
    monkeypatch.setattr(m, "claim_migration_send", lambda phone, get_conn: False)
    twilio, slick = FakeAdapter(), FakeAdapter()

    m.handle_migration("+15551234567", twilio, slick, lambda: None, new_number="+18556081717")

    assert len(twilio.calls) == 0
    assert len(slick.calls) == 0


def test_handle_migration_reaches_fan_even_when_slicktext_send_fails(monkeypatch):
    """On a downgraded/free SlickText plan the bridge send fails, but the fan is
    still reached via the Twilio opener."""
    monkeypatch.setattr(m, "claim_migration_send", lambda phone, get_conn: True)
    twilio = FakeAdapter()

    class DeadSlickText:
        def send_reply(self, *a, **k):
            raise RuntimeError("no outbound credits")

    m.handle_migration("+15551234567", twilio, DeadSlickText(), lambda: None, new_number="+18556081717")

    assert len(twilio.calls) == 1          # fan still hears from the new number


def test_handle_migration_noop_without_phone(monkeypatch):
    monkeypatch.setattr(m, "claim_migration_send", lambda phone, get_conn: True)
    twilio, slick = FakeAdapter(), FakeAdapter()
    m.handle_migration("", twilio, slick, lambda: None)
    assert twilio.calls == [] and slick.calls == []


def test_handle_migration_swallows_send_errors(monkeypatch):
    monkeypatch.setattr(m, "claim_migration_send", lambda phone, get_conn: True)

    class Boom:
        def send_reply(self, *a, **k):
            raise RuntimeError("twilio down")

    # Should not raise even if both adapters fail.
    m.handle_migration("+15551234567", Boom(), Boom(), lambda: None, new_number="+18556081717")


# ---------------------------------------------------------------------------
# Webhook wiring (source inspection)
# ---------------------------------------------------------------------------

def test_webhook_gates_migration_behind_flag_and_short_circuits():
    src = _read("main.py")
    assert "_slicktext_migration_enabled" in src
    fn_start = src.find("def slicktext_webhook(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "if _slicktext_migration_enabled():" in body
    assert "_handle_slicktext_migration" in body
    # Migration must short-circuit before the AI reply thread fires.
    assert body.index("if _slicktext_migration_enabled():") < body.index("_process_slicktext_message")
    # And it must sit AFTER opt-out handling so STOP is still recorded.
    assert body.index("_record_blast_optout") < body.index("if _slicktext_migration_enabled():")
