"""
Unit tests for operator.app.business_blast — tier classification, audience
resolution, AI cleanup fallback, and the send pipeline (Twilio mocked).

These tests run without a live Postgres by stubbing the cursor with a
fake that returns canned rows for the SQL the helper issues.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

# operator/app/ shares its package name ("app") with the main Flask app at
# the repo root, so we can't just `import operator.app...` — load the helper
# module directly from its file path instead.
ROOT = Path(__file__).resolve().parent.parent
_BB_PATH = ROOT / "operator" / "app" / "business_blast.py"
_spec = importlib.util.spec_from_file_location("operator_business_blast", _BB_PATH)
bb = importlib.util.module_from_spec(_spec)
sys.modules["operator_business_blast"] = bb
_spec.loader.exec_module(bb)


# ---------------------------------------------------------------------------
# Fake DB cursor / connection
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, responses):
        # responses: list of either (rows) or (sql_substr, rows)
        self._responses = list(responses)
        self.executed: list[tuple[str, tuple]] = []
        self._last_rows: list = []

    def execute(self, sql, params=()):
        self.executed.append((sql, tuple(params)))
        # Match by substring if a string-keyed response is queued.
        for i, item in enumerate(self._responses):
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str):
                if item[0] in sql:
                    self._last_rows = list(item[1])
                    self._responses.pop(i)
                    return
        # Otherwise pop the next plain-rows response in order.
        if self._responses:
            head = self._responses.pop(0)
            self._last_rows = list(head)
        else:
            self._last_rows = []

    def fetchall(self):
        return self._last_rows

    def fetchone(self):
        return self._last_rows[0] if self._last_rows else None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, responses):
        self._cur = _FakeCursor(responses)
        self.closed = False
        self.committed = False

    def cursor(self):
        return self._cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.committed = exc[0] is None
        return False

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# compute_tier_counts
# ---------------------------------------------------------------------------

def test_compute_tier_counts_returns_all_tiers_with_zero_default():
    # SQL only returns 'regular' and 'engaged'; helper must still emit zeros
    # for 'new' and 'lapsed' so the UI renders all four cards.
    conn = _FakeConn([[("regular", 12), ("engaged", 47)]])
    tiers = bb.compute_tier_counts("wscc", conn)

    assert [t["tier"] for t in tiers] == ["regular", "engaged", "new", "lapsed"]
    by_tier = {t["tier"]: t for t in tiers}
    assert by_tier["regular"]["count"] == 12
    assert by_tier["engaged"]["count"] == 47
    assert by_tier["new"]["count"] == 0
    assert by_tier["lapsed"]["count"] == 0
    # Cadence + label come from the module constants
    assert by_tier["regular"]["cadence_days"] == bb.CADENCE_DAYS["regular"]
    assert by_tier["engaged"]["label"] == bb.TIER_LABELS["engaged"]


def test_compute_tier_counts_runs_one_query_with_slug_param_repeated_twice():
    # The activity CTE references the slug param twice (subs + LEFT JOIN
    # smb_messages), so the helper must pass it twice or the query breaks.
    conn = _FakeConn([[]])
    bb.compute_tier_counts("wscc", conn)

    assert len(conn._cur.executed) == 1
    _sql, params = conn._cur.executed[0]
    assert params == ("wscc", "wscc")


# ---------------------------------------------------------------------------
# compute_smart_send_preview
# ---------------------------------------------------------------------------

def test_compute_smart_send_preview_aggregates_totals():
    # SQL returns (tier, total, sending) for every classified tier.
    rows = [
        ("regular", 50, 30),  # 20 suppressed
        ("engaged", 100, 90), # 10 suppressed
    ]
    conn = _FakeConn([rows])
    result = bb.compute_smart_send_preview("wscc", conn)

    assert result["total_sending"] == 120
    assert result["total_suppressed"] == 30
    assert result["tiers"]["regular"] == {
        "total": 50,
        "sending": 30,
        "suppressed": 20,
        "cadence_days": bb.CADENCE_DAYS["regular"],
    }
    # Tiers not returned by SQL still show up with zeros so the UI can render
    # all four rows in a stable order.
    assert result["tiers"]["new"]["total"] == 0
    assert result["tiers"]["lapsed"]["sending"] == 0


def test_compute_smart_send_preview_passes_slug_three_times():
    # CTE: subs uses slug, JOIN smb_messages uses slug, NOT EXISTS for
    # smb_blast_recipients uses slug → three positional params.
    conn = _FakeConn([[]])
    bb.compute_smart_send_preview("wscc", conn)
    _sql, params = conn._cur.executed[0]
    assert params == ("wscc", "wscc", "wscc")


# ---------------------------------------------------------------------------
# resolve_audience
# ---------------------------------------------------------------------------

def test_resolve_audience_all():
    conn = _FakeConn([[("+15551234567",), ("+15557654321",)]])
    phones = bb.resolve_audience("wscc", "all", conn, [])
    assert phones == ["+15551234567", "+15557654321"]
    sql, params = conn._cur.executed[0]
    assert "smb_subscribers" in sql
    assert "status='active'" in sql
    assert params == ("wscc",)


def test_resolve_audience_tier_known():
    conn = _FakeConn([[("+15551111111",)]])
    phones = bb.resolve_audience("wscc", "tier:regular", conn, [])
    assert phones == ["+15551111111"]
    sql, params = conn._cur.executed[0]
    # Tier predicate for 'regular' must be embedded in the SQL.
    assert "inbound_60d >= 5" in sql
    assert params == ("wscc", "wscc")


def test_resolve_audience_tier_unknown_raises():
    conn = _FakeConn([])
    with pytest.raises(bb.UnknownAudience):
        bb.resolve_audience("wscc", "tier:vip", conn, [])


def test_resolve_audience_smart_send_runs_one_query_with_three_params():
    conn = _FakeConn([[("+15551111111",), ("+15552222222",)]])
    phones = bb.resolve_audience("wscc", "smart-send", conn, [])
    assert phones == ["+15551111111", "+15552222222"]
    sql, params = conn._cur.executed[0]
    assert "smb_blast_recipients" in sql
    assert params == ("wscc", "wscc", "wscc")


def test_resolve_audience_segment_known():
    segments = [
        {"name": "LOCAL", "question_key": "neighborhood", "answers": ["UWS", "UES"]},
    ]
    conn = _FakeConn([[("+15551111111",)]])
    phones = bb.resolve_audience("wscc", "segment:LOCAL", conn, segments)
    assert phones == ["+15551111111"]
    sql, params = conn._cur.executed[0]
    assert "smb_preferences" in sql
    # slug + question_key + answers spread across params
    assert params == ("wscc", "neighborhood", "UWS", "UES")


def test_resolve_audience_segment_unknown_raises():
    with pytest.raises(bb.UnknownAudience):
        bb.resolve_audience("wscc", "segment:VIP", _FakeConn([]), [])


def test_resolve_audience_customer_of_the_week():
    conn = _FakeConn([[("+15551111111",)]])
    phones = bb.resolve_audience("wscc", "customer_of_the_week", conn, [])
    assert phones == ["+15551111111"]
    sql, params = conn._cur.executed[0]
    assert "smb_customer_of_the_week" in sql
    assert params == ("wscc",)


def test_resolve_audience_invalid_format_raises():
    with pytest.raises(bb.UnknownAudience):
        bb.resolve_audience("wscc", "bogus", _FakeConn([]), [])


# ---------------------------------------------------------------------------
# audience_label
# ---------------------------------------------------------------------------

def test_audience_label_normalizes_known_audiences():
    assert bb.audience_label("all") == "all subscribers"
    assert bb.audience_label("smart-send") == "Smart Send"
    assert bb.audience_label("tier:regular") == "Regular customers"
    assert bb.audience_label("customer_of_the_week") == "past Customers of the Week"
    assert bb.audience_label("segment:LOCAL") == "local"


# ---------------------------------------------------------------------------
# send_blast (Twilio mocked, scheduler short-circuited)
# ---------------------------------------------------------------------------

class _DummyTwilio:
    def __init__(self, *_a, **_kw):
        self.calls: list[dict] = []
        # `messages.create` lives on a `messages` attribute
        outer = self
        class _Messages:
            def create(self, body, from_, to):
                outer.calls.append({"body": body, "from_": from_, "to": to})
        self.messages = _Messages()


def _seq_conn_factory(seq_factory):
    """Return a get_conn() that yields a fresh _FakeConn from `seq_factory`."""
    def _get_conn():
        return _FakeConn(seq_factory())
    return _get_conn


def test_send_blast_returns_helpful_error_when_audience_empty(tmp_path):
    cfg_dir = tmp_path
    (cfg_dir / "wscc.json").write_text('{"display_name": "WSCC", "segments": []}')

    # First conn: resolve_audience -> empty list
    def seqs():
        return [[]]
    get_conn = _seq_conn_factory(seqs)

    result = bb.send_blast(
        slug="wscc",
        raw_message="tonight 25% off",
        audience="all",
        ai_cleanup=False,
        business_configs_dir=cfg_dir,
        get_conn=get_conn,
    )
    assert result["success"] is False
    assert "no subscribers" in result["error"].lower()


def test_send_blast_inserts_blast_row_then_dispatches_to_twilio(tmp_path, monkeypatch):
    cfg_dir = tmp_path
    (cfg_dir / "wscc.json").write_text('{"display_name": "WSCC", "segments": []}')

    monkeypatch.setenv("SMB_WSCC_SMS_NUMBER", "+15550009999")
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC123")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok123")

    # We need a fresh conn for: (1) audience resolve, (2) insert smb_blasts,
    # then per-recipient inserts + finalize from the worker thread.
    audience_conn = _FakeConn([[("+15551111111",), ("+15552222222",)]])
    insert_conn = _FakeConn([[(7,)]])  # RETURNING id → 7
    recipient_conns = [_FakeConn([[]]) for _ in range(2)]
    finalize_conn = _FakeConn([[]])

    seq = iter([audience_conn, insert_conn, *recipient_conns, finalize_conn])

    def get_conn():
        return next(seq)

    sent_event = threading.Event()
    dummy = _DummyTwilio()

    # Patch BOTH the import path (twilio.rest.Client) and the module-level
    # AI cleanup so we don't reach out to Gemini in unit tests.
    with patch.object(bb, "_ai_cleanup", return_value="WSCC: 25% off tonight!"), \
         patch("twilio.rest.Client", return_value=dummy):
        result = bb.send_blast(
            slug="wscc",
            raw_message="25% off tn",
            audience="all",
            ai_cleanup=True,
            business_configs_dir=cfg_dir,
            get_conn=get_conn,
        )

        # Wait briefly for the worker to drain — _dispatch sleeps 0.35s
        # between messages, so 2 phones ≤ 1s.
        for _ in range(40):
            if len(dummy.calls) >= 2:
                sent_event.set()
                break
            sent_event.wait(0.05)

    assert result["success"] is True
    assert result["recipient_count"] == 2
    assert result["audience_label"] == "all subscribers"
    assert result["body_preview"] == "WSCC: 25% off tonight!"
    assert result["ai_cleaned"] is True
    assert result["blast_id"] == 7

    assert sent_event.is_set(), f"Twilio was not called; got {dummy.calls!r}"
    sent_phones = sorted(c["to"] for c in dummy.calls)
    assert sent_phones == ["+15551111111", "+15552222222"]
    for c in dummy.calls:
        assert c["from_"] == "+15550009999"
        assert c["body"] == "WSCC: 25% off tonight!"


def test_send_blast_refuses_when_sms_number_missing(tmp_path, monkeypatch):
    cfg_dir = tmp_path
    (cfg_dir / "wscc.json").write_text('{"display_name": "WSCC", "segments": []}')

    monkeypatch.delenv("SMB_WSCC_SMS_NUMBER", raising=False)

    audience_conn = _FakeConn([[("+15551111111",)]])
    insert_conn = _FakeConn([[(99,)]])
    seq = iter([audience_conn, insert_conn])

    def get_conn():
        return next(seq)

    with patch.object(bb, "_ai_cleanup", return_value="cleaned"):
        result = bb.send_blast(
            slug="wscc",
            raw_message="hi",
            audience="all",
            ai_cleanup=True,
            business_configs_dir=cfg_dir,
            get_conn=get_conn,
        )

    assert result["success"] is False
    assert "sms number" in result["error"].lower()


# ---------------------------------------------------------------------------
# preview_count
# ---------------------------------------------------------------------------

def test_preview_count_uses_resolve_audience(tmp_path):
    cfg_dir = tmp_path
    (cfg_dir / "wscc.json").write_text('{"segments": []}')

    audience_conn = _FakeConn([[("+1",), ("+2",), ("+3",)]])

    def get_conn():
        return audience_conn

    n = bb.preview_count(
        slug="wscc",
        audience="all",
        business_configs_dir=cfg_dir,
        get_conn=get_conn,
    )
    assert n == 3
