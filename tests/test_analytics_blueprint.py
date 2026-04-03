"""
Phase 2 analytics blueprint tests.

Tests the JSON API endpoints using Flask's test client with a mocked DB.
No real Postgres required — we patch get_db_connection.
"""

import sys
import os
import json
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.gemini_test_util import ensure_placeholder_key_for_import
ensure_placeholder_key_for_import()

# psycopg2 is not installed in the local dev environment — stub it out so the
# blueprint's `import psycopg2.extras` doesn't blow up during testing.
if "psycopg2" not in sys.modules:
    _psycopg2_stub = MagicMock()
    _psycopg2_stub.extras = MagicMock()
    _psycopg2_stub.extras.DictCursor = None
    sys.modules["psycopg2"] = _psycopg2_stub
    sys.modules["psycopg2.extras"] = _psycopg2_stub.extras

from flask import Flask
from app.analytics.blueprint import analytics_bp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app():
    """Create a minimal Flask app with only the analytics blueprint — no twilio dependency."""
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(analytics_bp)
    return app.test_client()


def _auth_headers():
    import base64
    creds = base64.b64encode(b"admin:testpass").decode()
    return {"Authorization": f"Basic {creds}"}


def _mock_cursor(rows):
    """Return a mock psycopg2 DictCursor that yields `rows` on fetchall/fetchone."""
    cur = MagicMock()
    cur.__enter__ = lambda s: cur
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    return cur


def _mock_conn(rows):
    cur = _mock_cursor(rows)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.close = MagicMock()
    return conn, cur


# ---------------------------------------------------------------------------
# Auth guard tests
# ---------------------------------------------------------------------------

def test_analytics_endpoints_require_auth():
    client = _make_app()
    endpoints = [
        "/analytics/engagement-summary",
        "/analytics/intent-breakdown",
        "/analytics/tone-breakdown",
        "/analytics/dropoff-triggers",
        "/analytics/top-bot-replies",
        "/analytics/reply-length-buckets",
    ]
    for ep in endpoints:
        with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
            resp = client.get(ep)
            assert resp.status_code == 401, f"{ep} should return 401 without auth"
    print("✓ All analytics endpoints require auth when password is configured")


def test_analytics_returns_503_when_no_db():
    client = _make_app()
    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=None):
            resp = client.get("/analytics/engagement-summary", headers=_auth_headers())
            assert resp.status_code == 503
            data = json.loads(resp.data)
            assert "error" in data
    print("✓ engagement-summary returns 503 when no DB")


# ---------------------------------------------------------------------------
# engagement-summary
# ---------------------------------------------------------------------------

def test_engagement_summary_returns_expected_keys():
    client = _make_app()
    mock_row = {
        "scored_bot_replies": 100,
        "reply_rate_pct": 68.5,
        "dropoff_rate_pct": 21.3,
        "avg_reply_delay_s": 47,
        "median_reply_delay_s": 30,
        "avg_bot_reply_length": 120,
        "bot_msgs_with_link": 20,
        "link_clicks_1h": 3,
        "first_turn_replies": 50,
        "first_turn_reply_rate_pct": 72.0,
    }
    conn, cur = _mock_conn([mock_row])
    cur.fetchone.return_value = mock_row

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/engagement-summary", headers=_auth_headers())
            assert resp.status_code == 200
            data = json.loads(resp.data)
            assert data["scored_bot_replies"] == 100
            assert data["reply_rate_pct"] == 68.5
            assert data["link_ctr_pct"] == 15.0  # 3/20 * 100
            assert data["days"] == 30
    print("✓ engagement-summary returns correct data and computes link_ctr_pct")


def test_engagement_summary_link_ctr_zero_when_no_links():
    client = _make_app()
    mock_row = {
        "scored_bot_replies": 50,
        "reply_rate_pct": 60.0,
        "dropoff_rate_pct": 25.0,
        "avg_reply_delay_s": 60,
        "median_reply_delay_s": 40,
        "avg_bot_reply_length": 110,
        "bot_msgs_with_link": 0,
        "link_clicks_1h": 0,
        "first_turn_replies": 25,
        "first_turn_reply_rate_pct": 65.0,
    }
    conn, cur = _mock_conn([mock_row])
    cur.fetchone.return_value = mock_row

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/engagement-summary", headers=_auth_headers())
            data = json.loads(resp.data)
            assert data["link_ctr_pct"] is None  # no links sent → no CTR
    print("✓ engagement-summary returns link_ctr_pct=None when no links were sent")


# ---------------------------------------------------------------------------
# intent-breakdown
# ---------------------------------------------------------------------------

def test_intent_breakdown_returns_list():
    client = _make_app()
    mock_rows = [
        {"intent": "joke", "total": 80, "reply_rate_pct": 81.0,
         "dropoff_rate_pct": 12.0, "avg_delay_s": 35, "avg_reply_length": 110,
         "link_clicks_1h": 0, "bot_msgs_with_link": 2},
        {"intent": "show", "total": 40, "reply_rate_pct": 41.0,
         "dropoff_rate_pct": 34.0, "avg_delay_s": 90, "avg_reply_length": 180,
         "link_clicks_1h": 8, "bot_msgs_with_link": 20},
    ]
    conn, cur = _mock_conn(mock_rows)

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/intent-breakdown", headers=_auth_headers())
            assert resp.status_code == 200
            data = json.loads(resp.data)
            assert "breakdown" in data
            assert len(data["breakdown"]) == 2
            assert data["breakdown"][0]["intent"] == "joke"
            # link CTR for show: 8/20*100 = 40.0
            assert data["breakdown"][1]["link_ctr_pct"] == 40.0
    print("✓ intent-breakdown returns correct breakdown list with link_ctr_pct")


# ---------------------------------------------------------------------------
# tone-breakdown
# ---------------------------------------------------------------------------

def test_tone_breakdown_returns_list():
    client = _make_app()
    mock_rows = [
        {"tone_mode": "playful", "total": 60, "reply_rate_pct": 75.0, "dropoff_rate_pct": 15.0},
        {"tone_mode": "warm",    "total": 30, "reply_rate_pct": 70.0, "dropoff_rate_pct": 20.0},
    ]
    conn, cur = _mock_conn(mock_rows)

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/tone-breakdown", headers=_auth_headers())
            assert resp.status_code == 200
            data = json.loads(resp.data)
            assert len(data["breakdown"]) == 2
            assert data["breakdown"][0]["tone_mode"] == "playful"
    print("✓ tone-breakdown returns correct list")


# ---------------------------------------------------------------------------
# dropoff-triggers
# ---------------------------------------------------------------------------

def test_dropoff_triggers_returns_list():
    client = _make_app()
    from datetime import datetime
    mock_rows = [
        {"preview": "Here are all my upcoming shows!", "intent": "show",
         "tone_mode": "playful", "reply_length_chars": 200,
         "conversation_turn": 2, "created_at": datetime(2026, 4, 1, 12, 0)},
    ]
    conn, cur = _mock_conn(mock_rows)

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/dropoff-triggers", headers=_auth_headers())
            assert resp.status_code == 200
            data = json.loads(resp.data)
            assert "triggers" in data
            assert len(data["triggers"]) == 1
            assert data["triggers"][0]["intent"] == "show"
    print("✓ dropoff-triggers returns correct list")


# ---------------------------------------------------------------------------
# top-bot-replies
# ---------------------------------------------------------------------------

def test_top_bot_replies_default_metric():
    client = _make_app()
    from datetime import datetime
    mock_rows = [
        {"preview": "Haha yes!", "intent": "joke", "tone_mode": "playful",
         "routing_tier": "low", "reply_length_chars": 10, "reply_delay_seconds": 5,
         "conversation_turn": 1, "created_at": datetime(2026, 4, 1)},
    ]
    conn, cur = _mock_conn(mock_rows)

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/top-bot-replies", headers=_auth_headers())
            assert resp.status_code == 200
            data = json.loads(resp.data)
            assert "top_replies" in data
            assert data["metric"] == "reply_delay"
    print("✓ top-bot-replies returns correct structure")


# ---------------------------------------------------------------------------
# reply-length-buckets
# ---------------------------------------------------------------------------

def test_reply_length_buckets_returns_buckets():
    client = _make_app()
    mock_rows = [
        {"length_bucket": "< 50 chars",    "bucket_min": 0,   "total": 10, "reply_rate_pct": 80.0, "dropoff_rate_pct": 10.0, "avg_delay_s": 20},
        {"length_bucket": "50-99 chars",   "bucket_min": 50,  "total": 30, "reply_rate_pct": 72.0, "dropoff_rate_pct": 18.0, "avg_delay_s": 35},
        {"length_bucket": "250+ chars",    "bucket_min": 250, "total": 5,  "reply_rate_pct": 45.0, "dropoff_rate_pct": 40.0, "avg_delay_s": 90},
    ]
    conn, cur = _mock_conn(mock_rows)

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/reply-length-buckets", headers=_auth_headers())
            assert resp.status_code == 200
            data = json.loads(resp.data)
            assert "buckets" in data
            assert len(data["buckets"]) == 3
    print("✓ reply-length-buckets returns correct bucket list")


# ---------------------------------------------------------------------------
# days parameter validation
# ---------------------------------------------------------------------------

def test_days_param_clamped_to_max():
    client = _make_app()
    conn, cur = _mock_conn([{
        "scored_bot_replies": 0, "reply_rate_pct": None, "dropoff_rate_pct": None,
        "avg_reply_delay_s": None, "median_reply_delay_s": None,
        "avg_bot_reply_length": None, "bot_msgs_with_link": 0, "link_clicks_1h": 0,
        "first_turn_replies": 0, "first_turn_reply_rate_pct": None,
    }])
    cur.fetchone.return_value = cur.fetchall.return_value[0]

    with patch("app.admin_auth._ADMIN_PASSWORD", "testpass"):
        with patch("app.analytics.blueprint.get_db_connection", return_value=conn):
            resp = client.get("/analytics/engagement-summary?days=999", headers=_auth_headers())
            data = json.loads(resp.data)
            assert data["days"] == 90  # clamped to max
    print("✓ days param is clamped to max 90")


if __name__ == "__main__":
    test_analytics_endpoints_require_auth()
    test_analytics_returns_503_when_no_db()
    test_engagement_summary_returns_expected_keys()
    test_engagement_summary_link_ctr_zero_when_no_links()
    test_intent_breakdown_returns_list()
    test_tone_breakdown_returns_list()
    test_dropoff_triggers_returns_list()
    test_top_bot_replies_default_metric()
    test_reply_length_buckets_returns_buckets()
    test_days_param_clamped_to_max()
    print("\n✅ All Phase 2 analytics blueprint tests passed.")
