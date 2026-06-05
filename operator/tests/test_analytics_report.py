"""
Analytics media-kit report (Item 4): the /api/analytics/report endpoint returns
tenant-scoped headline counts, fan-tier breakdown, top intents, and engagement.

`messages` / `contacts` are main-app tables not created by the operator
migrations, so we create minimal versions here (mirrors test_inbox_full_numbers).
"""

import datetime

import pytest


@pytest.fixture()
def report_data():
    from app.db import get_conn
    conn = get_conn(); conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, role TEXT, text TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(), creator_slug TEXT,
                intent TEXT, conversation_turn INT, did_user_reply BOOLEAN)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, creator_slug TEXT,
                fan_tier TEXT, fan_tags TEXT[], fan_location TEXT, fan_name TEXT,
                fan_memory TEXT, created_at TIMESTAMPTZ DEFAULT NOW())
        """)
        cur.execute("TRUNCATE messages, contacts")

        # 3 Zarna fans across tiers + 1 fan on another tenant (must be excluded).
        cur.execute("INSERT INTO contacts (phone_number, creator_slug, fan_tier) VALUES "
                    "('+1001','zarna','superfan'),('+1002','zarna','engaged'),"
                    "('+1003','zarna','lurker'),('+9001','other','superfan')")

        base = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        def msg(phone, role, slug="zarna", intent=None, turn=None, replied=None, i=0):
            cur.execute(
                "INSERT INTO messages (phone_number, role, text, created_at, creator_slug, "
                "intent, conversation_turn, did_user_reply) VALUES (%s,%s,'x',%s,%s,%s,%s,%s)",
                (phone, role, base + datetime.timedelta(minutes=i), slug, intent, turn, replied),
            )

        # Conversation A (fan 1001): 2 turns.
        msg("+1001", "user", i=0)
        msg("+1001", "assistant", intent="GREETING", turn=1, replied=True, i=1)
        msg("+1001", "user", i=2)
        msg("+1001", "assistant", intent="PERSONAL", turn=2, replied=False, i=3)
        # Conversation B (fan 1002): 1 turn.
        msg("+1002", "user", i=4)
        msg("+1002", "assistant", intent="SHOW", turn=1, replied=True, i=5)
        # Other tenant — must never be counted.
        msg("+9001", "user", slug="other", i=6)
        msg("+9001", "assistant", slug="other", intent="SHOW", turn=1, replied=True, i=7)
    conn.close()


@pytest.fixture()
def performer(client, make_user):
    uid = make_user("perf@zarna.test", creator_slug="zarna", account_type="performer")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


def test_report_headline_counts(client, performer, report_data):
    body = client.get("/api/analytics/report").get_json()
    assert body["total_subscribers"] == 3          # other-tenant fan excluded
    assert body["total_conversations"] == 2         # fans 1001 + 1002 texted in
    assert body["longest_conversation"] == 4        # fan 1001: 2 user + 2 assistant
    assert body["superfans"] == 1
    assert body["total_fan_messages"] == 3          # 3 user rows for zarna
    assert body["avg_messages_per_fan"] == 1.5       # 3 fan msgs / 2 engaged fans


def test_report_tier_breakdown(client, performer, report_data):
    tiers = {row["tier"]: row["count"] for row in client.get("/api/analytics/report").get_json()["tier_breakdown"]}
    assert tiers == {"superfan": 1, "engaged": 1, "lurker": 1}


def test_report_top_intents(client, performer, report_data):
    intents = {row["intent"] for row in client.get("/api/analytics/report").get_json()["top_intents"]}
    assert {"GREETING", "PERSONAL", "SHOW"} <= intents


def test_report_engagement_rate(client, performer, report_data):
    # 3 scored assistant rows, 2 with did_user_reply=TRUE -> 67%.
    assert client.get("/api/analytics/report").get_json()["engagement_rate"] == 67


def test_report_requires_login(client):
    assert client.get("/api/analytics/report").status_code in (401, 403)
