"""
Inbox/thread now expose the FULL fan phone number (not just last-4) and the
thread resolver accepts either a last-4 or a full number. Full conversation
history is returned in order with roles intact (so the dashboard can put bot
replies on one side and the fan on the other).

`messages` / `contacts` are main-app tables not created by the operator
migrations, so we create minimal versions here.
"""

import datetime

import pytest


@pytest.fixture()
def inbox_data():
    from app.db import get_conn
    conn = get_conn(); conn.autocommit = True
    phone = "+12125551234"
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, role TEXT, text TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(), creator_slug TEXT,
                intent TEXT, tone_mode TEXT, sell_variant TEXT, source TEXT,
                media_url TEXT)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, creator_slug TEXT,
                fan_tier TEXT, fan_tags TEXT[], fan_location TEXT, fan_name TEXT,
                fan_memory TEXT, fan_score INT, created_at TIMESTAMPTZ DEFAULT NOW())
        """)
        cur.execute("TRUNCATE messages, contacts")
        cur.execute(
            "INSERT INTO contacts (phone_number, creator_slug, fan_tier, fan_tags, fan_location, fan_name) "
            "VALUES (%s,'zarna','superfan',%s,'Houston','Ferial')",
            (phone, ["indian"]),
        )
        convo = [("user", "Hello"), ("assistant", "Hello back!"),
                 ("user", "how are you"), ("assistant", "great, you?")]
        base = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        for i, (role, text) in enumerate(convo):
            cur.execute(
                "INSERT INTO messages (phone_number, role, text, created_at, creator_slug) "
                "VALUES (%s,%s,%s,%s,'zarna')",
                (phone, role, text, base + datetime.timedelta(minutes=i)),
            )
    conn.close()
    return phone


@pytest.fixture()
def performer(client, make_user):
    uid = make_user("perf@zarna.test", creator_slug="zarna", account_type="performer")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


def test_inbox_list_returns_full_number(client, performer, inbox_data):
    resp = client.get("/api/inbox")
    assert resp.status_code == 200
    convos = resp.get_json()["conversations"]
    assert len(convos) == 1
    assert convos[0]["phone_number"] == "+12125551234"   # full, not masked
    assert convos[0]["phone_last4"] == "1234"


def test_thread_by_full_number_returns_full_history(client, performer, inbox_data):
    # digits-only full number (len > 4) routes via trailing-digit match
    resp = client.get("/api/inbox/12125551234/thread")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["phone_number"] == "+12125551234"
    msgs = body["messages"]
    assert len(msgs) == 4                                  # full conversation
    assert [m["role"] for m in msgs] == ["user", "assistant", "user", "assistant"]
    assert [m["body"] for m in msgs][0] == "Hello"         # chronological order


def test_thread_by_last4_still_works(client, performer, inbox_data):
    resp = client.get("/api/inbox/1234/thread")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["phone_number"] == "+12125551234"
    assert len(body["messages"]) == 4


def test_thread_unknown_returns_404(client, performer, inbox_data):
    assert client.get("/api/inbox/0000/thread").status_code == 404
