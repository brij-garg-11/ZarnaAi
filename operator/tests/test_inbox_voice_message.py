"""
Voice-message (audio MMS) attachments in the operator inbox.

Operators can upload a recorded audio clip (e.g. a personal voice message from
the creator) and send it to a single fan as a Twilio MMS. These tests cover the
upload endpoint's validation, the inbox-send validation, and the happy path
where the clip is sent, persisted with its media_url, and surfaced back in the
thread history.

`messages` / `contacts` are main-app tables not created by the operator
migrations, so we create minimal versions here (mirroring test_inbox_full_numbers).
"""

import datetime
import io

import pytest


@pytest.fixture()
def inbox_data():
    from app.db import get_conn
    conn = get_conn(); conn.autocommit = True
    phone = "+12125559876"
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, role TEXT, text TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(), creator_slug TEXT,
                intent TEXT, tone_mode TEXT, sell_variant TEXT, source TEXT,
                media_url TEXT)
        """)
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_url TEXT")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, creator_slug TEXT,
                fan_tier TEXT, fan_tags TEXT[], fan_location TEXT, fan_name TEXT,
                fan_memory TEXT, fan_score INT, created_at TIMESTAMPTZ DEFAULT NOW())
        """)
        cur.execute("TRUNCATE messages, contacts")
        cur.execute(
            "INSERT INTO contacts (phone_number, creator_slug, fan_tier) "
            "VALUES (%s,'zarna','superfan')",
            (phone,),
        )
        base = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        cur.execute(
            "INSERT INTO messages (phone_number, role, text, created_at, creator_slug) "
            "VALUES (%s,'user','hi',%s,'zarna')",
            (phone, base),
        )
    conn.close()
    return phone


@pytest.fixture()
def performer(client, make_user):
    uid = make_user("voice@zarna.test", creator_slug="zarna", account_type="performer")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


@pytest.fixture()
def twilio_env(monkeypatch):
    """_send_twilio bails unless the core Twilio creds are present."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtest")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok")
    monkeypatch.setenv("TWILIO_PHONE_NUMBER", "+15550001111")
    monkeypatch.delenv("TWILIO_MESSAGING_SERVICE_SID", raising=False)


# ── upload-audio validation ───────────────────────────────────────────────────

def test_upload_audio_rejects_unsupported_format(client, performer):
    resp = client.post(
        "/api/inbox/upload-audio",
        data={"audio": (io.BytesIO(b"not audio"), "notes.txt")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 400
    assert "Unsupported" in resp.get_json()["error"]


def test_upload_audio_stores_and_returns_url(client, performer):
    resp = client.post(
        "/api/inbox/upload-audio",
        data={"audio": (io.BytesIO(b"ID3 fake mp3 bytes"), "hello.m4a")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert "/operator/blast/img/" in body["url"]


# ── inbox-send validation ──────────────────────────────────────────────────────

def test_inbox_send_requires_text_or_media(client, performer, inbox_data):
    resp = client.post("/api/inbox/9876/send", json={})
    assert resp.status_code == 400


def test_inbox_send_rejects_foreign_media_url(client, performer, inbox_data):
    resp = client.post(
        "/api/inbox/9876/send",
        json={"media_url": "https://evil.example.com/clip.mp3"},
    )
    assert resp.status_code == 400


# ── happy path: send an audio clip and see it in the thread ────────────────────

def test_send_voice_message_persists_and_appears_in_thread(
    client, performer, inbox_data, twilio_env
):
    up = client.post(
        "/api/inbox/upload-audio",
        data={"audio": (io.BytesIO(b"voice memo bytes"), "gift.m4a")},
        content_type="multipart/form-data",
    )
    media_url = up.get_json()["url"]

    sent = client.post("/api/inbox/9876/send", json={"media_url": media_url})
    assert sent.status_code == 200, sent.get_data(as_text=True)
    assert sent.get_json()["success"] is True

    thread = client.get("/api/inbox/9876/thread")
    assert thread.status_code == 200
    msgs = thread.get_json()["messages"]
    audio_msgs = [m for m in msgs if m.get("media_url")]
    assert len(audio_msgs) == 1
    assert audio_msgs[0]["media_url"] == media_url
    assert audio_msgs[0]["role"] == "assistant"
