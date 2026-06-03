"""
Tests for the B2B "Apply for access" flow:
  - public POST /api/access-request creates a lead ONLY (no account/bot)
  - admin list/approve/reject (super-admin gated)
  - approve builds the bot, creates the user, issues an invite token
"""

import time


def _count(table, where="", params=()):
    from app.db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} {where}", params)
            return cur.fetchone()[0]
    finally:
        conn.close()


def _fetchone(sql, params=()):
    from app.db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


# ── Public apply endpoint ────────────────────────────────────────────────────

def test_access_request_creates_lead_only(client):
    resp = client.post("/api/access-request", json={
        "name": "Matthew Berry",
        "email": "mb@example.com",
        "account_type": "performer",
        "link": "https://youtube.com/mb",
        "goal": "AI fantasy football bot",
    })
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True

    # Exactly one lead, and crucially NO account / bot / provisioning created.
    assert _count("access_requests") == 1
    assert _count("operator_users") == 0
    assert _count("bot_configs") == 0
    row = _fetchone("SELECT status, account_type FROM access_requests LIMIT 1")
    assert row[0] == "new"
    assert row[1] == "performer"


def test_access_request_requires_name_and_email(client):
    assert client.post("/api/access-request", json={"email": "a@b.com"}).status_code == 400
    assert client.post("/api/access-request", json={"name": "X"}).status_code == 400
    assert client.post("/api/access-request", json={"name": "X", "email": "bad"}).status_code == 400
    assert _count("access_requests") == 0


def test_access_request_defaults_account_type(client):
    resp = client.post("/api/access-request", json={"name": "Biz", "email": "b@c.com", "account_type": "junk"})
    assert resp.status_code == 200
    assert _fetchone("SELECT account_type FROM access_requests LIMIT 1")[0] == "performer"


def test_access_request_no_auth_required(client):
    # No session set — still works (public form).
    assert client.post("/api/access-request", json={"name": "N", "email": "n@n.com"}).status_code == 200


# ── Admin gating ─────────────────────────────────────────────────────────────

def test_admin_list_requires_super_admin(client, make_user):
    # Anonymous → 401 (login_required)
    assert client.get("/api/admin/access-requests").status_code == 401
    # Logged-in non-super-admin → 403
    uid = make_user("plain@user.test")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    assert client.get("/api/admin/access-requests").status_code == 403


def test_admin_list_returns_leads(client, super_admin):
    client.post("/api/access-request", json={"name": "Lead One", "email": "l1@x.com"})
    resp = client.get("/api/admin/access-requests")
    assert resp.status_code == 200
    reqs = resp.get_json()["requests"]
    assert len(reqs) == 1
    assert reqs[0]["email"] == "l1@x.com"


# ── Approve ──────────────────────────────────────────────────────────────────

def _poll(predicate, timeout=8.0, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_approve_business_lead_builds_bot_and_provisions(client, super_admin):
    # Business path runs the real (stub-phone) pipeline end to end.
    client.post("/api/access-request", json={
        "name": "Joe's Diner", "email": "joe@diner.com", "account_type": "business",
    })
    lead_id = _fetchone("SELECT id FROM access_requests WHERE email='joe@diner.com'")[0]

    resp = client.post(f"/api/admin/access-requests/{lead_id}/approve", json={
        "display_name": "Joe's Diner", "slug": "joesdiner", "tone": "warm",
    })
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["creator_slug"] == "joesdiner"
    assert body["account_type"] == "business"

    # Account + bot created, lead marked approved, invite token issued.
    assert _count("operator_users", "WHERE email='joe@diner.com'") == 1
    assert _count("bot_configs", "WHERE creator_slug='joesdiner'") == 1
    assert _fetchone("SELECT status FROM access_requests WHERE id=%s", (lead_id,))[0] == "approved"
    assert _count("password_reset_tokens") == 1

    # Provisioning (stub) reaches 'live' and stamps a phone number.
    assert _poll(lambda: _fetchone(
        "SELECT provisioning_status FROM bot_configs WHERE creator_slug='joesdiner'")[0] == "live")
    assert _fetchone("SELECT phone_number FROM operator_users WHERE email='joe@diner.com'")[0]


def test_approve_is_idempotent(client, super_admin):
    client.post("/api/access-request", json={"name": "Dup", "email": "dup@x.com", "account_type": "business"})
    lead_id = _fetchone("SELECT id FROM access_requests WHERE email='dup@x.com'")[0]
    form = {"display_name": "Dup", "slug": "dupbot"}

    r1 = client.post(f"/api/admin/access-requests/{lead_id}/approve", json=form)
    assert r1.status_code == 200
    _poll(lambda: _fetchone("SELECT provisioning_status FROM bot_configs WHERE creator_slug='dupbot'")[0] == "live")

    r2 = client.post(f"/api/admin/access-requests/{lead_id}/approve", json=form)
    assert r2.status_code == 200
    # Still exactly one user + one bot.
    assert _count("operator_users", "WHERE email='dup@x.com'") == 1
    assert _count("bot_configs", "WHERE creator_slug='dupbot'") == 1


def test_approve_requires_super_admin(client, make_user):
    # create a lead directly
    from app.db import get_conn
    conn = get_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("INSERT INTO access_requests (name, email) VALUES ('A','a@a.com') RETURNING id")
        lead_id = cur.fetchone()[0]
    conn.close()
    uid = make_user("plain2@user.test")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    assert client.post(f"/api/admin/access-requests/{lead_id}/approve", json={"display_name": "A"}).status_code == 403


def test_reject_marks_rejected(client, super_admin):
    client.post("/api/access-request", json={"name": "No", "email": "no@x.com"})
    lead_id = _fetchone("SELECT id FROM access_requests WHERE email='no@x.com'")[0]
    resp = client.post(f"/api/admin/access-requests/{lead_id}/reject", json={})
    assert resp.status_code == 200
    assert _fetchone("SELECT status FROM access_requests WHERE id=%s", (lead_id,))[0] == "rejected"
    # No account/bot created on reject.
    assert _count("operator_users", "WHERE email='no@x.com'") == 0


def test_approve_provisions_performer_too(client, super_admin, monkeypatch):
    # Patch the provisioning entrypoint so we assert it's invoked for performers
    # without running Gemini/RAG.
    import app.provisioning as prov
    calls = []
    import threading
    done = threading.Event()

    def _rec(user_id, slug, config):
        calls.append((user_id, slug, config.get("account_type")))
        done.set()

    monkeypatch.setattr(prov, "provision_new_creator", _rec)

    client.post("/api/access-request", json={"name": "Perf", "email": "perf@x.com", "account_type": "performer"})
    lead_id = _fetchone("SELECT id FROM access_requests WHERE email='perf@x.com'")[0]
    resp = client.post(f"/api/admin/access-requests/{lead_id}/approve", json={"display_name": "Perf", "slug": "perfbot"})
    assert resp.status_code == 200
    assert done.wait(timeout=5)
    assert calls and calls[0][1] == "perfbot" and calls[0][2] == "performer"
