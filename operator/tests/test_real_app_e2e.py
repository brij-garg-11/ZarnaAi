"""
End-to-end against the REAL create_app() WSGI stack — exercises the CSRF/CORS
origin enforcement that wraps every /api/* state-changing request (the minimal
app fixtures in other test modules don't install it).

Covers the production-shaped concern: the public apply form POSTs to
/api/access-request, which IS under /api/, so it must survive the CSRF gate
from an allowed browser origin and be rejected from a foreign origin.
"""

import time

import pytest


@pytest.fixture(scope="module")
def real_app():
    from app import create_app
    a = create_app()
    a.config["TESTING"] = True
    return a


@pytest.fixture()
def rc(real_app):
    return real_app.test_client()


def _fetchone(sql, params=()):
    from app.db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def _count(table):
    return _fetchone(f"SELECT COUNT(*) FROM {table}")[0]


def test_create_app_boots():
    # Regression guard for the routes.staging missing-import crash.
    from app import create_app
    assert create_app() is not None


def test_apply_allowed_from_lovable_origin(rc):
    resp = rc.post(
        "/api/access-request",
        json={"name": "Live Test", "email": "live@test.com", "account_type": "performer"},
        headers={"Origin": "https://my-preview.lovable.app"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True
    assert _count("access_requests") == 1


def test_apply_rejected_from_foreign_origin(rc):
    resp = rc.post(
        "/api/access-request",
        json={"name": "Evil", "email": "evil@x.com"},
        headers={"Origin": "https://evil.example.com"},
    )
    assert resp.status_code == 403
    assert _count("access_requests") == 0


def test_apply_allowed_without_origin_non_strict(rc):
    # Non-browser / no-Origin caller is allowed in default (non-strict) mode.
    resp = rc.post("/api/access-request", json={"name": "NoOrigin", "email": "no@o.com"})
    assert resp.status_code == 200


def test_full_flow_apply_then_approve(rc, make_user):
    # 1. Public applies (with a browser-like origin).
    assert rc.post(
        "/api/access-request",
        json={"name": "Diner", "email": "diner@x.com", "account_type": "business"},
        headers={"Origin": "https://app.lovable.app"},
    ).status_code == 200
    lead_id = _fetchone("SELECT id FROM access_requests WHERE email='diner@x.com'")[0]

    # 2. Super-admin logs in and approves through the real JSON API.
    uid = make_user("radmin@twowaybot.test", is_super_admin=True)
    with rc.session_transaction() as sess:
        sess["operator_user_id"] = uid
    resp = rc.post(
        f"/api/admin/access-requests/{lead_id}/approve",
        json={"display_name": "Diner", "slug": "dinerbot", "tone": "warm"},
        headers={"Origin": "https://app.lovable.app"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    # 3. Account + bot created, invite token issued, provisioning reaches live.
    assert _fetchone("SELECT id FROM operator_users WHERE email='diner@x.com'") is not None
    assert _fetchone("SELECT id FROM bot_configs WHERE creator_slug='dinerbot'") is not None
    assert _count("password_reset_tokens") == 1

    deadline = time.time() + 8
    status = None
    while time.time() < deadline:
        status = _fetchone("SELECT provisioning_status FROM bot_configs WHERE creator_slug='dinerbot'")[0]
        if status == "live":
            break
        time.sleep(0.2)
    assert status == "live"
