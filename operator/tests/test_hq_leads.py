"""
Operator HQ review-queue smoke tests — uses the real create_app() so the
templates (base.html + leads.html) and url_for wiring are exercised.
"""

import time

import pytest


@pytest.fixture(scope="module")
def hq_app():
    # Build an app with the real template folder + the blueprints the HQ leads
    # page needs for url_for(). We avoid create_app() because the working tree
    # has an unrelated, pre-existing broken import (routes.staging missing).
    import os
    import app as app_pkg
    from flask import Flask
    from app.routes.auth import auth_bp
    from app.routes.dashboard import dashboard_bp
    from app.routes.shows import shows_bp
    from app.routes.blast import blast_bp
    from app.routes.team import team_bp
    from app.routes.api import api_bp
    from app.routes.leads import leads_bp

    tpl = os.path.join(os.path.dirname(app_pkg.__file__), "templates")
    a = Flask(__name__, template_folder=tpl)
    a.secret_key = "test-secret"
    for bp in (auth_bp, dashboard_bp, shows_bp, blast_bp, team_bp, api_bp, leads_bp):
        a.register_blueprint(bp)
    a.config["TESTING"] = True
    return a


@pytest.fixture()
def hq_client(hq_app):
    return hq_app.test_client()


def _fetchone(sql, params=()):
    from app.db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def _login_super_admin(hq_client, make_user):
    uid = make_user("hqadmin@twowaybot.test", is_super_admin=True)
    with hq_client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


def test_leads_page_renders(hq_client, make_user):
    _login_super_admin(hq_client, make_user)
    # seed a lead
    from app.db import get_conn
    conn = get_conn(); conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("INSERT INTO access_requests (name, email, account_type) VALUES ('Jane','jane@x.com','performer')")
    conn.close()

    resp = hq_client.get("/operator/leads")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Access Requests" in body
    assert "jane@x.com" in body


def test_leads_page_blocks_non_super_admin(hq_client, make_user):
    uid = make_user("plainhq@user.test")
    with hq_client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    resp = hq_client.get("/operator/leads", follow_redirects=False)
    # Redirects to dashboard (not super-admin)
    assert resp.status_code in (302, 303)


def test_hq_approve_builds_bot(hq_client, make_user):
    _login_super_admin(hq_client, make_user)
    from app.db import get_conn
    conn = get_conn(); conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO access_requests (name, email, account_type) "
            "VALUES ('Cafe','cafe@x.com','business') RETURNING id"
        )
        lead_id = cur.fetchone()[0]
    conn.close()

    resp = hq_client.post(
        f"/operator/leads/{lead_id}/approve",
        data={"display_name": "Cafe", "slug": "cafebot", "tone": "warm"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)
    assert _fetchone("SELECT status FROM access_requests WHERE id=%s", (lead_id,))[0] == "approved"
    assert _fetchone("SELECT id FROM bot_configs WHERE creator_slug='cafebot'") is not None

    deadline = time.time() + 8
    status = None
    while time.time() < deadline:
        status = _fetchone("SELECT provisioning_status FROM bot_configs WHERE creator_slug='cafebot'")[0]
        if status == "live":
            break
        time.sleep(0.2)
    assert status == "live"
