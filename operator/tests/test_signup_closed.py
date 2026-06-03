"""
Public self-serve account + bot creation is closed in the B2B model:
  - POST /api/auth/signup  → 403 (apply for access)
  - POST /api/onboarding/submit → 403 for non-super-admins
"""


def test_public_signup_is_closed(client):
    resp = client.post("/api/auth/signup", json={
        "email": "new@user.com", "password": "longenough123", "name": "New",
    })
    assert resp.status_code == 403
    body = resp.get_json()
    assert body["success"] is False
    assert body.get("apply_required") is True
    # No account created.
    from app.db import get_conn
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM operator_users WHERE email='new@user.com'")
        assert cur.fetchone()[0] == 0
    conn.close()


def test_onboarding_submit_blocked_for_normal_user(client, make_user):
    uid = make_user("normal@user.com")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    resp = client.post("/api/onboarding/submit", json={"display_name": "X", "slug": "xbot"})
    assert resp.status_code == 403
    assert resp.get_json()["success"] is False


def test_onboarding_submit_allowed_for_super_admin(client, super_admin, monkeypatch):
    # Super-admin may still create a bot manually; patch provisioning to keep it fast.
    import app.provisioning as prov
    monkeypatch.setattr(prov, "provision_new_creator", lambda *a, **k: None)
    resp = client.post("/api/onboarding/submit", json={
        "account_type": "business", "display_name": "Admin Bot", "slug": "adminbot",
    })
    assert resp.status_code == 200
    assert resp.get_json()["creator_slug"] == "adminbot"
