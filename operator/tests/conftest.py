"""
Test harness for the operator service.

The operator service uses its own top-level `app` package (which collides with
the main app's `app/`). These tests MUST be run from the operator/ directory so
`import app` resolves to operator/app:

    cd operator && python -m pytest tests/

A disposable Postgres is expected at DATABASE_URL (default points at the local
docker test container on port 55432).
"""

import os
import sys
from unittest.mock import MagicMock

HERE = os.path.dirname(__file__)
OPERATOR_DIR = os.path.abspath(os.path.join(HERE, ".."))
if OPERATOR_DIR not in sys.path:
    sys.path.insert(0, OPERATOR_DIR)

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:test@localhost:55432/zarna_test")
os.environ.setdefault("PROVISIONING_PHONE_MODE", "stub")
os.environ.setdefault("TWILIO_WEBHOOK_BASE", "https://app.test.example")
# Disable outbound integrations in tests.
os.environ["RESEND_API_KEY"] = ""
os.environ["NOTION_TOKEN"] = ""
os.environ["NOTION_LEADS_DB_ID"] = ""

# Stub optional heavy deps that may not be installed in the local env.
for _m in ("resend", "twilio", "twilio.rest", "apscheduler",
           "apscheduler.schedulers", "apscheduler.schedulers.background"):
    sys.modules.setdefault(_m, MagicMock())

import pytest  # noqa: E402
from flask import Flask  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _migrate():
    from app.db import init_db
    init_db()


@pytest.fixture(autouse=True)
def _clean_tables():
    """Fresh slate before each test (dedicated test DB)."""
    from app.db import get_conn
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE access_requests, password_reset_tokens, team_members, "
                "bot_configs, operator_users RESTART IDENTITY CASCADE"
            )
    finally:
        conn.close()
    yield


@pytest.fixture()
def app():
    from app.routes.auth import auth_bp
    from app.routes.api import api_bp
    a = Flask(__name__)
    a.secret_key = "test-secret"
    a.register_blueprint(auth_bp)
    a.register_blueprint(api_bp)
    a.config["TESTING"] = True
    return a


@pytest.fixture()
def client(app):
    return app.test_client()


def _insert_user(email, *, is_super_admin=False, creator_slug=None, account_type=None):
    from app.db import get_conn
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO operator_users
                       (email, name, password_hash, is_active, is_owner, is_super_admin,
                        creator_slug, account_type)
                   VALUES (%s, %s, '', TRUE, %s, %s, %s, %s)
                   RETURNING id""",
                (email, email.split("@")[0].title(), is_super_admin, is_super_admin,
                 creator_slug, account_type),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


@pytest.fixture()
def super_admin(client):
    uid = _insert_user("admin@twowaybot.test", is_super_admin=True)
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


@pytest.fixture()
def make_user():
    return _insert_user
