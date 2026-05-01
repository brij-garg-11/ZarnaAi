"""
Tests that the performer-only guard added in operator/app/routes/api.py
correctly rejects business accounts (404) while letting super-admins and
performer accounts through.

We test the guard helper in isolation rather than spinning up the whole
operator Flask app — the operator package shares a top-level ``app`` name
with the root Flask app, which makes a clean side-by-side import painful
and isn't necessary to verify guard semantics.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest
from flask import Flask
from werkzeug.exceptions import HTTPException


ROOT = Path(__file__).resolve().parent.parent
API_PATH = ROOT / "operator" / "app" / "routes" / "api.py"


def _load_guard():
    """Load just the guard function from operator/app/routes/api.py.

    Importing the module pulls in the operator app's full route surface
    (queries, db, etc.), which is heavier than this test needs. We compile
    the file under a stub package and grab the symbol we want.
    """
    src = API_PATH.read_text()
    # Stub out heavy imports the helper doesn't actually rely on by giving
    # them shim modules in sys.modules before exec.
    for name, attrs in {
        "operator_app_pkg": {},
        "operator_app_pkg.routes": {},
        "operator_app_pkg.routes.auth": {
            "current_user": lambda: None,
            "login_required": lambda f: f,
            "resolve_slug": lambda: ("", 0),
            "get_authorized_slugs": lambda *a, **kw: set(),
        },
        "operator_app_pkg.queries": {
            "get_overview_stats": lambda **kw: {},
            "list_shows": lambda **kw: [],
            "list_blast_drafts": lambda **kw: [],
            "get_all_tags": lambda **kw: [],
        },
        "operator_app_pkg.db": {"get_conn": lambda: None},
    }.items():
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod
    # Rewrite relative imports so we can exec the file standalone.
    src = src.replace("from ..routes.auth import", "from operator_app_pkg.routes.auth import")
    src = src.replace("from ..queries import", "from operator_app_pkg.queries import")
    src = src.replace("from ..db import", "from operator_app_pkg.db import")
    # Strip the rest of the file beyond the helper we care about so we don't
    # re-execute hundreds of route definitions that import other heavy modules.
    marker = "\n# ── Dashboard ─────"
    src = src.split(marker, 1)[0]
    code = compile(src, str(API_PATH), "exec")
    ns: dict = {
        "__name__": "operator_routes_api_under_test",
        "__file__": str(API_PATH),
    }
    exec(code, ns)
    return ns


GUARD_NS = _load_guard()
_require_performer_account = GUARD_NS["_require_performer_account"]


def _run_under_request(user: dict | None):
    """Invoke the guard inside a Flask request context with current_user mocked."""
    app = Flask(__name__)
    with app.test_request_context("/"):
        with patch.dict(GUARD_NS, {"current_user": lambda: user}):
            try:
                _require_performer_account()
                return None
            except HTTPException as e:
                return e.code


def test_business_account_aborts_404():
    code = _run_under_request({"account_type": "business", "is_super_admin": False})
    assert code == 404


def test_super_admin_business_account_passes():
    code = _run_under_request({"account_type": "business", "is_super_admin": True})
    assert code is None


def test_performer_account_passes():
    code = _run_under_request({"account_type": "performer", "is_super_admin": False})
    assert code is None


def test_missing_account_type_defaults_to_performer():
    """Legacy users created before the account_type column was populated."""
    code = _run_under_request({"account_type": None, "is_super_admin": False})
    assert code is None


def test_no_user_passes_through():
    """Guard runs after @login_required, so an absent user is treated as
    'not a business account' — the login decorator handles the auth failure."""
    code = _run_under_request(None)
    assert code is None
