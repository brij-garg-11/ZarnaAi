"""Shared HTTP Basic Auth for admin dashboard and Live Shows tools."""

import hmac
import os

from flask import Response, request

_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")


def admin_password_configured() -> bool:
    return bool(_ADMIN_PASSWORD)


def check_admin_auth() -> bool:
    if not _ADMIN_PASSWORD:
        return False
    auth = request.authorization
    if not auth or not auth.password:
        return False
    return hmac.compare_digest(auth.password, _ADMIN_PASSWORD)


def require_admin_auth_response() -> Response:
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="Zarna AI Admin"'},
    )


def no_admin_password_response() -> Response:
    return Response(
        "<h2 style='font-family:sans-serif;padding:40px;color:#dc2626'>"
        "Admin access is disabled: ADMIN_PASSWORD environment variable is not set.<br>"
        "<small style='color:#6b7280'>Set it in Railway → Variables to enable the dashboard.</small>"
        "</h2>",
        503,
    )


def get_db_connection():
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        return None
    import psycopg2
    dsn = database_url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)
