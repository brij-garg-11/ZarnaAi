"""
Team management — owners can add, deactivate, and reset passwords for operators.
"""

import logging
from flask import Blueprint, flash, redirect, render_template, request, url_for
from werkzeug.security import generate_password_hash
from ..routes.auth import login_required, owner_required, current_user
from ..db import get_conn
import psycopg2.extras

logger = logging.getLogger(__name__)
team_bp = Blueprint("team", __name__)


def _list_users():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, email, name, is_owner, is_active, created_at, last_login_at
                FROM operator_users ORDER BY created_at ASC
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@team_bp.route("/operator/team")
@owner_required
def team_index():
    users = _list_users()
    return render_template("team.html", user=current_user(), users=users)


@team_bp.route("/operator/team/add", methods=["POST"])
@owner_required
def add_user():
    email = (request.form.get("email") or "").strip().lower()
    name = (request.form.get("name") or "").strip()
    password = (request.form.get("password") or "").strip()
    is_owner = request.form.get("is_owner") == "1"

    if not email or not password or not name:
        flash("Email, name, and password are required.", "error")
        return redirect(url_for("team.team_index"))

    if len(password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("team.team_index"))

    pw_hash = generate_password_hash(password)
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO operator_users (email, name, password_hash, is_owner)
                       VALUES (%s, %s, %s, %s)""",
                    (email, name, pw_hash, is_owner),
                )
        conn.close()
        flash(f"{name} added successfully.", "success")
    except Exception as e:
        if "unique" in str(e).lower():
            flash(f"{email} already has an account.", "error")
        else:
            flash(f"Error adding user: {e}", "error")

    return redirect(url_for("team.team_index"))


@team_bp.route("/operator/team/<int:user_id>/toggle", methods=["POST"])
@owner_required
def toggle_user(user_id: int):
    me = current_user()
    if me and me["id"] == user_id:
        flash("You cannot deactivate yourself.", "error")
        return redirect(url_for("team.team_index"))

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE operator_users SET is_active = NOT is_active WHERE id=%s",
                    (user_id,),
                )
        flash("User status updated.", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("team.team_index"))


@team_bp.route("/operator/team/<int:user_id>/reset-password", methods=["POST"])
@owner_required
def reset_password(user_id: int):
    new_pw = (request.form.get("new_password") or "").strip()
    if len(new_pw) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("team.team_index"))

    pw_hash = generate_password_hash(new_pw)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE operator_users SET password_hash=%s WHERE id=%s",
                    (pw_hash, user_id),
                )
        flash("Password reset successfully.", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("team.team_index"))
