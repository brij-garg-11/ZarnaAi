"""
Live Shows management — operator view.
Phone numbers are never shown; only counts and masked last-4 digits.
All DB mutations proxy to the main app's live_shows repository via direct SQL.
"""

import logging
import os
import sys

from flask import Blueprint, flash, redirect, render_template, request, url_for

from ..routes.auth import login_required, current_user
from ..queries import list_shows, get_show
from ..db import get_conn

logger = logging.getLogger(__name__)
shows_bp = Blueprint("shows", __name__)


def _update_show_status(show_id: int, new_status: str):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if new_status == "live":
                    # End any currently live show first
                    cur.execute(
                        "UPDATE live_shows SET status='ended' WHERE status='live' AND id != %s",
                        (show_id,),
                    )
                cur.execute(
                    "UPDATE live_shows SET status=%s WHERE id=%s",
                    (new_status, show_id),
                )
                cur.execute(
                    "INSERT INTO live_show_audit_log (show_id, action, detail) VALUES (%s, %s, %s)",
                    (show_id, "show_status", f"status → {new_status} (via operator dashboard)"),
                )
    finally:
        conn.close()


def _get_show_signups_count(show_id: int) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM live_show_signups WHERE show_id=%s", (show_id,))
            return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


def _get_recent_audit(show_id: int) -> list:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT action, detail, created_at FROM live_show_audit_log
                WHERE show_id=%s ORDER BY created_at DESC LIMIT 10
            """, (show_id,))
            return [{"action": r[0], "detail": r[1], "created_at": r[2]} for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


@shows_bp.route("/operator/shows")
@login_required
def list_shows_view():
    try:
        shows = list_shows()
    except Exception as e:
        logger.exception("list_shows error")
        shows = []

    live = [s for s in shows if (s.get("status") or "").lower() == "live"]
    drafts = [s for s in shows if (s.get("status") or "").lower() == "draft"]
    ended = [s for s in shows if (s.get("status") or "").lower() == "ended"]

    return render_template(
        "shows.html",
        user=current_user(),
        live_shows=live,
        draft_shows=drafts,
        ended_shows=ended,
    )


@shows_bp.route("/operator/shows/<int:show_id>")
@login_required
def show_detail(show_id: int):
    show = get_show(show_id)
    if not show:
        flash("Show not found.", "error")
        return redirect(url_for("shows.list_shows_view"))

    audit = _get_recent_audit(show_id)
    ok = request.args.get("ok", "")
    err = request.args.get("err", "")

    return render_template(
        "show_detail.html",
        user=current_user(),
        show=show,
        audit=audit,
        ok_msg=ok,
        err_msg=err,
    )


@shows_bp.route("/operator/shows/<int:show_id>/activate", methods=["POST"])
@login_required
def activate(show_id: int):
    try:
        _update_show_status(show_id, "live")
        return redirect(url_for("shows.show_detail", show_id=show_id, ok="show_live"))
    except Exception as e:
        logger.exception("activate show error")
        return redirect(url_for("shows.show_detail", show_id=show_id, err="activate_failed"))


@shows_bp.route("/operator/shows/<int:show_id>/end", methods=["POST"])
@login_required
def end_show(show_id: int):
    try:
        _update_show_status(show_id, "ended")
        return redirect(url_for("shows.show_detail", show_id=show_id, ok="show_ended"))
    except Exception as e:
        logger.exception("end show error")
        return redirect(url_for("shows.show_detail", show_id=show_id, err="end_failed"))
