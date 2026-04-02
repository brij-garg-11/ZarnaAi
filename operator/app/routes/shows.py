"""
Live Shows management — operator view.
Phone numbers are never shown; only counts and masked last-4 digits.
All DB mutations proxy to the main app's live_shows repository via direct SQL.
"""

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

from ..routes.auth import login_required, current_user
from ..queries import list_shows, get_show
from ..db import get_conn

EVENT_TIMEZONE_CHOICES = (
    ("America/New_York", "Eastern (New York)"),
    ("America/Chicago", "Central (Chicago)"),
    ("America/Denver", "Mountain (Denver)"),
    ("America/Los_Angeles", "Pacific (Los Angeles)"),
    ("America/Phoenix", "Arizona (Phoenix)"),
    ("UTC", "UTC"),
)
_ALLOWED_TZ = {z for z, _ in EVENT_TIMEZONE_CHOICES}


def _parse_local_dt(value, tz_name):
    if not value or not str(value).strip():
        return None
    v = str(value).strip()
    try:
        naive = datetime.strptime(v[:16], "%Y-%m-%dT%H:%M")
        tz_name = tz_name if tz_name in _ALLOWED_TZ else "America/New_York"
        aware = naive.replace(tzinfo=ZoneInfo(tz_name))
        return aware.astimezone(timezone.utc)
    except (ValueError, OSError):
        return None

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
                    "INSERT INTO admin_audit_log (show_id, action, detail) VALUES (%s, %s, %s)",
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
                SELECT action, detail, created_at FROM admin_audit_log
                WHERE show_id=%s ORDER BY created_at DESC LIMIT 10
            """, (show_id,))
            return [{"action": r[0], "detail": r[1], "created_at": r[2]} for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def _create_show(name, keyword, use_keyword, window_start, window_end, deliver_as, event_category, event_timezone):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO live_shows
                      (name, keyword, use_keyword_only, window_start, window_end,
                       deliver_as, event_category, event_timezone, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'draft')
                    RETURNING id
                """, (name, keyword or None, use_keyword, window_start, window_end,
                      deliver_as, event_category, event_timezone))
                show_id = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO admin_audit_log (show_id, action, detail) VALUES (%s,%s,%s)",
                    (show_id, "show_created", "created via operator dashboard"),
                )
                return show_id
    finally:
        conn.close()


@shows_bp.route("/operator/shows/new", methods=["GET", "POST"])
@login_required
def new_show():
    error = None
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        # Normalize: uppercase, collapse internal spaces, trim edges (allow phrases)
        keyword = " ".join((request.form.get("keyword") or "").upper().split())
        mode = request.form.get("signup_mode", "keyword")
        use_kw = (mode == "keyword")
        etz = request.form.get("event_timezone") or "America/New_York"
        ws = _parse_local_dt(request.form.get("window_start"), etz)
        we = _parse_local_dt(request.form.get("window_end"), etz)
        deliver = (request.form.get("deliver_as") or "sms").lower()
        if deliver not in ("sms", "whatsapp"):
            deliver = "sms"
        event_cat = (request.form.get("event_category") or "comedy").lower()
        if event_cat not in ("comedy", "live_stream", "other"):
            event_cat = "comedy"

        if not name:
            error = "Show name is required."
        elif use_kw and not keyword:
            error = "Keyword is required for keyword mode."
        elif not use_kw and (ws is None or we is None):
            error = "Window start and end are required for time-window mode."
        else:
            try:
                show_id = _create_show(name, keyword, use_kw, ws, we, deliver, event_cat, etz)
                flash(f'Show "{name}" created as a draft.', "success")
                return redirect(url_for("shows.show_detail", show_id=show_id))
            except Exception as e:
                logger.exception("create show error")
                error = f"Error creating show: {e}"

    return render_template(
        "show_new.html",
        user=current_user(),
        error=error,
        tz_choices=EVENT_TIMEZONE_CHOICES,
    )


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


@shows_bp.route("/operator/shows/<int:show_id>/live-status")
@login_required
def show_live_status(show_id: int):
    """Lightweight JSON endpoint polled by the browser for live signup counts."""
    show = get_show(show_id)
    if not show:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "status": show["status"],
        "signup_count": show["signup_count"],
    })


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
