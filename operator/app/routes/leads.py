"""
Operator HQ — Access Request review queue (B2B "Apply for access" leads).

Super-admin only. Lists leads from the public apply form and lets the operator
build the bot (filling in the details) + approve, or reject. Server-rendered;
reuses the shared approve/reject core in routes.api.
"""

import logging

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from ..db import get_conn
from ..routes.auth import login_required, current_user
from .api import approve_access_request, reject_access_request, BotCreationError

leads_bp = Blueprint("leads", __name__)
logger = logging.getLogger(__name__)


def _require_super_admin_page():
    """Returns the user dict if super-admin, else None (caller redirects)."""
    user = current_user() or {}
    return user if user.get("is_super_admin") else None


@leads_bp.route("/operator/leads")
@login_required
def leads_index():
    user = _require_super_admin_page()
    if not user:
        flash("Super-admin access required.", "error")
        return redirect(url_for("dashboard.index"))

    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT * FROM access_requests ORDER BY "
                "CASE status WHEN 'new' THEN 0 WHEN 'contacted' THEN 1 ELSE 2 END, "
                "created_at DESC LIMIT 500"
            )
            leads = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    new_count = sum(1 for l in leads if l["status"] == "new")
    return render_template("leads.html", user=user, leads=leads, new_count=new_count)


@leads_bp.route("/operator/leads/<int:req_id>/approve", methods=["POST"])
@login_required
def leads_approve(req_id: int):
    user = _require_super_admin_page()
    if not user:
        flash("Super-admin access required.", "error")
        return redirect(url_for("dashboard.index"))

    media_urls = [
        u.strip() for u in (request.form.get("media_urls") or "").splitlines() if u.strip()
    ]
    data = {
        "display_name":  request.form.get("display_name") or "",
        "slug":          request.form.get("slug") or "",
        "bio":           request.form.get("bio") or "",
        "tone":          request.form.get("tone") or "casual",
        "website_url":   request.form.get("website_url") or "",
        "podcast_url":   request.form.get("podcast_url") or "",
        "extra_context": request.form.get("extra_context") or "",
        "media_urls":    media_urls,
    }
    try:
        result = approve_access_request(req_id, user["id"], data)
        flash(f"Approved — building bot '{result['creator_slug']}' and invite sent.", "success")
    except LookupError:
        flash("Access request not found.", "error")
    except BotCreationError as exc:
        flash(f"Could not build bot: {exc.message}", "error")
    except Exception:
        logger.exception("leads_approve: failed for lead %s", req_id)
        flash("Something went wrong building the bot — check logs.", "error")
    return redirect(url_for("leads.leads_index"))


@leads_bp.route("/operator/leads/<int:req_id>/reject", methods=["POST"])
@login_required
def leads_reject(req_id: int):
    user = _require_super_admin_page()
    if not user:
        flash("Super-admin access required.", "error")
        return redirect(url_for("dashboard.index"))

    if reject_access_request(req_id, user["id"]):
        flash("Lead rejected.", "info")
    else:
        flash("Access request not found.", "error")
    return redirect(url_for("leads.leads_index"))
