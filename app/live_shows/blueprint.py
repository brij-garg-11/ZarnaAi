"""
Live Shows admin UI — same host and Basic Auth as /admin.

Provider reference (shown on pages + in app/messaging/broadcast.py):

- **Twilio:** One REST API call per recipient (`messages.create`). Optional
  `TWILIO_MESSAGING_SERVICE_SID` uses a sender pool. No single “blast array” API.
- **SlickText:** Either **one-by-one** (`send_reply`) or **Campaign mode** (v2 only): we create a
  temporary List, sync contacts, then `POST /campaigns` with `status: send`. See
  `app/messaging/slicktext_campaigns.py`.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from urllib.parse import quote

from flask import Blueprint, Response, redirect, request, url_for

from app.admin_auth import (
    admin_password_configured,
    check_admin_auth,
    no_admin_password_response,
    require_admin_auth_response,
)
from app.live_shows import repository as repo
from app.live_shows.broadcast_worker import start_broadcast_thread

logger = logging.getLogger(__name__)

live_shows_bp = Blueprint("live_shows", __name__)


def _e(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _auth_gate():
    if not admin_password_configured():
        return no_admin_password_response()
    if not check_admin_auth():
        return require_admin_auth_response()
    return None


def _parse_utc_datetime(value: str | None):
    if not value or not str(value).strip():
        return None
    v = str(value).strip()
    try:
        if len(v) == 16 and "T" in v:
            return datetime.strptime(v, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None


def _shell(title: str, body: str, nav_active: str = "live") -> str:
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_e(title)} — Zarna</title>
<style>
body {{ margin:0; background:#0a0f1e; color:#e2e8f0;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }}
.header {{ background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 60%,#0891b2 100%);
  padding:18px 28px; }}
.header h1 {{ margin:0; font-size:20px; color:white; }}
.nav {{ background:#111827; border-bottom:1px solid #1f2937; padding:0 28px;
  display:flex; gap:4px; flex-wrap:wrap; }}
.nav a {{ padding:14px 18px; color:#64748b; text-decoration:none; font-size:14px;
  border-bottom:2px solid transparent; }}
.nav a:hover {{ color:#e2e8f0; }}
.nav a.active {{ color:#a78bfa; border-bottom-color:#7c3aed; }}
.container {{ max-width:960px; margin:0 auto; padding:24px 28px; }}
.card {{ background:#111827; border:1px solid #1f2937; border-radius:12px;
  padding:20px; margin-bottom:20px; }}
label {{ display:block; color:#9ca3af; font-size:12px; margin:12px 0 6px; text-transform:uppercase; letter-spacing:0.06em; }}
input[type=text], input[type=datetime-local], textarea, select {{
  width:100%; max-width:520px; padding:10px 14px; border-radius:8px; border:1px solid #374151;
  background:#1f2937; color:#e2e8f0; font-size:14px; box-sizing:border-box; }}
textarea {{ min-height:100px; max-width:100%; }}
.btn {{ display:inline-block; background:#7c3aed; color:white; border:none; border-radius:8px;
  padding:10px 20px; font-size:14px; cursor:pointer; text-decoration:none; margin-right:8px; margin-top:8px; }}
.btn:hover {{ background:#6d28d9; }}
.btn-danger {{ background:#991b1b; }}
.btn-secondary {{ background:#374151; }}
.btn-secondary:hover {{ background:#4b5563; }}
small.hint {{ color:#64748b; display:block; margin-top:6px; font-size:12px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:12px; }}
th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #1f2937; }}
th {{ color:#6b7280; font-size:11px; text-transform:uppercase; }}
.badge {{ display:inline-block; padding:2px 10px; border-radius:12px; font-size:12px; }}
.badge-live {{ background:#065f46; color:#6ee7b7; }}
.badge-draft {{ background:#374151; color:#9ca3af; }}
.badge-ended {{ background:#312e81; color:#c4b5fd; }}
.callout {{ background:#1e3a5f; border:1px solid #2563eb; border-radius:8px; padding:14px;
  font-size:13px; color:#93c5fd; margin-bottom:20px; line-height:1.5; }}
.mono {{ font-family:ui-monospace,monospace; }}
</style></head><body>
<div class="header"><h1>Zarna AI — {_e(title)}</h1></div>
<nav class="nav">
  <a href="/admin?tab=overview">📊 Overview</a>
  <a href="/admin?tab=audience">👥 Audience</a>
  <a href="/admin?tab=convos">💬 Conversations</a>
  <a href="/admin/live-shows" class="{'active' if nav_active == 'live' else ''}">🎤 Live shows</a>
</nav>
<div class="container">{body}</div>
</body></html>"""


@live_shows_bp.before_request
def _before():
    g = _auth_gate()
    if g is not None:
        return g


@live_shows_bp.route("/admin/live-shows")
def list_shows():
    shows = repo.list_shows()
    rows = ""
    for s in shows:
        st = (s.get("status") or "draft").lower()
        badge = f"badge-{st}" if st in ("live", "draft", "ended") else "badge-draft"
        kw = _e((s.get("keyword") or "") or "—")
        rows += f"""<tr>
          <td><a href="/admin/live-shows/{s['id']}" style="color:#a78bfa">{_e(s['name'])}</a></td>
          <td><span class="badge {badge}">{st}</span></td>
          <td>{s.get('signup_count', 0)}</td>
          <td class="mono">{kw}</td>
        </tr>"""
    if not rows:
        rows = '<tr><td colspan="4" style="color:#6b7280">No shows yet.</td></tr>'
    body = f"""
<div class="callout">
  <strong>How bulk send works</strong><br>
  • <strong>Twilio:</strong> one <code>messages.create</code> per number (optional <code>TWILIO_MESSAGING_SERVICE_SID</code>).<br>
  • <strong>SlickText one-by-one:</strong> same API as chat; works on v1 and v2.<br>
  • <strong>SlickText campaign:</strong> v2 only — temp list + sync contacts + one Campaign send. Choose it on the show page. SMS only.<br>
  • <code>LIVE_SHOW_BROADCAST_PROVIDER</code> = <code>slicktext</code> | <code>twilio</code> | <code>auto</code>.
</div>
<p><a class="btn" href="/admin/live-shows/new">+ New live show</a></p>
<div class="card">
  <h2 style="margin-top:0;font-size:16px">All shows</h2>
  <table><thead><tr><th>Name</th><th>Status</th><th>Signups</th><th>Keyword</th></tr></thead>
  <tbody>{rows}</tbody></table>
</div>"""
    return _shell("Live shows", body)


@live_shows_bp.route("/admin/live-shows/new", methods=["GET", "POST"])
def new_show():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        keyword = request.form.get("keyword", "").strip()
        mode = request.form.get("signup_mode", "keyword")
        use_kw = mode == "keyword"
        ws = _parse_utc_datetime(request.form.get("window_start"))
        we = _parse_utc_datetime(request.form.get("window_end"))
        deliver = (request.form.get("deliver_as") or "sms").strip().lower()
        if deliver not in ("sms", "whatsapp"):
            deliver = "sms"
        if not name:
            return _shell("New live show", '<div class="card"><p style="color:#f87171">Name required.</p></div>')
        if use_kw and not keyword:
            return _shell("New live show", '<div class="card"><p style="color:#f87171">Keyword required for keyword mode.</p></div>')
        if not use_kw and (ws is None or we is None):
            return _shell(
                "New live show",
                '<div class="card"><p style="color:#f87171">Window start and end required for time-window mode.</p></div>',
            )
        try:
            sid = repo.create_show(name, keyword, use_kw, ws, we, deliver)
        except Exception as e:
            logger.exception("create_show")
            return _shell("New live show", f'<div class="card"><p style="color:#f87171">{_e(str(e))}</p></div>')
        return redirect(url_for("live_shows.show_detail", show_id=sid))

    form = """
<div class="card">
<form method="post">
  <label>Show name</label>
  <input type="text" name="name" required placeholder="e.g. Chicago Theatre — Mar 15">
  <label>How people join</label>
  <select name="signup_mode">
    <option value="keyword" selected>Keyword — fan texts this word (whole message or first word)</option>
    <option value="window">Time window only — any message counts (needs start + end)</option>
  </select>
  <label>Keyword (keyword mode)</label>
  <input type="text" name="keyword" placeholder="e.g. CHICAGO">
  <small class="hint">Leave blank only if using time-window-only mode (then keyword ignored).</small>
  <label>Window start (UTC, optional filter)</label>
  <input type="datetime-local" name="window_start">
  <label>Window end (UTC, optional filter)</label>
  <input type="datetime-local" name="window_end">
  <small class="hint">In keyword mode, optional window still restricts when signups count. Times are interpreted as UTC.</small>
  <label>Broadcast channel</label>
  <select name="deliver_as">
    <option value="sms">SMS (Twilio SMS / SlickText SMS)</option>
    <option value="whatsapp">WhatsApp (Twilio only — templates may apply outside 24h session)</option>
  </select>
  <p><button type="submit" class="btn">Create draft</button>
  <a class="btn btn-secondary" href="/admin/live-shows">Cancel</a></p>
</form>
</div>"""
    return _shell("New live show", form)


@live_shows_bp.route("/admin/live-shows/<int:show_id>")
def show_detail(show_id: int):
    show = repo.get_show(show_id)
    if not show:
        return _shell("Live show", '<div class="card"><p>Show not found.</p></div>'), 404
    err_banner = ""
    err = request.args.get("err", "")
    if err == "campaign_sms":
        err_banner = '<div class="callout" style="border-color:#f87171;color:#fecaca">Campaign mode is SMS-only. Change “Broadcast channel” to SMS or use one-by-one with Twilio.</div>'
    elif err == "campaign_slicktext":
        err_banner = '<div class="callout" style="border-color:#f87171;color:#fecaca">Campaign mode needs SlickText (not Twilio). Set provider to SlickText or Auto with v2 keys.</div>'
    signups = repo.signups_for_show(show_id)
    job = repo.latest_job_for_show(show_id)
    st = show["status"]
    status_btns = ""
    if st != "live":
        status_btns += f"""
<form method="post" action="/admin/live-shows/{show_id}/status" style="display:inline">
  <input type="hidden" name="status" value="live">
  <button type="submit" class="btn">Go live</button>
</form>"""
    if st == "live":
        status_btns += f"""
<form method="post" action="/admin/live-shows/{show_id}/status" style="display:inline">
  <input type="hidden" name="status" value="ended">
  <button type="submit" class="btn btn-secondary">End show</button>
</form>"""
    job_html = ""
    if job:
        job_html = f"""<p style="font-size:13px;color:#94a3b8">Last broadcast job: #{job['id']} — {job['status']} —
        {job.get('sent_count',0)} sent, {job.get('failed_count',0)} failed (of {job.get('total_recipients',0)})</p>"""
        if job.get("last_error"):
            job_html += f'<p style="color:#f87171;font-size:12px">{_e(job["last_error"][:500])}</p>'

    sig_rows = ""
    for u in signups[:500]:
        ph = u["phone_number"]
        q = quote(ph, safe="")
        sig_rows += f"""<tr><td class="mono">{_e(ph)}</td><td>{_e(u.get("channel") or "")}</td>
        <td>{u["signed_up_at"]}</td>
        <td><a href="/admin?tab=convos&thread={q}" style="color:#a78bfa">Thread</a></td></tr>"""
    if len(signups) > 500:
        sig_rows += f'<tr><td colspan="4" style="color:#6b7280">…and {len(signups)-500} more (export CSV for full list)</td></tr>'
    if not sig_rows:
        sig_rows = '<tr><td colspan="4" style="color:#6b7280">No signups yet.</td></tr>'

    broadcast_form = f"""
<div class="card">
  <h3 style="margin-top:0">Broadcast to this list</h3>
  <form method="post" action="/admin/live-shows/{show_id}/broadcast">
    <label>Message</label>
    <textarea name="body" required placeholder="Your text to everyone signed up…"></textarea>
    <label>SlickText delivery (when using SlickText v2)</label>
    <select name="slicktext_delivery">
      <option value="loop" selected>One-by-one — each text via Messages API (v1 + v2)</option>
      <option value="slicktext_campaign">Campaign — temp list + one Campaign send (v2 API key + brand only; fastest)</option>
    </select>
    <small class="hint">Campaign mode creates contacts in SlickText if needed, adds them to a new list, then fires
      <code>POST /campaigns</code> with <code>status: send</code>. Do not delete that list until SlickText finishes sending unless you know it is safe.</small>
    <label>Provider override</label>
    <select name="provider">
      <option value="">Auto (LIVE_SHOW_BROADCAST_PROVIDER / resolve)</option>
      <option value="slicktext">SlickText</option>
      <option value="twilio">Twilio</option>
    </select>
    <label><input type="checkbox" name="confirm" value="1" required> I confirm sending to {show.get("signup_count",0)} numbers</label>
    <button type="submit" class="btn">Queue send</button>
  </form>
  <small class="hint">Runs in the background; refresh for job status. WhatsApp blasts: use Twilio + one-by-one.</small>
  {job_html}
</div>"""

    body = f"""
{err_banner}
<p>{status_btns}
<a class="btn btn-secondary" href="/admin/live-shows/{show_id}/export">Export CSV</a>
<a class="btn btn-secondary" href="/admin/live-shows">All shows</a></p>
<div class="card">
  <h2 style="margin-top:0">{_e(show["name"])}</h2>
  <p>Status: <strong>{st}</strong> · Keyword: <code>{_e(show.get("keyword") or "")}</code>
  · Deliver as: <strong>{_e(show.get("deliver_as") or "sms")}</strong></p>
  <p style="color:#94a3b8;font-size:13px">Signups: {show.get("signup_count", 0)}</p>
</div>
{broadcast_form}
<div class="card"><h3>Signups</h3>
<table><thead><tr><th>Phone</th><th>Channel</th><th>Signed up</th><th></th></tr></thead><tbody>{sig_rows}</tbody></table>
</div>"""
    return _shell(show["name"], body)


@live_shows_bp.route("/admin/live-shows/<int:show_id>/status", methods=["POST"])
def show_status(show_id: int):
    new_st = request.form.get("status", "").strip().lower()
    if new_st not in ("live", "ended", "draft"):
        return redirect(url_for("live_shows.show_detail", show_id=show_id))
    repo.update_show_status(show_id, new_st)
    return redirect(url_for("live_shows.show_detail", show_id=show_id))


@live_shows_bp.route("/admin/live-shows/<int:show_id>/broadcast", methods=["POST"])
def show_broadcast(show_id: int):
    show = repo.get_show(show_id)
    if not show:
        return redirect("/admin/live-shows")
    body = request.form.get("body", "").strip()
    if not body or request.form.get("confirm") != "1":
        return redirect(url_for("live_shows.show_detail", show_id=show_id))
    prov = request.form.get("provider", "").strip().lower()
    if prov not in ("", "slicktext", "twilio"):
        prov = ""
    p_choice = prov if prov in ("slicktext", "twilio") else None
    resolve = __import__("app.messaging.broadcast", fromlist=["resolve_broadcast_provider"]).resolve_broadcast_provider
    resolved = p_choice or resolve()
    delivery = request.form.get("slicktext_delivery", "loop").strip().lower()
    if delivery not in ("loop", "slicktext_campaign"):
        delivery = "loop"
    deliver_as = (show.get("deliver_as") or "sms").lower()

    if delivery == "slicktext_campaign":
        if deliver_as == "whatsapp":
            return redirect(url_for("live_shows.show_detail", show_id=show_id, err="campaign_sms"))
        eff = p_choice or resolved
        if eff == "twilio" or p_choice == "twilio":
            return redirect(url_for("live_shows.show_detail", show_id=show_id, err="campaign_slicktext"))

    if delivery == "slicktext_campaign":
        job_provider = "slicktext_campaign"
    else:
        job_provider = resolved

    try:
        jid = repo.create_broadcast_job(show_id, body[:6400], job_provider)
    except Exception as e:
        logger.exception("create_broadcast_job")
        return _shell("Error", f'<div class="card"><p>{_e(str(e))}</p></div>')
    start_broadcast_thread(jid, show_id, body, p_choice, show.get("deliver_as") or "sms", delivery)
    return redirect(url_for("live_shows.show_detail", show_id=show_id) + "?broadcast=queued")


@live_shows_bp.route("/admin/live-shows/<int:show_id>/export")
def export_show(show_id: int):
    signups = repo.signups_for_show(show_id)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["phone_number", "channel", "signed_up_at"])
    for u in signups:
        w.writerow([u["phone_number"], u.get("channel") or "", u["signed_up_at"]])
    show = repo.get_show(show_id) or {}
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in (show.get("name") or "export"))[:40]
    fn = f"live-show-{show_id}-{safe or 'fans'}.csv"
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )
