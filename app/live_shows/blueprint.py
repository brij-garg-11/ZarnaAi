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
import json
import logging
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
from app.live_shows.event_time import (
    EVENT_TIMEZONE_CHOICES,
    format_window_human,
    parse_local_datetime,
    timezone_select_value_from_show,
    utc_to_datetime_local_value,
)
from app.messaging.broadcast import normalize_e164, resolve_broadcast_provider
from app.messaging.slicktext_adapter import create_slicktext_adapter
from app.messaging.twilio_adapter import create_twilio_adapter

logger = logging.getLogger(__name__)

live_shows_bp = Blueprint("live_shows", __name__)

# One-click broadcast starters (message body only; operator edits before send).
BROADCAST_BODY_TEMPLATES = {
    "doors": "Doors are open — come on in, find your seat, and get ready to laugh. So excited you're here!",
    "thanks": "Thank you for being here tonight — you brought the energy. Means the world. More fun coming!",
    "merch": "Merch table is open after the show — come say hi if you want a souvenir from tonight!",
    "link": "Here's the link I promised — let me know if it doesn't work. Can't wait to hear what you think!",
    "rain": "If you're running late or stuck in weather, no stress — we've got you. Text me if you need anything.",
}


def _mask_phone_display(phone: str) -> str:
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if len(digits) <= 4:
        return "****"
    return f"+{'·' * (len(digits) - 4)}{digits[-4:]}"


def _send_one_outbound(phone_raw: str, body: str, deliver_as: str, provider_override: str | None) -> bool:
    """Single test SMS/WhatsApp for operator verification."""
    prov = provider_override if provider_override in ("slicktext", "twilio") else resolve_broadcast_provider()
    wa = (deliver_as or "sms").lower() == "whatsapp"
    if prov == "slicktext" and wa:
        return False
    if prov == "slicktext":
        st = create_slicktext_adapter()
        to = normalize_e164(phone_raw)
        return bool(to) and st.send_reply(to, body[:1600])
    tw = create_twilio_adapter()
    return tw.send_reply(phone_raw, body[:1600])


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


def _timezone_options_html(selected: str) -> str:
    opts = []
    for z, lbl in EVENT_TIMEZONE_CHOICES:
        sel = " selected" if z == selected else ""
        opts.append(f'<option value="{_e(z)}"{sel}>{_e(lbl)}</option>')
    return "\n".join(opts)


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
.btn-sm {{ padding:6px 12px; font-size:12px; margin:0; }}
small.hint {{ color:#64748b; display:block; margin-top:6px; font-size:12px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:12px; }}
table .actions {{ white-space:nowrap; vertical-align:middle; }}
table .actions form {{ display:inline-block; vertical-align:middle; margin:2px 0; }}
table .actions form + form {{ margin-left:10px; }}
table .actions input[type=text] {{ max-width:min(200px,42vw); padding:6px 8px; font-size:12px; margin:0 6px 0 0; }}
.danger-zone {{ border-color:#7f1d1d !important; background:#1c1917; }}
th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #1f2937; }}
th {{ color:#6b7280; font-size:11px; text-transform:uppercase; }}
.badge {{ display:inline-block; padding:2px 10px; border-radius:12px; font-size:12px; }}
.badge-live {{ background:#065f46; color:#6ee7b7; }}
.badge-draft {{ background:#374151; color:#9ca3af; }}
.badge-ended {{ background:#312e81; color:#c4b5fd; }}
.callout {{ background:#1e3a5f; border:1px solid #2563eb; border-radius:8px; padding:14px;
  font-size:13px; color:#93c5fd; margin-bottom:20px; line-height:1.5; }}
.mono {{ font-family:ui-monospace,monospace; }}
.section-title {{ font-size:13px; font-weight:600; color:#94a3b8; text-transform:uppercase;
  letter-spacing:0.08em; margin:28px 0 10px 0; }}
.section-title:first-of-type {{ margin-top:0; }}
.stats-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(140px,1fr)); gap:12px; margin-top:14px; }}
.stat-box {{ background:#1f2937; border:1px solid #374151; border-radius:10px; padding:14px 16px; }}
.stat-box .num {{ font-size:26px; font-weight:700; color:#e2e8f0; line-height:1.1; }}
.stat-box .lbl {{ font-size:11px; color:#64748b; margin-top:4px; text-transform:uppercase; letter-spacing:0.05em; }}
.breadcrumb {{ font-size:13px; color:#64748b; margin-bottom:16px; }}
.breadcrumb a {{ color:#a78bfa; text-decoration:none; }}
.breadcrumb a:hover {{ text-decoration:underline; }}
.ended-banner {{ background:#312e81; border:1px solid #4c1d95; border-radius:10px; padding:12px 16px;
  color:#c4b5fd; font-size:14px; margin-bottom:16px; }}
</style></head><body>
<div class="header"><h1>Zarna AI — {_e(title)}</h1></div>
<nav class="nav">
  <a href="/admin?tab=overview">📊 Overview</a>
  <a href="/admin?tab=audience">👥 Audience</a>
  <a href="/admin?tab=convos">💬 Conversations</a>
  <a href="/admin?tab=conversions">🔗 Conversions</a>
  <a href="/admin?tab=insights">🧠 Insights</a>
  <a href="/admin?tab=learning">✨ Learning</a>
  <a href="/admin?tab=quality">🔍 Quality</a>
  <a href="/admin/live-shows" class="{'active' if nav_active == 'live' else ''}">🎤 Live shows</a>
</nav>
<div class="container">{body}</div>
</body></html>"""


@live_shows_bp.before_request
def _before():
    gate = _auth_gate()
    if gate is not None:
        return gate


def _show_table_rows(show_list: list, empty_msg: str) -> str:
    if not show_list:
        return f'<tr><td colspan="7" style="color:#6b7280">{_e(empty_msg)}</td></tr>'
    rows = ""
    for s in show_list:
        st = (s.get("status") or "draft").lower()
        badge = f"badge-{st}" if st in ("live", "draft", "ended") else "badge-draft"
        kw = _e((s.get("keyword") or "") or "—")
        sid = s["id"]
        n = s.get("signup_count", 0)
        ec = (s.get("event_category") or "other").lower()
        if ec == "comedy":
            et = "Comedy"
        elif ec in ("live_stream", "livestream"):
            et = "Live stream"
        else:
            et = "Other"
        exp = f'<a class="btn btn-secondary btn-sm" href="/admin/live-shows/{sid}/export">CSV</a>'
        nm = _e(s["name"])
        raw_name = (s.get("name") or "")[:80]
        confirm_js = json.dumps(
            f'Delete "{raw_name}" (show #{sid})? All signups and blast history for this show '
            f"will be removed. This cannot be undone."
        )
        rows += f"""<tr>
          <td><a href="/admin/live-shows/{sid}" style="color:#a78bfa;font-weight:500">{nm}</a></td>
          <td><span class="badge {badge}">{st}</span></td>
          <td>{_e(et)}</td>
          <td><strong>{n}</strong></td>
          <td class="mono">{kw}</td>
          <td>{exp}</td>
          <td class="actions">
            <form method="post" action="/admin/live-shows/{sid}/rename">
              <input type="hidden" name="next" value="/admin/live-shows">
              <input type="text" name="name" value="{nm}" aria-label="Rename show" title="New name">
              <button type="submit" class="btn btn-secondary btn-sm">Save name</button>
            </form>
            <form method="post" action="/admin/live-shows/{sid}/delete" style="display:inline-block"
              onsubmit='return confirm({confirm_js});'>
              <button type="submit" class="btn btn-danger btn-sm">Delete</button>
            </form>
          </td>
        </tr>"""
    return rows


@live_shows_bp.route("/admin/live-shows")
def list_shows():
    shows = repo.list_shows()
    live_s = [s for s in shows if (s.get("status") or "").lower() == "live"]
    draft_s = [s for s in shows if (s.get("status") or "").lower() == "draft"]
    ended_s = [s for s in shows if (s.get("status") or "").lower() == "ended"]

    body = f"""
<div class="callout">
  <strong>Live mode</strong> — Only one show can be <code>live</code> at a time. <strong>Go live</strong> ends any other live show.<br>
  <strong>Past events</strong> keep their signup list; open a show to see numbers or download CSV.<br><br>
  <strong>Bulk send</strong> — Twilio one-per-number; SlickText one-by-one or v2 Campaign. <code>LIVE_SHOW_BROADCAST_PROVIDER</code> = <code>slicktext</code> | <code>twilio</code> | <code>auto</code>.
</div>
<p><a class="btn" href="/admin/live-shows/new">+ New live show</a></p>
<div class="card">
  <h2 style="margin-top:0;font-size:16px">Live now</h2>
  <table><thead><tr><th>Name</th><th>Status</th><th>Type</th><th>Signups</th><th>Keyword</th><th>Export</th><th>Actions</th></tr></thead>
  <tbody>{_show_table_rows(live_s, "No live show — start one from a draft.")}</tbody></table>
</div>
<div class="card">
  <h2 style="margin-top:0;font-size:16px">Drafts</h2>
  <table><thead><tr><th>Name</th><th>Status</th><th>Type</th><th>Signups</th><th>Keyword</th><th>Export</th><th>Actions</th></tr></thead>
  <tbody>{_show_table_rows(draft_s, "No drafts.")}</tbody></table>
</div>
<div class="card">
  <h2 style="margin-top:0;font-size:16px">Past events</h2>
  <p style="color:#64748b;font-size:13px;margin:0 0 8px 0">Ended shows — audience is saved. Click the name for the full list.</p>
  <table><thead><tr><th>Name</th><th>Status</th><th>Type</th><th>Signups</th><th>Keyword</th><th>Export</th><th>Actions</th></tr></thead>
  <tbody>{_show_table_rows(ended_s, "No ended shows yet.")}</tbody></table>
</div>"""
    return _shell("Live shows", body)


@live_shows_bp.route("/admin/live-shows/new", methods=["GET", "POST"])
def new_show():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        keyword = request.form.get("keyword", "").strip()
        mode = request.form.get("signup_mode", "keyword")
        use_kw = mode == "keyword"
        etz_raw = request.form.get("event_timezone") or "America/New_York"
        ws = parse_local_datetime(request.form.get("window_start"), etz_raw)
        we = parse_local_datetime(request.form.get("window_end"), etz_raw)
        deliver = (request.form.get("deliver_as") or "sms").strip().lower()
        if deliver not in ("sms", "whatsapp"):
            deliver = "sms"
        event_cat = (request.form.get("event_category") or "other").strip().lower()
        if event_cat == "livestream":
            event_cat = "live_stream"
        if event_cat not in ("comedy", "live_stream", "other"):
            event_cat = "other"
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
            sid = repo.create_show(name, keyword, use_kw, ws, we, deliver, event_cat, etz_raw)
        except Exception as e:
            logger.exception("create_show")
            return _shell("New live show", f'<div class="card"><p style="color:#f87171">{_e(str(e))}</p></div>')
        return redirect(url_for("live_shows.show_detail", show_id=sid))

    form = f"""
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
  <small class="hint">Case-insensitive; minor typos allowed for 3+ character keywords. Keyword-only messages skip the AI; comedy and live stream send a fun rotating confirmation SMS.</small>
  <label>Event timezone</label>
  <select name="event_timezone">
{_timezone_options_html("America/New_York")}
  </select>
  <small class="hint">Where the show is (e.g. Eastern for NYC). The window times below are in this zone; we save exact instants in UTC.</small>
  <label>Window start</label>
  <input type="datetime-local" name="window_start">
  <label>Window end</label>
  <input type="datetime-local" name="window_end">
  <small class="hint">Keyword mode: leave blank for no time limit. Time-window mode: both required.</small>
  <label>Broadcast channel</label>
  <select name="deliver_as">
    <option value="sms">SMS (Twilio SMS / SlickText SMS)</option>
    <option value="whatsapp">WhatsApp (Twilio only — templates may apply outside 24h session)</option>
  </select>
  <label>Event type</label>
  <select name="event_category">
    <option value="comedy" selected>Comedy show — fun rotating confirmation SMS (keyword-only join)</option>
    <option value="live_stream">Live stream — fun rotating confirmation themed for the live (keyword-only join)</option>
    <option value="other">Other — keyword join stays silent (no auto confirmation)</option>
  </select>
  <small class="hint">Comedy &amp; live stream: upbeat welcome, joke, sign-off — all automated SMS; no human reply promised.</small>
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
    tpl_key = request.args.get("template", "").strip()
    body_prefill = BROADCAST_BODY_TEMPLATES.get(tpl_key, "")
    err_banner = ""
    err = request.args.get("err", "")
    if err == "campaign_sms":
        err_banner = '<div class="callout" style="border-color:#f87171;color:#fecaca">Campaign mode is SMS-only. Change “Broadcast channel” to SMS or use one-by-one with Twilio.</div>'
    elif err == "campaign_slicktext":
        err_banner = '<div class="callout" style="border-color:#f87171;color:#fecaca">Campaign mode needs SlickText (not Twilio). Set provider to SlickText or Auto with v2 keys.</div>'
    elif err == "test_bad_phone":
        err_banner = '<div class="callout" style="border-color:#f87171;color:#fecaca">Test send failed — check the phone number format and provider (SMS vs WhatsApp).</div>'
    elif err == "window":
        err_banner = '<div class="callout" style="border-color:#f87171;color:#fecaca">Time-window mode needs both start and end in the signup window form.</div>'
    ok_banner = ""
    if request.args.get("test") == "sent":
        ok_banner = '<div class="callout" style="border-color:#065f46;color:#6ee7b7">Test message sent — check that phone.</div>'
    signups = repo.signups_for_show(show_id)
    audit_log = repo.recent_audit_for_show(show_id)
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

    st_lower = (st or "").lower()
    ended_banner = ""
    if st_lower == "ended":
        ended_banner = """<div class="ended-banner">This event has ended — fans are no longer added via keyword. The audience list below is saved; use Export CSV for a spreadsheet.</div>"""

    created = show.get("created_at")
    created_line = ""
    if created:
        created_line = f'<p style="color:#64748b;font-size:12px;margin:8px 0 0 0">Created: {created}</p>'

    ec = (show.get("event_category") or "other").lower()
    if ec == "comedy":
        type_lbl = "Comedy"
    elif ec in ("live_stream", "livestream"):
        type_lbl = "Live stream"
    else:
        type_lbl = "Other"
    stats_block = f"""
<div class="stats-grid">
  <div class="stat-box"><div class="num">{show.get("signup_count", 0)}</div><div class="lbl">Signups</div></div>
  <div class="stat-box"><div class="num" style="font-size:17px;line-height:1.25;word-break:break-word">{_e(show.get("keyword") or "—")}</div><div class="lbl">Keyword</div></div>
  <div class="stat-box"><div class="num" style="font-size:15px">{_e(show.get("deliver_as") or "sms")}</div><div class="lbl">Channel</div></div>
  <div class="stat-box"><div class="num" style="font-size:15px">{_e(type_lbl)}</div><div class="lbl">Event type</div></div>
</div>"""

    tz_sel = timezone_select_value_from_show(show.get("event_timezone"))
    ws_val = utc_to_datetime_local_value(show.get("window_start"), show.get("event_timezone"))
    we_val = utc_to_datetime_local_value(show.get("window_end"), show.get("event_timezone"))
    win_human = format_window_human(
        show.get("window_start"), show.get("window_end"), show.get("event_timezone")
    )
    schedule_card = f"""
<div class="card">
  <h3 style="margin-top:0">Signup window</h3>
  <p style="font-size:14px;line-height:1.6;margin:0 0 14px 0">{win_human}</p>
  <form method="post" action="/admin/live-shows/{show_id}/schedule">
    <label>Event timezone</label>
    <select name="event_timezone">
{_timezone_options_html(tz_sel)}
    </select>
    <label>Window start</label>
    <input type="datetime-local" name="window_start" value="{_e(ws_val)}">
    <label>Window end</label>
    <input type="datetime-local" name="window_end" value="{_e(we_val)}">
    <small class="hint">Keyword mode: leave both blank for no time limit. Time-window mode: both required.</small>
    <p><button type="submit" class="btn">Save window</button></p>
  </form>
</div>"""

    sample_lines = [_mask_phone_display(u["phone_number"]) for u in signups[:10]]
    sample_html = ", ".join(_e(s) for s in sample_lines) if sample_lines else "— (no signups yet)"
    tmpl_links = " · ".join(
        f'<a href="/admin/live-shows/{show_id}?template={_e(k)}" style="color:#a78bfa">{_e(k)}</a>'
        for k in BROADCAST_BODY_TEMPLATES
    )
    audit_rows = ""
    for row in audit_log:
        audit_rows += f'<tr><td style="color:#94a3b8;font-size:12px">{row["created_at"]}</td><td>{_e(row.get("action") or "")}</td><td>{_e((row.get("detail") or "")[:120])}</td></tr>'
    if not audit_rows:
        audit_rows = '<tr><td colspan="3" style="color:#6b7280">No actions logged yet.</td></tr>'

    signups_card = f"""
<div class="card">
  <h3 style="margin-top:0">Audience ({len(signups)} on this page)</h3>
  <p style="margin:0 0 12px 0"><a class="btn btn-secondary" href="/admin/live-shows/{show_id}/export">Download all as CSV</a></p>
  <table><thead><tr><th>Phone</th><th>Channel</th><th>Signed up</th><th></th></tr></thead><tbody>{sig_rows}</tbody></table>
</div>"""

    broadcast_form = f"""
<div class="card">
  <h3 style="margin-top:0">Broadcast to this list</h3>
  <div class="callout" style="margin-bottom:14px;border-color:#4f46e5">
    <strong>Before you send</strong><br>
    Recipients: <strong>{show.get("signup_count", 0)}</strong> numbers on this show.<br>
    Sample (masked): {sample_html}<br>
    <small style="color:#94a3b8">Use “Send test” below to verify copy on your own phone first.</small>
  </div>
  <p style="font-size:13px;margin:0 0 10px 0">Message starters: {tmpl_links}</p>
  <form method="post" action="/admin/live-shows/{show_id}/broadcast">
    <label>Message</label>
    <textarea name="body" required placeholder="Your text to everyone signed up…">{_e(body_prefill)}</textarea>
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
    <label><input type="checkbox" name="confirm" value="1" required> I confirm sending to <strong>{show.get("signup_count",0)}</strong> numbers</label>
    <label><input type="checkbox" name="confirm_review" value="1" required> I verified the count matches this show’s audience list above</label>
    <button type="submit" class="btn">Queue send</button>
  </form>
  <small class="hint">Runs in the background; refresh for job status. WhatsApp blasts: use Twilio + one-by-one.</small>
  {job_html}
</div>
<div class="card">
  <h3 style="margin-top:0">Send test (one number)</h3>
  <form method="post" action="/admin/live-shows/{show_id}/broadcast-test">
    <label>Your phone (E.164, e.g. +15551234567)</label>
    <input type="text" name="test_phone" placeholder="+15551234567" required>
    <label>Test message (same copy you plan to blast)</label>
    <textarea name="body" required placeholder="Paste the broadcast message here…">{_e(body_prefill)}</textarea>
    <label>Provider override</label>
    <select name="provider">
      <option value="">Auto</option>
      <option value="slicktext">SlickText</option>
      <option value="twilio">Twilio</option>
    </select>
    <button type="submit" class="btn btn-secondary">Send one test SMS</button>
  </form>
  <small class="hint">Uses the same routing as bulk send (respects show “Deliver as” for SMS vs WhatsApp).</small>
</div>
<div class="card">
  <h3 style="margin-top:0">Recent actions (audit)</h3>
  <table><thead><tr><th>When</th><th>Action</th><th>Detail</th></tr></thead><tbody>{audit_rows}</tbody></table>
</div>"""

    rename_block = f"""
  <form method="post" action="/admin/live-shows/{show_id}/rename" style="margin-top:14px">
    <label>Show name</label>
    <input type="text" name="name" value="{_e(show["name"])}" style="max-width:100%">
    <p style="margin:8px 0 0 0"><button type="submit" class="btn btn-secondary">Save name</button></p>
  </form>"""
    del_confirm = json.dumps(
        "Delete this show and all signups and broadcast history? This cannot be undone."
    )
    danger_card = f"""
<div class="card danger-zone">
  <h3 style="margin-top:0;color:#fca5a5">Danger zone</h3>
  <p style="color:#94a3b8;font-size:13px;margin:0 0 10px 0">Removes this event, its audience list rows, and blast job records. Notion pages are not deleted automatically.</p>
  <form method="post" action="/admin/live-shows/{show_id}/delete" onsubmit='return confirm({del_confirm});'>
    <button type="submit" class="btn btn-danger">Delete show</button>
  </form>
</div>"""

    header_card = f"""
<div class="card">
  <h2 style="margin-top:0">{_e(show["name"])}</h2>
  <p style="margin:0">Status: <span class="badge badge-{st_lower if st_lower in ('live','draft','ended') else 'draft'}">{st}</span></p>
  {created_line}
  {stats_block}
  {rename_block}
</div>"""

    body = f"""
{err_banner}{ok_banner}
<div class="breadcrumb"><a href="/admin/live-shows">Live shows</a> · {_e(show["name"])}</div>
{ended_banner}
<p style="margin-bottom:16px">{status_btns}
<a class="btn btn-secondary" href="/admin/live-shows/{show_id}/export">Export CSV</a>
<a class="btn btn-secondary" href="/admin/live-shows">All shows</a></p>
{header_card}
{schedule_card}
{signups_card}
{broadcast_form}
{danger_card}"""
    return _shell(show["name"], body)


@live_shows_bp.route("/admin/live-shows/<int:show_id>/schedule", methods=["POST"])
def show_schedule(show_id: int):
    show = repo.get_show(show_id)
    if not show:
        return redirect("/admin/live-shows")
    etz_raw = request.form.get("event_timezone") or "America/New_York"
    ws = parse_local_datetime(request.form.get("window_start"), etz_raw)
    we = parse_local_datetime(request.form.get("window_end"), etz_raw)
    if not show.get("use_keyword_only"):
        if ws is None or we is None:
            return redirect(url_for("live_shows.show_detail", show_id=show_id, err="window"))
    repo.update_show_schedule(show_id, ws, we, etz_raw)
    repo.log_audit("show_schedule", "signup window / timezone updated", show_id)
    return redirect(url_for("live_shows.show_detail", show_id=show_id))


@live_shows_bp.route("/admin/live-shows/<int:show_id>/status", methods=["POST"])
def show_status(show_id: int):
    new_st = request.form.get("status", "").strip().lower()
    if new_st not in ("live", "ended", "draft"):
        return redirect(url_for("live_shows.show_detail", show_id=show_id))
    repo.update_show_status(show_id, new_st)
    repo.log_audit("show_status", f"status → {new_st}", show_id)
    return redirect(url_for("live_shows.show_detail", show_id=show_id))


def _safe_admin_redirect(url: str, fallback_show_id: int) -> str:
    if (
        url.startswith("/admin")
        and not url.startswith("//")
        and "\n" not in url
        and "\r" not in url
        and ".." not in url
    ):
        return url
    return url_for("live_shows.show_detail", show_id=fallback_show_id)


@live_shows_bp.route("/admin/live-shows/<int:show_id>/rename", methods=["POST"])
def show_rename(show_id: int):
    if not repo.get_show(show_id):
        return redirect("/admin/live-shows")
    name = (request.form.get("name") or "").strip()
    next_url = (request.form.get("next") or "").strip()
    if not name:
        return redirect(_safe_admin_redirect(next_url, show_id))
    if repo.update_show_name(show_id, name):
        repo.log_audit("show_rename", "show name updated", show_id)
    return redirect(_safe_admin_redirect(next_url, show_id))


@live_shows_bp.route("/admin/live-shows/<int:show_id>/delete", methods=["POST"])
def show_delete(show_id: int):
    show = repo.get_show(show_id)
    if not show:
        return redirect("/admin/live-shows")
    label = (show.get("name") or "")[:120]
    repo.delete_show(show_id)
    repo.log_audit("show_delete", f"deleted show_id={show_id} ({label})", None)
    return redirect("/admin/live-shows")


@live_shows_bp.route("/admin/live-shows/<int:show_id>/broadcast-test", methods=["POST"])
def broadcast_test(show_id: int):
    show = repo.get_show(show_id)
    if not show:
        return redirect("/admin/live-shows")
    phone = (request.form.get("test_phone") or "").strip()
    body = (request.form.get("body") or "").strip()
    prov = request.form.get("provider", "").strip().lower()
    p_choice = prov if prov in ("slicktext", "twilio") else None
    if not phone or not body:
        return redirect(url_for("live_shows.show_detail", show_id=show_id))
    ok = _send_one_outbound(phone, body, show.get("deliver_as") or "sms", p_choice)
    if ok:
        repo.log_audit("broadcast_test", f"1 msg to ...{phone[-4:]}", show_id)
        return redirect(url_for("live_shows.show_detail", show_id=show_id) + "?test=sent")
    return redirect(url_for("live_shows.show_detail", show_id=show_id, err="test_bad_phone"))


@live_shows_bp.route("/admin/live-shows/<int:show_id>/broadcast", methods=["POST"])
def show_broadcast(show_id: int):
    show = repo.get_show(show_id)
    if not show:
        return redirect("/admin/live-shows")
    body = request.form.get("body", "").strip()
    if not body or request.form.get("confirm") != "1" or request.form.get("confirm_review") != "1":
        return redirect(url_for("live_shows.show_detail", show_id=show_id))
    prov = request.form.get("provider", "").strip().lower()
    if prov not in ("", "slicktext", "twilio"):
        prov = ""
    p_choice = prov if prov in ("slicktext", "twilio") else None
    resolved = p_choice or resolve_broadcast_provider()
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
    n = int(show.get("signup_count") or 0)
    repo.log_audit("broadcast_queued", f"job #{jid} → {n} recipients ({job_provider})", show_id)
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
