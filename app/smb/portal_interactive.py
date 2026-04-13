"""
SMB Client Portal (interactive) — a full self-serve dashboard for business owners.

Mounted alongside the existing read-only portal (app/smb/portal.py).
  Read-only stats:  GET /portal/<slug>?token=<token>      (app/smb/portal.py)
  Interactive mgmt: GET /portal/<slug>/login              (this file)

Authentication: password per tenant stored in env var
  SMB_PORTAL_<SLUG_UPPER>_PASSWORD   e.g. SMB_PORTAL_WEST_SIDE_COMEDY_PASSWORD

URL: /portal/<slug>/login  →  /portal/<slug>/  (dashboard)
                            →  /portal/<slug>/shows
                            →  /portal/<slug>/blast
"""

import logging
import os
from functools import wraps

from flask import Blueprint, redirect, request, session, url_for

from app.admin_auth import get_db_connection
from app.smb.tenants import get_registry
from app.smb import storage as smb_storage
from app.smb.blast import send_blast_from_portal

logger = logging.getLogger(__name__)

portal_interactive_bp = Blueprint("client_portal", __name__)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _session_key(slug: str) -> str:
    return f"smb_portal_{slug}"


def _portal_password(slug: str) -> str:
    key = f"SMB_PORTAL_{slug.upper().replace('-', '_')}_PASSWORD"
    return os.getenv(key, "")


def _is_authenticated(slug: str) -> bool:
    return session.get(_session_key(slug)) is True


def _require_auth(slug: str):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not _is_authenticated(slug):
                return redirect(url_for("client_portal.portal_login", slug=slug))
            return f(*args, **kwargs)
        return wrapped
    return decorator


# ---------------------------------------------------------------------------
# Page chrome
# ---------------------------------------------------------------------------

_STYLE = """
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #0d0d1a; color: #e2e8f0; min-height: 100vh;
}
a { color: #a78bfa; text-decoration: none; }
a:hover { color: #c4b5fd; }
.shell { display: flex; min-height: 100vh; }
.sidebar {
  width: 220px; flex-shrink: 0;
  background: #111122; border-right: 1px solid #1e1e35;
  display: flex; flex-direction: column;
  position: sticky; top: 0; height: 100vh; overflow-y: auto;
}
.sidebar-logo { padding: 20px 20px 16px; border-bottom: 1px solid #1e1e35; }
.sidebar-logo .club-name { font-size: 14px; font-weight: 700; color: #f1f5f9; line-height: 1.3; }
.sidebar-logo .powered  { font-size: 10px; color: #475569; margin-top: 3px; }
nav { padding: 12px 0; flex: 1; }
.nav-link {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 20px; font-size: 13.5px; color: #94a3b8;
  border-left: 3px solid transparent; transition: all .15s;
}
.nav-link:hover { color: #e2e8f0; background: #161628; }
.nav-link.active { color: #c4b5fd; background: #1a1a30; border-left-color: #7c3aed; }
.nav-icon { font-size: 16px; width: 20px; text-align: center; }
.main { flex: 1; padding: 28px 32px; overflow: hidden; }
.page-header { margin-bottom: 24px; }
.page-header h1 { font-size: 22px; font-weight: 700; color: #f1f5f9; }
.page-header p  { font-size: 14px; color: #64748b; margin-top: 4px; }
.card {
  background: #14142a; border: 1px solid #1e1e35;
  border-radius: 12px; padding: 22px 24px; margin-bottom: 20px;
}
.card-title {
  font-size: 11px; font-weight: 700; color: #6b7280;
  text-transform: uppercase; letter-spacing: .08em; margin-bottom: 16px;
}
.stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 14px; margin-bottom: 20px; }
.stat-card { background: #14142a; border: 1px solid #1e1e35; border-radius: 10px; padding: 16px 18px; }
.stat-num { font-size: 28px; font-weight: 800; color: #a78bfa; }
.stat-lbl { font-size: 11px; color: #64748b; margin-top: 3px; }
table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
th { text-align: left; padding: 9px 12px; font-size: 11px; font-weight: 700;
     color: #4b5563; border-bottom: 1px solid #1e1e35; text-transform: uppercase; letter-spacing: .05em; }
td { padding: 11px 12px; border-bottom: 1px solid #111122; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #111126; }
.empty { text-align: center; padding: 36px 0; color: #374151; font-size: 13px; }
.form-row  { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; }
.form-group { display: flex; flex-direction: column; gap: 5px; }
label { font-size: 12px; color: #94a3b8; }
input[type=text], input[type=date], input[type=password], select, textarea {
  background: #0d0d1a; border: 1px solid #2d2d44; border-radius: 7px;
  color: #e2e8f0; padding: 9px 13px; font-size: 14px; outline: none; transition: border .15s;
}
input:focus, select:focus, textarea:focus { border-color: #7c3aed; }
textarea { resize: vertical; min-height: 110px; line-height: 1.5; }
.btn {
  padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 600;
  cursor: pointer; border: none; display: inline-block; transition: all .15s;
}
.btn-primary   { background: #7c3aed; color: #fff; }
.btn-primary:hover { background: #6d28d9; }
.btn-secondary { background: #1e1e35; color: #94a3b8; border: 1px solid #2d2d44; }
.btn-secondary:hover { border-color: #7c3aed; color: #a78bfa; }
.btn-sm { padding: 6px 13px; font-size: 12px; border-radius: 6px; }
.badge { display: inline-block; padding: 3px 9px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.badge-green  { background: #052e16; color: #4ade80; }
.badge-gray   { background: #1e293b; color: #64748b; }
.keyword { font-family: monospace; background: #1a1a2e; border: 1px solid #2d2d44;
           border-radius: 5px; padding: 3px 8px; font-size: 12px; color: #a78bfa; }
.alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 13px; }
.alert-success { background: #052e16; border: 1px solid #166534; color: #86efac; }
.alert-error   { background: #450a0a; border: 1px solid #991b1b; color: #fca5a5; }
.audience-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; margin-bottom: 16px; }
.audience-card {
  border: 2px solid #1e1e35; border-radius: 9px; padding: 13px 15px;
  cursor: pointer; transition: all .15s; background: #111122;
}
.audience-card:hover { border-color: #5b21b6; }
.audience-card.selected { border-color: #7c3aed; background: #1a0d35; }
.audience-card input[type=radio] { display: none; }
.audience-card .aud-name  { font-size: 13px; font-weight: 600; color: #e2e8f0; }
.audience-card .aud-count { font-size: 12px; color: #64748b; margin-top: 3px; }
.audience-card .aud-icon  { font-size: 20px; margin-bottom: 6px; }
.login-wrap {
  min-height: 100vh; display: flex; align-items: center; justify-content: center;
  background: #0d0d1a;
}
.login-box {
  background: #14142a; border: 1px solid #1e1e35; border-radius: 16px;
  padding: 40px 36px; width: 100%; max-width: 360px;
}
.login-box h1 { font-size: 22px; font-weight: 800; color: #f1f5f9; margin-bottom: 4px; }
.login-box p  { font-size: 13px; color: #64748b; margin-bottom: 24px; }
.login-box .logo { font-size: 32px; margin-bottom: 16px; }
</style>
"""


def _page(slug: str, active_tab: str, title: str, body: str, tenant_name: str = "") -> str:
    nav_items = [
        ("dashboard", "📊", "Dashboard",     f"/portal/{slug}/"),
        ("shows",     "🎭", "Show Attendance", f"/portal/{slug}/shows"),
        ("blast",     "📣", "Send a Blast",   f"/portal/{slug}/blast"),
    ]
    nav_html = "".join(
        f'<a href="{href}" class="nav-link {"active" if active_tab == tid else ""}">'
        f'<span class="nav-icon">{icon}</span>{label}</a>\n'
        for tid, icon, label, href in nav_items
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  {_STYLE}
</head>
<body>
<div class="shell">
  <aside class="sidebar">
    <div class="sidebar-logo">
      <div class="club-name">{tenant_name or slug}</div>
      <div class="powered">powered by Zarna</div>
    </div>
    <nav>{nav_html}</nav>
    <div style="padding:16px 20px;border-top:1px solid #1e1e35">
      <a href="/portal/{slug}/logout" style="font-size:12px;color:#475569">Sign out</a>
    </div>
  </aside>
  <main class="main">{body}</main>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Login / logout
# ---------------------------------------------------------------------------

@portal_interactive_bp.route("/portal/<slug>/login", methods=["GET", "POST"])
def portal_login(slug):
    if _is_authenticated(slug):
        return redirect(url_for("client_portal.portal_dashboard", slug=slug))

    tenant = get_registry().get_by_slug(slug)
    if tenant is None:
        return f"<p style='font-family:sans-serif;padding:40px;color:#ccc'>Portal not found: {slug}</p>", 404

    error = ""
    if request.method == "POST":
        pwd = request.form.get("password", "")
        expected = _portal_password(slug)
        if not expected:
            error = "Portal password not configured — contact your Zarna team."
        elif pwd == expected:
            session[_session_key(slug)] = True
            return redirect(url_for("client_portal.portal_dashboard", slug=slug))
        else:
            error = "Incorrect password. Try again."

    error_html = f'<div class="alert alert-error">{error}</div>' if error else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in — {tenant.display_name}</title>{_STYLE}</head>
<body>
<div class="login-wrap">
  <div class="login-box">
    <div class="logo">🎭</div>
    <h1>{tenant.display_name}</h1>
    <p>Sign in to your dashboard</p>
    {error_html}
    <form method="post">
      <div class="form-group" style="margin-bottom:16px">
        <label>Password</label>
        <input type="password" name="password" autofocus style="width:100%">
      </div>
      <button type="submit" class="btn btn-primary" style="width:100%">Sign in</button>
    </form>
  </div>
</div>
</body></html>"""


@portal_interactive_bp.route("/portal/<slug>/logout")
def portal_logout(slug):
    session.pop(_session_key(slug), None)
    return redirect(url_for("client_portal.portal_login", slug=slug))


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@portal_interactive_bp.route("/portal/<slug>/")
def portal_dashboard(slug):
    if not _is_authenticated(slug):
        return redirect(url_for("client_portal.portal_login", slug=slug))

    tenant = get_registry().get_by_slug(slug)
    if tenant is None:
        return "Client not found", 404

    conn = get_db_connection()
    try:
        with conn:
            total = len(smb_storage.get_active_subscribers(conn, slug))
            seg_counts = []
            for seg in (tenant.segments or []):
                cnt = len(smb_storage.get_subscribers_by_segment(
                    conn, slug, seg["question_key"], seg["answers"]
                ))
                seg_counts.append({"name": seg["name"], "desc": seg.get("description", ""), "count": cnt})

            with conn.cursor() as cur:
                cur.execute(
                    """SELECT body, owner_message, attempted, succeeded, segment, sent_at
                       FROM smb_blasts WHERE tenant_slug=%s ORDER BY sent_at DESC LIMIT 5""",
                    (slug,),
                )
                recent_blasts = [
                    {"body": r[0], "owner_message": r[1], "attempted": r[2],
                     "succeeded": r[3], "segment": r[4] or "Everyone", "sent_at": r[5]}
                    for r in cur.fetchall()
                ]
            recent_shows = smb_storage.list_shows(conn, slug, limit=5)
    except Exception:
        logger.exception("portal: dashboard query failed for %s", slug)
        total = 0; seg_counts = []; recent_blasts = []; recent_shows = []
    finally:
        conn.close()

    seg_cards = "".join(
        f'<div class="stat-card"><div class="stat-num" style="font-size:22px">{s["count"]}</div>'
        f'<div class="stat-lbl">{s["desc"] or s["name"]}</div></div>'
        for s in seg_counts
    )

    if recent_blasts:
        blast_rows = "".join(
            f'<tr><td style="max-width:300px;color:#cbd5e1">{(b["body"] or b["owner_message"] or "")[:80]}{"…" if len(b["body"] or "") > 80 else ""}</td>'
            f'<td><span class="badge badge-gray">{b["segment"]}</span></td>'
            f'<td style="color:#4ade80">{b["succeeded"]}</td>'
            f'<td style="color:#64748b;font-size:12px">{b["sent_at"].strftime("%-d %b, %-I:%M %p") if b["sent_at"] else "—"}</td></tr>'
            for b in recent_blasts
        )
        blast_table = f'<table><thead><tr><th>Message</th><th>Audience</th><th>Sent to</th><th>When</th></tr></thead><tbody>{blast_rows}</tbody></table>'
    else:
        blast_table = f'<div class="empty">No blasts yet — <a href="/portal/{slug}/blast">send your first one</a></div>'

    if recent_shows:
        show_rows = "".join(
            f'<tr><td>{str(s["show_date"])}</td>'
            f'<td><strong style="color:#f1f5f9">{s["name"]}</strong></td>'
            f'<td><span class="badge {"badge-green" if s.get("checkin_count",0) > 0 else "badge-gray"}">{s.get("checkin_count",0)} checked in</span></td>'
            f'<td><span class="keyword">{s["checkin_keyword"]}</span></td>'
            f'<td><a href="/portal/{slug}/shows/{s["id"]}" class="btn btn-sm btn-secondary">View</a></td></tr>'
            for s in recent_shows
        )
        show_table = f'<table><thead><tr><th>Date</th><th>Show</th><th>Attendance</th><th>Keyword</th><th></th></tr></thead><tbody>{show_rows}</tbody></table>'
    else:
        show_table = f'<div class="empty">No shows yet — <a href="/portal/{slug}/shows">create your first one</a></div>'

    body = f"""
    <div class="page-header">
      <h1>👋 Hey, {tenant.display_name.split()[0]}!</h1>
      <p>Here's what's happening with your community.</p>
    </div>
    <div class="stat-grid">
      <div class="stat-card"><div class="stat-num">{total}</div><div class="stat-lbl">Active subscribers</div></div>
      {seg_cards}
    </div>
    <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px">
      <a href="/portal/{slug}/blast" class="btn btn-primary">📣 Send a Blast</a>
      <a href="/portal/{slug}/shows" class="btn btn-secondary">🎭 Manage Shows</a>
    </div>
    <div class="card"><div class="card-title">Recent Blasts</div>{blast_table}</div>
    <div class="card"><div class="card-title">Recent Shows</div>{show_table}</div>
    """
    return _page(slug, "dashboard", f"Dashboard — {tenant.display_name}", body, tenant.display_name)


# ---------------------------------------------------------------------------
# Shows
# ---------------------------------------------------------------------------

@portal_interactive_bp.route("/portal/<slug>/shows", methods=["GET", "POST"])
def portal_shows(slug):
    if not _is_authenticated(slug):
        return redirect(url_for("client_portal.portal_login", slug=slug))

    tenant = get_registry().get_by_slug(slug)
    if tenant is None:
        return "Client not found", 404

    from datetime import date as _date
    error = success = ""

    if request.method == "POST":
        name    = request.form.get("name", "").strip()
        show_date = request.form.get("show_date", "").strip()
        keyword = request.form.get("checkin_keyword", "").strip().upper()
        if not all([name, show_date, keyword]):
            error = "All fields are required."
        else:
            conn = get_db_connection()
            try:
                with conn:
                    result = smb_storage.create_show(conn, slug, name, show_date, keyword)
                    if result is None:
                        error = f"The keyword <strong>{keyword}</strong> is already used. Pick another."
                    else:
                        success = f"Show <strong>{name}</strong> created! Fans check in by texting <strong>{keyword}</strong>."
            except Exception:
                logger.exception("portal: create show failed")
                error = "Something went wrong — try again."
            finally:
                conn.close()

    conn = get_db_connection()
    try:
        with conn:
            shows = smb_storage.list_shows(conn, slug)
    except Exception:
        shows = []
    finally:
        conn.close()

    today = _date.today().isoformat()
    alert = f'<div class="alert alert-error">{error}</div>' if error else (f'<div class="alert alert-success">{success}</div>' if success else "")

    rows = "".join(
        f'<tr><td>{str(s["show_date"])}</td>'
        f'<td><strong style="color:#f1f5f9">{s["name"]}</strong></td>'
        f'<td><span class="keyword">{s["checkin_keyword"]}</span></td>'
        f'<td><span class="badge {"badge-green" if s.get("checkin_count",0)>0 else "badge-gray"}">{s.get("checkin_count",0)} people</span></td>'
        f'<td><a href="/portal/{slug}/shows/{s["id"]}" class="btn btn-sm btn-secondary">View Attendees</a></td></tr>'
        for s in shows
    ) if shows else ""

    table = (f'<table><thead><tr><th>Date</th><th>Show</th><th>Check-in Word</th><th>Attendance</th><th></th></tr></thead><tbody>{rows}</tbody></table>'
             if shows else '<div class="empty">No shows yet — create one below!</div>')

    sms_number = tenant.sms_number or "[your number]"
    body = f"""
    <div class="page-header">
      <h1>🎭 Show Attendance</h1>
      <p>Create a show, post the check-in word at the door, see who shows up.</p>
    </div>
    {alert}
    <div class="card">
      <div class="card-title">How it works</div>
      <p style="font-size:13.5px;color:#94a3b8;line-height:1.7">
        1. Create a show and give it a short check-in word (e.g. <span class="keyword">APR18</span>)<br>
        2. Put a sign at the door: <em style="color:#e2e8f0">"Text APR18 to {sms_number} tonight!"</em><br>
        3. Fans text it when they arrive — they're recorded as attendees<br>
        4. After the show, hit <a href="/portal/{slug}/blast">Send a Blast</a> and pick that show as your audience
      </p>
    </div>
    <div class="card">
      <div class="card-title">Create New Show</div>
      <form method="post">
        <div class="form-row" style="margin-bottom:8px">
          <div class="form-group">
            <label>Show Name</label>
            <input type="text" name="name" placeholder="e.g. Friday Night Standup" style="width:260px" required>
          </div>
          <div class="form-group">
            <label>Date</label>
            <input type="date" name="show_date" value="{today}" required>
          </div>
          <div class="form-group">
            <label>Check-in Word <span style="color:#475569">(fans text this)</span></label>
            <input type="text" name="checkin_keyword" placeholder="e.g. APR18"
                   style="width:140px;font-family:monospace;text-transform:uppercase"
                   required oninput="this.value=this.value.toUpperCase()">
          </div>
          <div class="form-group" style="justify-content:flex-end">
            <button type="submit" class="btn btn-primary">Create Show</button>
          </div>
        </div>
      </form>
    </div>
    <div class="card"><div class="card-title">All Shows</div>{table}</div>
    """
    return _page(slug, "shows", f"Shows — {tenant.display_name}", body, tenant.display_name)


@portal_interactive_bp.route("/portal/<slug>/shows/<int:show_id>")
def portal_show_detail(slug, show_id):
    if not _is_authenticated(slug):
        return redirect(url_for("client_portal.portal_login", slug=slug))

    tenant = get_registry().get_by_slug(slug)
    if tenant is None:
        return "Client not found", 404

    conn = get_db_connection()
    try:
        with conn:
            show = smb_storage.get_show_by_id(conn, show_id)
            if show is None or show.get("tenant_slug") != slug:
                return "Show not found", 404
            attendees = smb_storage.get_show_attendees(conn, show_id)
    except Exception:
        logger.exception("portal: show detail failed")
        return "Error loading show", 500
    finally:
        conn.close()

    count = len(attendees)
    rows = "".join(
        f'<tr><td style="color:#64748b">{i}</td>'
        f'<td style="font-family:monospace;color:#94a3b8">({a["phone_number"][:3]}) {a["phone_number"][3:6]}-****</td>'
        f'<td style="color:#94a3b8;font-size:12px">{a["checked_in_at"].strftime("%-I:%M %p") if hasattr(a["checked_in_at"],"strftime") else "—"}</td></tr>'
        for i, a in enumerate(attendees, 1)
    ) if attendees else ""

    table = (f'<table><thead><tr><th>#</th><th>Phone</th><th>Checked in at</th></tr></thead><tbody>{rows}</tbody></table>'
             if attendees else '<div class="empty">No check-ins yet. Put the sign out at the door!</div>')

    blast_btn = (f'<div style="margin-bottom:16px"><a href="/portal/{slug}/blast?show={show["checkin_keyword"]}" class="btn btn-primary">'
                 f'📣 Send Thank-You Blast to These {count} People</a></div>') if count > 0 else ""

    body = f"""
    <div class="page-header">
      <a href="/portal/{slug}/shows" style="font-size:13px;color:#64748b">← All shows</a>
      <h1 style="margin-top:10px">{show['name']}</h1>
      <p>{show['show_date']} · <span class="keyword">{show['checkin_keyword']}</span></p>
    </div>
    <div class="stat-grid" style="max-width:200px">
      <div class="stat-card"><div class="stat-num">{count}</div><div class="stat-lbl">Checked in</div></div>
    </div>
    {blast_btn}
    <div class="card"><div class="card-title">Attendees</div>{table}</div>
    """
    return _page(slug, "shows", f"{show['name']} — {tenant.display_name}", body, tenant.display_name)


# ---------------------------------------------------------------------------
# Blast compose
# ---------------------------------------------------------------------------

@portal_interactive_bp.route("/portal/<slug>/blast", methods=["GET", "POST"])
def portal_blast(slug):
    if not _is_authenticated(slug):
        return redirect(url_for("client_portal.portal_login", slug=slug))

    tenant = get_registry().get_by_slug(slug)
    if tenant is None:
        return "Client not found", 404

    preselect_show = request.args.get("show", "").strip().upper()
    sent_msg = error = ""

    if request.method == "POST":
        message  = request.form.get("message", "").strip()
        audience = request.form.get("audience", "all").strip()
        if not message:
            error = "Please write a message before sending."
        else:
            try:
                sent_msg = send_blast_from_portal(message, tenant, audience)
            except Exception:
                logger.exception("portal: blast failed for %s", slug)
                error = "Something went wrong — check the logs."

    conn = get_db_connection()
    try:
        with conn:
            total = len(smb_storage.get_active_subscribers(conn, slug))
            seg_opts = []
            for seg in (tenant.segments or []):
                cnt = len(smb_storage.get_subscribers_by_segment(conn, slug, seg["question_key"], seg["answers"]))
                seg_opts.append({"value": f"segment:{seg['name']}", "label": seg["name"].title(),
                                 "desc": seg.get("description", ""), "count": cnt})
            shows = smb_storage.list_shows(conn, slug, limit=12)
            show_opts = [{"value": f"show:{s['checkin_keyword']}", "label": s["name"],
                          "desc": str(s["show_date"]), "count": s.get("checkin_count", 0),
                          "keyword": s["checkin_keyword"]}
                         for s in shows]
    except Exception:
        logger.exception("portal: blast options query failed for %s", slug)
        total = 0; seg_opts = []; show_opts = []
    finally:
        conn.close()

    def aud_card(value, icon, label, desc, count):
        is_selected = (value == f"show:{preselect_show}") if preselect_show else (value == "all")
        cls = " selected" if is_selected else ""
        return (f'<label class="audience-card{cls}">'
                f'<input type="radio" name="audience" value="{value}" {"checked" if is_selected else ""}>'
                f'<div class="aud-icon">{icon}</div>'
                f'<div class="aud-name">{label}</div>'
                f'<div class="aud-count">{count:,} {"person" if count == 1 else "people"}</div>'
                f'{"<div style=\\"font-size:11px;color:#475569;margin-top:3px\\">" + desc + "</div>" if desc else ""}'
                f'</label>')

    audience_html = '<div class="audience-grid">'
    audience_html += aud_card("all", "👥", "Everyone", "All active subscribers", total)
    for s in seg_opts:
        audience_html += aud_card(s["value"], "🎯", s["label"], s["desc"], s["count"])
    for s in show_opts:
        audience_html += aud_card(s["value"], "🎭", s["label"], s["desc"], s["count"])
    audience_html += "</div>"

    alert = (f'<div class="alert alert-error">{error}</div>' if error else
             f'<div class="alert alert-success">✅ {sent_msg} You\'ll get a text when it\'s done.</div>' if sent_msg else "")

    body = f"""
    <div class="page-header">
      <h1>📣 Send a Blast</h1>
      <p>Pick your audience, write your message, and send.</p>
    </div>
    {alert}
    <form method="post" onsubmit="return confirmSend()">
      <div class="card">
        <div class="card-title">Who are you sending to?</div>
        {audience_html}
      </div>
      <div class="card">
        <div class="card-title">Your Message</div>
        <div class="form-group" style="margin-bottom:12px">
          <textarea name="message" style="width:100%"
            placeholder="e.g. Thanks so much for coming out last night — you made it an incredible show. Hope to see you again soon!"></textarea>
          <p style="font-size:11px;color:#475569;margin-top:6px">
            ✨ Zarna will clean up typos and shorthand automatically — your facts stay exactly the same.
          </p>
        </div>
        <button type="submit" class="btn btn-primary" style="min-width:160px">Send Blast →</button>
      </div>
    </form>
    <script>
    document.querySelectorAll('.audience-card').forEach(c => {{
      c.addEventListener('click', () => {{
        document.querySelectorAll('.audience-card').forEach(x => x.classList.remove('selected'));
        c.classList.add('selected');
        c.querySelector('input[type=radio]').checked = true;
      }});
    }});
    function confirmSend() {{
      const sel = document.querySelector('.audience-card.selected');
      const aud = sel ? sel.querySelector('.aud-name').textContent : 'everyone';
      const cnt = sel ? sel.querySelector('.aud-count').textContent : '';
      const msg = document.querySelector('textarea[name=message]').value.trim();
      if (!msg) {{ alert('Please write a message first!'); return false; }}
      return confirm('Send this blast to ' + aud + ' (' + cnt + ')?\n\n"' + msg.slice(0,120) + (msg.length>120?'…':'') + '"');
    }}
    </script>
    """
    return _page(slug, "blast", f"Send Blast — {tenant.display_name}", body, tenant.display_name)
