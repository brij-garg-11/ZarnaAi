"""
SMB Client Portal — a self-serve dashboard for business owners.

Each SMB tenant gets their own URL at /portal/<slug> where they can:
  • View subscriber and engagement stats
  • Create shows and see who checked in at the door
  • Compose and send blasts to any audience slice

Authentication: simple password per tenant via env var
  SMB_PORTAL_<SLUG_UPPER>_PASSWORD   (e.g. SMB_PORTAL_WEST_SIDE_COMEDY_PASSWORD)

Session key: smb_portal_<slug>
"""

import logging
import os
import sys
from functools import wraps
from pathlib import Path

from flask import Blueprint, redirect, request, session, url_for

from ..db import get_conn

# The operator process runs from the operator/ directory. The main app's
# `app.smb.*` modules live one level up (the repo root). Add the repo root
# to sys.path so those imports resolve correctly in both local and Railway.
_REPO_ROOT = str(Path(__file__).resolve().parents[3])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

logger = logging.getLogger(__name__)
smb_portal_bp = Blueprint("smb_portal", __name__)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _session_key(slug: str) -> str:
    return f"smb_portal_{slug}"


def _portal_password(slug: str) -> str:
    env_key = f"SMB_PORTAL_{slug.upper().replace('-', '_')}_PASSWORD"
    return os.getenv(env_key, "")


def _is_authenticated(slug: str) -> bool:
    return session.get(_session_key(slug)) is True


def _portal_login_required(slug: str):
    """Decorator factory that guards a route for a specific tenant slug."""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not _is_authenticated(slug):
                return redirect(url_for("smb_portal.portal_login", slug=slug))
            return f(*args, **kwargs)
        return wrapped
    return decorator


# ---------------------------------------------------------------------------
# Data helpers (all scoped to the tenant slug)
# ---------------------------------------------------------------------------

def _get_tenant(slug: str):
    """Return BusinessTenant or None."""
    try:
        from app.smb.tenants import get_registry
        return get_registry().get_by_slug(slug)
    except Exception:
        logger.exception("smb_portal: failed to load tenant %s", slug)
        return None


def _get_stats(conn, tenant_slug: str) -> dict:
    """Subscriber count + segment breakdown."""
    from app.smb import storage as smb_storage
    subs = smb_storage.get_active_subscribers(conn, tenant_slug)
    tenant = _get_tenant(tenant_slug)
    segs = []
    if tenant and tenant.segments:
        for seg in tenant.segments:
            seg_subs = smb_storage.get_subscribers_by_segment(
                conn, tenant_slug, seg["question_key"], seg["answers"]
            )
            segs.append({"name": seg["name"], "description": seg.get("description", ""), "count": len(seg_subs)})
    return {"total": len(subs), "segments": segs}


def _get_recent_blasts(conn, tenant_slug: str, limit: int = 8) -> list:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, owner_message, body, attempted, succeeded, segment, sent_at
            FROM smb_blasts
            WHERE tenant_slug = %s
            ORDER BY sent_at DESC
            LIMIT %s
            """,
            (tenant_slug, limit),
        )
        return [
            {
                "id": r[0], "owner_message": r[1], "body": r[2],
                "attempted": r[3], "succeeded": r[4],
                "segment": r[5] or "Everyone", "sent_at": r[6],
            }
            for r in cur.fetchall()
        ]


def _count_audience(conn, tenant, audience_type: str) -> int:
    """Return recipient count for a given audience_type string."""
    from app.smb import storage as smb_storage
    if audience_type == "all":
        return len(smb_storage.get_active_subscribers(conn, tenant.slug))
    if audience_type.startswith("segment:"):
        seg_name = audience_type[8:].strip().upper()
        seg = next((s for s in (tenant.segments or []) if s["name"].upper() == seg_name), None)
        if not seg:
            return 0
        return len(smb_storage.get_subscribers_by_segment(conn, tenant.slug, seg["question_key"], seg["answers"]))
    if audience_type.startswith("show:"):
        keyword = audience_type[5:].strip()
        show = smb_storage.get_show_by_keyword(conn, tenant.slug, keyword)
        if not show:
            return 0
        return len(smb_storage.get_show_attendees(conn, show["id"]))
    return 0


# ---------------------------------------------------------------------------
# Page style + shared chrome
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

/* ── Layout ── */
.shell { display: flex; min-height: 100vh; }
.sidebar {
  width: 220px; flex-shrink: 0;
  background: #111122; border-right: 1px solid #1e1e35;
  display: flex; flex-direction: column;
  position: sticky; top: 0; height: 100vh;
  overflow-y: auto;
}
.sidebar-logo {
  padding: 20px 20px 16px;
  border-bottom: 1px solid #1e1e35;
}
.sidebar-logo .club-name {
  font-size: 14px; font-weight: 700; color: #f1f5f9; line-height: 1.3;
}
.sidebar-logo .powered {
  font-size: 10px; color: #475569; margin-top: 3px;
}
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

/* ── Cards ── */
.card {
  background: #14142a; border: 1px solid #1e1e35;
  border-radius: 12px; padding: 22px 24px; margin-bottom: 20px;
}
.card-title {
  font-size: 11px; font-weight: 700; color: #6b7280;
  text-transform: uppercase; letter-spacing: .08em; margin-bottom: 16px;
}

/* ── Stat grid ── */
.stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 14px; margin-bottom: 20px; }
.stat-card {
  background: #14142a; border: 1px solid #1e1e35; border-radius: 10px;
  padding: 16px 18px;
}
.stat-num { font-size: 28px; font-weight: 800; color: #a78bfa; }
.stat-lbl { font-size: 11px; color: #64748b; margin-top: 3px; }

/* ── Tables ── */
table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
th { text-align: left; padding: 9px 12px; font-size: 11px; font-weight: 700;
     color: #4b5563; border-bottom: 1px solid #1e1e35; text-transform: uppercase; letter-spacing: .05em; }
td { padding: 11px 12px; border-bottom: 1px solid #111122; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #111126; }
.empty { text-align: center; padding: 36px 0; color: #374151; font-size: 13px; }

/* ── Forms ── */
.form-row  { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; }
.form-group { display: flex; flex-direction: column; gap: 5px; }
label { font-size: 12px; color: #94a3b8; }
input[type=text], input[type=date], input[type=password], select, textarea {
  background: #0d0d1a; border: 1px solid #2d2d44; border-radius: 7px;
  color: #e2e8f0; padding: 9px 13px; font-size: 14px; outline: none;
  transition: border .15s;
}
input:focus, select:focus, textarea:focus { border-color: #7c3aed; }
textarea { resize: vertical; min-height: 110px; line-height: 1.5; }

/* ── Buttons ── */
.btn {
  padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 600;
  cursor: pointer; border: none; display: inline-block; transition: all .15s;
}
.btn-primary   { background: #7c3aed; color: #fff; }
.btn-primary:hover { background: #6d28d9; }
.btn-secondary { background: #1e1e35; color: #94a3b8; border: 1px solid #2d2d44; }
.btn-secondary:hover { border-color: #7c3aed; color: #a78bfa; }
.btn-sm { padding: 6px 13px; font-size: 12px; border-radius: 6px; }
.btn-danger { background: #7f1d1d; color: #fca5a5; }

/* ── Badges ── */
.badge { display: inline-block; padding: 3px 9px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.badge-green  { background: #052e16; color: #4ade80; }
.badge-purple { background: #3b0764; color: #c084fc; }
.badge-gray   { background: #1e293b; color: #64748b; }
.badge-blue   { background: #0c1a3d; color: #60a5fa; }

/* ── Misc ── */
.keyword { font-family: monospace; background: #1a1a2e; border: 1px solid #2d2d44;
           border-radius: 5px; padding: 3px 8px; font-size: 12px; color: #a78bfa; }
.alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 13px; }
.alert-success { background: #052e16; border: 1px solid #166534; color: #86efac; }
.alert-error   { background: #450a0a; border: 1px solid #991b1b; color: #fca5a5; }
.divider { border: none; border-top: 1px solid #1e1e35; margin: 20px 0; }

/* ── Audience picker ── */
.audience-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; margin-bottom: 16px; }
.audience-card {
  border: 2px solid #1e1e35; border-radius: 9px; padding: 13px 15px;
  cursor: pointer; transition: all .15s; background: #111122;
}
.audience-card:hover { border-color: #5b21b6; }
.audience-card.selected { border-color: #7c3aed; background: #1a0d35; }
.audience-card input[type=radio] { display: none; }
.audience-card .aud-name { font-size: 13px; font-weight: 600; color: #e2e8f0; }
.audience-card .aud-count { font-size: 12px; color: #64748b; margin-top: 3px; }
.audience-card .aud-icon  { font-size: 20px; margin-bottom: 6px; }

/* ── Login page ── */
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
        ("dashboard", "📊", "Dashboard", f"/portal/{slug}/"),
        ("shows",     "🎭", "Show Attendance", f"/portal/{slug}/shows"),
        ("blast",     "📣", "Send a Blast", f"/portal/{slug}/blast"),
    ]
    nav_html = ""
    for tab_id, icon, label, href in nav_items:
        cls = "nav-link active" if active_tab == tab_id else "nav-link"
        nav_html += f'<a href="{href}" class="{cls}"><span class="nav-icon">{icon}</span>{label}</a>\n'

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
    <nav>
      {nav_html}
    </nav>
    <div style="padding:16px 20px;border-top:1px solid #1e1e35">
      <a href="/portal/{slug}/logout" style="font-size:12px;color:#475569">Sign out</a>
    </div>
  </aside>
  <main class="main">
    {body}
  </main>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/<slug>/login", methods=["GET", "POST"])
def portal_login(slug):
    if _is_authenticated(slug):
        return redirect(url_for("smb_portal.portal_dashboard", slug=slug))

    tenant = _get_tenant(slug)
    if tenant is None:
        return f"<h3 style='font-family:sans-serif;color:#ccc;padding:40px'>Client portal not found: {slug}</h3>", 404

    error = ""
    if request.method == "POST":
        pwd = request.form.get("password", "")
        expected = _portal_password(slug)
        if not expected:
            error = "Portal password not configured — contact your Zarna team."
        elif pwd == expected:
            session[_session_key(slug)] = True
            return redirect(url_for("smb_portal.portal_dashboard", slug=slug))
        else:
            error = "Incorrect password. Try again."

    error_html = f'<div class="alert alert-error">{error}</div>' if error else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in — {tenant.display_name}</title>
{_STYLE}
</head>
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


@smb_portal_bp.route("/portal/<slug>/logout")
def portal_logout(slug):
    session.pop(_session_key(slug), None)
    return redirect(url_for("smb_portal.portal_login", slug=slug))


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/<slug>/")
def portal_dashboard(slug):
    if not _is_authenticated(slug):
        return redirect(url_for("smb_portal.portal_login", slug=slug))

    tenant = _get_tenant(slug)
    if tenant is None:
        return "Client not found", 404

    conn = get_conn()
    try:
        with conn:
            stats = _get_stats(conn, slug)
            recent_blasts = _get_recent_blasts(conn, slug, limit=5)
            from app.smb import storage as smb_storage
            recent_shows = smb_storage.list_shows(conn, slug, limit=5)
    except Exception:
        logger.exception("smb_portal: dashboard query failed for %s", slug)
        stats = {"total": 0, "segments": []}
        recent_blasts = []
        recent_shows = []
    finally:
        conn.close()

    # Stat cards
    seg_cards = ""
    for seg in stats["segments"]:
        seg_cards += f"""
        <div class="stat-card">
          <div class="stat-num" style="font-size:22px">{seg['count']}</div>
          <div class="stat-lbl">{seg['description'] or seg['name']}</div>
        </div>"""

    # Recent blasts table
    if recent_blasts:
        blast_rows = ""
        for b in recent_blasts:
            msg = (b["body"] or b["owner_message"] or "")[:80]
            if len(b.get("body", "")) > 80:
                msg += "…"
            sent_str = b["sent_at"].strftime("%b %-d, %-I:%M %p") if b["sent_at"] else "—"
            blast_rows += f"""
            <tr>
              <td style="max-width:300px;color:#cbd5e1">{msg}</td>
              <td><span class="badge badge-gray">{b['segment']}</span></td>
              <td style="color:#4ade80">{b['succeeded']}</td>
              <td style="color:#64748b;font-size:12px">{sent_str}</td>
            </tr>"""
        blast_table = f"""<table>
          <thead><tr><th>Message</th><th>Audience</th><th>Sent to</th><th>When</th></tr></thead>
          <tbody>{blast_rows}</tbody>
        </table>"""
    else:
        blast_table = '<div class="empty">No blasts sent yet — <a href="/portal/' + slug + '/blast">send your first one</a></div>'

    # Recent shows
    if recent_shows:
        show_rows = ""
        for s in recent_shows:
            count = s.get("checkin_count", 0)
            date_str = str(s["show_date"]) if s.get("show_date") else "—"
            badge = f'<span class="badge badge-green">{count} checked in</span>' if count > 0 else f'<span class="badge badge-gray">0 checked in</span>'
            show_rows += f"""
            <tr>
              <td>{date_str}</td>
              <td><strong style="color:#f1f5f9">{s['name']}</strong></td>
              <td>{badge}</td>
              <td><span class="keyword">{s['checkin_keyword']}</span></td>
              <td>
                <a href="/portal/{slug}/shows/{s['id']}" class="btn btn-sm btn-secondary">View</a>
              </td>
            </tr>"""
        show_table = f"""<table>
          <thead><tr><th>Date</th><th>Show</th><th>Attendance</th><th>Check-in Word</th><th></th></tr></thead>
          <tbody>{show_rows}</tbody>
        </table>"""
    else:
        show_table = '<div class="empty">No shows yet — <a href="/portal/' + slug + '/shows">create your first show</a></div>'

    body = f"""
    <div class="page-header">
      <h1>👋 Hey, {tenant.display_name.split()[0]}!</h1>
      <p>Here's what's happening with your community.</p>
    </div>

    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-num">{stats['total']}</div>
        <div class="stat-lbl">Active subscribers</div>
      </div>
      {seg_cards}
    </div>

    <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px">
      <a href="/portal/{slug}/blast" class="btn btn-primary">📣 Send a Blast</a>
      <a href="/portal/{slug}/shows" class="btn btn-secondary">🎭 Manage Shows</a>
    </div>

    <div class="card">
      <div class="card-title">Recent Blasts</div>
      {blast_table}
    </div>

    <div class="card">
      <div class="card-title">Recent Shows</div>
      {show_table}
    </div>
    """

    return _page(slug, "dashboard", f"Dashboard — {tenant.display_name}", body, tenant.display_name)


# ---------------------------------------------------------------------------
# Shows
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/<slug>/shows", methods=["GET", "POST"])
def portal_shows(slug):
    if not _is_authenticated(slug):
        return redirect(url_for("smb_portal.portal_login", slug=slug))

    tenant = _get_tenant(slug)
    if tenant is None:
        return "Client not found", 404

    error = success = ""
    from app.smb import storage as smb_storage
    from datetime import date as _date

    if request.method == "POST":
        name    = request.form.get("name", "").strip()
        show_date = request.form.get("show_date", "").strip()
        keyword = request.form.get("checkin_keyword", "").strip().upper()

        if not all([name, show_date, keyword]):
            error = "All fields are required."
        else:
            conn = get_conn()
            try:
                with conn:
                    result = smb_storage.create_show(conn, slug, name, show_date, keyword)
                    if result is None:
                        error = f"The keyword <strong>{keyword}</strong> is already used. Choose a different one."
                    else:
                        success = f"Show <strong>{name}</strong> created! Fans can check in by texting <strong>{keyword}</strong>."
            except Exception:
                logger.exception("smb_portal: create show failed for %s", slug)
                error = "Something went wrong — try again."
            finally:
                conn.close()

    conn = get_conn()
    try:
        with conn:
            shows = smb_storage.list_shows(conn, slug)
    except Exception:
        shows = []
    finally:
        conn.close()

    today = _date.today().isoformat()
    alert = ""
    if error:
        alert = f'<div class="alert alert-error">{error}</div>'
    elif success:
        alert = f'<div class="alert alert-success">{success}</div>'

    # Build rows
    if shows:
        rows = ""
        for s in shows:
            count = s.get("checkin_count", 0)
            date_str = str(s["show_date"]) if s.get("show_date") else "—"
            badge_cls = "badge-green" if count > 0 else "badge-gray"
            rows += f"""
            <tr>
              <td>{date_str}</td>
              <td><strong style="color:#f1f5f9">{s['name']}</strong></td>
              <td><span class="keyword">{s['checkin_keyword']}</span></td>
              <td><span class="badge {badge_cls}">{count} people</span></td>
              <td><a href="/portal/{slug}/shows/{s['id']}" class="btn btn-sm btn-secondary">View Attendees</a></td>
            </tr>"""
        table = f"""<table>
          <thead><tr><th>Date</th><th>Show</th><th>Check-in Word</th><th>Checked in</th><th></th></tr></thead>
          <tbody>{rows}</tbody>
        </table>"""
    else:
        table = '<div class="empty">No shows yet — create one below!</div>'

    sms_number = tenant.sms_number or "[your SMS number]"

    body = f"""
    <div class="page-header">
      <h1>🎭 Show Attendance</h1>
      <p>Create a show, put the check-in word on a sign at the door, and see who shows up.</p>
    </div>

    {alert}

    <div class="card">
      <div class="card-title">How it works</div>
      <p style="font-size:13.5px;color:#94a3b8;line-height:1.7">
        1. Create a show below and give it a short check-in word (e.g. <span class="keyword">APR18</span>)<br>
        2. Put a sign at the door: <em style="color:#e2e8f0">"Text APR18 to {sms_number} tonight!"</em><br>
        3. Fans text it when they arrive — they get a welcome message and are recorded<br>
        4. After the show, go to <a href="/portal/{slug}/blast">Send a Blast</a> and pick that show as your audience
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

    <div class="card">
      <div class="card-title">All Shows</div>
      {table}
    </div>
    """

    return _page(slug, "shows", f"Shows — {tenant.display_name}", body, tenant.display_name)


@smb_portal_bp.route("/portal/<slug>/shows/<int:show_id>")
def portal_show_detail(slug, show_id):
    if not _is_authenticated(slug):
        return redirect(url_for("smb_portal.portal_login", slug=slug))

    tenant = _get_tenant(slug)
    if tenant is None:
        return "Client not found", 404

    from app.smb import storage as smb_storage

    conn = get_conn()
    try:
        with conn:
            show = smb_storage.get_show_by_id(conn, show_id)
            if show is None or show.get("tenant_slug") != slug:
                return "Show not found", 404
            attendees = smb_storage.get_show_attendees(conn, show_id)
    except Exception:
        logger.exception("smb_portal: show detail failed for show_id=%s", show_id)
        return "Error loading show", 500
    finally:
        conn.close()

    count = len(attendees)
    date_str = str(show.get("show_date", ""))

    if attendees:
        rows = ""
        for i, a in enumerate(attendees, 1):
            phone = a["phone_number"]
            masked = f"({phone[:3]}) {phone[3:6]}-****" if len(phone) >= 10 else "—"
            ts = a["checked_in_at"]
            ts_str = ts.strftime("%-I:%M %p") if hasattr(ts, "strftime") else "—"
            rows += f"""
            <tr>
              <td style="color:#64748b">{i}</td>
              <td style="font-family:monospace;color:#94a3b8">{masked}</td>
              <td style="color:#94a3b8;font-size:12px">{ts_str}</td>
            </tr>"""
        table = f"""<table>
          <thead><tr><th>#</th><th>Phone</th><th>Checked in at</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>"""
    else:
        table = '<div class="empty">No check-ins yet. Put the sign out at the door!</div>'

    body = f"""
    <div class="page-header">
      <a href="/portal/{slug}/shows" style="font-size:13px;color:#64748b">← All shows</a>
      <h1 style="margin-top:10px">{show['name']}</h1>
      <p>{date_str} · <span class="keyword">{show['checkin_keyword']}</span></p>
    </div>

    <div class="stat-grid" style="max-width:400px">
      <div class="stat-card">
        <div class="stat-num">{count}</div>
        <div class="stat-lbl">Checked in</div>
      </div>
    </div>

    {'<div style="margin-bottom:16px"><a href="/portal/' + slug + '/blast?show=' + show["checkin_keyword"] + '" class="btn btn-primary">📣 Send Thank-You Blast to These ' + str(count) + ' People</a></div>' if count > 0 else ''}

    <div class="card">
      <div class="card-title">Attendees</div>
      {table}
    </div>
    """

    return _page(slug, "shows", f"{show['name']} — {tenant.display_name}", body, tenant.display_name)


# ---------------------------------------------------------------------------
# Blast compose
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/<slug>/blast", methods=["GET", "POST"])
def portal_blast(slug):
    if not _is_authenticated(slug):
        return redirect(url_for("smb_portal.portal_login", slug=slug))

    tenant = _get_tenant(slug)
    if tenant is None:
        return "Client not found", 404

    from app.smb import storage as smb_storage
    from app.smb.blast import send_blast_from_portal

    # Pre-selected audience from query string (e.g. from show detail page)
    preselect_show = request.args.get("show", "").strip().upper()

    sent_msg = ""
    error = ""

    if request.method == "POST":
        message   = request.form.get("message", "").strip()
        audience  = request.form.get("audience", "all").strip()

        if not message:
            error = "Please write a message before sending."
        else:
            try:
                result = send_blast_from_portal(message, tenant, audience)
                sent_msg = result
                logger.info("smb_portal: blast fired for tenant=%s audience=%s", slug, audience)
            except Exception:
                logger.exception("smb_portal: blast failed for tenant=%s", slug)
                error = "Something went wrong sending the blast. Check the logs."

    # Build audience options (fetch counts)
    conn = get_conn()
    try:
        with conn:
            total = len(smb_storage.get_active_subscribers(conn, slug))
            seg_options = []
            for seg in (tenant.segments or []):
                cnt = len(smb_storage.get_subscribers_by_segment(
                    conn, slug, seg["question_key"], seg["answers"]
                ))
                seg_options.append({
                    "value": f"segment:{seg['name']}",
                    "label": seg["name"].title(),
                    "desc": seg.get("description", ""),
                    "count": cnt,
                    "icon": "🎯",
                })
            shows = smb_storage.list_shows(conn, slug, limit=12)
            show_options = []
            for s in shows:
                cnt = s.get("checkin_count", 0)
                show_options.append({
                    "value": f"show:{s['checkin_keyword']}",
                    "label": s["name"],
                    "desc": str(s["show_date"]),
                    "count": cnt,
                    "icon": "🎭",
                    "keyword": s["checkin_keyword"],
                })
    except Exception:
        logger.exception("smb_portal: blast page query failed for %s", slug)
        total = 0
        seg_options = []
        show_options = []
    finally:
        conn.close()

    # Audience cards HTML
    def aud_card(value, icon, label, desc, count, extra_cls=""):
        is_pre = (value == f"show:{preselect_show}") if preselect_show else (value == "all")
        selected_cls = " selected" if is_pre else ""
        return f"""
        <label class="audience-card{selected_cls}{extra_cls}">
          <input type="radio" name="audience" value="{value}" {'checked' if is_pre else ''}>
          <div class="aud-icon">{icon}</div>
          <div class="aud-name">{label}</div>
          <div class="aud-count">{count:,} {'person' if count == 1 else 'people'}</div>
          {f'<div style="font-size:11px;color:#475569;margin-top:3px">{desc}</div>' if desc else ''}
        </label>"""

    audience_html = '<div class="audience-grid">'
    audience_html += aud_card("all", "👥", "Everyone", "All active subscribers", total)
    for s in seg_options:
        audience_html += aud_card(s["value"], s["icon"], s["label"], s["desc"], s["count"])
    for s in show_options:
        audience_html += aud_card(s["value"], s["icon"], s["label"], s["desc"], s["count"])
    audience_html += "</div>"

    alert = ""
    if error:
        alert = f'<div class="alert alert-error">{error}</div>'
    elif sent_msg:
        alert = f'<div class="alert alert-success">✅ {sent_msg} You\'ll get a confirmation text when it\'s done.</div>'

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
          <textarea name="message" placeholder="e.g. Thanks so much for coming out last night — you made it an incredible show. Hope to see you again soon!" style="width:100%"></textarea>
          <p style="font-size:11px;color:#475569;margin-top:6px">
            ✨ Zarna will lightly clean up typos and shorthand automatically — your facts and message stay exactly the same.
          </p>
        </div>
        <button type="submit" class="btn btn-primary" style="min-width:160px">Send Blast →</button>
      </div>
    </form>

    <script>
    // Make audience cards clickable
    document.querySelectorAll('.audience-card').forEach(card => {{
      card.addEventListener('click', () => {{
        document.querySelectorAll('.audience-card').forEach(c => c.classList.remove('selected'));
        card.classList.add('selected');
        card.querySelector('input[type=radio]').checked = true;
      }});
    }});

    function confirmSend() {{
      const selected = document.querySelector('.audience-card.selected');
      const aud = selected ? selected.querySelector('.aud-name').textContent : 'everyone';
      const count = selected ? selected.querySelector('.aud-count').textContent : '';
      const msg = document.querySelector('textarea[name=message]').value.trim();
      if (!msg) {{ alert('Please write a message first!'); return false; }}
      return confirm('Send this blast to ' + aud + ' (' + count + ')?\n\n"' + msg.slice(0,120) + (msg.length > 120 ? '…' : '') + '"');
    }}
    </script>
    """

    return _page(slug, "blast", f"Send Blast — {tenant.display_name}", body, tenant.display_name)
