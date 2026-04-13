"""
SMB show attendance admin — create shows, view check-ins, export attendees.

Each show has a short check-in keyword fans text at the door. This page lets
the operator create shows and see who showed up so the owner can blast them.

Routes registered via register_shows_routes(bp) called from app/admin/__init__.py.
"""

import csv
import io
import logging
from datetime import date, timezone

from flask import request, redirect, url_for, Response

from app.admin_auth import get_db_connection
from app.smb import storage as smb_storage
from app.smb.tenants import get_registry

logger = logging.getLogger(__name__)

_PAGE_STYLE = """
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f0f1a; color: #e2e8f0; min-height: 100vh; }
  .topbar { background: #1e1e2e; border-bottom: 1px solid #2d2d44; padding: 14px 24px;
            display: flex; align-items: center; gap: 16px; }
  .topbar a { color: #a78bfa; text-decoration: none; font-size: 14px; }
  .topbar a:hover { color: #c4b5fd; }
  .topbar h1 { font-size: 18px; font-weight: 600; color: #f1f5f9; }
  .container { max-width: 960px; margin: 0 auto; padding: 28px 20px; }
  .card { background: #1e1e2e; border: 1px solid #2d2d44; border-radius: 10px;
          padding: 20px 24px; margin-bottom: 20px; }
  .card-title { font-size: 14px; font-weight: 600; color: #94a3b8;
                text-transform: uppercase; letter-spacing: .06em; margin-bottom: 16px; }
  .form-row { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; margin-bottom: 4px; }
  .form-group { display: flex; flex-direction: column; gap: 5px; }
  .form-group label { font-size: 12px; color: #94a3b8; }
  input[type=text], input[type=date], select {
    background: #0f0f1a; border: 1px solid #374151; border-radius: 6px;
    color: #e2e8f0; padding: 8px 12px; font-size: 14px; outline: none; }
  input[type=text]:focus, input[type=date]:focus, select:focus { border-color: #7c3aed; }
  .btn { padding: 9px 18px; border-radius: 6px; font-size: 13px; font-weight: 600;
         cursor: pointer; border: none; text-decoration: none; display: inline-block; }
  .btn-primary { background: #7c3aed; color: #fff; }
  .btn-primary:hover { background: #6d28d9; }
  .btn-sm { padding: 6px 12px; font-size: 12px; border-radius: 5px; }
  .btn-outline { background: transparent; border: 1px solid #374151; color: #94a3b8; }
  .btn-outline:hover { border-color: #7c3aed; color: #a78bfa; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th { text-align: left; padding: 10px 12px; font-size: 12px; font-weight: 600;
       color: #64748b; border-bottom: 1px solid #2d2d44; }
  td { padding: 11px 12px; border-bottom: 1px solid #1a1a2e; vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #161625; }
  .badge { display: inline-block; padding: 3px 9px; border-radius: 12px; font-size: 11px;
           font-weight: 600; }
  .badge-green { background: #052e16; color: #4ade80; }
  .badge-gray  { background: #1e293b; color: #64748b; }
  .badge-purple { background: #3b0764; color: #c084fc; }
  .keyword-chip { font-family: monospace; background: #1a1a2e; border: 1px solid #374151;
                  border-radius: 5px; padding: 3px 8px; font-size: 12px; color: #a78bfa; }
  .empty-state { text-align: center; padding: 40px 0; color: #475569; font-size: 14px; }
  .alert { padding: 12px 16px; border-radius: 7px; margin-bottom: 16px; font-size: 13px; }
  .alert-error { background: #450a0a; border: 1px solid #991b1b; color: #fca5a5; }
  .alert-success { background: #052e16; border: 1px solid #166534; color: #86efac; }
  .back-link { color: #7c3aed; text-decoration: none; font-size: 13px; }
  .back-link:hover { text-decoration: underline; }
  .attendee-tag { font-size: 12px; color: #94a3b8; font-family: monospace; }
  .stats-row { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
  .stat-pill { background: #1e1e2e; border: 1px solid #2d2d44; border-radius: 8px;
               padding: 12px 18px; min-width: 120px; }
  .stat-pill .num { font-size: 24px; font-weight: 700; color: #a78bfa; }
  .stat-pill .lbl { font-size: 11px; color: #64748b; margin-top: 3px; }
</style>
"""

_NAV = """
<div class="topbar">
  <a href="/admin">← Admin</a>
  <h1>🎭 Show Attendance</h1>
</div>
"""


def _all_tenant_slugs() -> list:
    """Return all registered SMB tenant slugs."""
    try:
        return [t.slug for t in get_registry().all_tenants()]
    except Exception:
        return []


def _fetch_all_shows() -> list:
    """Return all shows across all tenants, newest first."""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn:
            slugs = _all_tenant_slugs()
            if not slugs:
                return []
            all_shows = []
            for slug in slugs:
                shows = smb_storage.list_shows(conn, slug)
                for s in shows:
                    s["tenant_slug"] = slug
                all_shows.extend(shows)
            all_shows.sort(key=lambda s: (str(s["show_date"]), str(s["created_at"])), reverse=True)
            return all_shows
    except Exception:
        logger.exception("shows admin: failed to fetch all shows")
        return []
    finally:
        conn.close()


def _fetch_show_with_attendees(show_id: int):
    """Return (show_dict, [attendee_dicts]) or (None, []) if not found."""
    conn = get_db_connection()
    if not conn:
        return None, []
    try:
        with conn:
            show = smb_storage.get_show_by_id(conn, show_id)
            if not show:
                return None, []
            attendees = smb_storage.get_show_attendees(conn, show_id)
            return show, attendees
    except Exception:
        logger.exception("shows admin: failed to fetch show %s", show_id)
        return None, []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def _render_shows_list(shows: list, error: str = "", success: str = "") -> str:
    tenant_slugs = _all_tenant_slugs()
    slug_options = "".join(f'<option value="{s}">{s}</option>' for s in tenant_slugs)
    today = date.today().isoformat()

    alert_html = ""
    if error:
        alert_html = f'<div class="alert alert-error">{error}</div>'
    elif success:
        alert_html = f'<div class="alert alert-success">{success}</div>'

    rows = ""
    if shows:
        for s in shows:
            count = s.get("checkin_count", 0)
            badge_cls = "badge-green" if count > 0 else "badge-gray"
            date_str = str(s["show_date"]) if s.get("show_date") else "—"
            rows += f"""
            <tr>
              <td>{date_str}</td>
              <td><strong>{s['name']}</strong></td>
              <td><span class="keyword-chip">{s['checkin_keyword']}</span></td>
              <td><span class="badge {badge_cls}">{count} checked in</span></td>
              <td style="color:#64748b;font-size:12px">{s.get('tenant_slug','')}</td>
              <td>
                <a href="/admin/smb-shows/{s['id']}" class="btn btn-sm btn-outline">View</a>
              </td>
            </tr>"""
    else:
        rows = '<tr><td colspan="6"><div class="empty-state">No shows yet — create one above.</div></td></tr>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Show Attendance</title>{_PAGE_STYLE}</head>
<body>
{_NAV}
<div class="container">

  {alert_html}

  <div class="card">
    <div class="card-title">Create New Show</div>
    <form method="post" action="/admin/smb-shows">
      <div class="form-row">
        <div class="form-group">
          <label>Client</label>
          <select name="tenant_slug" required style="width:180px">
            {slug_options}
          </select>
        </div>
        <div class="form-group">
          <label>Show Name</label>
          <input type="text" name="name" placeholder="e.g. Friday Night Standup" required style="width:240px">
        </div>
        <div class="form-group">
          <label>Date</label>
          <input type="date" name="show_date" value="{today}" required>
        </div>
        <div class="form-group">
          <label>Check-in Keyword</label>
          <input type="text" name="checkin_keyword" placeholder="e.g. APR13"
                 required style="width:140px;font-family:monospace"
                 title="Fans text this word to check in. Keep it short and memorable.">
        </div>
        <div class="form-group" style="justify-content:flex-end">
          <button type="submit" class="btn btn-primary">Create Show</button>
        </div>
      </div>
      <p style="font-size:12px;color:#475569;margin-top:8px">
        💡 Tip: post this keyword on a sign at the door — "Text <em>KEYWORD</em> to [your number] to stay in the loop!"
      </p>
    </form>
  </div>

  <div class="card">
    <div class="card-title">All Shows</div>
    <table>
      <thead>
        <tr>
          <th>Date</th><th>Show</th><th>Check-in Keyword</th><th>Attendance</th><th>Client</th><th></th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </div>

</div>
</body></html>"""


def _render_show_detail(show: dict, attendees: list) -> str:
    count = len(attendees)
    date_str = str(show.get("show_date", ""))

    rows = ""
    if attendees:
        for i, a in enumerate(attendees, 1):
            phone = a["phone_number"]
            masked = f"({phone[:3]}) {phone[3:6]}-****" if len(phone) >= 10 else phone
            ts = a["checked_in_at"]
            if ts:
                ts_str = ts.strftime("%-I:%M %p") if hasattr(ts, "strftime") else str(ts)
            else:
                ts_str = "—"
            rows += f"""
            <tr>
              <td style="color:#64748b">{i}</td>
              <td class="attendee-tag">{masked}</td>
              <td style="color:#94a3b8;font-size:13px">{ts_str}</td>
            </tr>"""
    else:
        rows = '<tr><td colspan="3"><div class="empty-state">No check-ins yet for this show.</div></td></tr>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{show['name']} — Attendance</title>{_PAGE_STYLE}</head>
<body>
{_NAV}
<div class="container">

  <p style="margin-bottom:20px">
    <a href="/admin/smb-shows" class="back-link">← All shows</a>
  </p>

  <div class="stats-row">
    <div class="stat-pill">
      <div class="num">{count}</div>
      <div class="lbl">Checked in</div>
    </div>
    <div class="stat-pill">
      <div class="num" style="color:#e2e8f0;font-size:16px">{show['name']}</div>
      <div class="lbl">{date_str} · {show.get('tenant_slug','')}</div>
    </div>
    <div class="stat-pill">
      <div class="num" style="font-size:16px;font-family:monospace;color:#a78bfa">{show['checkin_keyword']}</div>
      <div class="lbl">Check-in keyword</div>
    </div>
  </div>

  <div class="card">
    <div class="card-title" style="display:flex;justify-content:space-between;align-items:center">
      <span>Attendees</span>
      {f'<a href="/admin/smb-shows/{show["id"]}/export" class="btn btn-sm btn-outline">⬇ Export CSV</a>' if count > 0 else ''}
    </div>

    <p style="font-size:13px;color:#64748b;margin-bottom:16px">
      To thank these {count} people, text your bot:
      <strong style="color:#c4b5fd">"Send a thank you to everyone who came to the {show['name']}"</strong>
      — the bot will handle the rest.
    </p>

    <table>
      <thead><tr><th>#</th><th>Phone</th><th>Checked in at</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>

</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

def render_shows_page() -> str:
    shows = _fetch_all_shows()
    return _render_shows_list(shows)


def register_shows_routes(bp):
    """Register show attendance admin routes on the admin blueprint."""

    @bp.route("/admin/smb-shows", methods=["GET"])
    def smb_shows_list():
        shows = _fetch_all_shows()
        return _render_shows_list(shows)

    @bp.route("/admin/smb-shows", methods=["POST"])
    def smb_shows_create():
        tenant_slug = request.form.get("tenant_slug", "").strip()
        name = request.form.get("name", "").strip()
        show_date = request.form.get("show_date", "").strip()
        keyword = request.form.get("checkin_keyword", "").strip().upper()

        if not all([tenant_slug, name, show_date, keyword]):
            shows = _fetch_all_shows()
            return _render_shows_list(shows, error="All fields are required.")

        conn = get_db_connection()
        if not conn:
            shows = _fetch_all_shows()
            return _render_shows_list(shows, error="Database connection failed — try again.")

        try:
            with conn:
                show = smb_storage.create_show(conn, tenant_slug, name, show_date, keyword)
        except Exception:
            logger.exception("shows admin: failed to create show")
            show = None
        finally:
            conn.close()

        if show is None:
            shows = _fetch_all_shows()
            return _render_shows_list(
                shows,
                error=f"Keyword <strong>{keyword}</strong> is already taken for this client. Choose a different one.",
            )

        shows = _fetch_all_shows()
        return _render_shows_list(
            shows,
            success=f"Show <strong>{name}</strong> created! Keyword: <strong>{keyword}</strong>",
        )

    @bp.route("/admin/smb-shows/<int:show_id>", methods=["GET"])
    def smb_show_detail(show_id):
        show, attendees = _fetch_show_with_attendees(show_id)
        if show is None:
            return "Show not found", 404
        return _render_show_detail(show, attendees)

    @bp.route("/admin/smb-shows/<int:show_id>/export", methods=["GET"])
    def smb_show_export(show_id):
        show, attendees = _fetch_show_with_attendees(show_id)
        if show is None:
            return "Show not found", 404

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["phone_number", "checked_in_at", "show_name", "show_date"])
        for a in attendees:
            writer.writerow([
                a["phone_number"],
                a["checked_in_at"].isoformat() if a["checked_in_at"] else "",
                show["name"],
                str(show["show_date"]),
            ])

        slug = show["name"].lower().replace(" ", "_")[:30]
        filename = f"attendees_{slug}_{show['show_date']}.csv"
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
