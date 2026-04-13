"""
WSCC Client Portal — self-contained Flask blueprint for the operator app.

Felicia (the owner) logs in here to manage shows, view check-ins, and send blasts.

URL: /portal/west_side_comedy/login

Authentication: password stored in env var SMB_PORTAL_WEST_SIDE_COMEDY_PASSWORD
SMS blast:       Twilio, from the WSCC number in env var WEST_SIDE_COMEDY_SMS_NUMBER
"""

import logging
import os
import threading
import time
from functools import wraps

from flask import Blueprint, redirect, request, session, url_for

from ..db import get_conn

logger = logging.getLogger(__name__)

smb_portal_bp = Blueprint("wscc_portal", __name__)

# ---------------------------------------------------------------------------
# Tenant config — read from env vars, fall back to known defaults
# ---------------------------------------------------------------------------

_SLUG = "west_side_comedy"
_DISPLAY_NAME = "West Side Comedy Club"
_LOGO_URL = "https://imagedelivery.net/7Ze32-hXUdrEDVtqvlbDMQ/2fa2cff3-e63c-4d6d-c193-0cbd39e86d00/public"


def _sms_number() -> str:
    return os.getenv("WEST_SIDE_COMEDY_SMS_NUMBER", "")


def _portal_password() -> str:
    return os.getenv("SMB_PORTAL_WEST_SIDE_COMEDY_PASSWORD", "")


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

_SESSION_KEY = f"smb_portal_auth_{_SLUG}"


def _is_authenticated() -> bool:
    return bool(session.get(_SESSION_KEY))


def _login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _is_authenticated():
            return redirect(url_for("wscc_portal.portal_login"))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_subscribers():
    """Return total subscriber count and recent subscribers."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug = %s",
                (_SLUG,),
            )
            total = cur.fetchone()[0]
            cur.execute(
                """
                SELECT subscribed_at
                FROM smb_subscribers
                WHERE tenant_slug = %s
                ORDER BY subscribed_at DESC
                LIMIT 1
                """,
                (_SLUG,),
            )
            row = cur.fetchone()
            latest_at = row[0] if row else None
        return total, latest_at
    finally:
        conn.close()


def _get_recent_blasts(limit=5):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, message_text, audience_type, sent_at, recipient_count
                FROM smb_blasts
                WHERE tenant_slug = %s
                ORDER BY sent_at DESC
                LIMIT %s
                """,
                (_SLUG, limit),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        logger.exception("smb_portal: get_recent_blasts failed")
        return []
    finally:
        conn.close()


def _list_shows():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.id, s.name, s.show_date, s.checkin_keyword, s.status,
                       COUNT(c.id) AS checkin_count
                FROM smb_shows s
                LEFT JOIN smb_show_checkins c ON c.show_id = s.id
                WHERE s.tenant_slug = %s
                GROUP BY s.id
                ORDER BY s.show_date DESC
                """,
                (_SLUG,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        logger.exception("smb_portal: list_shows failed")
        return []
    finally:
        conn.close()


def _create_show(name: str, show_date: str, checkin_keyword: str) -> str | None:
    """Insert a new show. Returns None on success, error string on failure."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO smb_shows (tenant_slug, name, show_date, checkin_keyword, status)
                    VALUES (%s, %s, %s, %s, 'active')
                    """,
                    (_SLUG, name.strip(), show_date, checkin_keyword.upper().strip()),
                )
        return None
    except Exception as exc:
        logger.exception("smb_portal: create_show failed")
        return str(exc)
    finally:
        conn.close()


def _get_show(show_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.id, s.name, s.show_date, s.checkin_keyword, s.status,
                       COUNT(c.id) AS checkin_count
                FROM smb_shows s
                LEFT JOIN smb_show_checkins c ON c.show_id = s.id
                WHERE s.id = %s AND s.tenant_slug = %s
                GROUP BY s.id
                """,
                (show_id, _SLUG),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


def _get_show_attendees(show_id: int):
    """Return count of attendees only (no raw phone numbers in UI)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM smb_show_checkins WHERE show_id = %s",
                (show_id,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _get_segments():
    """Return available blast audience segments from smb_preferences."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT question_key, answer, COUNT(*) as cnt
                FROM smb_preferences
                WHERE tenant_slug = %s
                GROUP BY question_key, answer
                ORDER BY question_key, cnt DESC
                """,
                (_SLUG,),
            )
            return cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def _get_all_subscriber_phones():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phone_number FROM smb_subscribers WHERE tenant_slug = %s",
                (_SLUG,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _get_segment_phones(question_key: str, answer: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT s.phone_number
                FROM smb_subscribers s
                JOIN smb_preferences p ON p.phone_number = s.phone_number AND p.tenant_slug = s.tenant_slug
                WHERE s.tenant_slug = %s AND p.question_key = %s AND p.answer = %s
                """,
                (_SLUG, question_key, answer),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _get_show_attendee_phones(show_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phone_number FROM smb_show_checkins WHERE show_id = %s",
                (show_id,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _record_blast(message_text: str, audience_type: str, recipient_count: int):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO smb_blasts (tenant_slug, message_text, audience_type, sent_at, recipient_count)
                    VALUES (%s, %s, %s, NOW(), %s)
                    """,
                    (_SLUG, message_text, audience_type, recipient_count),
                )
    except Exception:
        logger.exception("smb_portal: record_blast failed")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# SMS helper
# ---------------------------------------------------------------------------

def _send_sms(to: str, body: str) -> bool:
    from_number = _sms_number()
    if not from_number:
        logger.error("smb_portal: WEST_SIDE_COMEDY_SMS_NUMBER not set")
        return False
    try:
        from twilio.rest import Client
        sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        token = os.getenv("TWILIO_AUTH_TOKEN", "")
        if not sid or not token:
            logger.error("smb_portal: Twilio credentials not configured")
            return False
        Client(sid, token).messages.create(to=to, from_=from_number, body=body)
        return True
    except Exception as exc:
        logger.warning("smb_portal: Twilio send to ...%s failed: %s", to[-4:] if to else "?", exc)
        return False


def _blast_async(phones: list, message_text: str, audience_type: str):
    def run():
        sent = 0
        for phone in phones:
            if _send_sms(phone, message_text):
                sent += 1
            time.sleep(0.05)  # gentle rate limiting
        _record_blast(message_text, audience_type, sent)
        logger.info("smb_portal blast done: sent=%d/%d audience=%s", sent, len(phones), audience_type)

    threading.Thread(target=run, daemon=True).start()


# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------

_STYLE = """
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f172a; color: #e2e8f0; min-height: 100vh; }
  a { color: #818cf8; text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* Nav */
  .nav { background: #1e293b; border-bottom: 1px solid #334155;
         display: flex; align-items: center; gap: 8px; padding: 0 24px; height: 56px; }
  .nav-logo { width: 32px; height: 32px; border-radius: 6px; object-fit: cover; }
  .nav-title { font-weight: 700; font-size: 15px; color: #f1f5f9; flex: 1; }
  .nav-links { display: flex; gap: 4px; }
  .nav-links a { padding: 6px 12px; border-radius: 6px; font-size: 13px;
                 color: #94a3b8; transition: background .15s; }
  .nav-links a:hover, .nav-links a.active { background: #334155; color: #e2e8f0;
                                             text-decoration: none; }
  .nav-logout { padding: 6px 12px; border-radius: 6px; font-size: 13px;
                color: #64748b; transition: color .15s; }
  .nav-logout:hover { color: #e2e8f0; text-decoration: none; }

  /* Page wrapper */
  .page { max-width: 960px; margin: 0 auto; padding: 32px 24px; }
  h1 { font-size: 22px; font-weight: 700; color: #f1f5f9; margin-bottom: 4px; }
  .page-sub { color: #64748b; font-size: 14px; margin-bottom: 28px; }

  /* Stats row */
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 32px; }
  .stat-card { background: #1e293b; border: 1px solid #334155; border-radius: 12px;
               padding: 20px; }
  .stat-label { font-size: 12px; color: #64748b; text-transform: uppercase;
                letter-spacing: .5px; margin-bottom: 6px; }
  .stat-value { font-size: 28px; font-weight: 700; color: #f1f5f9; }

  /* Cards / panels */
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px;
          padding: 24px; margin-bottom: 24px; }
  .card-title { font-size: 15px; font-weight: 600; color: #f1f5f9; margin-bottom: 16px; }

  /* Table */
  .tbl { width: 100%; border-collapse: collapse; font-size: 14px; }
  .tbl th { text-align: left; color: #64748b; font-weight: 500; font-size: 12px;
             text-transform: uppercase; letter-spacing: .4px;
             padding: 0 12px 10px; border-bottom: 1px solid #334155; }
  .tbl td { padding: 12px; border-bottom: 1px solid #1e293b; color: #cbd5e1; }
  .tbl tr:last-child td { border-bottom: none; }
  .tbl tr:hover td { background: #0f172a; }
  .tbl-empty { padding: 32px; text-align: center; color: #475569; font-size: 14px; }

  /* Badge */
  .badge { display: inline-block; padding: 2px 8px; border-radius: 99px; font-size: 11px;
           font-weight: 600; }
  .badge-green { background: #14532d; color: #4ade80; }
  .badge-gray  { background: #1e293b; color: #64748b; border: 1px solid #334155; }

  /* Forms */
  .form-group { margin-bottom: 16px; }
  label { display: block; font-size: 13px; color: #94a3b8; margin-bottom: 6px; }
  input[type=text], input[type=date], input[type=password], textarea, select {
    width: 100%; background: #0f172a; border: 1px solid #334155; border-radius: 8px;
    color: #e2e8f0; font-size: 14px; padding: 10px 12px;
    transition: border-color .15s; outline: none; }
  input:focus, textarea:focus, select:focus { border-color: #818cf8; }
  textarea { resize: vertical; min-height: 100px; }

  /* Buttons */
  .btn { display: inline-flex; align-items: center; gap: 6px; padding: 10px 18px;
         border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer;
         border: none; transition: opacity .15s; }
  .btn:hover { opacity: .85; text-decoration: none; }
  .btn-primary { background: #6366f1; color: #fff; }
  .btn-sm { padding: 6px 12px; font-size: 12px; }
  .btn-outline { background: transparent; border: 1px solid #334155; color: #94a3b8; }
  .btn-danger { background: #7f1d1d; color: #fca5a5; }

  /* Audience cards */
  .audience-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; }
  .audience-card { background: #0f172a; border: 2px solid #334155; border-radius: 10px;
                   padding: 14px; cursor: pointer; transition: border-color .15s; display: block; }
  .audience-card input[type=radio] { display: none; }
  .audience-card.selected, .audience-card:has(input:checked) { border-color: #6366f1; background: #1e1b4b; }
  .aud-icon { font-size: 20px; margin-bottom: 6px; }
  .aud-name { font-size: 13px; font-weight: 600; color: #e2e8f0; margin-bottom: 2px; }
  .aud-count { font-size: 12px; color: #64748b; }
  .aud-desc { font-size: 11px; color: #475569; margin-top: 3px; }

  /* Alert */
  .alert { padding: 12px 16px; border-radius: 8px; font-size: 14px; margin-bottom: 16px; }
  .alert-success { background: #14532d; color: #4ade80; border: 1px solid #166534; }
  .alert-error   { background: #7f1d1d; color: #fca5a5; border: 1px solid #991b1b; }
  .alert-info    { background: #1e3a5f; color: #93c5fd; border: 1px solid #1e40af; }

  /* Login */
  .login-wrap { display: flex; align-items: center; justify-content: center;
                min-height: 100vh; padding: 24px; }
  .login-box { background: #1e293b; border: 1px solid #334155; border-radius: 16px;
               padding: 40px; width: 100%; max-width: 380px; text-align: center; }
  .login-logo { width: 64px; height: 64px; border-radius: 12px; object-fit: cover;
                margin: 0 auto 16px; display: block; }
  .login-box h1 { font-size: 20px; margin-bottom: 6px; }
  .login-box p  { color: #64748b; font-size: 14px; margin-bottom: 24px; }

  /* Two-col layout */
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
  @media (max-width: 640px) { .two-col { grid-template-columns: 1fr; } }
</style>
"""


def _nav(active: str) -> str:
    links = [
        ("dashboard", url_for("wscc_portal.portal_dashboard"), "Dashboard"),
        ("shows",     url_for("wscc_portal.portal_shows"),    "Shows"),
        ("blast",     url_for("wscc_portal.portal_blast"),    "Send Blast"),
    ]
    items = "".join(
        f'<a href="{url}" class="{"active" if tab == active else ""}">{label}</a>'
        for tab, url, label in links
    )
    logout_url = url_for("wscc_portal.portal_logout")
    return f"""
    <nav class="nav">
      <img src="{_LOGO_URL}" class="nav-logo" alt="">
      <span class="nav-title">{_DISPLAY_NAME}</span>
      <div class="nav-links">{items}</div>
      <a href="{logout_url}" class="nav-logout">Sign out</a>
    </nav>"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/west_side_comedy/login", methods=["GET", "POST"])
def portal_login():
    if _is_authenticated():
        return redirect(url_for("wscc_portal.portal_dashboard"))

    error = ""
    if request.method == "POST":
        pwd = request.form.get("password", "")
        expected = _portal_password()
        if not expected:
            error = "Portal password not configured — contact your Zarna team."
        elif pwd == expected:
            session[_SESSION_KEY] = True
            return redirect(url_for("wscc_portal.portal_dashboard"))
        else:
            error = "Incorrect password. Try again."

    error_html = f'<div class="alert alert-error">{error}</div>' if error else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in — {_DISPLAY_NAME}</title>{_STYLE}</head>
<body>
<div class="login-wrap">
  <div class="login-box">
    <img src="{_LOGO_URL}" class="login-logo" alt="">
    <h1>{_DISPLAY_NAME}</h1>
    <p>Sign in to your dashboard</p>
    {error_html}
    <form method="post">
      <div class="form-group" style="margin-bottom:16px;text-align:left">
        <label>Password</label>
        <input type="password" name="password" autofocus>
      </div>
      <button type="submit" class="btn btn-primary" style="width:100%">Sign in</button>
    </form>
  </div>
</div>
</body></html>"""


@smb_portal_bp.route("/portal/west_side_comedy/logout")
def portal_logout():
    session.pop(_SESSION_KEY, None)
    return redirect(url_for("wscc_portal.portal_login"))


@smb_portal_bp.route("/portal/west_side_comedy/")
@smb_portal_bp.route("/portal/west_side_comedy")
@_login_required
def portal_dashboard():
    total_subs, latest_at = _get_subscribers()
    shows = _list_shows()
    active_shows = [s for s in shows if s["status"] == "active"]
    recent_blasts = _get_recent_blasts(5)

    latest_str = latest_at.strftime("%-m/%-d/%Y") if latest_at else "—"

    shows_html = ""
    if active_shows:
        rows = ""
        for s in active_shows[:5]:
            rows += f"""
            <tr>
              <td><a href="{url_for('wscc_portal.portal_show_detail', show_id=s['id'])}">{s['name']}</a></td>
              <td>{s['show_date']}</td>
              <td><code style="font-size:13px;background:#0f172a;padding:2px 6px;border-radius:4px">{s['checkin_keyword']}</code></td>
              <td><strong>{s['checkin_count']}</strong></td>
            </tr>"""
        shows_html = f"""
        <div class="card">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
            <span class="card-title" style="margin:0">Active Shows</span>
            <a href="{url_for('wscc_portal.portal_shows')}" class="btn btn-sm btn-outline">All shows →</a>
          </div>
          <table class="tbl">
            <thead><tr><th>Show</th><th>Date</th><th>Check-in Word</th><th>Check-ins</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""
    else:
        shows_html = f"""
        <div class="card" style="text-align:center;padding:32px">
          <div style="font-size:32px;margin-bottom:12px">🎭</div>
          <div style="color:#94a3b8;margin-bottom:16px">No active shows yet. Create one so fans can check in!</div>
          <a href="{url_for('wscc_portal.portal_shows')}" class="btn btn-primary">Create first show</a>
        </div>"""

    blasts_html = ""
    if recent_blasts:
        rows = ""
        for b in recent_blasts:
            sent_str = b["sent_at"].strftime("%-m/%-d %I:%M %p") if b.get("sent_at") else "—"
            preview = (b.get("message_text") or "")[:60]
            if len(b.get("message_text") or "") > 60:
                preview += "…"
            rows += f"<tr><td>{sent_str}</td><td>{preview}</td><td>{b.get('audience_type','')}</td><td>{b.get('recipient_count') or 0}</td></tr>"
        blasts_html = f"""
        <div class="card">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
            <span class="card-title" style="margin:0">Recent Blasts</span>
            <a href="{url_for('wscc_portal.portal_blast')}" class="btn btn-sm btn-primary">Send blast →</a>
          </div>
          <table class="tbl">
            <thead><tr><th>Sent</th><th>Message</th><th>Audience</th><th>Recipients</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""
    else:
        blasts_html = f"""
        <div class="card" style="text-align:center;padding:32px">
          <div style="font-size:32px;margin-bottom:12px">📱</div>
          <div style="color:#94a3b8;margin-bottom:16px">No blasts sent yet.</div>
          <a href="{url_for('wscc_portal.portal_blast')}" class="btn btn-primary">Send your first blast</a>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dashboard — {_DISPLAY_NAME}</title>{_STYLE}</head>
<body>
{_nav("dashboard")}
<div class="page">
  <h1>Dashboard</h1>
  <p class="page-sub">Welcome back, Felicia 👋</p>
  <div class="stats">
    <div class="stat-card">
      <div class="stat-label">Total Subscribers</div>
      <div class="stat-value">{total_subs:,}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Latest Subscriber</div>
      <div class="stat-value" style="font-size:18px">{latest_str}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Shows</div>
      <div class="stat-value">{len(shows)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Blasts Sent</div>
      <div class="stat-value">{len(recent_blasts)}</div>
    </div>
  </div>
  {shows_html}
  {blasts_html}
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Shows
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/west_side_comedy/shows", methods=["GET", "POST"])
@_login_required
def portal_shows():
    error = ""
    success = ""

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        show_date = request.form.get("show_date", "").strip()
        keyword = request.form.get("checkin_keyword", "").strip().upper()

        if not name or not show_date or not keyword:
            error = "All fields are required."
        elif not keyword.isalpha():
            error = "Check-in keyword must be letters only (e.g. STANDUP, COMEDY24)."
        else:
            err = _create_show(name, show_date, keyword)
            if err:
                if "unique" in err.lower():
                    error = f"The keyword '{keyword}' is already used by another show. Pick a different one."
                else:
                    error = "Could not create show. Please try again."
            else:
                success = f"Show '{name}' created! Fans text '{keyword}' to check in."

    shows = _list_shows()

    alert_html = ""
    if success:
        alert_html = f'<div class="alert alert-success">{success}</div>'
    elif error:
        alert_html = f'<div class="alert alert-error">{error}</div>'

    rows = ""
    for s in shows:
        status_badge = (
            '<span class="badge badge-green">Active</span>'
            if s["status"] == "active"
            else '<span class="badge badge-gray">Closed</span>'
        )
        rows += f"""
        <tr>
          <td><a href="{url_for('wscc_portal.portal_show_detail', show_id=s['id'])}">{s['name']}</a></td>
          <td>{s['show_date']}</td>
          <td><code style="font-size:13px;background:#0f172a;padding:2px 6px;border-radius:4px">{s['checkin_keyword']}</code></td>
          <td>{status_badge}</td>
          <td><strong>{s['checkin_count']}</strong></td>
          <td><a href="{url_for('wscc_portal.portal_show_detail', show_id=s['id'])}" class="btn btn-sm btn-outline">View</a></td>
        </tr>"""

    table_html = f"""
    <table class="tbl">
      <thead><tr><th>Show Name</th><th>Date</th><th>Check-in Word</th><th>Status</th><th>Check-ins</th><th></th></tr></thead>
      <tbody>
        {rows if rows else '<tr><td colspan="6" class="tbl-empty">No shows yet. Create one below!</td></tr>'}
      </tbody>
    </table>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Shows — {_DISPLAY_NAME}</title>{_STYLE}</head>
<body>
{_nav("shows")}
<div class="page">
  <h1>Shows</h1>
  <p class="page-sub">Create a show and give it a check-in keyword. Fans text that word to check in at the door.</p>
  {alert_html}
  <div class="two-col" style="align-items:start">
    <div>
      <div class="card">
        <div class="card-title">All Shows</div>
        {table_html}
      </div>
    </div>
    <div>
      <div class="card">
        <div class="card-title">Create a New Show</div>
        <form method="post">
          <div class="form-group">
            <label>Show Name</label>
            <input type="text" name="name" placeholder="e.g. Friday Night Comedy" required>
          </div>
          <div class="form-group">
            <label>Date</label>
            <input type="date" name="show_date" required>
          </div>
          <div class="form-group">
            <label>Check-in Keyword</label>
            <input type="text" name="checkin_keyword" placeholder="e.g. STANDUP or COMEDY24"
                   pattern="[A-Za-z0-9]+" title="Letters only, no spaces" required>
            <div style="font-size:12px;color:#475569;margin-top:4px">
              Fans text this word to check in. Keep it short and memorable.
            </div>
          </div>
          <button type="submit" class="btn btn-primary" style="width:100%">Create Show</button>
        </form>
      </div>
    </div>
  </div>
</div>
</body></html>"""


@smb_portal_bp.route("/portal/west_side_comedy/shows/<int:show_id>")
@_login_required
def portal_show_detail(show_id: int):
    show = _get_show(show_id)
    if not show:
        return "Show not found", 404

    count = _get_show_attendees(show_id)
    status_badge = (
        '<span class="badge badge-green">Active</span>'
        if show["status"] == "active"
        else '<span class="badge badge-gray">Closed</span>'
    )

    blast_url = url_for("wscc_portal.portal_blast") + f"?show_id={show_id}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{show['name']} — {_DISPLAY_NAME}</title>{_STYLE}</head>
<body>
{_nav("shows")}
<div class="page">
  <div style="margin-bottom:8px"><a href="{url_for('wscc_portal.portal_shows')}" style="font-size:13px;color:#64748b">← Back to shows</a></div>
  <h1>{show['name']}</h1>
  <p class="page-sub">{show['show_date']} · {status_badge}</p>

  <div class="stats">
    <div class="stat-card">
      <div class="stat-label">Check-in Keyword</div>
      <div class="stat-value" style="font-size:22px;font-family:monospace">{show['checkin_keyword']}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Check-ins</div>
      <div class="stat-value">{count}</div>
    </div>
  </div>

  <div class="card">
    <div class="card-title">How Check-ins Work</div>
    <p style="color:#94a3b8;font-size:14px;line-height:1.6">
      Fans text <strong style="color:#e2e8f0">{show['checkin_keyword']}</strong> to your WSCC number when they arrive.
      The system automatically records their check-in.<br><br>
      After the show, you can send a thank-you blast to everyone who attended.
    </p>
    <div style="margin-top:16px">
      <a href="{blast_url}" class="btn btn-primary">Send blast to attendees →</a>
    </div>
  </div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Blast
# ---------------------------------------------------------------------------

@smb_portal_bp.route("/portal/west_side_comedy/blast", methods=["GET", "POST"])
@_login_required
def portal_blast():
    total_subs, _ = _get_subscribers()
    shows = _list_shows()
    preselect_show_id = request.args.get("show_id", "")

    success = ""
    error = ""

    if request.method == "POST":
        message_text = request.form.get("message", "").strip()
        audience = request.form.get("audience", "all")

        if not message_text:
            error = "Message cannot be empty."
        elif len(message_text) > 1600:
            error = "Message too long (max 1,600 characters)."
        else:
            phones = []
            audience_label = ""

            if audience == "all":
                phones = _get_all_subscriber_phones()
                audience_label = "All subscribers"
            elif audience.startswith("show:"):
                try:
                    show_id = int(audience.split(":")[1])
                    phones = _get_show_attendee_phones(show_id)
                    show = _get_show(show_id)
                    audience_label = f"Attendees of {show['name']}" if show else "Show attendees"
                except (ValueError, IndexError):
                    error = "Invalid show selection."
            elif audience.startswith("seg:"):
                parts = audience[4:].split(":", 1)
                if len(parts) == 2:
                    phones = _get_segment_phones(parts[0], parts[1])
                    audience_label = f"Segment: {parts[1]}"
                else:
                    error = "Invalid segment."

            if not error:
                if not phones:
                    error = "No recipients in that audience — nobody has subscribed or checked in yet."
                elif not _sms_number():
                    error = "SMS number not configured. Ask your Zarna team to set WEST_SIDE_COMEDY_SMS_NUMBER."
                else:
                    _blast_async(phones, message_text, audience_label)
                    success = f"Blast is sending to {len(phones):,} people! They'll receive it within a few minutes."

    # Build audience cards
    def _card(value, icon, label, count, desc="", checked=False):
        cls = " selected" if checked else ""
        checked_attr = "checked" if checked else ""
        desc_html = f'<div class="aud-desc">{desc}</div>' if desc else ""
        person_label = "person" if count == 1 else "people"
        return (
            f'<label class="audience-card{cls}">'
            f'<input type="radio" name="audience" value="{value}" {checked_attr}>'
            f'<div class="aud-icon">{icon}</div>'
            f'<div class="aud-name">{label}</div>'
            f'<div class="aud-count">{count:,} {person_label}</div>'
            f'{desc_html}'
            f'</label>'
        )

    cards = _card("all", "📱", "Everyone", total_subs, "All your subscribers", checked=(not preselect_show_id))

    for show in shows:
        if show["status"] == "active":
            is_preselected = str(show["id"]) == preselect_show_id
            cards += _card(
                f"show:{show['id']}", "🎭", show["name"],
                show["checkin_count"],
                f"Checked in {show['show_date']}",
                checked=is_preselected,
            )

    alert_html = ""
    if success:
        alert_html = f'<div class="alert alert-success">{success}</div>'
    elif error:
        alert_html = f'<div class="alert alert-error">{error}</div>'

    char_note = "Characters: <span id='cc'>0</span> / 1600"
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Send Blast — {_DISPLAY_NAME}</title>{_STYLE}
<script>
  document.addEventListener('DOMContentLoaded', function() {{
    var ta = document.getElementById('msg');
    var cc = document.getElementById('cc');
    function update() {{ cc.textContent = ta.value.length; }}
    ta.addEventListener('input', update);
    update();
    // Audience card click → check radio
    document.querySelectorAll('.audience-card').forEach(function(card) {{
      card.addEventListener('click', function() {{
        document.querySelectorAll('.audience-card').forEach(function(c) {{ c.classList.remove('selected'); }});
        this.classList.add('selected');
        this.querySelector('input[type=radio]').checked = true;
      }});
    }});
  }});
</script>
</head>
<body>
{_nav("blast")}
<div class="page">
  <h1>Send Blast</h1>
  <p class="page-sub">Send a text message to your audience. It goes out via your WSCC number.</p>
  {alert_html}
  <form method="post">
    <div class="card">
      <div class="card-title">1. Choose your audience</div>
      <div class="audience-grid">{cards}</div>
    </div>
    <div class="card">
      <div class="card-title">2. Write your message</div>
      <div class="form-group">
        <textarea id="msg" name="message" rows="5"
          placeholder="e.g. Hey! Thanks so much for coming out last night — it meant the world to us. Hope to see you again soon! 🎭"></textarea>
        <div style="font-size:12px;color:#475569;margin-top:4px;text-align:right">{char_note}</div>
      </div>
      <button type="submit" class="btn btn-primary">Send blast →</button>
    </div>
  </form>
</div>
</body></html>"""
