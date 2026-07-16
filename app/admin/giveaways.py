"""
Giveaway tab — run weekly giveaway drawings from the dashboard.

Two views, mirroring the Newsletter tab:

  1. Campaign list  — one card per giveaway; collapsed it shows keyword, status,
                      date created, date ended, and # entered. Click to open.
  2. Campaign detail — the people entered into that week's drawing, each linking
                       to their conversation thread (where the inbox star lives).

A campaign has a keyword and an active window. When a fan texts a message
containing the keyword while the campaign is active, app/giveaway/entry.py
records them here (once) and replies with the confirmation message.

Routes registered via register_giveaways_routes(bp) from app/admin/__init__.py.
"""

import csv
import io
import logging
from datetime import datetime, timezone
from urllib.parse import quote

import psycopg2.extras
from flask import Response, redirect, request

from app.admin_auth import check_admin_auth, get_db_connection, require_admin_auth_response

logger = logging.getLogger(__name__)


def _get_db():
    return get_db_connection()


def _esc(s) -> str:
    return (
        str(s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _status_of(starts_at, ends_at) -> str:
    """'scheduled' | 'active' | 'ended' from the campaign window vs now."""
    now = datetime.now(timezone.utc)
    if starts_at and now < starts_at:
        return "scheduled"
    if ends_at and now > ends_at:
        return "ended"
    return "active"


_STATUS_STYLE = {
    "active":    ("#0f2e1f", "#4ade80", "#16653444", "● Active"),
    "scheduled": ("#1e293b", "#93c5fd", "#2563eb44", "◔ Scheduled"),
    "ended":     ("#1a1a2e", "#6b7280", "#37415144", "✓ Ended"),
}


def _status_pill(status: str) -> str:
    bg, fg, border, label = _STATUS_STYLE.get(status, _STATUS_STYLE["ended"])
    return (
        f'<span style="background:{bg};color:{fg};border:1px solid {border};'
        f'border-radius:99px;padding:3px 12px;font-size:12px;font-weight:700">{label}</span>'
    )


def _fmt_dt(dt, with_time: bool = False) -> str:
    if not dt:
        return "—"
    return dt.strftime("%b %-d, %Y %-I:%M %p" if with_time else "%b %-d, %Y")


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _get_campaigns() -> list:
    conn = _get_db()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.id, c.label, c.keyword, c.starts_at, c.ends_at,
                       c.created_at, COUNT(e.id) AS entries
                FROM giveaway_campaigns c
                LEFT JOIN giveaway_entries e ON e.campaign_id = c.id
                GROUP BY c.id
                ORDER BY c.created_at DESC
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        logger.exception("[ADMIN] Failed to load giveaway campaigns")
        return []
    finally:
        conn.close()


def _get_campaign(campaign_id: int) -> dict | None:
    conn = _get_db()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, label, keyword, starts_at, ends_at, confirmation_message, created_at "
                "FROM giveaway_campaigns WHERE id = %s",
                (campaign_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception:
        logger.exception("[ADMIN] Failed to load giveaway campaign")
        return None
    finally:
        conn.close()


def _get_entries(campaign_id: int) -> list:
    conn = _get_db()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT e.id, e.phone_number, e.entered_at, e.source,
                       c.fan_name AS contact_name
                FROM giveaway_entries e
                LEFT JOIN contacts c ON c.phone_number = e.phone_number
                WHERE e.campaign_id = %s
                ORDER BY e.entered_at ASC
            """, (campaign_id,))
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        logger.exception("[ADMIN] Failed to load giveaway entries")
        return []
    finally:
        conn.close()


def _create_campaign(label, keyword, starts_at, ends_at, confirmation) -> bool:
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO giveaway_campaigns "
                    "(label, keyword, starts_at, ends_at, confirmation_message) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (label.strip(), keyword.strip(), starts_at or None,
                     ends_at or None, (confirmation or "").strip()),
                )
        _invalidate()
        return True
    except Exception:
        logger.exception("[ADMIN] Failed to create giveaway campaign")
        return False
    finally:
        conn.close()


def _delete_campaign(campaign_id: int) -> bool:
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM giveaway_campaigns WHERE id = %s", (campaign_id,))
        _invalidate()
        return True
    except Exception:
        logger.exception("[ADMIN] Failed to delete giveaway campaign")
        return False
    finally:
        conn.close()


def _delete_entry(entry_id: int) -> bool:
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM giveaway_entries WHERE id = %s", (entry_id,))
        return True
    except Exception:
        logger.exception("[ADMIN] Failed to delete giveaway entry")
        return False
    finally:
        conn.close()


def _invalidate():
    """Bust the live entry hook's active-campaign cache after list changes."""
    try:
        from app.giveaway.entry import invalidate_cache
        invalidate_cache()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# HTML rendering — entry point
# ---------------------------------------------------------------------------

def render_giveaways_tab() -> str:
    """Router: campaign list by default, campaign detail when ?campaign_id= is set."""
    campaign_id_raw = request.args.get("campaign_id", "")
    try:
        campaign_id = int(campaign_id_raw)
    except (ValueError, TypeError):
        campaign_id = None

    if campaign_id:
        campaign = _get_campaign(campaign_id)
        if campaign:
            return _render_campaign_detail(campaign)
    return _render_campaign_list()


# ---------------------------------------------------------------------------
# View 1 — campaign list
# ---------------------------------------------------------------------------

def _render_campaign_list() -> str:
    campaigns = _get_campaigns()

    if campaigns:
        cards = ""
        for c in campaigns:
            status = _status_of(c["starts_at"], c["ends_at"])
            entries = c["entries"] or 0
            cards += f"""
            <a href="/admin?tab=giveaways&campaign_id={c['id']}"
               style="display:block;text-decoration:none;background:#111827;border:1px solid #1f2937;
                      border-radius:12px;padding:20px 24px;margin-bottom:12px;transition:border-color .15s"
               onmouseover="this.style.borderColor='#0891b2'"
               onmouseout="this.style.borderColor='#1f2937'">
              <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap">
                <div>
                  <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">
                    <span style="font-size:16px;font-weight:700;color:#e2e8f0">🎁 {_esc(c['label'])}</span>
                    {_status_pill(status)}
                  </div>
                  <div style="font-size:12px;color:#64748b">
                    Keyword <span style="background:#0f0f1a;color:#67e8f9;padding:2px 8px;border-radius:4px;
                                         font-weight:700;letter-spacing:.04em">{_esc(c['keyword'])}</span>
                    &nbsp;·&nbsp; Created {_fmt_dt(c['created_at'])}
                    &nbsp;·&nbsp; Ends {_fmt_dt(c['ends_at'])}
                  </div>
                </div>
                <div style="display:flex;align-items:center;gap:14px">
                  <span style="color:#94a3b8;font-size:13px">
                    <strong style="color:#67e8f9;font-size:20px">{entries}</strong> entered
                  </span>
                  <span style="color:#4b5563;font-size:18px">›</span>
                </div>
              </div>
            </a>"""
        list_html = cards
    else:
        list_html = """
        <div style="text-align:center;padding:48px 24px;color:#4b5563">
          <div style="font-size:32px;margin-bottom:12px">🎁</div>
          <div style="font-size:15px;font-weight:600;color:#6b7280;margin-bottom:8px">No giveaways yet</div>
          <div style="font-size:13px;color:#4b5563;max-width:460px;margin:0 auto;line-height:1.6">
            Create your first giveaway below. Pick a keyword and a window — anyone who
            texts that word while it's active gets entered and added as a subscriber.
          </div>
        </div>"""

    return f"""
    <div style="margin-bottom:8px;color:#94a3b8;font-size:13px;line-height:1.6">
      Each giveaway has a keyword and an active window. When a fan texts a message
      containing the keyword while it's live, they're entered once and get your
      confirmation reply. Click a giveaway to see everyone entered.
    </div>

    <!-- Campaign cards -->
    <div style="margin:16px 0 24px">
      {list_html}
    </div>

    <!-- Create new campaign -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:20px 24px">
      <div style="font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:14px">
        Create New Giveaway
      </div>
      <form method="post" action="/admin/giveaways/campaign/new"
            style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
        <div style="display:flex;flex-direction:column;gap:5px;flex:2;min-width:200px">
          <label style="font-size:11px;color:#64748b">Giveaway Label</label>
          <input type="text" name="label" required maxlength="200" placeholder="e.g. Week 1 — July 24"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none">
        </div>
        <div style="display:flex;flex-direction:column;gap:5px;flex:1;min-width:120px">
          <label style="font-size:11px;color:#64748b">Keyword</label>
          <input type="text" name="keyword" required maxlength="60" placeholder="e.g. FREE"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none;text-transform:uppercase">
        </div>
        <div style="display:flex;flex-direction:column;gap:5px;flex:1;min-width:170px">
          <label style="font-size:11px;color:#64748b">Starts</label>
          <input type="datetime-local" name="starts_at"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none">
        </div>
        <div style="display:flex;flex-direction:column;gap:5px;flex:1;min-width:170px">
          <label style="font-size:11px;color:#64748b">Ends</label>
          <input type="datetime-local" name="ends_at"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none">
        </div>
        <div style="display:flex;flex-direction:column;gap:5px;flex:3;min-width:280px">
          <label style="font-size:11px;color:#64748b">Confirmation reply (blank = default)</label>
          <input type="text" name="confirmation_message" maxlength="320"
                 placeholder="🎉 You're in! Winners get a call live every Friday — good luck!"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none">
        </div>
        <button type="submit"
                style="padding:9px 20px;border-radius:6px;font-size:13px;font-weight:600;
                       cursor:pointer;border:none;background:#0891b2;color:#fff">
          Create Giveaway →
        </button>
      </form>
      <div style="font-size:11px;color:#4b5563;margin-top:10px;line-height:1.5">
        Leave Starts/Ends blank for an open-ended window. Matching is
        case-insensitive and "contains", so "FREE", "free!", and "I want free stuff" all count.
      </div>
    </div>
    """


# ---------------------------------------------------------------------------
# View 2 — campaign detail (entrants)
# ---------------------------------------------------------------------------

def _render_campaign_detail(campaign: dict) -> str:
    cid = campaign["id"]
    entries = _get_entries(cid)
    status = _status_of(campaign["starts_at"], campaign["ends_at"])

    if entries:
        rows = ""
        for e in entries:
            rows += _entry_row(e)
        body = f"""
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:14px">
            <thead>
              <tr style="text-align:left;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:.05em">
                <th style="padding:8px 0">Fan</th>
                <th style="padding:8px 0">Phone</th>
                <th style="padding:8px 0">Entered</th>
                <th style="padding:8px 0"></th>
              </tr>
            </thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""
        export_btn = (
            f'<a href="/admin/giveaways/export/{cid}" '
            f'style="font-size:12px;color:#67e8f9;text-decoration:none;border:1px solid #374151;'
            f'padding:5px 12px;border-radius:6px">⬇ Export entries (CSV)</a>'
        )
    else:
        body = """
        <div style="text-align:center;padding:36px 12px;color:#4b5563">
          <div style="font-size:14px;font-weight:600;color:#6b7280;margin-bottom:6px">No entries yet</div>
          <div style="font-size:12px;color:#4b5563;line-height:1.6">
            As fans text the keyword during the active window, they'll appear here.
          </div>
        </div>"""
        export_btn = ""

    window = f"{_fmt_dt(campaign['starts_at'], True)} → {_fmt_dt(campaign['ends_at'], True)}"

    return f"""
    {_detail_js()}

    <!-- Header -->
    <div style="margin-bottom:20px">
      <a href="/admin?tab=giveaways"
         style="color:#67e8f9;text-decoration:none;font-size:13px">← All giveaways</a>
      <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-top:8px">
        <div>
          <div style="display:flex;align-items:center;gap:10px">
            <span style="font-size:20px;font-weight:700;color:#e2e8f0">🎁 {_esc(campaign['label'])}</span>
            {_status_pill(status)}
          </div>
          <div style="font-size:12px;color:#64748b;margin-top:4px">
            Keyword <span style="background:#0f0f1a;color:#67e8f9;padding:2px 8px;border-radius:4px;
                                 font-weight:700">{_esc(campaign['keyword'])}</span>
            &nbsp;·&nbsp; {window}
          </div>
        </div>
        <div style="display:flex;gap:18px;align-items:center">
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:700;color:#67e8f9">{len(entries)}</div>
            <div style="font-size:11px;color:#64748b">entered</div>
          </div>
        </div>
      </div>
    </div>

    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px 20px">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;gap:10px;flex-wrap:wrap">
        <div style="font-size:13px;font-weight:700;color:#67e8f9;text-transform:uppercase;letter-spacing:.06em">
          People Entered
        </div>
        <div style="display:flex;gap:10px;align-items:center">
          {export_btn}
          <button onclick="gwDeleteCampaign({cid})"
                  style="font-size:12px;color:#f87171;background:none;border:1px solid #7f1d1d;
                         padding:5px 12px;border-radius:6px;cursor:pointer">Delete giveaway</button>
        </div>
      </div>
      {body}
    </div>
    """


def _entry_row(e: dict) -> str:
    fan_name = _esc(e["contact_name"] or "")
    phone = e["phone_number"] or ""
    phone_q = quote(phone, safe="")
    entered = e["entered_at"].strftime("%b %-d, %-I:%M %p") if e["entered_at"] else ""
    name_html = (
        f'<strong style="color:#e2e8f0">{fan_name}</strong>'
        if fan_name else '<span style="color:#4b5563;font-style:italic">unknown</span>'
    )
    return f"""
    <tr id="gw-entry-row-{e['id']}" style="border-bottom:1px solid #1a1a2e">
      <td style="padding:12px 8px 12px 0">{name_html}</td>
      <td style="padding:12px 0;color:#94a3b8;font-family:monospace;font-size:13px">{_esc(phone)}</td>
      <td style="padding:12px 0;color:#94a3b8;font-size:13px">{entered}</td>
      <td style="padding:12px 0;text-align:right;white-space:nowrap">
        <a href="/admin?tab=convos&thread={phone_q}"
           style="font-size:12px;color:#67e8f9;text-decoration:none;margin-right:14px">open inbox →</a>
        <button onclick="gwRemoveEntry({e['id']})"
                style="font-size:11px;padding:0;border:none;background:none;color:#6b7280;cursor:pointer">remove ✕</button>
      </td>
    </tr>"""


def _detail_js() -> str:
    return """
    <script>
    function gwRemoveEntry(id) {
      if (!confirm('Remove this entry?')) return;
      fetch('/admin/giveaways/entry/' + id + '/delete', {method: 'POST'})
        .then(r => r.json())
        .then(d => { if (d.ok) { var el = document.getElementById('gw-entry-row-' + id); if (el) el.remove(); } else alert('Failed'); });
    }
    function gwDeleteCampaign(id) {
      if (!confirm('Delete this entire giveaway and all its entries? This cannot be undone.')) return;
      fetch('/admin/giveaways/campaign/' + id + '/delete', {method: 'POST'})
        .then(r => r.json())
        .then(d => { if (d.ok) location.href = '/admin?tab=giveaways'; else alert('Failed'); });
    }
    </script>"""


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

def register_giveaways_routes(bp):
    from flask import jsonify

    @bp.route("/admin/giveaways/campaign/new", methods=["POST"])
    def giveaways_campaign_new():
        if not check_admin_auth():
            return require_admin_auth_response()
        label = request.form.get("label", "").strip()
        keyword = request.form.get("keyword", "").strip()
        starts_at = request.form.get("starts_at", "").strip()
        ends_at = request.form.get("ends_at", "").strip()
        confirmation = request.form.get("confirmation_message", "").strip()
        if label and keyword:
            _create_campaign(label, keyword, starts_at or None, ends_at or None, confirmation)
        return redirect("/admin?tab=giveaways")

    @bp.route("/admin/giveaways/campaign/<int:campaign_id>/delete", methods=["POST"])
    def giveaways_campaign_delete(campaign_id):
        if not check_admin_auth():
            return require_admin_auth_response()
        ok = _delete_campaign(campaign_id)
        return jsonify({"ok": ok})

    @bp.route("/admin/giveaways/entry/<int:entry_id>/delete", methods=["POST"])
    def giveaways_entry_delete(entry_id):
        if not check_admin_auth():
            return require_admin_auth_response()
        ok = _delete_entry(entry_id)
        return jsonify({"ok": ok})

    @bp.route("/admin/giveaways/export/<int:campaign_id>")
    def giveaways_export(campaign_id):
        if not check_admin_auth():
            return require_admin_auth_response()
        rows = _get_entries(campaign_id)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["entered_at", "fan_name", "phone_number", "source"])
        for r in rows:
            name = r["contact_name"] or ""
            date_str = r["entered_at"].strftime("%Y-%m-%d %H:%M") if r["entered_at"] else ""
            w.writerow([date_str, name, r["phone_number"], r["source"] or ""])
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=giveaway_entries_{campaign_id}.csv"},
        )
