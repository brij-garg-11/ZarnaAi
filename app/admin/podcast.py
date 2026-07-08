"""
Podcast Q&A Submissions tab — track fan questions submitted for shout-outs.

Fans text in a question + their name as part of a marketing campaign. This tab
has two views:

  1. Campaign list  — click a campaign card to open it
  2. Campaign detail — two sections:
       • "To Review"  — incoming submissions; check off the real ones
       • "Confirmed"  — the ones you checked off, ready for the podcast

Routes registered via register_podcast_routes(bp) from app/admin/__init__.py.
"""

import csv
import io
import logging
from urllib.parse import quote

import psycopg2.extras
from flask import Response, redirect, request

from app.admin_auth import check_admin_auth, get_db_connection, require_admin_auth_response

logger = logging.getLogger(__name__)

# Status model:
#   new       — incoming, not yet reviewed  → shows in "To Review"
#   confirmed — a real submission you picked → shows in "Confirmed"
#   answered  — confirmed AND already answered on the podcast → "Confirmed" (dimmed)
#   skip      — dismissed as not a real submission → hidden
_STATUSES = ("new", "confirmed", "answered", "skip")
_REVIEW_STATUSES = ("new",)
_CONFIRMED_STATUSES = ("confirmed", "answered")


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
                SELECT c.id, c.label, c.promoted_at, c.created_at,
                       COUNT(s.id) FILTER (WHERE s.status = 'new')                    AS to_review,
                       COUNT(s.id) FILTER (WHERE s.status IN ('confirmed','answered')) AS confirmed,
                       COUNT(s.id) FILTER (WHERE s.status = 'answered')               AS answered,
                       COUNT(s.id) FILTER (WHERE s.status != 'skip')                  AS total
                FROM podcast_campaigns c
                LEFT JOIN podcast_submissions s ON s.campaign_id = c.id
                GROUP BY c.id
                ORDER BY c.created_at DESC
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        logger.exception("[ADMIN] Failed to load podcast campaigns")
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
                "SELECT id, label, promoted_at, created_at FROM podcast_campaigns WHERE id = %s",
                (campaign_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception:
        logger.exception("[ADMIN] Failed to load podcast campaign")
        return None
    finally:
        conn.close()


def _get_submissions(campaign_id: int, statuses: tuple) -> list:
    conn = _get_db()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT s.id, s.phone_number, s.message_id, s.question,
                       s.fan_name, s.status, s.created_at,
                       c.fan_name AS contact_name
                FROM podcast_submissions s
                LEFT JOIN contacts c ON c.phone_number = s.phone_number
                WHERE s.campaign_id = %s
                  AND s.status = ANY(%s)
                ORDER BY s.created_at ASC
            """, (campaign_id, list(statuses)))
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        logger.exception("[ADMIN] Failed to load podcast submissions")
        return []
    finally:
        conn.close()


def _create_campaign(label: str, promoted_at) -> bool:
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO podcast_campaigns (label, promoted_at) VALUES (%s, %s)",
                    (label.strip(), promoted_at or None),
                )
        return True
    except Exception:
        logger.exception("[ADMIN] Failed to create podcast campaign")
        return False
    finally:
        conn.close()


def _update_status(submission_id: int, status: str) -> bool:
    if status not in _STATUSES:
        return False
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE podcast_submissions SET status = %s WHERE id = %s",
                    (status, submission_id),
                )
        return True
    except Exception:
        logger.exception("[ADMIN] Failed to update submission status")
        return False
    finally:
        conn.close()


def _delete_submission(submission_id: int) -> bool:
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM podcast_submissions WHERE id = %s", (submission_id,))
        return True
    except Exception:
        logger.exception("[ADMIN] Failed to delete submission")
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTML rendering — entry point
# ---------------------------------------------------------------------------

def render_podcast_tab() -> str:
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
            to_review = c["to_review"] or 0
            confirmed = c["confirmed"] or 0
            promo = f"Promoted {c['promoted_at']}" if c["promoted_at"] else "No promotion date set"
            review_badge = (
                f'<span style="background:#1e3a5f;color:#60a5fa;border:1px solid #2563eb44;'
                f'border-radius:99px;padding:3px 12px;font-size:12px;font-weight:700">'
                f'{to_review} to review</span>'
                if to_review else
                '<span style="color:#4b5563;font-size:12px">Nothing to review</span>'
            )
            cards += f"""
            <a href="/admin?tab=podcast&campaign_id={c['id']}"
               style="display:block;text-decoration:none;background:#111827;border:1px solid #1f2937;
                      border-radius:12px;padding:20px 24px;margin-bottom:12px;transition:border-color .15s"
               onmouseover="this.style.borderColor='#7c3aed'"
               onmouseout="this.style.borderColor='#1f2937'">
              <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap">
                <div>
                  <div style="font-size:16px;font-weight:700;color:#e2e8f0;margin-bottom:4px">🎙 {_esc(c['label'])}</div>
                  <div style="font-size:12px;color:#64748b">{promo}</div>
                </div>
                <div style="display:flex;align-items:center;gap:14px">
                  {review_badge}
                  <span style="color:#94a3b8;font-size:13px">
                    <strong style="color:#c4b5fd">{confirmed}</strong> confirmed
                  </span>
                  <span style="color:#4b5563;font-size:18px">›</span>
                </div>
              </div>
            </a>"""
        list_html = cards
    else:
        list_html = """
        <div style="text-align:center;padding:48px 24px;color:#4b5563">
          <div style="font-size:32px;margin-bottom:12px">🎙</div>
          <div style="font-size:15px;font-weight:600;color:#6b7280;margin-bottom:8px">No campaigns yet</div>
          <div style="font-size:13px;color:#4b5563;max-width:460px;margin:0 auto;line-height:1.6">
            Create your first campaign below — one per podcast episode you promote Q&amp;A submissions for.
          </div>
        </div>"""

    return f"""
    <div style="margin-bottom:8px;color:#94a3b8;font-size:13px;line-height:1.6">
      Each campaign is one podcast episode's worth of fan question submissions.
      Click a campaign to review submissions and pick the real ones.
    </div>

    <!-- Campaign cards -->
    <div style="margin:16px 0 24px">
      {list_html}
    </div>

    <!-- Create new campaign -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:20px 24px">
      <div style="font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:14px">
        Create New Campaign
      </div>
      <form method="post" action="/admin/podcast/campaign/new"
            style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
        <div style="display:flex;flex-direction:column;gap:5px;flex:2;min-width:200px">
          <label style="font-size:11px;color:#64748b">Campaign Label</label>
          <input type="text" name="label" required maxlength="200" placeholder="e.g. Episode 42 — July 2026"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none">
        </div>
        <div style="display:flex;flex-direction:column;gap:5px;flex:1;min-width:140px">
          <label style="font-size:11px;color:#64748b">Promoted Date (when you told fans to text in)</label>
          <input type="date" name="promoted_at"
                 style="background:#0f0f1a;border:1px solid #374151;border-radius:6px;color:#e2e8f0;
                        padding:9px 12px;font-size:14px;outline:none">
        </div>
        <button type="submit"
                style="padding:9px 20px;border-radius:6px;font-size:13px;font-weight:600;
                       cursor:pointer;border:none;background:#7c3aed;color:#fff">
          Create Campaign →
        </button>
      </form>
    </div>
    """


# ---------------------------------------------------------------------------
# View 2 — campaign detail (To Review + Confirmed sections)
# ---------------------------------------------------------------------------

def _render_campaign_detail(campaign: dict) -> str:
    cid = campaign["id"]
    to_review = _get_submissions(cid, _REVIEW_STATUSES)
    confirmed = _get_submissions(cid, _CONFIRMED_STATUSES)

    promo = f"Promoted {campaign['promoted_at']}" if campaign["promoted_at"] else ""

    # ── "To Review" section ────────────────────────────────────────────
    if to_review:
        review_rows = ""
        for sub in to_review:
            review_rows += _review_row(sub)
        review_body = f"""
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:14px">
            <tbody>{review_rows}</tbody>
          </table>
        </div>"""
    else:
        review_body = _mini_empty(
            "Nothing to review.",
            f"Run the detection script to pull in submissions:<br>"
            f"<code style='background:#0f0f1a;padding:3px 8px;border-radius:4px;font-size:12px'>"
            f"python scripts/detect_podcast_submissions.py --campaign-id {cid} --since YYYY-MM-DD</code>",
        )

    # ── "Confirmed" section ────────────────────────────────────────────
    if confirmed:
        conf_rows = ""
        for sub in confirmed:
            conf_rows += _confirmed_row(sub)
        confirmed_body = f"""
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:14px">
            <tbody>{conf_rows}</tbody>
          </table>
        </div>"""
        export_btn = (
            f'<a href="/admin/podcast/export/{cid}" '
            f'style="font-size:12px;color:#a5b4fc;text-decoration:none;border:1px solid #374151;'
            f'padding:5px 12px;border-radius:6px">⬇ Export confirmed (CSV)</a>'
        )
    else:
        confirmed_body = _mini_empty(
            "No confirmed questions yet.",
            "Check off the real submissions on the left and they'll appear here.",
        )
        export_btn = ""

    return f"""
    {_detail_js()}

    <!-- Header -->
    <div style="margin-bottom:20px">
      <a href="/admin?tab=podcast"
         style="color:#818cf8;text-decoration:none;font-size:13px">← All campaigns</a>
      <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-top:8px">
        <div>
          <div style="font-size:20px;font-weight:700;color:#e2e8f0">🎙 {_esc(campaign['label'])}</div>
          <div style="font-size:12px;color:#64748b;margin-top:2px">{promo}</div>
        </div>
        <div style="display:flex;gap:18px;align-items:center">
          <div style="text-align:center">
            <div style="font-size:20px;font-weight:700;color:#60a5fa">{len(to_review)}</div>
            <div style="font-size:11px;color:#64748b">to review</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:20px;font-weight:700;color:#c4b5fd">{len(confirmed)}</div>
            <div style="font-size:11px;color:#64748b">confirmed</div>
          </div>
        </div>
      </div>
    </div>

    <!-- Two-column layout: To Review | Confirmed -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start">

      <!-- To Review -->
      <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px 20px">
        <div style="font-size:13px;font-weight:700;color:#60a5fa;text-transform:uppercase;
                    letter-spacing:.06em;margin-bottom:6px">📥 To Review</div>
        <div style="font-size:12px;color:#64748b;margin-bottom:14px">
          Tick the box on real question submissions — they move to Confirmed →
        </div>
        {review_body}
      </div>

      <!-- Confirmed -->
      <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px 20px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;gap:10px;flex-wrap:wrap">
          <div style="font-size:13px;font-weight:700;color:#c4b5fd;text-transform:uppercase;letter-spacing:.06em">
            ⭐ Confirmed
          </div>
          {export_btn}
        </div>
        <div style="font-size:12px;color:#64748b;margin-bottom:14px">
          Your real questions for this episode. Mark them answered after recording.
        </div>
        {confirmed_body}
      </div>

    </div>

    <style>
      @media (max-width: 900px) {{
        #tab-podcast [style*="grid-template-columns:1fr 1fr"] {{ grid-template-columns: 1fr !important; }}
      }}
    </style>
    """


def _review_row(sub: dict) -> str:
    fan_name = _esc(sub["fan_name"] or sub["contact_name"] or "")
    question = _esc(sub["question"])
    phone_q = quote(sub["phone_number"], safe="")
    last4 = _esc(sub["phone_number"][-4:])
    sub_date = sub["created_at"].strftime("%b %d") if sub["created_at"] else ""
    name_html = (
        f'<strong style="color:#e2e8f0">{fan_name}</strong>'
        if fan_name else '<span style="color:#4b5563;font-style:italic">unknown</span>'
    )
    return f"""
    <tr id="sub-row-{sub['id']}" style="border-bottom:1px solid #1a1a2e">
      <td style="padding:12px 8px 12px 0;vertical-align:top;width:28px">
        <input type="checkbox" onchange="confirmSub({sub['id']})"
               style="width:18px;height:18px;cursor:pointer;accent-color:#7c3aed"
               title="Mark as a real submission">
      </td>
      <td style="padding:12px 0;vertical-align:top">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
          {name_html}
          <span style="font-size:11px;color:#4b5563">·{last4} · {sub_date}</span>
        </div>
        <div style="color:#cbd5e1;line-height:1.5;font-size:13px">{question}</div>
        <div style="margin-top:6px;display:flex;gap:12px">
          <a href="/admin?tab=convos&thread={phone_q}" target="_blank"
             style="font-size:11px;color:#818cf8;text-decoration:none">view thread</a>
          <button onclick="skipSub({sub['id']})"
                  style="font-size:11px;padding:0;border:none;background:none;color:#6b7280;cursor:pointer">
            not a submission ✕
          </button>
        </div>
      </td>
    </tr>"""


def _confirmed_row(sub: dict) -> str:
    fan_name = _esc(sub["fan_name"] or sub["contact_name"] or "")
    question = _esc(sub["question"])
    phone_q = quote(sub["phone_number"], safe="")
    last4 = _esc(sub["phone_number"][-4:])
    answered = sub["status"] == "answered"
    name_html = (
        f'<strong style="color:#e2e8f0">{fan_name}</strong>'
        if fan_name else '<span style="color:#4b5563;font-style:italic">unknown</span>'
    )
    dim = "opacity:.55;" if answered else ""
    answered_badge = (
        '<span style="background:#1a2e2e;color:#5eead4;border:1px solid #0f766e44;'
        'border-radius:99px;padding:2px 9px;font-size:10px;font-weight:700">ANSWERED ✓</span>'
        if answered else ""
    )
    answer_toggle = (
        f'<button onclick="setStatus({sub["id"]},\'confirmed\')" '
        f'style="font-size:11px;padding:0;border:none;background:none;color:#5eead4;cursor:pointer">undo answered</button>'
        if answered else
        f'<button onclick="setStatus({sub["id"]},\'answered\')" '
        f'style="font-size:11px;padding:0;border:none;background:none;color:#818cf8;cursor:pointer">mark answered ✓</button>'
    )
    return f"""
    <tr id="sub-row-{sub['id']}" style="border-bottom:1px solid #1a1a2e;{dim}">
      <td style="padding:12px 0;vertical-align:top">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
          {name_html}
          <span style="font-size:11px;color:#4b5563">·{last4}</span>
          {answered_badge}
        </div>
        <div style="color:#cbd5e1;line-height:1.5;font-size:13px">{question}</div>
        <div style="margin-top:6px;display:flex;gap:12px;flex-wrap:wrap">
          <a href="/admin?tab=convos&thread={phone_q}" target="_blank"
             style="font-size:11px;color:#818cf8;text-decoration:none">view thread</a>
          {answer_toggle}
          <button onclick="setStatus({sub['id']},'new')"
                  style="font-size:11px;padding:0;border:none;background:none;color:#6b7280;cursor:pointer">
            ← back to review
          </button>
        </div>
      </td>
    </tr>"""


def _mini_empty(title: str, body: str) -> str:
    return f"""
    <div style="text-align:center;padding:28px 12px;color:#4b5563">
      <div style="font-size:14px;font-weight:600;color:#6b7280;margin-bottom:6px">{title}</div>
      <div style="font-size:12px;color:#4b5563;line-height:1.6">{body}</div>
    </div>"""


def _detail_js() -> str:
    return """
    <script>
    function _post(url, body) {
      return fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: body ? JSON.stringify(body) : null
      }).then(r => r.json());
    }
    function confirmSub(id) {
      _post('/admin/podcast/submission/' + id + '/status', {status: 'confirmed'})
        .then(d => { if (d.ok) location.reload(); else alert('Failed'); });
    }
    function skipSub(id) {
      _post('/admin/podcast/submission/' + id + '/status', {status: 'skip'})
        .then(d => { if (d.ok) { var el = document.getElementById('sub-row-' + id); if (el) el.remove(); } else alert('Failed'); });
    }
    function setStatus(id, status) {
      _post('/admin/podcast/submission/' + id + '/status', {status: status})
        .then(d => { if (d.ok) location.reload(); else alert('Failed'); });
    }
    </script>"""


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

def register_podcast_routes(bp):
    from flask import jsonify

    @bp.route("/admin/podcast/campaign/new", methods=["POST"])
    def podcast_campaign_new():
        if not check_admin_auth():
            return require_admin_auth_response()
        label = request.form.get("label", "").strip()
        promoted_at = request.form.get("promoted_at", "").strip()
        if label:
            _create_campaign(label, promoted_at or None)
        return redirect("/admin?tab=podcast")

    @bp.route("/admin/podcast/submission/<int:sub_id>/status", methods=["POST"])
    def podcast_submission_status(sub_id):
        if not check_admin_auth():
            return require_admin_auth_response()
        data = request.get_json(force=True, silent=True) or {}
        ok = _update_status(sub_id, data.get("status", ""))
        return jsonify({"ok": ok})

    @bp.route("/admin/podcast/submission/<int:sub_id>/delete", methods=["POST"])
    def podcast_submission_delete(sub_id):
        if not check_admin_auth():
            return require_admin_auth_response()
        ok = _delete_submission(sub_id)
        return jsonify({"ok": ok})

    @bp.route("/admin/podcast/export/<int:campaign_id>")
    def podcast_export(campaign_id):
        if not check_admin_auth():
            return require_admin_auth_response()
        rows = _get_submissions(campaign_id, _CONFIRMED_STATUSES)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["date", "fan_name", "phone_last4", "question", "status"])
        for r in rows:
            name = r["fan_name"] or r["contact_name"] or ""
            date_str = r["created_at"].strftime("%Y-%m-%d") if r["created_at"] else ""
            w.writerow([date_str, name, r["phone_number"][-4:], r["question"], r["status"]])
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=podcast_confirmed_{campaign_id}.csv"},
        )
