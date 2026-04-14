"""
SMB client detail page — per-tenant deep-dive view.

Accessible at /admin/smb/<slug> — linked from the SMB Clients tab.
Shows: subscriber overview, outreach campaign stats, segment breakdown,
subscriber table (click any row to view full conversation), and blast history.

Registered via register_smb_routes() in app/admin/smb.py.
"""

import logging
from datetime import timezone

from app.admin_auth import check_admin_auth, get_db_connection, require_admin_auth_response

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _esc(s) -> str:
    return (
        str(s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _fmt_dt(dt) -> str:
    if not dt:
        return "—"
    if hasattr(dt, "strftime"):
        return dt.strftime("%b %d, %Y")
    return str(dt)[:10]


def _fmt_time(dt) -> str:
    if not dt:
        return ""
    if hasattr(dt, "strftime"):
        return dt.astimezone(timezone.utc).strftime("%b %d %H:%M UTC")
    return str(dt)[:16]


def _mask(phone: str) -> str:
    if len(phone) >= 10:
        return f"({phone[2:5]}) ***-{phone[-4:]}"
    return phone


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _fetch_detail(slug: str) -> dict:
    """Fetch all data needed to render the client detail page."""
    from app.smb.tenants import get_registry
    from app.smb import storage as smb_storage

    registry = get_registry()
    tenant = registry.get_by_slug(slug)
    if not tenant:
        return {"error": f"No tenant found with slug '{slug}'"}

    conn = get_db_connection()
    if not conn:
        return {"error": "Database unavailable"}

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # All active subscribers with their preferences joined
            cur.execute("""
                SELECT
                    s.id,
                    s.phone_number,
                    s.onboarding_step,
                    s.status,
                    s.created_at,
                    p0.answer  AS comedy_type,
                    pi.answer  AS interest_text
                FROM smb_subscribers s
                LEFT JOIN smb_preferences p0
                    ON p0.subscriber_id = s.id AND p0.question_key = '0'
                LEFT JOIN smb_preferences pi
                    ON pi.subscriber_id = s.id AND pi.question_key = 'interest'
                WHERE s.tenant_slug = %s AND s.status = 'active'
                ORDER BY s.created_at DESC
            """, (slug,))
            subscribers = [dict(r) for r in cur.fetchall()]

            # Blast history for this tenant
            cur.execute("""
                SELECT owner_message, body, attempted, succeeded, segment, sent_at
                FROM smb_blasts
                WHERE tenant_slug = %s
                ORDER BY sent_at DESC
                LIMIT 30
            """, (slug,))
            blasts = [dict(r) for r in cur.fetchall()]

            # Link click counts for this tenant
            cur.execute("""
                SELECT link_key, COUNT(*) AS clicks
                FROM smb_link_clicks
                WHERE tenant_slug = %s
                GROUP BY link_key
                ORDER BY clicks DESC
            """, (slug,))
            link_clicks = [dict(r) for r in cur.fetchall()]

            # Outreach campaign stats (totals)
            cur.execute("""
                SELECT
                    COUNT(*)                               AS invites_sent,
                    COUNT(claimed_at)                      AS tickets_claimed
                FROM smb_outreach_invites
                WHERE tenant_slug = %s
            """, (slug,))
            outreach_row = dict(cur.fetchone() or {})

            # How many of those invited actually subscribed (campaign opt-ins)
            cur.execute("""
                SELECT COUNT(DISTINCT s.phone_number) AS opted_in
                FROM smb_subscribers s
                JOIN smb_outreach_invites o
                    ON o.phone_number = s.phone_number AND o.tenant_slug = s.tenant_slug
                WHERE s.tenant_slug = %s AND s.status = 'active'
            """, (slug,))
            opted_in_row = cur.fetchone()
            opted_in = (opted_in_row["opted_in"] if opted_in_row else 0) or 0

            # Per-batch stats
            cur.execute("""
                SELECT
                    COALESCE(batch_name, '(unlabelled)') AS batch,
                    MIN(sent_at)                         AS first_sent,
                    COUNT(*)                             AS invites,
                    COUNT(claimed_at)                    AS claimed
                FROM smb_outreach_invites
                WHERE tenant_slug = %s
                GROUP BY COALESCE(batch_name, '(unlabelled)')
                ORDER BY MIN(sent_at) DESC
            """, (slug,))
            batch_rows = [dict(r) for r in cur.fetchall()]

            # Last 50 claimed tickets
            cur.execute("""
                SELECT phone_number, ticket_number, claimed_at, batch_name
                FROM smb_outreach_invites
                WHERE tenant_slug = %s AND claimed_at IS NOT NULL AND ticket_number IS NOT NULL
                ORDER BY ticket_number ASC
                LIMIT 50
            """, (slug,))
            ticket_log = [dict(r) for r in cur.fetchall()]

        outreach_stats = {
            "invites_sent": outreach_row.get("invites_sent", 0),
            "opted_in": opted_in,
            "tickets_claimed": outreach_row.get("tickets_claimed", 0),
            "batch_rows": batch_rows,
            "ticket_log": ticket_log,
        }

        # Segment counts
        total = len(subscribers)
        seg_counts = []
        for seg in tenant.segments:
            with conn.cursor() as cur2:
                placeholders = ",".join(["%s"] * len(seg["answers"]))
                cur2.execute(
                    f"""
                    SELECT COUNT(DISTINCT s.id)
                    FROM smb_subscribers s
                    JOIN smb_preferences p ON p.subscriber_id = s.id
                    WHERE s.tenant_slug = %s
                      AND s.status = 'active'
                      AND p.question_key = %s
                      AND LOWER(p.answer) IN ({placeholders})
                    """,
                    (slug, seg["question_key"], *[a.lower() for a in seg["answers"]]),
                )
                count = cur2.fetchone()[0]
            pct = round((count / total) * 100) if total else 0
            seg_counts.append({
                "name": seg["name"],
                "description": seg.get("description", ""),
                "question_key": seg["question_key"],
                "count": count,
                "pct": pct,
            })

        return {
            "tenant": tenant,
            "subscribers": subscribers,
            "blasts": blasts,
            "link_clicks": link_clicks,
            "seg_counts": seg_counts,
            "outreach": outreach_stats,
            "total": total,
        }

    except Exception:
        logger.exception("SMB detail: failed to fetch data for slug=%s", slug)
        return {"error": "Failed to load data — check logs"}
    finally:
        conn.close()


def _fetch_conversation(slug: str, phone: str) -> list:
    """Return full conversation history for one subscriber, oldest-first."""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT role, body, created_at
                FROM smb_messages
                WHERE tenant_slug = %s AND phone_number = %s
                ORDER BY created_at ASC
            """, (slug, phone))
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        logger.exception("SMB detail: failed to fetch conversation slug=%s phone=...%s", slug, phone[-4:])
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_SEG_COLORS = ["#a78bfa", "#34d399", "#fbbf24", "#f87171", "#60a5fa", "#fb923c"]

_PAGE_STYLE = """
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #0d1117;
  color: #f3f4f6;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  padding: 32px 24px;
  max-width: 1100px;
  margin: 0 auto;
}
.section {
  background: #111827;
  border: 1px solid #1f2937;
  border-radius: 12px;
  padding: 22px 24px;
  margin-bottom: 20px;
}
.section-title {
  font-size: 13px;
  font-weight: 700;
  color: #6b7280;
  margin-bottom: 16px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
a { color: #a78bfa; text-decoration: none; }
a:hover { text-decoration: underline; }
code {
  background: #1f2937;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 12px;
  color: #94a3b8;
}
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th {
  text-align: left; padding: 8px 12px 10px; font-size: 11px; font-weight: 700;
  color: #4b5563; border-bottom: 1px solid #1f2937; text-transform: uppercase;
  letter-spacing: 0.05em;
}
td { padding: 11px 12px; border-bottom: 1px solid #111827; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tr.sub-row:hover td { background: #131f2e; cursor: pointer; }

/* Conversation panel */
#conv-overlay {
  display: none; position: fixed; inset: 0;
  background: rgba(0,0,0,.65); z-index: 100;
}
#conv-panel {
  position: fixed; top: 0; right: -480px; width: 460px; height: 100vh;
  background: #111827; border-left: 1px solid #1f2937;
  z-index: 101; transition: right .25s ease;
  display: flex; flex-direction: column;
}
#conv-panel.open { right: 0; }
#conv-header {
  padding: 18px 20px; border-bottom: 1px solid #1f2937;
  display: flex; justify-content: space-between; align-items: center; flex-shrink: 0;
}
#conv-header h2 { font-size: 15px; font-weight: 700; color: #f3f4f6; }
#conv-close {
  background: none; border: none; color: #6b7280; font-size: 20px;
  cursor: pointer; line-height: 1; padding: 0 4px;
}
#conv-close:hover { color: #f3f4f6; }
#conv-body { flex: 1; overflow-y: auto; padding: 16px 20px; }
.bubble-wrap { display: flex; flex-direction: column; margin-bottom: 10px; }
.bubble-wrap.user { align-items: flex-start; }
.bubble-wrap.bot  { align-items: flex-end; }
.bubble {
  max-width: 80%; padding: 9px 13px; border-radius: 12px;
  font-size: 13px; line-height: 1.45; color: #e2e8f0;
}
.bubble-wrap.user .bubble { background: #1e293b; border-bottom-left-radius: 3px; }
.bubble-wrap.bot  .bubble { background: #312e81; border-bottom-right-radius: 3px; }
.bubble-time { font-size: 10px; color: #374151; margin-top: 3px; }
.bubble-sender { font-size: 10px; color: #4b5563; margin-bottom: 2px; }

/* Stat mini-cards */
.mini-stats { display: grid; gap: 12px; }
.mini-stat {
  background: #0f172a; border: 1px solid #1f2937; border-radius: 10px;
  padding: 14px 16px; text-align: center;
}
.mini-stat-num { font-size: 26px; font-weight: 800; }
.mini-stat-lbl { font-size: 11px; color: #6b7280; margin-top: 3px; }

/* Blast cards */
.blast-card {
  background: #0f172a; border: 1px solid #1f2937; border-radius: 10px;
  padding: 14px 16px; margin-bottom: 10px;
}
.blast-card:last-child { margin-bottom: 0; }
.blast-meta { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }
.blast-msg { font-size: 13px; color: #d1d5db; line-height: 1.5; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
</style>
"""


def render_client_detail(slug: str) -> str:
    data = _fetch_detail(slug)

    if "error" in data:
        return f"""<!doctype html><html><head><title>SMB Detail</title>
        <style>body{{background:#0d1117;color:#f3f4f6;font-family:system-ui;padding:40px}}</style>
        </head><body>
        <a href="/admin?tab=smb" style="color:#a78bfa;text-decoration:none">← SMB Clients</a>
        <div style="margin-top:24px;color:#f87171;font-size:15px">{_esc(data['error'])}</div>
        </body></html>"""

    tenant      = data["tenant"]
    subscribers = data["subscribers"]
    blasts      = data["blasts"]
    link_clicks = data["link_clicks"]
    seg_counts  = data["seg_counts"]
    outreach    = data["outreach"]
    total       = data["total"]

    pref_answered = sum(1 for s in subscribers if s["comedy_type"])
    pref_pct      = round((pref_answered / total) * 100) if total else 0

    # ── Config badges ──
    def badge(ok, label_ok, label_bad):
        if ok:
            return f'<span style="color:#4ade80;font-size:12px">✓ {label_ok}</span>'
        return f'<span style="color:#f87171;font-size:12px">⚠ {label_bad}</span>'

    config_badges = " &nbsp;·&nbsp; ".join([
        badge(tenant.sms_number, f"SMS {tenant.sms_number[-4:] if tenant.sms_number else ''}", "no SMS number"),
        badge(tenant.owner_phone, "owner set", "no owner phone"),
        badge(tenant.keyword, f"keyword: {tenant.keyword}", "no keyword"),
    ])

    # ── Top stat cards ──
    total_clicks = sum(r["clicks"] for r in link_clicks)
    stats_html = f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px">
      <div class="mini-stat">
        <div class="mini-stat-num" style="color:#f3f4f6">{total}</div>
        <div class="mini-stat-lbl">Active subscribers</div>
      </div>
      <div class="mini-stat">
        <div class="mini-stat-num" style="color:#a78bfa">{pref_answered}</div>
        <div class="mini-stat-lbl">Preference answered ({pref_pct}%)</div>
      </div>
      <div class="mini-stat">
        <div class="mini-stat-num" style="color:#34d399">{len(blasts)}</div>
        <div class="mini-stat-lbl">Blasts sent</div>
      </div>
      <div class="mini-stat">
        <div class="mini-stat-num" style="color:#fbbf24">{total_clicks}</div>
        <div class="mini-stat-lbl">Link clicks</div>
      </div>
    </div>"""

    # ── Outreach campaign section ──
    invites_sent    = outreach["invites_sent"]
    opted_in        = outreach["opted_in"]
    tickets_claimed = outreach["tickets_claimed"]
    batch_rows      = outreach.get("batch_rows", [])
    ticket_log      = outreach.get("ticket_log", [])
    reply_rate      = round((opted_in / invites_sent) * 100) if invites_sent else 0

    if invites_sent > 0:
        # Summary stats
        outreach_html = f"""
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
          <div class="mini-stat">
            <div class="mini-stat-num" style="color:#60a5fa">{invites_sent}</div>
            <div class="mini-stat-lbl">Invites sent</div>
          </div>
          <div class="mini-stat">
            <div class="mini-stat-num" style="color:#4ade80">{opted_in}</div>
            <div class="mini-stat-lbl">Signed up</div>
          </div>
          <div class="mini-stat">
            <div class="mini-stat-num" style="color:#fbbf24">{reply_rate}%</div>
            <div class="mini-stat-lbl">Reply rate</div>
          </div>
          <div class="mini-stat">
            <div class="mini-stat-num" style="color:#fb923c">{tickets_claimed}</div>
            <div class="mini-stat-lbl">Free tickets claimed</div>
          </div>
        </div>"""

        # Per-batch table
        if batch_rows:
            batch_trs = "".join(
                f"""<tr>
                  <td style="padding:6px 10px;color:#e5e7eb">{_esc(str(b["batch"]))}</td>
                  <td style="padding:6px 10px;color:#9ca3af">{str(b["first_sent"])[:10] if b["first_sent"] else "—"}</td>
                  <td style="padding:6px 10px;color:#60a5fa;text-align:right">{b["invites"]}</td>
                  <td style="padding:6px 10px;color:#fb923c;text-align:right">{b["claimed"]}</td>
                </tr>"""
                for b in batch_rows
            )
            outreach_html += f"""
        <div style="margin-top:4px">
          <div style="font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;
                      letter-spacing:.05em;margin-bottom:6px">Blast batches</div>
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead>
              <tr style="border-bottom:1px solid #1f2937">
                <th style="padding:5px 10px;text-align:left;color:#6b7280;font-weight:500">Batch</th>
                <th style="padding:5px 10px;text-align:left;color:#6b7280;font-weight:500">Date</th>
                <th style="padding:5px 10px;text-align:right;color:#6b7280;font-weight:500">Sent</th>
                <th style="padding:5px 10px;text-align:right;color:#6b7280;font-weight:500">Claimed</th>
              </tr>
            </thead>
            <tbody>{batch_trs}</tbody>
          </table>
        </div>"""

        # Ticket log
        if ticket_log:
            ticket_trs = "".join(
                f"""<tr>
                  <td style="padding:5px 10px;color:#fbbf24;font-weight:700;text-align:center">#{t["ticket_number"]}</td>
                  <td style="padding:5px 10px;color:#9ca3af">{_mask(t["phone_number"] or "")}</td>
                  <td style="padding:5px 10px;color:#6b7280">{str(t["claimed_at"])[:16] if t["claimed_at"] else "—"}</td>
                  <td style="padding:5px 10px;color:#4b5563">{_esc(str(t["batch_name"] or "—"))}</td>
                </tr>"""
                for t in ticket_log
            )
            outreach_html += f"""
        <div style="margin-top:16px">
          <div style="font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;
                      letter-spacing:.05em;margin-bottom:6px">Claimed tickets</div>
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead>
              <tr style="border-bottom:1px solid #1f2937">
                <th style="padding:5px 10px;text-align:center;color:#6b7280;font-weight:500">Ticket #</th>
                <th style="padding:5px 10px;text-align:left;color:#6b7280;font-weight:500">Phone</th>
                <th style="padding:5px 10px;text-align:left;color:#6b7280;font-weight:500">Claimed at</th>
                <th style="padding:5px 10px;text-align:left;color:#6b7280;font-weight:500">Batch</th>
              </tr>
            </thead>
            <tbody>{ticket_trs}</tbody>
          </table>
        </div>"""
    else:
        outreach_html = '<div style="color:#4b5563;font-size:13px;padding:8px 0">No outreach campaigns sent yet. Use <code>scripts/send_outreach_invites.py</code> to run one.</div>'

    # ── Segment breakdown ──
    if seg_counts:
        seg_cards = []
        for i, seg in enumerate(seg_counts):
            color = _SEG_COLORS[i % len(_SEG_COLORS)]
            seg_cards.append(f"""
            <div style="background:#0f172a;border:1px solid #1f2937;border-radius:10px;
                        padding:16px;flex:1;min-width:150px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                <span style="font-weight:700;color:{color};font-size:13px">{_esc(seg['name'])}</span>
                <span style="font-size:20px;font-weight:800;color:#f3f4f6">{seg['count']}</span>
              </div>
              <div style="font-size:11px;color:#6b7280;margin-bottom:8px">{_esc(seg['description'])}</div>
              <div style="background:#1f2937;border-radius:4px;height:5px;overflow:hidden">
                <div style="width:{min(seg['pct'],100)}%;height:100%;background:{color};border-radius:4px"></div>
              </div>
              <div style="font-size:11px;color:#4b5563;margin-top:4px">{seg['pct']}% of subscribers</div>
            </div>""")

        seg_unclassified = total - pref_answered
        if seg_unclassified > 0:
            unclass_pct = round((seg_unclassified / total) * 100) if total else 0
            seg_cards.append(f"""
            <div style="background:#0f172a;border:1px solid #1f2937;border-radius:10px;
                        padding:16px;flex:1;min-width:150px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                <span style="font-weight:700;color:#6b7280;font-size:13px">UNCLASSIFIED</span>
                <span style="font-size:20px;font-weight:800;color:#6b7280">{seg_unclassified}</span>
              </div>
              <div style="font-size:11px;color:#4b5563;margin-bottom:8px">Haven't answered yet</div>
              <div style="background:#1f2937;border-radius:4px;height:5px;overflow:hidden">
                <div style="width:{unclass_pct}%;height:100%;background:#374151;border-radius:4px"></div>
              </div>
              <div style="font-size:11px;color:#4b5563;margin-top:4px">{unclass_pct}% of subscribers</div>
            </div>""")

        seg_html = f'<div style="display:flex;flex-wrap:wrap;gap:12px">{"".join(seg_cards)}</div>'
    else:
        seg_html = '<div style="color:#6b7280;font-size:13px">No segments configured.</div>'

    # ── Subscriber table ──
    if not subscribers:
        sub_html = '<div style="color:#6b7280;font-size:13px;padding:24px;text-align:center">No subscribers yet.</div>'
    else:
        rows = []
        for s in subscribers:
            phone = s["phone_number"] or ""
            masked = _mask(phone)
            comedy_type = s["comedy_type"] or ""
            interest = s["interest_text"] or ""

            if comedy_type:
                type_color = {"STANDUP": "#a78bfa", "IMPROV": "#34d399", "BOTH": "#fbbf24"}.get(
                    comedy_type.upper(), "#9ca3af"
                )
                type_badge = (
                    f'<span class="badge" style="background:#1f2937;color:{type_color}">'
                    f'{_esc(comedy_type.upper())}</span>'
                )
            else:
                type_badge = '<span style="color:#4b5563;font-size:11px">—</span>'

            status_badge = (
                '<span style="color:#4ade80;font-size:11px">✓ answered</span>'
                if s["comedy_type"] else
                '<span style="color:#6b7280;font-size:11px">pending</span>'
            )

            rows.append(f"""
            <tr class="sub-row" data-phone="{_esc(phone)}" onclick="openConv(this)">
              <td style="color:#9ca3af;white-space:nowrap">{_esc(masked)}</td>
              <td>{type_badge}</td>
              <td style="color:#6b7280;font-size:12px;max-width:280px;overflow:hidden;
                         text-overflow:ellipsis;white-space:nowrap"
                  title="{_esc(interest)}">{_esc(interest[:80])}{"…" if len(interest) > 80 else ""}</td>
              <td style="color:#4b5563;font-size:12px;white-space:nowrap">{_fmt_dt(s['created_at'])}</td>
              <td>{status_badge}</td>
              <td style="color:#6366f1;font-size:12px;white-space:nowrap">Chat →</td>
            </tr>""")

        sub_html = f"""
        <p style="font-size:12px;color:#4b5563;margin-bottom:12px">Click any row to view the full conversation.</p>
        <div style="overflow-x:auto">
          <table>
            <thead>
              <tr>
                <th>Phone</th><th>Type</th><th>What they said</th>
                <th>Signed up</th><th>Status</th><th></th>
              </tr>
            </thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>"""

    # ── Blast history (card layout) ──
    if not blasts:
        blast_html = '<div style="color:#6b7280;font-size:13px;padding:8px 0">No blasts sent yet.</div>'
    else:
        total_sent = sum(b["attempted"] for b in blasts)
        total_delivered = sum(b["succeeded"] for b in blasts)
        overall_rate = round((total_delivered / total_sent) * 100) if total_sent else 0

        summary = f"""
        <div style="display:flex;gap:20px;margin-bottom:16px;flex-wrap:wrap">
          <div style="font-size:13px;color:#9ca3af">
            <span style="font-weight:700;color:#f3f4f6;font-size:18px">{len(blasts)}</span>
            <span style="margin-left:5px">blasts</span>
          </div>
          <div style="font-size:13px;color:#9ca3af">
            <span style="font-weight:700;color:#34d399;font-size:18px">{total_sent}</span>
            <span style="margin-left:5px">total sent</span>
          </div>
          <div style="font-size:13px;color:#9ca3af">
            <span style="font-weight:700;color:#4ade80;font-size:18px">{overall_rate}%</span>
            <span style="margin-left:5px">avg delivery</span>
          </div>
        </div>"""

        cards = []
        for b in blasts:
            success_rate = round((b["succeeded"] / b["attempted"]) * 100) if b["attempted"] else 0
            rate_color = "#4ade80" if success_rate >= 90 else "#fbbf24" if success_rate >= 70 else "#f87171"
            seg_label = (
                f'<span class="badge" style="background:#1e1b4b;color:#a78bfa">{_esc(b["segment"])}</span>'
                if b.get("segment") else
                '<span class="badge" style="background:#1f2937;color:#6b7280">everyone</span>'
            )
            msg = b["body"] or b["owner_message"] or ""
            cards.append(f"""
            <div class="blast-card">
              <div class="blast-meta">
                <span style="font-size:11px;color:#4b5563">{_fmt_time(b['sent_at'])}</span>
                {seg_label}
                <span style="font-size:12px;color:#9ca3af">→ {b['attempted']} sent</span>
                <span style="font-size:12px;font-weight:700;color:{rate_color}">{success_rate}% delivered</span>
              </div>
              <div class="blast-msg">{_esc(msg)}</div>
            </div>""")

        blast_html = summary + "".join(cards)

    # ── Link clicks ──
    clicks_section = ""
    if link_clicks:
        pills = " ".join(
            f'<span style="background:#1f2937;color:#9ca3af;padding:4px 10px;border-radius:6px;font-size:12px">'
            f'<span style="color:#fbbf24;font-weight:700">{r["clicks"]}</span> {_esc(r["link_key"])}</span>'
            for r in link_clicks
        )
        clicks_section = f"""
        <div class="section">
          <div class="section-title">Link Clicks</div>
          <div style="display:flex;flex-wrap:wrap;gap:8px">{pills}</div>
        </div>"""

    # ── Conversation panel (JS-driven) ──
    conv_panel = f"""
<div id="conv-overlay" onclick="closeConv()"></div>
<div id="conv-panel">
  <div id="conv-header">
    <h2 id="conv-title">Conversation</h2>
    <button id="conv-close" onclick="closeConv()">✕</button>
  </div>
  <div id="conv-body">
    <div style="color:#4b5563;font-size:13px;padding:20px 0;text-align:center">Loading…</div>
  </div>
</div>
<script>
const SLUG = "{_esc(slug)}";

function openConv(row) {{
  const phone = row.getAttribute("data-phone");
  document.getElementById("conv-title").textContent = row.cells[0].textContent + " — Chat";
  document.getElementById("conv-body").innerHTML =
    '<div style="color:#4b5563;font-size:13px;padding:20px 0;text-align:center">Loading…</div>';
  document.getElementById("conv-panel").classList.add("open");
  document.getElementById("conv-overlay").style.display = "block";

  fetch("/admin/smb/" + SLUG + "/conversation?phone=" + encodeURIComponent(phone))
    .then(r => r.json())
    .then(msgs => {{
      if (!msgs.length) {{
        document.getElementById("conv-body").innerHTML =
          '<div style="color:#4b5563;font-size:13px;padding:20px 0;text-align:center">No messages yet.</div>';
        return;
      }}
      let html = "";
      msgs.forEach(m => {{
        const isUser = m.role === "user";
        html += `<div class="bubble-wrap ${{isUser ? 'user' : 'bot'}}">
          <div class="bubble-sender">${{isUser ? "Subscriber" : "Bot"}}</div>
          <div class="bubble">${{escHtml(m.body)}}</div>
          <div class="bubble-time">${{m.created_at || ""}}</div>
        </div>`;
      }});
      document.getElementById("conv-body").innerHTML = html;
      document.getElementById("conv-body").scrollTop = 999999;
    }})
    .catch(() => {{
      document.getElementById("conv-body").innerHTML =
        '<div style="color:#f87171;font-size:13px;padding:20px 0;text-align:center">Failed to load.</div>';
    }});
}}

function closeConv() {{
  document.getElementById("conv-panel").classList.remove("open");
  document.getElementById("conv-overlay").style.display = "none";
}}

function escHtml(s) {{
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}}

document.addEventListener("keydown", e => {{ if (e.key === "Escape") closeConv(); }});
</script>"""

    # ── Full page ──
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{_esc(tenant.display_name)} — SMB Detail</title>
  {_PAGE_STYLE}
</head>
<body>
  <div style="margin-bottom:24px">
    <a href="/admin?tab=smb" style="font-size:13px;color:#6b7280">← SMB Clients</a>
  </div>

  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:24px">
    <div>
      <h1 style="font-size:24px;font-weight:800;color:#f3f4f6">{_esc(tenant.display_name)}</h1>
      <div style="font-size:13px;color:#6b7280;margin-top:4px">
        <code>{_esc(tenant.slug)}</code> &nbsp;·&nbsp; {_esc(tenant.business_type)}
      </div>
      <div style="margin-top:8px;font-size:12px">{config_badges}</div>
    </div>
  </div>

  {stats_html}

  <div class="section">
    <div class="section-title">Outreach Campaigns</div>
    {outreach_html}
  </div>

  <div class="section">
    <div class="section-title">Customer Segments</div>
    {seg_html}
  </div>

  {clicks_section}

  <div class="section">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <div class="section-title" style="margin-bottom:0">Subscribers ({total})</div>
    </div>
    {sub_html}
  </div>

  <div class="section">
    <div class="section-title">Blast History (last 30)</div>
    {blast_html}
  </div>

  {conv_panel}
</body>
</html>"""


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

def smb_client_detail_view(slug: str):
    """Flask view for GET /admin/smb/<slug>."""
    from flask import Response
    if not check_admin_auth():
        return require_admin_auth_response()
    html = render_client_detail(slug)
    return Response(html, mimetype="text/html")


def smb_conversation_view(slug: str):
    """Flask view for GET /admin/smb/<slug>/conversation?phone=+1..."""
    from flask import jsonify, request
    if not check_admin_auth():
        return require_admin_auth_response()
    phone = request.args.get("phone", "").strip()
    if not phone:
        return jsonify([])
    messages = _fetch_conversation(slug, phone)
    # Format timestamps for display
    result = []
    for m in messages:
        ts = m.get("created_at")
        if ts and hasattr(ts, "strftime"):
            ts_str = ts.astimezone(timezone.utc).strftime("%b %d, %-I:%M %p UTC")
        else:
            ts_str = str(ts)[:16] if ts else ""
        result.append({"role": m["role"], "body": m["body"], "created_at": ts_str})
    return jsonify(result)
