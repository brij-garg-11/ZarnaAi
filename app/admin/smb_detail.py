"""
SMB client detail page — per-tenant deep-dive view.

Accessible at /admin/smb/<slug> — linked from the SMB Clients tab.
Shows subscriber overview, full segment breakdown, subscriber table
with preferences, and blast history for the selected client.

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

        # Segment counts — use tenant config to define segments, query DB for counts
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
            "total": total,
        }

    except Exception:
        logger.exception("SMB detail: failed to fetch data for slug=%s", slug)
        return {"error": "Failed to load data — check logs"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_SEG_COLORS = ["#a78bfa", "#34d399", "#fbbf24", "#f87171", "#60a5fa", "#fb923c"]


def render_client_detail(slug: str) -> str:
    """Return the full HTML page for one SMB client's detail view."""
    data = _fetch_detail(slug)

    if "error" in data:
        return f"""
        <!doctype html><html><head><title>SMB Detail</title>
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
    total       = data["total"]

    pref_answered = sum(1 for s in subscribers if s["comedy_type"])
    pref_pct      = round((pref_answered / total) * 100) if total else 0

    # ── Config status badges ──
    def badge(ok, label_ok, label_bad):
        if ok:
            return f'<span style="color:#4ade80;font-size:12px">✓ {label_ok}</span>'
        return f'<span style="color:#f87171;font-size:12px">⚠ {label_bad}</span>'

    config_badges = " &nbsp;·&nbsp; ".join([
        badge(tenant.sms_number, f"SMS {tenant.sms_number[-4:] if tenant.sms_number else ''}", "no SMS number"),
        badge(tenant.owner_phone, "owner set", "no owner phone"),
        badge(tenant.keyword, f"keyword: {tenant.keyword}", "no keyword"),
    ])

    # ── Stat cards ──
    total_clicks = sum(r["clicks"] for r in link_clicks)
    stats_html = f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px">
      <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px;text-align:center">
        <div style="font-size:28px;font-weight:700;color:#f3f4f6">{total}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:4px">Active subscribers</div>
      </div>
      <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px;text-align:center">
        <div style="font-size:28px;font-weight:700;color:#a78bfa">{pref_answered}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:4px">Preference answered ({pref_pct}%)</div>
      </div>
      <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px;text-align:center">
        <div style="font-size:28px;font-weight:700;color:#34d399">{len(blasts)}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:4px">Blasts sent</div>
      </div>
      <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px;text-align:center">
        <div style="font-size:28px;font-weight:700;color:#fbbf24">{total_clicks}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:4px">Link clicks</div>
      </div>
    </div>"""

    # ── Segment breakdown ──
    if seg_counts:
        seg_cards = []
        for i, seg in enumerate(seg_counts):
            color = _SEG_COLORS[i % len(_SEG_COLORS)]
            bar_pct = min(seg["pct"], 100)
            seg_cards.append(f"""
            <div style="background:#0f172a;border:1px solid #1f2937;border-radius:10px;padding:16px;flex:1;min-width:160px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <span style="font-weight:700;color:{color};font-size:14px">{_esc(seg['name'])}</span>
                <span style="font-size:20px;font-weight:700;color:#f3f4f6">{seg['count']}</span>
              </div>
              <div style="font-size:11px;color:#6b7280;margin-bottom:10px">{_esc(seg['description'])}</div>
              <div style="background:#1f2937;border-radius:4px;height:6px;overflow:hidden">
                <div style="width:{bar_pct}%;height:100%;background:{color};border-radius:4px"></div>
              </div>
              <div style="font-size:11px;color:#4b5563;margin-top:5px">{seg['pct']}% of subscribers</div>
            </div>""")

        seg_unclassified = total - pref_answered
        if seg_unclassified > 0:
            seg_cards.append(f"""
            <div style="background:#0f172a;border:1px solid #1f2937;border-radius:10px;padding:16px;flex:1;min-width:160px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <span style="font-weight:700;color:#6b7280;font-size:14px">UNCLASSIFIED</span>
                <span style="font-size:20px;font-weight:700;color:#6b7280">{seg_unclassified}</span>
              </div>
              <div style="font-size:11px;color:#4b5563;margin-bottom:10px">Haven't answered yet (onboarding step 0)</div>
              <div style="background:#1f2937;border-radius:4px;height:6px;overflow:hidden">
                <div style="width:{round((seg_unclassified/total)*100) if total else 0}%;height:100%;background:#374151;border-radius:4px"></div>
              </div>
              <div style="font-size:11px;color:#4b5563;margin-top:5px">{round((seg_unclassified/total)*100) if total else 0}% of subscribers</div>
            </div>""")

        seg_html = f"""
        <div style="display:flex;flex-wrap:wrap;gap:14px">
          {"".join(seg_cards)}
        </div>"""
    else:
        seg_html = '<div style="color:#6b7280;font-size:13px;padding:16px 0">No segments configured for this client.</div>'

    # ── Subscriber table ──
    if not subscribers:
        sub_html = '<div style="color:#6b7280;font-size:13px;padding:24px;text-align:center">No subscribers yet.</div>'
    else:
        rows = []
        for s in subscribers:
            phone = s["phone_number"] or ""
            masked = f"({phone[2:5]}) ***-{phone[-4:]}" if len(phone) >= 10 else phone
            comedy_type = s["comedy_type"] or ""
            interest = s["interest_text"] or ""

            if comedy_type:
                type_color = {"STANDUP": "#a78bfa", "IMPROV": "#34d399", "BOTH": "#fbbf24"}.get(
                    comedy_type.upper(), "#9ca3af"
                )
                type_badge = (
                    f'<span style="background:#1f2937;color:{type_color};padding:2px 8px;'
                    f'border-radius:4px;font-size:11px;font-weight:600">{_esc(comedy_type.upper())}</span>'
                )
            else:
                type_badge = '<span style="color:#4b5563;font-size:11px">—</span>'

            rows.append(f"""
            <tr style="border-bottom:1px solid #1f2937">
              <td style="padding:10px 12px 10px 0;color:#9ca3af;font-size:13px;white-space:nowrap">{_esc(masked)}</td>
              <td style="padding:10px 12px;white-space:nowrap">{type_badge}</td>
              <td style="padding:10px 12px;color:#6b7280;font-size:12px;max-width:300px;
                         overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                  title="{_esc(interest)}">{_esc(interest[:80])}{"…" if len(interest) > 80 else ""}</td>
              <td style="padding:10px 12px;color:#4b5563;font-size:12px;white-space:nowrap">{_fmt_dt(s["created_at"])}</td>
              <td style="padding:10px 12px;font-size:11px">
                {"<span style='color:#4ade80'>✓ answered</span>" if s["comedy_type"]
                  else "<span style='color:#6b7280'>pending</span>"}
              </td>
            </tr>""")

        sub_html = f"""
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead>
              <tr style="color:#4b5563;border-bottom:1px solid #1f2937;text-align:left">
                <th style="padding:8px 12px 8px 0;font-weight:600">Phone</th>
                <th style="padding:8px 12px;font-weight:600">Comedy Type</th>
                <th style="padding:8px 12px;font-weight:600">What they said</th>
                <th style="padding:8px 12px;font-weight:600">Signed up</th>
                <th style="padding:8px 12px;font-weight:600">Status</th>
              </tr>
            </thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>"""

    # ── Blast history ──
    if not blasts:
        blast_html = '<div style="color:#6b7280;font-size:13px;padding:24px;text-align:center">No blasts sent yet.</div>'
    else:
        b_rows = []
        for b in blasts:
            success_rate = round((b["succeeded"] / b["attempted"]) * 100) if b["attempted"] else 0
            rate_color = "#4ade80" if success_rate >= 90 else "#fbbf24" if success_rate >= 70 else "#f87171"
            seg_label = (
                f'<span style="background:#1f2937;color:#a78bfa;padding:2px 6px;border-radius:4px;font-size:11px">'
                f'{_esc(b["segment"])}</span>'
                if b.get("segment") else
                '<span style="color:#4b5563;font-size:11px">everyone</span>'
            )
            b_rows.append(f"""
            <tr style="border-bottom:1px solid #111827">
              <td style="padding:10px 12px 10px 0;color:#6b7280;font-size:12px;white-space:nowrap">{_fmt_time(b["sent_at"])}</td>
              <td style="padding:10px 12px">{seg_label}</td>
              <td style="padding:10px 12px;color:#d1d5db;max-width:300px;overflow:hidden;
                         text-overflow:ellipsis;white-space:nowrap;font-size:12px"
                  title="{_esc(b['body'])}">{_esc(b['body'][:90])}{"…" if len(b['body']) > 90 else ""}</td>
              <td style="padding:10px 12px;text-align:center;color:#9ca3af;font-size:13px">{b["attempted"]}</td>
              <td style="padding:10px 12px;text-align:center;font-weight:600;font-size:13px;color:{rate_color}">{success_rate}%</td>
            </tr>""")

        blast_html = f"""
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead>
              <tr style="color:#4b5563;border-bottom:1px solid #1f2937;text-align:left">
                <th style="padding:8px 12px 8px 0;font-weight:600">Sent</th>
                <th style="padding:8px 12px;font-weight:600">Audience</th>
                <th style="padding:8px 12px;font-weight:600">Message</th>
                <th style="padding:8px 12px;text-align:center;font-weight:600">Sent to</th>
                <th style="padding:8px 12px;text-align:center;font-weight:600">Success</th>
              </tr>
            </thead>
            <tbody>{"".join(b_rows)}</tbody>
          </table>
        </div>"""

    # ── Link clicks ──
    if link_clicks:
        click_pills = " ".join(
            f'<span style="background:#1f2937;color:#9ca3af;padding:4px 10px;border-radius:6px;font-size:12px">'
            f'<span style="color:#fbbf24;font-weight:600">{r["clicks"]}</span> {_esc(r["link_key"])}</span>'
            for r in link_clicks
        )
        clicks_section = f"""
        <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:20px;margin-bottom:20px">
          <div style="font-size:14px;font-weight:600;color:#9ca3af;margin-bottom:12px">Link Clicks</div>
          <div style="display:flex;flex-wrap:wrap;gap:8px">{click_pills}</div>
        </div>"""
    else:
        clicks_section = ""

    # ── Full page ──
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{_esc(tenant.display_name)} — SMB Detail</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: #0d1117;
      color: #f3f4f6;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      padding: 32px 24px;
      max-width: 1100px;
      margin: 0 auto;
    }}
    .section {{
      background: #111827;
      border: 1px solid #1f2937;
      border-radius: 12px;
      padding: 22px 24px;
      margin-bottom: 20px;
    }}
    .section-title {{
      font-size: 14px;
      font-weight: 600;
      color: #9ca3af;
      margin-bottom: 16px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    a {{ color: #a78bfa; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    code {{
      background: #1f2937;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 12px;
      color: #94a3b8;
    }}
  </style>
</head>
<body>
  <div style="margin-bottom:24px">
    <a href="/admin?tab=smb" style="font-size:13px;color:#6b7280">← SMB Clients</a>
  </div>

  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:24px">
    <div>
      <h1 style="font-size:24px;font-weight:700;color:#f3f4f6">{_esc(tenant.display_name)}</h1>
      <div style="font-size:13px;color:#6b7280;margin-top:4px">
        <code>{_esc(tenant.slug)}</code> &nbsp;·&nbsp; {_esc(tenant.business_type)}
        &nbsp;·&nbsp; {_esc(tenant.location if hasattr(tenant, 'location') else '')}
      </div>
      <div style="margin-top:8px;font-size:12px">{config_badges}</div>
    </div>
  </div>

  {stats_html}

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

</body>
</html>"""


# ---------------------------------------------------------------------------
# Route handler (called from register_smb_routes in smb.py)
# ---------------------------------------------------------------------------

def smb_client_detail_view(slug: str):
    """Flask view for /admin/smb/<slug>."""
    from flask import Response
    if not check_admin_auth():
        return require_admin_auth_response()
    html = render_client_detail(slug)
    return Response(html, mimetype="text/html")
