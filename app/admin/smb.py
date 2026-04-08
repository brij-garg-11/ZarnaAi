"""
SMB admin tab — internal dashboard for the Small-Medium Business vertical.

Shows all SMB clients, their subscriber counts, onboarding funnel,
blast history, and engagement at a glance.

Registered via register_smb_routes(bp) called from app/admin/__init__.py.
"""

import logging
from datetime import timezone

from app.admin_auth import get_db_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _get_db():
    return get_db_connection()


def _fetch_recent_conversations(limit_threads: int = 20) -> list:
    """
    Return the most recent conversations across all SMB tenants.
    Each entry is a dict with tenant_slug, phone_number, and a list of messages
    (oldest-first), capped to the last 8 turns per thread.
    """
    conn = _get_db()
    if not conn:
        return []
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get the most recently active threads
            cur.execute("""
                SELECT DISTINCT ON (tenant_slug, phone_number)
                    tenant_slug, phone_number, MAX(created_at) AS last_active
                FROM smb_messages
                GROUP BY tenant_slug, phone_number
                ORDER BY tenant_slug, phone_number, last_active DESC
                LIMIT %s
            """, (limit_threads,))
            threads = [dict(r) for r in cur.fetchall()]

            # Sort all threads by most recent activity
            threads.sort(key=lambda t: t["last_active"] or "", reverse=True)

            # Fetch the last 8 messages for each thread
            result = []
            for thread in threads:
                cur.execute("""
                    SELECT role, body, created_at FROM (
                        SELECT role, body, created_at
                        FROM smb_messages
                        WHERE tenant_slug = %s AND phone_number = %s
                        ORDER BY created_at DESC LIMIT 8
                    ) sub ORDER BY created_at ASC
                """, (thread["tenant_slug"], thread["phone_number"]))
                messages = [dict(m) for m in cur.fetchall()]
                result.append({
                    "tenant_slug": thread["tenant_slug"],
                    "phone_number": thread["phone_number"],
                    "last_active": thread["last_active"],
                    "messages": messages,
                })
        return result
    except Exception:
        logger.exception("SMB admin: failed to fetch conversations")
        return []
    finally:
        conn.close()


def _fetch_smb_stats() -> dict:
    """Pull all SMB metrics needed to render the tab."""
    conn = _get_db()
    if not conn:
        return {"clients": [], "totals": {}, "blasts": []}

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # Per-tenant subscriber counts
            cur.execute("""
                SELECT
                    tenant_slug,
                    COUNT(*) FILTER (WHERE status = 'active' AND onboarding_step > 0)  AS active,
                    COUNT(*) FILTER (WHERE status = 'active' AND onboarding_step = 0)  AS onboarding,
                    COUNT(*)                                                             AS total,
                    MIN(created_at)                                                      AS first_signup,
                    MAX(created_at)                                                      AS last_signup
                FROM smb_subscribers
                GROUP BY tenant_slug
                ORDER BY tenant_slug
            """)
            subscriber_rows = {r["tenant_slug"]: dict(r) for r in cur.fetchall()}

            # Blast history — last 50 across all tenants
            cur.execute("""
                SELECT tenant_slug, owner_message, body, attempted, succeeded, sent_at
                FROM smb_blasts
                ORDER BY sent_at DESC
                LIMIT 50
            """)
            blasts = [dict(r) for r in cur.fetchall()]

            # Per-tenant blast counts
            cur.execute("""
                SELECT tenant_slug, COUNT(*) AS blast_count, MAX(sent_at) AS last_blast
                FROM smb_blasts
                GROUP BY tenant_slug
            """)
            blast_counts = {r["tenant_slug"]: dict(r) for r in cur.fetchall()}

        # Merge tenant registry with DB stats
        from app.smb.tenants import get_registry
        registry = get_registry()
        clients = []
        for tenant in registry.all_tenants():
            subs = subscriber_rows.get(tenant.slug, {
                "active": 0, "onboarding": 0, "total": 0,
                "first_signup": None, "last_signup": None,
            })
            # total is our canonical "active subscribers" count (status='active', any onboarding_step)
            blasts_info = blast_counts.get(tenant.slug, {"blast_count": 0, "last_blast": None})
            clients.append({
                "tenant": tenant,
                "active": subs["active"],
                "onboarding": subs["onboarding"],
                "total": subs["total"],
                "first_signup": subs["first_signup"],
                "last_signup": subs["last_signup"],
                "blast_count": blasts_info["blast_count"],
                "last_blast": blasts_info["last_blast"],
            })

        totals = {
            "clients": len(clients),
            "active_subscribers": sum(c["total"] for c in clients),
            "pref_answered": sum(c["active"] for c in clients),
            "total_blasts": sum(c["blast_count"] for c in clients),
        }

        return {"clients": clients, "totals": totals, "blasts": blasts}

    except Exception:
        logger.exception("SMB admin: failed to fetch stats")
        return {"clients": [], "totals": {}, "blasts": []}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTML rendering
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
        return dt.astimezone(timezone.utc).strftime("%b %d %H:%M")
    return str(dt)[:16]


def render_smb_tab() -> str:
    """Return the full inner HTML for the SMB admin tab."""
    data = _fetch_smb_stats()
    totals = data.get("totals", {})
    clients = data.get("clients", [])
    blasts = data.get("blasts", [])

    # ── Top-level stat cards ──
    stats_html = f"""
    <div class="stats-grid" style="grid-template-columns:repeat(4,1fr)">
      <div class="stat-card">
        <div class="stat-label">SMB Clients</div>
        <div class="stat-value">{totals.get("clients", 0)}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">active businesses</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Active Subscribers</div>
        <div class="stat-value purple">{totals.get("active_subscribers", 0):,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">all signed-up subscribers</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Preference Answered</div>
        <div class="stat-value teal">{totals.get("pref_answered", 0):,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">completed preference question</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Blasts Sent</div>
        <div class="stat-value green">{totals.get("total_blasts", 0):,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">across all clients</div>
      </div>
    </div>"""

    # ── Per-client cards ──
    if not clients:
        client_html = """
        <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;
                    padding:48px 40px;text-align:center;color:#6b7280;margin-top:8px;">
          <div style="font-size:36px;margin-bottom:14px">🏪</div>
          <div style="font-size:16px;font-weight:600;color:#9ca3af;margin-bottom:8px">
            No SMB clients yet
          </div>
          <div style="font-size:13px">
            Add a business config to <code>creator_config/</code> to get started.
          </div>
        </div>"""
    else:
        cards = []
        for c in clients:
            t = c["tenant"]
            total = c["total"] or 1  # avoid div/0
            completion_pct = round((c["active"] / total) * 100) if total else 0
            bar_color = "#4ade80" if completion_pct >= 70 else "#fbbf24" if completion_pct >= 40 else "#f87171"

            owner_status = (
                '<span style="color:#4ade80">✓ set</span>'
                if t.owner_phone else
                '<span style="color:#f87171">⚠ TBD</span>'
            )
            sms_status = (
                '<span style="color:#4ade80">✓ set</span>'
                if t.sms_number else
                '<span style="color:#f87171">⚠ TBD</span>'
            )
            keyword_status = (
                f'<code style="background:#1f2937;padding:2px 6px;border-radius:4px">{_esc(t.keyword)}</code>'
                if t.keyword else
                '<span style="color:#f87171">⚠ TBD</span>'
            )

            cards.append(f"""
            <div class="card" style="margin-bottom:16px">
              <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px">
                <div>
                  <div style="font-size:17px;font-weight:700;color:#f3f4f6">{_esc(t.display_name)}</div>
                  <div style="font-size:12px;color:#6b7280;margin-top:2px">
                    {_esc(t.business_type)} &nbsp;·&nbsp; <code style="background:#1f2937;padding:1px 5px;border-radius:3px">{_esc(t.slug)}</code>
                  </div>
                </div>
                <div style="text-align:right;font-size:12px;color:#6b7280">
                  <div>Owner phone: {owner_status}</div>
                  <div style="margin-top:2px">SMS number: {sms_status}</div>
                  <div style="margin-top:2px">Keyword: {keyword_status}</div>
                </div>
              </div>

              <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#f3f4f6">{c["total"]}</div>
                  <div style="font-size:11px;color:#6b7280;margin-top:2px">Active subscribers</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#a78bfa">{c["onboarding"]}</div>
                  <div style="font-size:11px;color:#6b7280;margin-top:2px">Pref. pending</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#34d399">{c["blast_count"]}</div>
                  <div style="font-size:11px;color:#6b7280;margin-top:2px">Blasts sent</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#fbbf24">{completion_pct}%</div>
                  <div style="font-size:11px;color:#6b7280;margin-top:2px">Pref. answered</div>
                </div>
              </div>

              <div style="margin-bottom:6px">
                <div style="display:flex;justify-content:space-between;font-size:11px;color:#6b7280;margin-bottom:4px">
                  <span>Preference answered</span>
                  <span>{c["active"]} of {c["total"]} answered</span>
                </div>
                <div style="background:#1f2937;border-radius:4px;height:6px;overflow:hidden">
                  <div style="width:{completion_pct}%;height:100%;background:{bar_color};border-radius:4px;transition:width 0.3s"></div>
                </div>
              </div>

              <div style="display:flex;justify-content:space-between;font-size:11px;color:#4b5563;margin-top:10px">
                <span>First signup: {_fmt_dt(c["first_signup"])}</span>
                <span>Last signup: {_fmt_dt(c["last_signup"])}</span>
                <span>Last blast: {_fmt_dt(c["last_blast"])}</span>
              </div>
            </div>""")

        client_html = "\n".join(cards)

    # ── Blast history table ──
    if not blasts:
        blast_html = """
        <div style="color:#6b7280;font-size:13px;padding:24px;text-align:center">
          No blasts sent yet.
        </div>"""
    else:
        rows = []
        for b in blasts:
            success_rate = round((b["succeeded"] / b["attempted"]) * 100) if b["attempted"] else 0
            rate_color = "#4ade80" if success_rate >= 90 else "#fbbf24" if success_rate >= 70 else "#f87171"
            rows.append(f"""
            <tr>
              <td style="color:#6b7280;white-space:nowrap">{_fmt_dt(b["sent_at"])}</td>
              <td><code style="background:#1f2937;padding:1px 5px;border-radius:3px;font-size:11px">{_esc(b["tenant_slug"])}</code></td>
              <td style="color:#d1d5db;max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                  title="{_esc(b["body"])}">{_esc(b["body"][:80])}{"…" if len(b["body"]) > 80 else ""}</td>
              <td style="text-align:center;color:#9ca3af">{b["attempted"]}</td>
              <td style="text-align:center;color:{rate_color};font-weight:600">{success_rate}%</td>
            </tr>""")

        blast_html = f"""
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead>
              <tr style="color:#6b7280;border-bottom:1px solid #1f2937;text-align:left">
                <th style="padding:8px 12px 8px 0">Date</th>
                <th style="padding:8px 12px">Client</th>
                <th style="padding:8px 12px">Message</th>
                <th style="padding:8px 12px;text-align:center">Sent to</th>
                <th style="padding:8px 12px;text-align:center">Success</th>
              </tr>
            </thead>
            <tbody style="color:#9ca3af">
              {"".join(rows)}
            </tbody>
          </table>
        </div>"""

    # ── Conversation log ──
    conversations = _fetch_recent_conversations()
    if not conversations:
        convo_html = """
        <div style="color:#6b7280;font-size:13px;padding:24px;text-align:center">
          No conversations yet — subscribers will appear here once they start texting.
        </div>"""
    else:
        threads = []
        for thread in conversations:
            phone = thread["phone_number"]
            masked = f"({phone[2:5]}) ***-{phone[-4:]}" if len(phone) >= 10 else phone
            msgs_html = ""
            for m in thread["messages"]:
                is_user = m["role"] == "user"
                align = "flex-start" if is_user else "flex-end"
                bubble_bg = "#1e293b" if is_user else "#312e81"
                label = masked if is_user else "Bot"
                msgs_html += f"""
                <div style="display:flex;flex-direction:column;align-items:{align};margin-bottom:6px">
                  <div style="font-size:10px;color:#4b5563;margin-bottom:2px">{label}</div>
                  <div style="background:{bubble_bg};border-radius:10px;padding:8px 12px;
                              max-width:80%;font-size:12px;color:#d1d5db;line-height:1.4">
                    {_esc(m["body"])}
                  </div>
                  <div style="font-size:10px;color:#374151;margin-top:2px">{_fmt_time(m.get("created_at"))}</div>
                </div>"""

            threads.append(f"""
            <div style="border:1px solid #1f2937;border-radius:10px;padding:14px;margin-bottom:12px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                <div>
                  <span style="font-size:13px;font-weight:600;color:#f3f4f6">{masked}</span>
                  <code style="background:#1f2937;padding:1px 6px;border-radius:3px;font-size:11px;
                               color:#94a3b8;margin-left:8px">{_esc(thread["tenant_slug"])}</code>
                </div>
                <span style="font-size:11px;color:#4b5563">{_fmt_time(thread["last_active"])}</span>
              </div>
              <div style="display:flex;flex-direction:column">{msgs_html}</div>
            </div>""")

        convo_html = "".join(threads)

    return f"""
    {stats_html}

    <div class="card" style="margin-top:20px">
      <div class="card-title">SMB Clients</div>
      {client_html}
    </div>

    <div class="card">
      <div class="card-title">Conversations (last 20 threads)</div>
      {convo_html}
    </div>

    <div class="card">
      <div class="card-title">Blast History (last 50)</div>
      {blast_html}
    </div>
    """


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

def register_smb_routes(bp):
    """Register SMB-specific admin routes on the admin blueprint."""
    # No additional routes needed yet — the tab renders via the main /admin route.
    pass
