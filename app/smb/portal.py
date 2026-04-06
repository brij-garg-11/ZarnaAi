"""
SMB Client Portal — read-only dashboard for business owners.

Each client gets a unique magic-link URL:
  GET /portal/<slug>?token=<token>

Token validation: the token is compared against the env var
  SMB_<SLUG_UPPERCASED>_PORTAL_TOKEN
If the env var is not set the portal returns 404 (not exposed at all).
If it is set but the token doesn't match → 403.

No JS framework, no external CDN assets. Pure server-rendered HTML
so it works instantly on any device, including slow mobile connections.
"""

import logging
import os
from collections import Counter, defaultdict

from flask import Blueprint, request

from app.admin_auth import get_db_connection
from app.smb.tenants import get_registry

logger = logging.getLogger(__name__)

portal_bp = Blueprint("smb_portal", __name__, url_prefix="/portal")


# ---------------------------------------------------------------------------
# Token auth helper
# ---------------------------------------------------------------------------

def _portal_token_for(slug: str) -> str | None:
    """Return the expected token for this slug, or None if not configured."""
    key = "SMB_" + slug.upper() + "_PORTAL_TOKEN"
    return os.getenv(key, "").strip() or None


def _check_token(slug: str) -> bool:
    expected = _portal_token_for(slug)
    if not expected:
        return False
    provided = (request.args.get("token") or "").strip()
    if not provided:
        return False
    # Constant-time comparison to avoid timing attacks
    import hmac
    return hmac.compare_digest(expected, provided)


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

def _get_db():
    return get_db_connection()


def _fetch_portal_data(slug: str) -> dict:
    conn = _get_db()
    if not conn:
        return {}
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # Subscriber counts
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'active')      AS active,
                    COUNT(*) FILTER (WHERE status = 'onboarding')   AS onboarding,
                    COUNT(*)                                         AS total,
                    MIN(created_at)                                  AS first_signup,
                    MAX(created_at)                                  AS last_signup
                FROM smb_subscribers
                WHERE tenant_slug = %s
            """, (slug,))
            subs = dict(cur.fetchone() or {})

            # Blast history — last 30 for this tenant
            cur.execute("""
                SELECT owner_message, body, attempted, succeeded, sent_at
                FROM smb_blasts
                WHERE tenant_slug = %s
                ORDER BY sent_at DESC
                LIMIT 30
            """, (slug,))
            blasts = [dict(r) for r in cur.fetchall()]

            # Preference breakdown — all answers for subscribers of this tenant
            cur.execute("""
                SELECT p.question_key, p.answer, COUNT(*) AS cnt
                FROM smb_preferences p
                JOIN smb_subscribers s ON s.id = p.subscriber_id
                WHERE s.tenant_slug = %s
                GROUP BY p.question_key, p.answer
                ORDER BY p.question_key, cnt DESC
            """, (slug,))
            pref_rows = [dict(r) for r in cur.fetchall()]

        # Group preferences by question
        pref_by_question = defaultdict(list)
        for row in pref_rows:
            pref_by_question[row["question_key"]].append({
                "answer": row["answer"],
                "cnt": row["cnt"],
            })

        return {
            "subscribers": subs,
            "blasts": blasts,
            "preferences": dict(pref_by_question),
        }

    except Exception:
        logger.exception("Portal: failed to fetch data for %s", slug)
        return {}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def _esc(s) -> str:
    return (
        str(s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _fmt_dt(dt, fmt="%b %d, %Y") -> str:
    if not dt:
        return "—"
    if hasattr(dt, "strftime"):
        return dt.strftime(fmt)
    return str(dt)[:10]


def _fmt_dt_full(dt) -> str:
    return _fmt_dt(dt, "%b %d, %Y · %I:%M %p")


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def _render_hero(tenant, subs: dict) -> str:
    total = subs.get("total") or 0
    active = subs.get("active") or 0
    onboarding = subs.get("onboarding") or 0
    pct = round((active / total) * 100) if total else 0
    bar_color = "#4ade80" if pct >= 70 else "#fbbf24" if pct >= 40 else "#f87171"

    # Status badge
    if not tenant.sms_number:
        badge = '<span class="badge badge-warn">Setup in progress</span>'
    elif total == 0:
        badge = '<span class="badge badge-info">Waiting for first subscriber</span>'
    else:
        badge = '<span class="badge badge-live">Live</span>'

    return f"""
    <header class="hero">
      <div class="hero-inner">
        <div class="hero-icon">{_esc(tenant.display_name[0].upper())}</div>
        <div class="hero-text">
          <div class="hero-name">{_esc(tenant.display_name)}</div>
          <div class="hero-meta">
            {_esc(tenant.business_type.replace("_", " ").title())}
            &nbsp;·&nbsp;
            {_esc(tenant.raw.get("location", ""))}
            &nbsp;&nbsp;{badge}
          </div>
        </div>
      </div>
    </header>

    <section class="stats-strip">
      <div class="stat-tile">
        <div class="stat-num">{active:,}</div>
        <div class="stat-lbl">Active subscribers</div>
      </div>
      <div class="stat-tile">
        <div class="stat-num accent-purple">{onboarding:,}</div>
        <div class="stat-lbl">Finishing sign-up</div>
      </div>
      <div class="stat-tile">
        <div class="stat-num accent-teal">{total:,}</div>
        <div class="stat-lbl">Total sign-ups</div>
      </div>
      <div class="stat-tile">
        <div class="stat-num accent-amber">{pct}%</div>
        <div class="stat-lbl">Completion rate</div>
      </div>
    </section>

    <section class="card">
      <div class="card-title">Sign-up funnel</div>
      <div class="funnel-wrap">
        <div class="funnel-labels">
          <span>Signed up · {total:,}</span>
          <span>Completed · {active:,}</span>
        </div>
        <div class="funnel-track">
          <div class="funnel-fill" style="width:{pct}%;background:{bar_color}"></div>
        </div>
        <div class="funnel-sub">
          {pct}% of people who texted in finished the sign-up questions.
          {f'First sign-up {_fmt_dt(subs.get("first_signup"))} · latest {_fmt_dt(subs.get("last_signup"))}.' if total else ''}
        </div>
      </div>
    </section>"""


def _render_blasts(blasts: list) -> str:
    if not blasts:
        return """
        <section class="card">
          <div class="card-title">Blast history</div>
          <div class="empty-state">
            <div class="empty-icon">📢</div>
            <div class="empty-msg">No blasts sent yet.</div>
            <div class="empty-sub">Text your business number with a message like<br>
              <em>"Seats available tonight at 8pm — come through!"</em><br>
              and it will go out to all your subscribers instantly.</div>
          </div>
        </section>"""

    total_sent = sum(b.get("attempted") or 0 for b in blasts)
    total_ok = sum(b.get("succeeded") or 0 for b in blasts)
    overall_rate = round((total_ok / total_sent) * 100) if total_sent else 0

    rows = []
    for b in blasts:
        attempted = b.get("attempted") or 0
        succeeded = b.get("succeeded") or 0
        rate = round((succeeded / attempted) * 100) if attempted else 0
        rate_color = "#4ade80" if rate >= 90 else "#fbbf24" if rate >= 70 else "#f87171"
        msg = b.get("body") or b.get("owner_message") or ""
        rows.append(f"""
          <tr>
            <td class="td-date">{_fmt_dt_full(b.get("sent_at"))}</td>
            <td class="td-msg" title="{_esc(msg)}">{_esc(msg[:100])}{"…" if len(msg) > 100 else ""}</td>
            <td class="td-num">{attempted:,}</td>
            <td class="td-rate" style="color:{rate_color}">{rate}%</td>
          </tr>""")

    return f"""
    <section class="card">
      <div class="card-header-row">
        <div class="card-title">Blast history</div>
        <div class="card-meta">{len(blasts)} blasts · {total_sent:,} total sends · {overall_rate}% avg delivery</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Message</th>
              <th>Sent to</th>
              <th>Delivered</th>
            </tr>
          </thead>
          <tbody>
            {"".join(rows)}
          </tbody>
        </table>
      </div>
    </section>"""


def _render_preferences(prefs: dict, signup_questions: list) -> str:
    if not prefs:
        return """
        <section class="card">
          <div class="card-title">Audience preferences</div>
          <div class="empty-state">
            <div class="empty-icon">🎯</div>
            <div class="empty-msg">No preference data yet.</div>
            <div class="empty-sub">Once subscribers answer your sign-up questions,<br>
              you'll see a breakdown of their preferences here.</div>
          </div>
        </section>"""

    # Map question key → question text from config
    q_label_map = {}
    for q in signup_questions:
        if isinstance(q, str):
            # Use first 6 words as label, key is the full string normalised
            key = q.lower().replace(" ", "_")[:40]
            q_label_map[key] = q
        elif isinstance(q, dict):
            q_label_map[q.get("key", "")] = q.get("text", q.get("key", ""))

    blocks = []
    for q_key, answers in sorted(prefs.items()):
        label = q_label_map.get(q_key, q_key.replace("_", " ").title())
        total_q = sum(a["cnt"] for a in answers)

        bars = []
        for a in answers:
            pct = round((a["cnt"] / total_q) * 100) if total_q else 0
            bars.append(f"""
              <div class="pref-row">
                <div class="pref-label">{_esc(a["answer"])}</div>
                <div class="pref-bar-wrap">
                  <div class="pref-bar" style="width:{pct}%"></div>
                </div>
                <div class="pref-pct">{pct}%&nbsp;<span class="pref-cnt">({a["cnt"]:,})</span></div>
              </div>""")

        blocks.append(f"""
          <div class="pref-block">
            <div class="pref-q">{_esc(label)}</div>
            {"".join(bars)}
            <div class="pref-total">{total_q:,} responses</div>
          </div>""")

    return f"""
    <section class="card">
      <div class="card-title">Audience preferences</div>
      <div class="pref-grid">
        {"".join(blocks)}
      </div>
    </section>"""


# ---------------------------------------------------------------------------
# Full page renderer
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
html { font-size: 16px; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #060912;
  color: #e2e8f0;
  min-height: 100vh;
  padding-bottom: 60px;
}

/* ── header ── */
.topbar {
  background: #0d1117;
  border-bottom: 1px solid #1a2035;
  padding: 14px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.topbar-brand { font-size: 13px; color: #4b5563; letter-spacing: 0.08em; text-transform: uppercase; }
.topbar-powered { font-size: 12px; color: #374151; }
.topbar-powered span { color: #4ade80; }

/* ── hero ── */
.hero { padding: 32px 24px 0; max-width: 860px; margin: 0 auto; }
.hero-inner { display: flex; align-items: center; gap: 18px; }
.hero-icon {
  width: 56px; height: 56px; border-radius: 14px;
  background: linear-gradient(135deg, #1d4ed8 0%, #7c3aed 100%);
  display: flex; align-items: center; justify-content: center;
  font-size: 24px; font-weight: 800; color: #fff; flex-shrink: 0;
}
.hero-name { font-size: 26px; font-weight: 700; color: #f8fafc; line-height: 1.2; }
.hero-meta { font-size: 13px; color: #6b7280; margin-top: 4px; }

/* badges */
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 20px;
  font-size: 11px; font-weight: 600; letter-spacing: 0.04em;
  text-transform: uppercase;
}
.badge-live    { background: #052e16; color: #4ade80; border: 1px solid #166534; }
.badge-warn    { background: #1c1917; color: #fbbf24; border: 1px solid #78350f; }
.badge-info    { background: #0c1a3a; color: #60a5fa; border: 1px solid #1e3a6e; }

/* ── stat strip ── */
.stats-strip {
  display: grid; grid-template-columns: repeat(4, 1fr);
  gap: 12px; padding: 24px 24px 0; max-width: 860px; margin: 0 auto;
}
.stat-tile {
  background: #0d1117; border: 1px solid #1a2035; border-radius: 12px;
  padding: 18px 16px; text-align: center;
}
.stat-num { font-size: 28px; font-weight: 800; color: #f8fafc; line-height: 1; }
.stat-lbl { font-size: 11px; color: #4b5563; margin-top: 6px; text-transform: uppercase; letter-spacing: 0.06em; }
.accent-purple { color: #a78bfa; }
.accent-teal   { color: #34d399; }
.accent-amber  { color: #fbbf24; }

/* ── cards ── */
.card {
  background: #0d1117; border: 1px solid #1a2035; border-radius: 14px;
  padding: 24px; margin: 16px auto 0; max-width: 860px;
}
.card-title { font-size: 14px; font-weight: 700; color: #9ca3af; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 18px; }
.card-header-row { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 18px; flex-wrap: wrap; gap: 8px; }
.card-meta { font-size: 12px; color: #4b5563; }

/* ── funnel ── */
.funnel-wrap { }
.funnel-labels { display: flex; justify-content: space-between; font-size: 12px; color: #6b7280; margin-bottom: 8px; }
.funnel-track { background: #1f2937; border-radius: 6px; height: 10px; overflow: hidden; }
.funnel-fill { height: 100%; border-radius: 6px; transition: width 0.5s ease; }
.funnel-sub { font-size: 12px; color: #4b5563; margin-top: 10px; line-height: 1.5; }

/* ── blast table ── */
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead tr { border-bottom: 1px solid #1f2937; }
th { color: #4b5563; font-weight: 600; padding: 8px 12px; text-align: left; white-space: nowrap; }
td { padding: 12px 12px; border-bottom: 1px solid #111827; color: #9ca3af; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
.td-date { white-space: nowrap; font-size: 12px; color: #4b5563; width: 1px; padding-right: 20px; }
.td-msg  { color: #d1d5db; max-width: 360px; }
.td-num  { text-align: right; width: 80px; color: #6b7280; }
.td-rate { text-align: right; width: 80px; font-weight: 700; }

/* ── preferences ── */
.pref-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 28px; }
.pref-block { }
.pref-q { font-size: 13px; color: #e2e8f0; font-weight: 600; margin-bottom: 14px; line-height: 1.4; }
.pref-row { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; }
.pref-label { font-size: 12px; color: #9ca3af; width: 90px; flex-shrink: 0; }
.pref-bar-wrap { flex: 1; background: #1f2937; border-radius: 4px; height: 8px; overflow: hidden; }
.pref-bar { height: 100%; background: linear-gradient(90deg, #34d399, #059669); border-radius: 4px; transition: width 0.4s ease; }
.pref-pct { font-size: 12px; color: #6b7280; width: 70px; text-align: right; white-space: nowrap; }
.pref-cnt { color: #374151; }
.pref-total { font-size: 11px; color: #374151; margin-top: 6px; text-align: right; }

/* ── empty states ── */
.empty-state { text-align: center; padding: 36px 24px; }
.empty-icon { font-size: 36px; margin-bottom: 12px; }
.empty-msg { font-size: 15px; font-weight: 600; color: #6b7280; margin-bottom: 8px; }
.empty-sub { font-size: 13px; color: #374151; line-height: 1.6; }

/* ── footer ── */
.footer {
  text-align: center; font-size: 12px; color: #1f2937;
  padding: 40px 24px 0; max-width: 860px; margin: 0 auto;
}

/* ── mobile ── */
@media (max-width: 600px) {
  .stats-strip { grid-template-columns: repeat(2, 1fr); }
  .card { padding: 18px 16px; border-radius: 10px; }
  .hero-name { font-size: 20px; }
  .pref-grid { grid-template-columns: 1fr; }
  .td-date { display: none; }
}
"""


def _render_page(tenant, data: dict, token: str) -> str:
    subs = data.get("subscribers", {})
    blasts = data.get("blasts", [])
    prefs = data.get("preferences", {})

    hero_html = _render_hero(tenant, subs)
    blast_html = _render_blasts(blasts)
    pref_html = _render_preferences(prefs, tenant.signup_questions)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_esc(tenant.display_name)} — Dashboard</title>
  <style>{_CSS}</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-brand">Client Dashboard</div>
  <div class="topbar-powered">Powered by <span>Zarna AI</span></div>
</div>

{hero_html}
{blast_html}
{pref_html}

<div class="footer">
  Data refreshes each time you load this page.
  Share this link only with people you trust — it gives read access to your subscriber stats.
</div>

</body>
</html>"""


def _render_error(code: int, headline: str, detail: str) -> tuple:
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{code} — Client Portal</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; background:#060912; color:#6b7280;
           display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; }}
    .box {{ text-align:center; padding:40px; }}
    .code {{ font-size:64px; font-weight:800; color:#1f2937; line-height:1; }}
    .msg  {{ font-size:18px; color:#4b5563; margin-top:12px; }}
    .sub  {{ font-size:13px; color:#1f2937; margin-top:8px; }}
  </style>
</head>
<body>
  <div class="box">
    <div class="code">{code}</div>
    <div class="msg">{_esc(headline)}</div>
    <div class="sub">{_esc(detail)}</div>
  </div>
</body>
</html>"""
    return html, code


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@portal_bp.route("/<slug>", methods=["GET"])
def client_portal(slug: str):
    registry = get_registry()
    tenant = registry.get_by_slug(slug)

    if tenant is None or _portal_token_for(slug) is None:
        return _render_error(404, "Not found", "This portal does not exist.")

    if not _check_token(slug):
        return _render_error(403, "Access denied", "Invalid or missing token.")

    data = _fetch_portal_data(slug)
    token = (request.args.get("token") or "")
    page = _render_page(tenant, data, token)
    return page, 200, {"Content-Type": "text/html; charset=utf-8"}
