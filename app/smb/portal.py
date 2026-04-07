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
            # step > 0 = answered the preference question (fully onboarded)
            # step = 0 = signed up but hasn't answered yet (still blastable)
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'active' AND onboarding_step > 0) AS active,
                    COUNT(*) FILTER (WHERE status = 'active' AND onboarding_step = 0) AS onboarding,
                    COUNT(*)                                                            AS total,
                    MIN(created_at)                                                     AS first_signup,
                    MAX(created_at)                                                     AS last_signup
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
                WHERE s.tenant_slug = %s AND s.status = 'active'
                GROUP BY p.question_key, p.answer
                ORDER BY p.question_key, cnt DESC
            """, (slug,))
            pref_rows = [dict(r) for r in cur.fetchall()]

            # Blast segment breakdown — how many sent per segment
            cur.execute("""
                SELECT COALESCE(segment, 'all') AS seg, COUNT(*) AS blast_count,
                       SUM(attempted) AS total_sent
                FROM smb_blasts
                WHERE tenant_slug = %s
                GROUP BY segment
            """, (slug,))
            blast_seg_rows = [dict(r) for r in cur.fetchall()]

        # Group preferences by question
        pref_by_question = defaultdict(list)
        for row in pref_rows:
            pref_by_question[row["question_key"]].append({
                "answer": row["answer"],
                "cnt": row["cnt"],
            })

        # Build answer → count lookup for segment size calculations
        answer_counts: dict[tuple, int] = {}
        for row in pref_rows:
            answer_counts[(row["question_key"], row["answer"].upper())] = row["cnt"]

        blast_by_seg = {r["seg"]: r for r in blast_seg_rows}

        return {
            "subscribers": subs,
            "blasts": blasts,
            "preferences": dict(pref_by_question),
            "answer_counts": answer_counts,
            "blast_by_seg": blast_by_seg,
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
    bar_gradient = (
        "linear-gradient(90deg,#10b981,#059669)" if pct >= 70
        else "linear-gradient(90deg,#f59e0b,#d97706)" if pct >= 40
        else "linear-gradient(90deg,#ef4444,#dc2626)"
    )

    if not tenant.sms_number:
        badge = '<span class="badge badge-warn"><span class="badge-dot"></span>Setup in progress</span>'
    elif total == 0:
        badge = '<span class="badge badge-info"><span class="badge-dot"></span>Waiting for first subscriber</span>'
    else:
        badge = '<span class="badge badge-live"><span class="badge-dot"></span>Live</span>'

    website = tenant.raw.get("website", "")
    website_html = (
        f'<a href="{_esc(website)}" target="_blank" rel="noopener" class="hero-website">'
        f'{_esc(website.replace("https://", "").rstrip("/"))}</a>'
    ) if website else ""

    return f"""
    <div class="hero">
      <div class="hero-card">
        <div class="hero-left">
          <div class="hero-icon">{_esc(tenant.display_name[0].upper())}</div>
          <div>
            <div class="hero-name">{_esc(tenant.display_name)}</div>
            <div class="hero-meta">
              {_esc(tenant.business_type.replace("_", " ").title())}
              &nbsp;·&nbsp;
              {_esc(tenant.raw.get("location", ""))}
            </div>
            {website_html}
          </div>
        </div>
        <div>{badge}</div>
      </div>
    </div>

    <div class="stats-strip">
      <div class="stat-tile c-indigo">
        <div class="stat-icon">👥</div>
        <div class="stat-num c-indigo">{active:,}</div>
        <div class="stat-lbl">Active subscribers</div>
      </div>
      <div class="stat-tile c-violet">
        <div class="stat-icon">⏳</div>
        <div class="stat-num c-violet">{onboarding:,}</div>
        <div class="stat-lbl">Preference pending</div>
      </div>
      <div class="stat-tile c-teal">
        <div class="stat-icon">📋</div>
        <div class="stat-num c-teal">{total:,}</div>
        <div class="stat-lbl">Total sign-ups</div>
      </div>
      <div class="stat-tile c-amber">
        <div class="stat-icon">✅</div>
        <div class="stat-num c-amber">{pct}%</div>
        <div class="stat-lbl">Completion rate</div>
      </div>
    </div>

    <div class="section">
      <div class="card">
        <div class="card-title">Sign-up funnel</div>
        <div class="funnel-labels">
          <span>Signed up · {total:,}</span>
          <span>Preference answered · {active:,}</span>
        </div>
        <div class="funnel-track">
          <div class="funnel-fill" style="width:{pct}%;background:{bar_gradient}"></div>
        </div>
        <div class="funnel-sub">
          {pct}% of subscribers answered the preference question — the rest still receive all blasts.
          {f'&nbsp;First sign-up <strong>{_fmt_dt(subs.get("first_signup"))}</strong> · most recent <strong>{_fmt_dt(subs.get("last_signup"))}</strong>.' if total else ' Share your sign-up keyword to start growing your list.'}
        </div>
      </div>
    </div>"""


def _render_blasts(blasts: list) -> str:
    if not blasts:
        return """
        <div class="section">
          <div class="card">
            <div class="card-title">Blast history</div>
            <div class="empty-state">
              <div class="empty-icon">📢</div>
              <div class="empty-msg">No blasts sent yet</div>
              <div class="empty-sub">
                Text your business number with something like<br>
                <em>"Seats available tonight at 8pm — come through!"</em><br>
                and it goes out to all your subscribers instantly.
              </div>
            </div>
          </div>
        </div>"""

    total_sent = sum(b.get("attempted") or 0 for b in blasts)
    total_ok = sum(b.get("succeeded") or 0 for b in blasts)
    overall_rate = round((total_ok / total_sent) * 100) if total_sent else 0

    rows = []
    for b in blasts:
        attempted = b.get("attempted") or 0
        succeeded = b.get("succeeded") or 0
        rate = round((succeeded / attempted) * 100) if attempted else 0
        rate_cls = "rate-green" if rate >= 90 else "rate-yellow" if rate >= 70 else "rate-red"
        sent_msg = b.get("body") or b.get("owner_message") or ""
        owner_msg = b.get("owner_message") or ""
        rows.append(f"""
          <tr>
            <td class="td-date">{_fmt_dt_full(b.get("sent_at"))}</td>
            <td class="td-msg">
              {_esc(sent_msg[:90])}{"…" if len(sent_msg) > 90 else ""}
              {f'<small>Owner sent: {_esc(owner_msg[:60])}{"…" if len(owner_msg) > 60 else ""}</small>' if owner_msg and owner_msg != sent_msg else ""}
            </td>
            <td class="td-num">{attempted:,}</td>
            <td class="td-rate"><span class="rate-pill {rate_cls}">{rate}%</span></td>
          </tr>""")

    return f"""
    <div class="section">
      <div class="card">
        <div class="card-header-row">
          <div class="card-title">Blast history</div>
          <div class="card-meta">{len(blasts)} blasts &nbsp;·&nbsp; {total_sent:,} sends &nbsp;·&nbsp; {overall_rate}% avg delivery</div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Message</th>
                <th style="text-align:right">Sent to</th>
                <th style="text-align:right">Delivered</th>
              </tr>
            </thead>
            <tbody>
              {"".join(rows)}
            </tbody>
          </table>
        </div>
      </div>
    </div>"""


def _render_segments(tenant, answer_counts: dict, blast_by_seg: dict, active_total: int) -> str:
    """Segment audience breakdown — one tile per configured segment."""
    if not tenant.segments:
        return ""

    tiles = []
    for seg in tenant.segments:
        name = seg["name"]
        q_key = seg["question_key"]
        answers = [a.upper() for a in seg["answers"]]

        # Count subscribers in this segment (union of all matching answers)
        seen_ids: set = set()
        count = sum(
            answer_counts.get((q_key, a), 0)
            for a in answers
        )
        # Deduplicate: if subscriber answered BOTH, they appear in STANDUP and IMPROV
        # The portal shows raw counts (each person counted in every segment they belong to)
        pct = round((count / active_total) * 100) if active_total else 0

        blasts_info = blast_by_seg.get(name.lower(), blast_by_seg.get(name, {}))
        blast_cnt = blasts_info.get("blast_count", 0) if blasts_info else 0

        desc = _esc(seg.get("description", name))

        tiles.append(f"""
          <div class="seg-tile">
            <div class="seg-name">{_esc(name)}</div>
            <div class="seg-count">{count:,}</div>
            <div class="seg-bar-wrap">
              <div class="seg-bar" style="width:{pct}%"></div>
            </div>
            <div class="seg-meta">{pct}% of active &nbsp;·&nbsp; {blast_cnt} blast{"s" if blast_cnt != 1 else ""}</div>
            <div class="seg-desc">{desc}</div>
          </div>""")

    return f"""
    <div class="section">
      <div class="card">
        <div class="card-title">Audience segments</div>
        <div class="seg-grid">
          {"".join(tiles)}
        </div>
        <div class="seg-note">
          Segments overlap — subscribers who chose "BOTH" appear in both STANDUP and IMPROV.
          Use a segment prefix when texting your number to target a group:
          <code>STANDUP: Great show tonight 8pm!</code>
        </div>
      </div>
    </div>"""


def _render_preferences(prefs: dict, signup_questions: list) -> str:
    if not prefs:
        return """
        <div class="section">
          <div class="card">
            <div class="card-title">Audience preferences</div>
            <div class="empty-state">
              <div class="empty-icon">🎯</div>
              <div class="empty-msg">No preference data yet</div>
              <div class="empty-sub">
                Once your subscribers answer the sign-up questions,
                you'll see a breakdown of their preferences here —
                like which types of content they want and how often.
              </div>
            </div>
          </div>
        </div>"""

    q_label_map = {}
    for q in signup_questions:
        if isinstance(q, str):
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
                <div class="pref-pct">{pct}% <span class="pref-cnt">({a["cnt"]:,})</span></div>
              </div>""")

        blocks.append(f"""
          <div class="pref-block">
            <div class="pref-q">{_esc(label)}</div>
            {"".join(bars)}
            <div class="pref-total">Based on {total_q:,} response{"s" if total_q != 1 else ""}</div>
          </div>""")

    return f"""
    <div class="section">
      <div class="card">
        <div class="card-title">Audience preferences</div>
        <div class="pref-grid">
          {"".join(blocks)}
        </div>
      </div>
    </div>"""


# ---------------------------------------------------------------------------
# Full page renderer
# ---------------------------------------------------------------------------

_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

* { box-sizing: border-box; margin: 0; padding: 0; }
html { font-size: 16px; -webkit-font-smoothing: antialiased; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #f3f4f6;
  color: #111827;
  min-height: 100vh;
  padding-bottom: 80px;
}

/* ── topbar ── */
.topbar {
  background: #fff;
  border-bottom: 1px solid #e5e7eb;
  padding: 0 32px;
  height: 56px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 10;
}
.topbar-brand {
  font-size: 13px; font-weight: 600; color: #6b7280;
  letter-spacing: 0.06em; text-transform: uppercase;
}
.topbar-powered { font-size: 12px; color: #9ca3af; display: flex; align-items: center; gap: 5px; }
.topbar-dot {
  width: 6px; height: 6px; border-radius: 50%;
  background: #10b981; display: inline-block;
}

/* ── hero ── */
.hero { padding: 36px 32px 0; max-width: 900px; margin: 0 auto; }
.hero-card {
  background: #fff;
  border-radius: 16px;
  box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 4px 16px rgba(0,0,0,.04);
  padding: 28px 32px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  flex-wrap: wrap;
}
.hero-left { display: flex; align-items: center; gap: 18px; }
.hero-icon {
  width: 60px; height: 60px; border-radius: 14px;
  background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
  display: flex; align-items: center; justify-content: center;
  font-size: 26px; font-weight: 800; color: #fff; flex-shrink: 0;
  box-shadow: 0 4px 12px rgba(79,70,229,.3);
}
.hero-name { font-size: 22px; font-weight: 800; color: #111827; line-height: 1.2; }
.hero-meta { font-size: 13px; color: #9ca3af; margin-top: 4px; }
.hero-website { font-size: 12px; color: #6366f1; text-decoration: none; margin-top: 3px; display: inline-block; }
.hero-website:hover { text-decoration: underline; }

/* badges */
.badge {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 4px 10px; border-radius: 20px;
  font-size: 11px; font-weight: 600; letter-spacing: 0.04em;
  text-transform: uppercase;
}
.badge-dot { width: 6px; height: 6px; border-radius: 50%; }
.badge-live { background: #d1fae5; color: #065f46; }
.badge-live .badge-dot { background: #10b981; }
.badge-warn { background: #fef3c7; color: #92400e; }
.badge-warn .badge-dot { background: #f59e0b; }
.badge-info { background: #dbeafe; color: #1e40af; }
.badge-info .badge-dot { background: #3b82f6; }

/* ── stat strip ── */
.stats-strip {
  display: grid; grid-template-columns: repeat(4, 1fr);
  gap: 16px; padding: 20px 32px 0; max-width: 900px; margin: 0 auto;
}
.stat-tile {
  background: #fff;
  border-radius: 14px;
  box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 2px 8px rgba(0,0,0,.03);
  padding: 20px 20px 18px;
  border-top: 3px solid transparent;
}
.stat-tile.c-indigo { border-color: #6366f1; }
.stat-tile.c-violet { border-color: #8b5cf6; }
.stat-tile.c-teal   { border-color: #14b8a6; }
.stat-tile.c-amber  { border-color: #f59e0b; }
.stat-icon { font-size: 18px; margin-bottom: 10px; }
.stat-num { font-size: 32px; font-weight: 800; color: #111827; line-height: 1; letter-spacing: -0.02em; }
.stat-num.c-indigo { color: #4f46e5; }
.stat-num.c-violet { color: #7c3aed; }
.stat-num.c-teal   { color: #0d9488; }
.stat-num.c-amber  { color: #d97706; }
.stat-lbl { font-size: 12px; font-weight: 500; color: #6b7280; margin-top: 6px; }

/* ── section wrapper ── */
.section { padding: 20px 32px 0; max-width: 900px; margin: 0 auto; }

/* ── cards ── */
.card {
  background: #fff;
  border-radius: 14px;
  box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 2px 8px rgba(0,0,0,.03);
  padding: 28px 28px;
}
.card + .card { margin-top: 16px; }
.card-title {
  font-size: 13px; font-weight: 700; color: #6b7280;
  text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 20px;
}
.card-header-row {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 20px; flex-wrap: wrap; gap: 8px;
}
.card-meta {
  font-size: 12px; font-weight: 500; color: #9ca3af;
  background: #f9fafb; padding: 4px 10px; border-radius: 20px;
}

/* ── funnel ── */
.funnel-labels {
  display: flex; justify-content: space-between;
  font-size: 13px; font-weight: 500; color: #374151; margin-bottom: 10px;
}
.funnel-labels span:last-child { color: #6b7280; }
.funnel-track {
  background: #f3f4f6; border-radius: 8px; height: 12px;
  overflow: hidden; box-shadow: inset 0 1px 2px rgba(0,0,0,.06);
}
.funnel-fill { height: 100%; border-radius: 8px; }
.funnel-sub {
  font-size: 13px; color: #9ca3af; margin-top: 12px; line-height: 1.6;
}

/* ── blast table ── */
.table-wrap { overflow-x: auto; margin: 0 -4px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead tr { border-bottom: 2px solid #f3f4f6; }
th {
  color: #9ca3af; font-weight: 600; font-size: 11px; text-transform: uppercase;
  letter-spacing: 0.06em; padding: 0 12px 12px; text-align: left; white-space: nowrap;
}
td { padding: 14px 12px; border-bottom: 1px solid #f9fafb; color: #374151; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tbody tr:hover td { background: #fafafa; }
.td-date { white-space: nowrap; font-size: 12px; color: #9ca3af; width: 1px; padding-right: 24px; }
.td-msg  { color: #111827; font-weight: 500; max-width: 380px; }
.td-msg small { display: block; font-size: 11px; color: #9ca3af; font-weight: 400; margin-top: 2px; }
.td-num  { text-align: right; width: 80px; color: #6b7280; font-variant-numeric: tabular-nums; }
.td-rate { text-align: right; width: 90px; font-weight: 700; font-variant-numeric: tabular-nums; }
.rate-pill {
  display: inline-block; padding: 2px 8px; border-radius: 20px;
  font-size: 12px; font-weight: 700;
}
.rate-green  { background: #d1fae5; color: #065f46; }
.rate-yellow { background: #fef3c7; color: #92400e; }
.rate-red    { background: #fee2e2; color: #991b1b; }

/* ── preferences ── */
.pref-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 32px; }
.pref-block { }
.pref-q {
  font-size: 13px; font-weight: 700; color: #374151;
  margin-bottom: 16px; line-height: 1.4; padding-bottom: 10px;
  border-bottom: 1px solid #f3f4f6;
}
.pref-row { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
.pref-label { font-size: 12px; font-weight: 600; color: #374151; width: 100px; flex-shrink: 0; }
.pref-bar-wrap {
  flex: 1; background: #f3f4f6; border-radius: 6px; height: 8px;
  overflow: hidden;
}
.pref-bar {
  height: 100%;
  background: linear-gradient(90deg, #6366f1, #8b5cf6);
  border-radius: 6px;
}
.pref-pct { font-size: 12px; font-weight: 700; color: #374151; width: 36px; text-align: right; }
.pref-cnt { font-size: 11px; color: #9ca3af; font-weight: 400; }
.pref-total {
  font-size: 11px; color: #9ca3af; margin-top: 4px;
  display: flex; align-items: center; gap: 4px;
}

/* ── empty states ── */
.empty-state {
  text-align: center; padding: 48px 24px;
  border: 1.5px dashed #e5e7eb; border-radius: 12px;
}
.empty-icon { font-size: 32px; margin-bottom: 14px; opacity: .7; }
.empty-msg { font-size: 15px; font-weight: 700; color: #374151; margin-bottom: 8px; }
.empty-sub { font-size: 13px; color: #9ca3af; line-height: 1.7; max-width: 340px; margin: 0 auto; }
.empty-sub em { color: #6b7280; font-style: normal; font-weight: 500; }

/* ── divider ── */
.divider { height: 1px; background: #f3f4f6; margin: 20px 0; }

/* ── segment tiles ── */
.seg-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 14px; margin-bottom: 16px;
}
.seg-tile {
  background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 10px;
  padding: 16px 14px;
}
.seg-name {
  font-size: 11px; font-weight: 700; color: #6366f1;
  text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px;
}
.seg-count { font-size: 28px; font-weight: 800; color: #111827; line-height: 1; margin-bottom: 8px; letter-spacing: -0.02em; }
.seg-bar-wrap { background: #e5e7eb; border-radius: 4px; height: 4px; overflow: hidden; margin-bottom: 8px; }
.seg-bar { height: 100%; background: linear-gradient(90deg, #6366f1, #8b5cf6); border-radius: 4px; }
.seg-meta { font-size: 11px; color: #9ca3af; margin-bottom: 4px; }
.seg-desc { font-size: 11px; color: #6b7280; }
.seg-note {
  font-size: 12px; color: #9ca3af; line-height: 1.6;
  border-top: 1px solid #f3f4f6; padding-top: 14px;
}
.seg-note code {
  background: #f3f4f6; padding: 1px 6px; border-radius: 4px;
  font-size: 11px; color: #374151;
}

/* ── footer ── */
.footer {
  text-align: center; font-size: 12px; color: #d1d5db;
  padding: 40px 32px 0; max-width: 900px; margin: 0 auto;
}
.footer a { color: #9ca3af; text-decoration: none; }
.footer a:hover { color: #6b7280; }

/* ── mobile ── */
@media (max-width: 640px) {
  .topbar { padding: 0 16px; }
  .hero { padding: 20px 16px 0; }
  .hero-card { padding: 20px; }
  .stats-strip { grid-template-columns: repeat(2, 1fr); gap: 10px; padding: 14px 16px 0; }
  .section { padding: 14px 16px 0; }
  .card { padding: 20px 18px; }
  .hero-name { font-size: 18px; }
  .stat-num { font-size: 26px; }
  .pref-grid { grid-template-columns: 1fr; }
  .td-date { display: none; }
  .hero-card { flex-direction: column; align-items: flex-start; }
}
"""


def _render_page(tenant, data: dict, token: str) -> str:
    subs = data.get("subscribers", {})
    blasts = data.get("blasts", [])
    prefs = data.get("preferences", {})
    answer_counts = data.get("answer_counts", {})
    blast_by_seg = data.get("blast_by_seg", {})

    hero_html = _render_hero(tenant, subs)
    seg_html = _render_segments(tenant, answer_counts, blast_by_seg, subs.get("active") or 0)
    blast_html = _render_blasts(blasts)
    pref_html = _render_preferences(prefs, tenant.signup_questions)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_esc(tenant.display_name)} — Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <style>{_CSS}</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-brand">Client Dashboard</div>
  <div class="topbar-powered">
    <span class="topbar-dot"></span>
    Powered by Zarna AI
  </div>
</div>

{hero_html}
{seg_html}
{blast_html}
{pref_html}

<div class="footer">
  Data refreshes every time you load this page &nbsp;·&nbsp;
  Keep this link private — it provides read access to your subscriber stats.
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
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f3f4f6; color: #374151;
           display: flex; align-items: center; justify-content: center;
           min-height: 100vh; margin: 0; }}
    .box {{ text-align: center; padding: 40px; max-width: 360px; }}
    .code {{
      font-size: 72px; font-weight: 800; color: #e5e7eb; line-height: 1;
      letter-spacing: -0.04em;
    }}
    .msg  {{ font-size: 17px; font-weight: 700; color: #374151; margin-top: 16px; }}
    .sub  {{ font-size: 13px; color: #9ca3af; margin-top: 8px; line-height: 1.6; }}
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
