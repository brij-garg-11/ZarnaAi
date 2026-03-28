"""
Admin analytics dashboard — password-protected read-only view of all activity.

Access: https://your-railway-url.app/admin
Login: HTTP Basic Auth — username anything, password = ADMIN_PASSWORD env var

Tabs:
  /admin              → Overview (stats + charts)
  /admin?tab=audience → Audience (tags, fan profiles, location)
  /admin?tab=convos   → Conversations (searchable, filterable)
"""

import csv
import io
import os
from collections import Counter
from datetime import datetime, timezone

from flask import Blueprint, Response, request

admin_bp = Blueprint("admin", __name__)

_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")


def _check_auth():
    if not _ADMIN_PASSWORD:
        return True
    auth = request.authorization
    return auth and auth.password == _ADMIN_PASSWORD


def _require_auth():
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="Zarna AI Admin"'},
    )


def _get_db():
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        return None
    import psycopg2
    dsn = database_url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def _fetch_export(tag_filter="", location_filter=""):
    """Query fans matching the given filters and return rows for CSV export."""
    conn = _get_db()
    if not conn:
        return []
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if tag_filter and location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                      AND LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (tag_filter.lower(), f"%{location_filter.lower()}%"))
            elif tag_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                """, (tag_filter.lower(),))
            elif location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (f"%{location_filter.lower()}%",))
            else:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts ORDER BY created_at DESC
                """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@admin_bp.route("/admin/export")
def admin_export():
    if not _check_auth():
        return _require_auth()

    tag_filter      = request.args.get("tag", "").strip().lower()
    location_filter = request.args.get("location", "").strip()

    rows = _fetch_export(tag_filter=tag_filter, location_filter=location_filter)

    # Build CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["phone_number", "fan_memory", "fan_tags", "fan_location", "joined_at"])
    for r in rows:
        writer.writerow([
            r["phone_number"],
            r.get("fan_memory") or "",
            ", ".join(r.get("fan_tags") or []),
            r.get("fan_location") or "",
            r["created_at"].strftime("%Y-%m-%d") if r.get("created_at") else "",
        ])

    # Build a descriptive filename
    parts = []
    if tag_filter:
        parts.append(tag_filter)
    if location_filter:
        parts.append(location_filter.replace(" ", "-").lower())
    if not parts:
        parts.append("all-fans")
    filename = f"zarna-fans-{'_'.join(parts)}-{datetime.now().strftime('%Y%m%d')}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _fetch_stats(tag_filter="", phone_search="", location_filter=""):
    conn = _get_db()
    if not conn:
        return None
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # ── Core counts ────────────────────────────────────────────────
            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
            total_subscribers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role = 'user'")
            total_messages = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '24 hours'")
            messages_today = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '7 days'")
            messages_week = cur.fetchone()[0]

            # ── Trend: this week vs last week ──────────────────────────────
            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '14 days' AND created_at < NOW()-INTERVAL '7 days'")
            messages_prev_week = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE created_at >= NOW()-INTERVAL '7 days'")
            new_subscribers_week = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE created_at >= NOW()-INTERVAL '14 days' AND created_at < NOW()-INTERVAL '7 days'")
            new_subscribers_prev_week = cur.fetchone()[0]

            # ── Charts ─────────────────────────────────────────────────────
            cur.execute("""
                SELECT DATE(created_at AT TIME ZONE 'America/New_York') as day, COUNT(*) as cnt
                FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '14 days'
                GROUP BY day ORDER BY day
            """)
            messages_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]

            cur.execute("""
                SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int as hr, COUNT(*) as cnt
                FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '30 days'
                GROUP BY hr ORDER BY hr
            """)
            rows = cur.fetchall()
            hour_map = {r["hr"]: r["cnt"] for r in rows}
            messages_by_hour = [hour_map.get(h, 0) for h in range(24)]

            # ── Top messages ───────────────────────────────────────────────
            cur.execute("""
                SELECT LOWER(TRIM(text)) as msg, COUNT(*) as cnt
                FROM messages WHERE role='user'
                GROUP BY LOWER(TRIM(text))
                ORDER BY cnt DESC LIMIT 20
            """)
            top_messages = [(r["msg"], r["cnt"]) for r in cur.fetchall()]

            # ── Area codes ─────────────────────────────────────────────────
            cur.execute("SELECT phone_number FROM contacts")
            all_phones = [r[0] for r in cur.fetchall()]
            area_codes = Counter()
            for p in all_phones:
                digits = "".join(c for c in p if c.isdigit())
                if len(digits) == 11 and digits[0] == "1":
                    area_codes[digits[1:4]] += 1
                elif len(digits) == 10:
                    area_codes[digits[:3]] += 1
            top_area_codes = area_codes.most_common(15)

            # ── Conversations (with optional phone or tag filter) ──────────
            if phone_search:
                cur.execute("""
                    SELECT phone_number, role, text, created_at
                    FROM messages
                    WHERE phone_number LIKE %s
                    ORDER BY created_at DESC LIMIT 200
                """, (f"%{phone_search}%",))
            elif tag_filter:
                cur.execute("""
                    SELECT m.phone_number, m.role, m.text, m.created_at
                    FROM messages m
                    JOIN contacts c ON c.phone_number = m.phone_number
                    WHERE %s = ANY(c.fan_tags)
                    ORDER BY m.created_at DESC LIMIT 200
                """, (tag_filter.lower(),))
            else:
                cur.execute("""
                    SELECT phone_number, role, text, created_at
                    FROM messages ORDER BY created_at DESC LIMIT 200
                """)
            conversations = [
                {
                    "phone": r["phone_number"],
                    "role": r["role"],
                    "text": r["text"],
                    "time": r["created_at"].strftime("%m/%d %I:%M%p") if r["created_at"] else "",
                }
                for r in cur.fetchall()
            ]

            # ── Audience tags ──────────────────────────────────────────────
            cur.execute("""
                SELECT UNNEST(fan_tags) as tag, COUNT(*) as cnt
                FROM contacts
                WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                GROUP BY tag ORDER BY cnt DESC LIMIT 30
            """)
            tag_breakdown = [(r["tag"], r["cnt"]) for r in cur.fetchall()]

            # ── Fan profiles (supports tag + location filtering) ───────────
            if tag_filter and location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE %s = ANY(fan_tags) AND LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (tag_filter.lower(), f"%{location_filter.lower()}%"))
            elif tag_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                """, (tag_filter.lower(),))
            elif location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (f"%{location_filter.lower()}%",))
            else:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE fan_memory IS NOT NULL AND fan_memory != ''
                    ORDER BY created_at DESC LIMIT 100
                """)
            fan_profiles = [dict(r) for r in cur.fetchall()]

            # ── Fans with profiles count ───────────────────────────────────
            cur.execute("SELECT COUNT(*) FROM contacts WHERE fan_memory IS NOT NULL AND fan_memory != ''")
            profiled_fans = cur.fetchone()[0]

        return {
            "total_subscribers": total_subscribers,
            "total_messages": total_messages,
            "messages_today": messages_today,
            "messages_week": messages_week,
            "messages_prev_week": messages_prev_week,
            "new_subscribers_week": new_subscribers_week,
            "new_subscribers_prev_week": new_subscribers_prev_week,
            "profiled_fans": profiled_fans,
            "messages_by_day": messages_by_day,
            "messages_by_hour": messages_by_hour,
            "top_messages": top_messages,
            "top_area_codes": top_area_codes,
            "conversations": conversations,
            "tag_breakdown": tag_breakdown,
            "fan_profiles": fan_profiles,
            "tag_filter": tag_filter,
            "phone_search": phone_search,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        }
    finally:
        conn.close()


def _trend_html(current, previous, label="vs last week"):
    if previous == 0:
        return f'<span style="color:#64748b;font-size:12px">{label}</span>'
    diff = current - previous
    pct = round(abs(diff) / previous * 100)
    if diff > 0:
        return f'<span style="color:#10b981;font-size:12px">▲ {pct}% {label}</span>'
    elif diff < 0:
        return f'<span style="color:#f87171;font-size:12px">▼ {pct}% {label}</span>'
    else:
        return f'<span style="color:#64748b;font-size:12px">— same {label}</span>'


@admin_bp.route("/admin")
def admin():
    if not _check_auth():
        return _require_auth()

    tab             = request.args.get("tab", "overview").strip().lower()
    tag_filter      = request.args.get("tag", "").strip().lower()
    phone_search    = request.args.get("phone", "").strip()
    location_filter = request.args.get("location", "").strip()

    # Tag or location filter implies audience tab
    if (tag_filter or location_filter) and tab == "overview":
        tab = "audience"

    stats = _fetch_stats(tag_filter=tag_filter, phone_search=phone_search, location_filter=location_filter)
    if stats is None:
        return "<h2 style='font-family:sans-serif;padding:40px'>No database configured (DATABASE_URL not set).</h2>", 503

    # ── Pre-render chart data ──────────────────────────────────────────────
    days_labels = [d for d, _ in stats["messages_by_day"]]
    days_data   = [c for _, c in stats["messages_by_day"]]
    hour_labels = [f"{h}" for h in range(24)]
    hour_data   = stats["messages_by_hour"]

    # ── Trend badges ──────────────────────────────────────────────────────
    week_trend = _trend_html(stats["messages_week"], stats["messages_prev_week"])
    sub_trend  = _trend_html(stats["new_subscribers_week"], stats["new_subscribers_prev_week"], "new vs last week")

    # ── Top messages ──────────────────────────────────────────────────────
    top_msgs_html = ""
    for i, (msg, cnt) in enumerate(stats["top_messages"], 1):
        msg_e = msg.replace("<", "&lt;").replace(">", "&gt;")
        top_msgs_html += f"""
        <div class="top-msg-row">
          <span class="top-msg-rank">#{i}</span>
          <span class="top-msg-text">{msg_e}</span>
          <span class="top-msg-badge">{cnt}</span>
        </div>"""

    # ── Area codes ────────────────────────────────────────────────────────
    area_html = ""
    for ac, cnt in stats["top_area_codes"]:
        area_html += f"""
        <div class="area-row">
          <span>({ac})</span>
          <span class="area-cnt">{cnt}</span>
        </div>"""

    # ── Tag pills ─────────────────────────────────────────────────────────
    tag_breakdown_html = ""
    if stats["tag_breakdown"]:
        for tag, cnt in stats["tag_breakdown"]:
            active = "tag-pill-active" if tag == tag_filter else ""
            tag_breakdown_html += f'<a href="/admin?tab=audience&tag={tag}" class="tag-pill {active}">{tag} <span class="tag-cnt">{cnt}</span></a>'
    else:
        tag_breakdown_html = '<p class="empty-note">Tags build automatically as fans text in after the next show.</p>'

    # ── Fan profiles ──────────────────────────────────────────────────────
    fan_profiles_html = ""
    for fan in stats["fan_profiles"]:
        phone_d   = fan["phone_number"][-4:]
        mem_e     = (fan["fan_memory"] or "").replace("<", "&lt;").replace(">", "&gt;")
        loc       = fan.get("fan_location") or ""
        loc_html  = f'<span class="fan-loc">📍 {loc}</span>' if loc else ""
        tags_html = " ".join(
            f'<a href="/admin?tab=audience&tag={t}" class="fan-tag">{t}</a>'
            for t in (fan["fan_tags"] or [])
        )
        fan_profiles_html += f"""
        <div class="fan-card">
          <div class="fan-header">
            <span class="fan-phone">...{phone_d}</span>
            {loc_html}
          </div>
          <p class="fan-memory">{mem_e or "<em class='empty-note'>No profile yet</em>"}</p>
          <div class="fan-tags">{tags_html}</div>
        </div>"""

    if not fan_profiles_html:
        fan_profiles_html = '<p class="empty-note">Fan profiles build automatically as fans text in. Check back after the next show.</p>'

    # ── Conversations table ───────────────────────────────────────────────
    rows_html = ""
    for msg in stats["conversations"]:
        role_cls  = "badge-fan" if msg["role"] == "user" else "badge-bot"
        role_lbl  = "Fan" if msg["role"] == "user" else "Bot"
        phone_d   = msg["phone"][-4:] if len(msg["phone"]) >= 4 else msg["phone"]
        text_e    = msg["text"].replace("<", "&lt;").replace(">", "&gt;")
        rows_html += f"""
        <tr>
          <td class="col-phone">...{phone_d}</td>
          <td class="col-role"><span class="badge {role_cls}">{role_lbl}</span></td>
          <td class="col-msg">{text_e}</td>
          <td class="col-time">{msg["time"]}</td>
        </tr>"""

    # ── Active filter banner ──────────────────────────────────────────────
    filter_banner = ""
    if tag_filter:
        filter_banner = f"""
        <div class="filter-banner">
          <span>Filtering by tag: <strong>{tag_filter}</strong> — {len(stats["fan_profiles"])} fan{"s" if len(stats["fan_profiles"]) != 1 else ""}</span>
          <a href="/admin?tab=audience" class="filter-clear">✕ Clear</a>
        </div>"""
    elif phone_search:
        filter_banner = f"""
        <div class="filter-banner">
          <span>Searching phone: <strong>{phone_search}</strong> — {len(stats["conversations"])} message{"s" if len(stats["conversations"]) != 1 else ""}</span>
          <a href="/admin?tab=convos" class="filter-clear">✕ Clear</a>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zarna AI — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
/* ── Reset & Base ── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: #0a0f1e; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; min-height: 100vh; }}

/* ── Header ── */
.header {{ background: linear-gradient(135deg, #7c3aed 0%, #4f46e5 60%, #0891b2 100%); padding: 20px 28px; display: flex; justify-content: space-between; align-items: center; }}
.header-left {{ display: flex; align-items: center; gap: 16px; }}
.header-logo {{ font-size: 22px; font-weight: 800; color: white; letter-spacing: -0.5px; }}
.header-logo span {{ color: rgba(255,255,255,0.6); font-weight: 400; font-size: 14px; margin-left: 8px; }}
.header-right {{ display: flex; align-items: center; gap: 12px; }}
.refresh-btn {{ background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.25); color: white; border-radius: 8px; padding: 7px 14px; font-size: 13px; cursor: pointer; transition: background 0.2s; }}
.refresh-btn:hover {{ background: rgba(255,255,255,0.25); }}
.refresh-btn.active {{ background: rgba(16,185,129,0.3); border-color: #10b981; }}
.updated-time {{ color: rgba(255,255,255,0.55); font-size: 12px; }}

/* ── Navigation tabs ── */
.nav-tabs {{ background: #111827; border-bottom: 1px solid #1f2937; padding: 0 28px; display: flex; gap: 4px; }}
.nav-tab {{ padding: 14px 20px; font-size: 14px; font-weight: 500; color: #64748b; text-decoration: none; border-bottom: 2px solid transparent; transition: all 0.15s; white-space: nowrap; }}
.nav-tab:hover {{ color: #e2e8f0; }}
.nav-tab.active {{ color: #a78bfa; border-bottom-color: #7c3aed; }}

/* ── Container ── */
.container {{ max-width: 1400px; margin: 0 auto; padding: 24px 28px; }}

/* ── Stat cards ── */
.stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 24px; }}
.stat-card {{ background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 20px 22px; transition: border-color 0.2s; }}
.stat-card:hover {{ border-color: #374151; }}
.stat-label {{ color: #6b7280; font-size: 12px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 10px; }}
.stat-value {{ font-size: 34px; font-weight: 800; color: white; line-height: 1; margin-bottom: 8px; }}
.stat-value.purple {{ color: #a78bfa; }}
.stat-value.teal {{ color: #2dd4bf; }}
.stat-value.green {{ color: #4ade80; }}
.stat-trend {{ font-size: 12px; }}

/* ── Cards ── */
.card {{ background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 20px 22px; margin-bottom: 20px; }}
.card-title {{ color: #9ca3af; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 16px; }}
.grid-2 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 14px; margin-bottom: 20px; }}
.grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; margin-bottom: 20px; }}

/* ── Top messages ── */
.top-msg-row {{ display: flex; align-items: center; gap: 10px; padding: 9px 0; border-bottom: 1px solid #1f2937; }}
.top-msg-rank {{ color: #4b5563; font-size: 11px; min-width: 24px; }}
.top-msg-text {{ color: #d1d5db; font-size: 13px; flex: 1; }}
.top-msg-badge {{ background: #312e81; color: #a5b4fc; padding: 2px 9px; border-radius: 10px; font-size: 12px; font-weight: 600; }}

/* ── Area codes ── */
.area-row {{ display: flex; justify-content: space-between; padding: 7px 0; border-bottom: 1px solid #1f2937; font-size: 13px; }}
.area-cnt {{ color: #2dd4bf; font-weight: 600; }}

/* ── Tag pills ── */
.tag-pill {{ display: inline-flex; align-items: center; gap: 6px; background: #1e3a5f; color: #93c5fd; padding: 5px 12px; border-radius: 20px; font-size: 13px; text-decoration: none; margin: 4px; transition: background 0.15s; }}
.tag-pill:hover {{ background: #1d4ed8; color: white; }}
.tag-pill-active {{ background: #1d4ed8; color: white; }}
.tag-cnt {{ background: #3b82f6; color: white; border-radius: 10px; padding: 1px 7px; font-size: 11px; }}

/* ── Fan cards ── */
.fan-card {{ padding: 14px 0; border-bottom: 1px solid #1f2937; }}
.fan-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }}
.fan-phone {{ color: #6b7280; font-size: 12px; font-family: monospace; }}
.fan-loc {{ color: #fbbf24; font-size: 12px; }}
.fan-memory {{ color: #d1d5db; font-size: 14px; margin-bottom: 8px; line-height: 1.4; }}
.fan-tags {{ display: flex; flex-wrap: wrap; gap: 4px; }}
.fan-tag {{ background: #1e3a5f; color: #93c5fd; padding: 2px 8px; border-radius: 8px; font-size: 11px; text-decoration: none; }}
.fan-tag:hover {{ background: #1d4ed8; color: white; }}

/* ── Conversations ── */
.search-row {{ display: flex; gap: 10px; margin-bottom: 16px; }}
.search-input {{ flex: 1; background: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 9px 14px; color: #e2e8f0; font-size: 14px; outline: none; }}
.search-input:focus {{ border-color: #7c3aed; }}
.search-btn {{ background: #7c3aed; color: white; border: none; border-radius: 8px; padding: 9px 18px; font-size: 14px; cursor: pointer; white-space: nowrap; }}
.search-btn:hover {{ background: #6d28d9; }}
.export-btn {{ display:inline-flex;align-items:center;gap:6px;background:#064e3b;color:#6ee7b7;border:1px solid #065f46;border-radius:8px;padding:9px 18px;font-size:14px;text-decoration:none;transition:background 0.15s; }}
.export-btn:hover {{ background:#065f46;color:white; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ color: #6b7280; font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; padding: 8px 12px; text-align: left; border-bottom: 1px solid #1f2937; white-space: nowrap; }}
.col-phone {{ color: #6b7280; font-size: 12px; font-family: monospace; white-space: nowrap; }}
.col-role {{ white-space: nowrap; }}
.col-msg {{ max-width: 520px; word-break: break-word; color: #d1d5db; line-height: 1.4; padding: 10px 12px; }}
.col-time {{ color: #6b7280; font-size: 12px; white-space: nowrap; }}
tr:hover td {{ background: rgba(124,58,237,0.04); }}
.badge {{ padding: 2px 8px; border-radius: 8px; font-size: 11px; font-weight: 600; }}
.badge-fan {{ background: #312e81; color: #a5b4fc; }}
.badge-bot {{ background: #064e3b; color: #6ee7b7; }}

/* ── Filter banner ── */
.filter-banner {{ background: #1e3a5f; border: 1px solid #2563eb; border-radius: 8px; padding: 10px 18px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; font-size: 13px; color: #93c5fd; }}
.filter-clear {{ color: #6b7280; text-decoration: none; font-size: 13px; }}
.filter-clear:hover {{ color: #e2e8f0; }}

/* ── Misc ── */
.empty-note {{ color: #6b7280; font-size: 13px; font-style: italic; padding: 8px 0; }}
.section-divider {{ height: 1px; background: #1f2937; margin: 8px 0 20px; }}
.tab-content {{ display: none; }}
.tab-content.active {{ display: block; }}

/* ── Mobile ── */
@media (max-width: 768px) {{
  .header {{ padding: 16px; }}
  .header-logo {{ font-size: 18px; }}
  .nav-tab {{ padding: 12px 14px; font-size: 13px; }}
  .container {{ padding: 16px; }}
  .stats-grid {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
  .stat-value {{ font-size: 26px; }}
  .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
  .col-msg {{ max-width: 200px; font-size: 12px; }}
  th, td {{ padding: 8px; }}
  .updated-time {{ display: none; }}
}}
@media (max-width: 480px) {{
  .stats-grid {{ grid-template-columns: 1fr 1fr; }}
  .nav-tabs {{ padding: 0 12px; gap: 0; }}
  .nav-tab {{ padding: 10px 10px; font-size: 12px; }}
}}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div class="header-left">
    <div class="header-logo">Zarna AI <span>Dashboard</span></div>
  </div>
  <div class="header-right">
    <button class="refresh-btn" id="autoRefreshBtn" onclick="toggleAutoRefresh()">⟳ Auto-refresh: Off</button>
    <span class="updated-time">Updated: {stats["generated_at"]}</span>
  </div>
</div>

<!-- Navigation Tabs -->
<nav class="nav-tabs">
  <a href="/admin?tab=overview"  class="nav-tab {'active' if tab == 'overview'  else ''}">📊 Overview</a>
  <a href="/admin?tab=audience"  class="nav-tab {'active' if tab == 'audience'  else ''}">👥 Audience</a>
  <a href="/admin?tab=convos"    class="nav-tab {'active' if tab == 'convos'    else ''}">💬 Conversations</a>
</nav>

<div class="container">

  {filter_banner}

  <!-- ══════════════════════════════════════════════════════════ OVERVIEW -->
  <div class="tab-content {'active' if tab == 'overview' else ''}" id="tab-overview">

    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Total Subscribers</div>
        <div class="stat-value">{stats["total_subscribers"]:,}</div>
        <div class="stat-trend">{sub_trend}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Messages</div>
        <div class="stat-value purple">{stats["total_messages"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">from fans (all time)</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Messages Today</div>
        <div class="stat-value teal">{stats["messages_today"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">last 24 hours</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">This Week</div>
        <div class="stat-value green">{stats["messages_week"]:,}</div>
        <div class="stat-trend">{week_trend}</div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Messages Per Day — Last 14 Days</div>
        <canvas id="dayChart" height="110"></canvas>
      </div>
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Activity by Hour (ET)</div>
        <canvas id="hourChart" height="110"></canvas>
      </div>
    </div>

    <div style="margin-bottom:20px"></div>

    <div class="grid-3">
      <div class="card" style="grid-column:span 2;margin-bottom:0">
        <div class="card-title">Top Messages from Fans</div>
        {top_msgs_html}
      </div>
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Top Area Codes</div>
        {area_html}
      </div>
    </div>

  </div><!-- /overview -->

  <!-- ══════════════════════════════════════════════════════════ AUDIENCE -->
  <div class="tab-content {'active' if tab == 'audience' else ''}" id="tab-audience">

    <div class="stats-grid" style="grid-template-columns:repeat(2,1fr);max-width:500px">
      <div class="stat-card">
        <div class="stat-label">Total Fans</div>
        <div class="stat-value">{stats["total_subscribers"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">unique numbers</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Profiled Fans</div>
        <div class="stat-value purple">{stats["profiled_fans"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">with memory built</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">Audience Tags — click any to filter</div>
      <div style="margin-top:4px">{tag_breakdown_html}</div>
    </div>

    <!-- Location search + export row -->
    <div class="card">
      <div class="card-title">Search & Export by Location or Tag</div>
      <form method="get" action="/admin" style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px">
        <input type="hidden" name="tab" value="audience">
        <input type="text" name="tag" class="search-input" style="flex:1;min-width:160px"
               placeholder="Tag: doctor, lawyer, married…"
               value="{tag_filter}">
        <input type="text" name="location" class="search-input" style="flex:1;min-width:160px"
               placeholder="Location: Rhode Island, Boston, Chicago…"
               value="{request.args.get('location', '')}">
        <button type="submit" class="search-btn">Filter</button>
        {'<a href="/admin?tab=audience" class="search-btn" style="background:#374151;text-decoration:none">Clear</a>' if tag_filter or request.args.get('location') else ''}
      </form>
      {'<a href="/admin/export?tag=' + tag_filter + '&location=' + request.args.get("location", "") + '" class="export-btn">⬇ Export CSV (' + str(len(stats["fan_profiles"])) + ' fans)</a>' if tag_filter or request.args.get("location") else '<a href="/admin/export" class="export-btn">⬇ Export All Fans</a>'}
    </div>

    <div class="card">
      <div class="card-title">
        {'Fan Profiles — ' + tag_filter + (' in ' + request.args.get('location','') if request.args.get('location') else '') if tag_filter else ('Fans in ' + request.args.get('location','') if request.args.get('location') else 'Fan Profiles (most recent 100 with memory)')}
      </div>
      {fan_profiles_html}
    </div>

  </div><!-- /audience -->

  <!-- ══════════════════════════════════════════════════════ CONVERSATIONS -->
  <div class="tab-content {'active' if tab == 'convos' else ''}" id="tab-convos">

    <form method="get" action="/admin" class="search-row">
      <input type="hidden" name="tab" value="convos">
      <input type="text" name="phone" class="search-input"
             placeholder="Search by last 4 digits of phone number…"
             value="{phone_search}">
      <button type="submit" class="search-btn">Search</button>
      {'<a href="/admin?tab=convos" class="search-btn" style="background:#374151;text-decoration:none">Clear</a>' if phone_search else ''}
    </form>

    <div class="card" style="padding:0;overflow:hidden">
      <div style="overflow-x:auto">
        <table>
          <thead>
            <tr>
              <th>Phone</th>
              <th>From</th>
              <th>Message</th>
              <th>Time (ET)</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>

  </div><!-- /convos -->

</div><!-- /container -->

<script>
// ── Charts ────────────────────────────────────────────────────────────────
const chartCfg = {{
  plugins: {{ legend: {{ display: false }} }},
  scales: {{
    x: {{ ticks: {{ color:'#6b7280', font:{{ size:10 }} }}, grid:{{ color:'#1f2937' }} }},
    y: {{ ticks: {{ color:'#6b7280', font:{{ size:10 }} }}, grid:{{ color:'#1f2937' }} }}
  }}
}};

const dayEl = document.getElementById('dayChart');
if (dayEl) new Chart(dayEl, {{
  type: 'bar',
  data: {{ labels: {days_labels}, datasets: [{{ data: {days_data}, backgroundColor:'#7c3aed', borderRadius:4 }}] }},
  options: chartCfg
}});

const hourEl = document.getElementById('hourChart');
if (hourEl) new Chart(hourEl, {{
  type: 'bar',
  data: {{ labels: {hour_labels}, datasets: [{{ data: {hour_data}, backgroundColor:'#0891b2', borderRadius:4 }}] }},
  options: chartCfg
}});

// ── Auto-refresh ───────────────────────────────────────────────────────────
let _refreshTimer = null;
let _refreshOn = false;

function toggleAutoRefresh() {{
  const btn = document.getElementById('autoRefreshBtn');
  if (_refreshOn) {{
    clearInterval(_refreshTimer);
    _refreshOn = false;
    btn.textContent = '⟳ Auto-refresh: Off';
    btn.classList.remove('active');
  }} else {{
    _refreshOn = true;
    btn.textContent = '⟳ Auto-refresh: On (30s)';
    btn.classList.add('active');
    _refreshTimer = setInterval(() => location.reload(), 30000);
  }}
}}
</script>

</body>
</html>"""

    return html
