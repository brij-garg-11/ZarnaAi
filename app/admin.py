"""
Admin analytics dashboard — password-protected read-only view of all activity.

Access: https://your-railway-url.app/admin
Login: HTTP Basic Auth — username anything, password = ADMIN_PASSWORD env var

Tabs:
  /admin              → Overview (stats + charts)
  /admin?tab=audience → Audience (tags, fan profiles, location)
  /admin?tab=convos   → Inbox (per-member list) + click for threaded conversation
"""

import csv
import io
import os
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

from flask import Blueprint, Response, request

from app.admin_auth import (
    admin_password_configured,
    check_admin_auth,
    get_db_connection,
    no_admin_password_response,
    require_admin_auth_response,
)

admin_bp = Blueprint("admin", __name__)

INBOX_PAGE_SIZE = 60
THREAD_PAGE_SIZE = 120
_VALID_CHART_DAYS = frozenset((7, 14, 30, 90))


def _safe_chart_days(raw) -> int:
    try:
        v = int(raw)
        return v if v in _VALID_CHART_DAYS else 14
    except (TypeError, ValueError):
        return 14


def _esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _check_auth():
    return check_admin_auth()


def _require_auth():
    return require_admin_auth_response()


def _no_password_configured():
    return no_admin_password_response()


def _get_db():
    return get_db_connection()


def _fetch_export(tag_filter="", location_filter=""):
    conn = _get_db()
    if not conn:
        return []
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if tag_filter and location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                      AND LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (tag_filter.lower(), f"%{location_filter.lower()}%"))
            elif tag_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                """, (tag_filter.lower(),))
            elif location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (f"%{location_filter.lower()}%",))
            else:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts ORDER BY created_at DESC
                """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@admin_bp.route("/admin/export")
def admin_export():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    tag_filter = request.args.get("tag", "").strip().lower()
    location_filter = request.args.get("location", "").strip()

    rows = _fetch_export(tag_filter=tag_filter, location_filter=location_filter)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["phone_number", "source", "fan_memory", "fan_tags", "fan_location", "joined_at"])
    for r in rows:
        writer.writerow([
            r["phone_number"],
            r.get("source") or "",
            r.get("fan_memory") or "",
            ", ".join(r.get("fan_tags") or []),
            r.get("fan_location") or "",
            r["created_at"].strftime("%Y-%m-%d") if r.get("created_at") else "",
        ])

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


@admin_bp.route("/admin/export/thread")
def admin_export_thread():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    thread_phone = request.args.get("thread", "").strip()
    if not thread_phone:
        return Response("Missing thread (phone) parameter.", 400)

    conn = _get_db()
    if not conn:
        return Response("No database.", 503)
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT phone_number, role, text, created_at
                FROM messages
                WHERE phone_number = %s
                ORDER BY created_at ASC
            """, (thread_phone,))
            rows = cur.fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["phone_number", "role", "text", "created_at_utc"])
    for r in rows:
        writer.writerow([
            r["phone_number"],
            r["role"],
            r["text"],
            r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else "",
        ])

    tail = "".join(c for c in thread_phone if c.isdigit())[-4:] or "thread"
    filename = f"zarna-conversation-{tail}-{datetime.now().strftime('%Y%m%d')}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _fetch_dashboard(
    tab: str,
    chart_days: int,
    tag_filter: str,
    location_filter: str,
    thread_phone: str,
    inbox_phone_q: str,
    msg_body_q: str,
    inbox_page: int,
    thread_page: int,
):
    conn = _get_db()
    if not conn:
        return None
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            total_subscribers = total_messages = messages_today = messages_week = 0
            messages_prev_week = new_subscribers_week = new_subscribers_prev_week = 0
            profiled_fans = 0
            messages_by_day = []
            messages_by_hour = []
            top_messages = []
            top_area_codes = []
            tag_breakdown = []
            fan_profiles = []
            messages_last_hour = 0
            inbox_rows = []
            thread_rows = []
            thread_total = 0

            # ── Core counts & charts (overview + shared) ──────────────────
            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
            total_subscribers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role = 'user'")
            total_messages = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '1 hour'"
            )
            messages_last_hour = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '24 hours'"
            )
            messages_today = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '7 days'"
            )
            messages_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE role='user' AND created_at >= NOW()-INTERVAL '14 days' AND created_at < NOW()-INTERVAL '7 days'
            """)
            messages_prev_week = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE created_at >= NOW()-INTERVAL '7 days'"
            )
            new_subscribers_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(DISTINCT phone_number) FROM contacts
                WHERE created_at >= NOW()-INTERVAL '14 days' AND created_at < NOW()-INTERVAL '7 days'
            """)
            new_subscribers_prev_week = cur.fetchone()[0]

            cur.execute("""
                SELECT DATE(created_at AT TIME ZONE 'America/New_York') as day, COUNT(*) as cnt
                FROM messages
                WHERE role='user' AND created_at >= NOW()- make_interval(days => %s)
                GROUP BY day ORDER BY day
            """, (chart_days,))
            messages_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]

            cur.execute("""
                SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int as hr, COUNT(*) as cnt
                FROM messages
                WHERE role='user' AND created_at >= NOW()- make_interval(days => %s)
                GROUP BY hr ORDER BY hr
            """, (chart_days,))
            hour_map = {r["hr"]: r["cnt"] for r in cur.fetchall()}
            messages_by_hour = [hour_map.get(h, 0) for h in range(24)]

            cur.execute("""
                SELECT LOWER(TRIM(text)) as msg, COUNT(*) as cnt
                FROM messages
                WHERE role='user' AND created_at >= NOW()- make_interval(days => %s)
                GROUP BY LOWER(TRIM(text))
                ORDER BY cnt DESC LIMIT 20
            """, (chart_days,))
            top_messages = [(r["msg"], r["cnt"]) for r in cur.fetchall()]

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

            cur.execute("""
                SELECT UNNEST(fan_tags) as tag, COUNT(*) as cnt
                FROM contacts
                WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                GROUP BY tag ORDER BY cnt DESC LIMIT 30
            """)
            tag_breakdown = [(r["tag"], r["cnt"]) for r in cur.fetchall()]

            if tag_filter and location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE %s = ANY(fan_tags) AND LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (tag_filter.lower(), f"%{location_filter.lower()}%"))
            elif tag_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                """, (tag_filter.lower(),))
            elif location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (f"%{location_filter.lower()}%",))
            else:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE fan_memory IS NOT NULL AND fan_memory != ''
                    ORDER BY created_at DESC LIMIT 100
                """)
            fan_profiles = [dict(r) for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*) FROM contacts WHERE fan_memory IS NOT NULL AND fan_memory != ''"
            )
            profiled_fans = cur.fetchone()[0]

            # ── Conversations tab ─────────────────────────────────────────
            if tab == "convos":
                inbox_off = max(0, inbox_page) * INBOX_PAGE_SIZE
                thread_off = max(0, thread_page) * THREAD_PAGE_SIZE

                if thread_phone:
                    cur.execute(
                        "SELECT COUNT(*) FROM messages WHERE phone_number = %s",
                        (thread_phone,),
                    )
                    thread_total = cur.fetchone()[0]
                    cur.execute("""
                        SELECT phone_number, role, text, created_at
                        FROM messages
                        WHERE phone_number = %s
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, (thread_phone, THREAD_PAGE_SIZE, thread_off))
                    rows_desc = [dict(r) for r in cur.fetchall()]
                    thread_rows = list(reversed(rows_desc))
                else:
                    phone_like = f"%{inbox_phone_q}%" if inbox_phone_q else "%"
                    if msg_body_q:
                        pat = f"%{msg_body_q}%"
                        cur.execute("""
                            WITH latest AS (
                                SELECT DISTINCT ON (m.phone_number)
                                    m.phone_number, m.role, m.text, m.created_at
                                FROM messages m
                                INNER JOIN (
                                    SELECT DISTINCT phone_number FROM messages WHERE text ILIKE %s
                                ) hit ON hit.phone_number = m.phone_number
                                ORDER BY m.phone_number, m.created_at DESC
                            )
                            SELECT * FROM latest
                            WHERE phone_number LIKE %s
                            ORDER BY created_at DESC
                            LIMIT %s OFFSET %s
                        """, (pat, phone_like, INBOX_PAGE_SIZE, inbox_off))
                    else:
                        cur.execute("""
                            SELECT * FROM (
                                SELECT DISTINCT ON (phone_number)
                                    phone_number, role, text, created_at
                                FROM messages
                                WHERE phone_number LIKE %s
                                ORDER BY phone_number, created_at DESC
                            ) sub
                            ORDER BY created_at DESC
                            LIMIT %s OFFSET %s
                        """, (phone_like, INBOX_PAGE_SIZE, inbox_off))
                    inbox_rows = [dict(r) for r in cur.fetchall()]

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
            "tag_breakdown": tag_breakdown,
            "fan_profiles": fan_profiles,
            "tag_filter": tag_filter,
            "messages_last_hour": messages_last_hour,
            "inbox_rows": inbox_rows,
            "thread_rows": thread_rows,
            "thread_total": thread_total,
            "chart_days": chart_days,
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


def _range_links(active: int) -> str:
    parts = []
    for d in (7, 14, 30, 90):
        cls = "range-pill-active" if d == active else "range-pill"
        parts.append(f'<a class="{cls}" href="/admin?tab=overview&range={d}">{d}d</a>')
    return " ".join(parts)


@admin_bp.route("/admin")
def admin():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    tab = request.args.get("tab", "overview").strip().lower()
    tag_filter = request.args.get("tag", "").strip().lower()
    location_filter = request.args.get("location", "").strip()
    chart_days = _safe_chart_days(request.args.get("range"))
    thread_phone = request.args.get("thread", "").strip()
    inbox_phone_q = request.args.get("phone", "").strip()
    msg_body_q = request.args.get("q", "").strip()
    try:
        inbox_page = max(0, int(request.args.get("inbox_page", "0")))
    except ValueError:
        inbox_page = 0
    try:
        thread_page = max(0, int(request.args.get("thread_page", "0")))
    except ValueError:
        thread_page = 0

    if (tag_filter or location_filter) and tab == "overview":
        tab = "audience"

    stats = _fetch_dashboard(
        tab=tab,
        chart_days=chart_days,
        tag_filter=tag_filter,
        location_filter=location_filter,
        thread_phone=thread_phone,
        inbox_phone_q=inbox_phone_q,
        msg_body_q=msg_body_q,
        inbox_page=inbox_page,
        thread_page=thread_page,
    )
    if stats is None:
        return "<h2 style='font-family:sans-serif;padding:40px'>No database configured (DATABASE_URL not set).</h2>", 503

    days_labels = [d for d, _ in stats["messages_by_day"]]
    days_data = [c for _, c in stats["messages_by_day"]]
    hour_labels = [f"{h}" for h in range(24)]
    hour_data = stats["messages_by_hour"]

    week_trend = _trend_html(stats["messages_week"], stats["messages_prev_week"])
    sub_trend = _trend_html(
        stats["new_subscribers_week"],
        stats["new_subscribers_prev_week"],
        "new vs last week",
    )

    top_msgs_html = ""
    for i, (msg, cnt) in enumerate(stats["top_messages"], 1):
        msg_e = _esc(msg)
        top_msgs_html += f"""
        <div class="top-msg-row">
          <span class="top-msg-rank">#{i}</span>
          <span class="top-msg-text">{msg_e}</span>
          <span class="top-msg-badge">{cnt}</span>
        </div>"""

    area_html = ""
    for ac, cnt in stats["top_area_codes"]:
        area_html += f"""
        <div class="area-row">
          <span>({ac})</span>
          <span class="area-cnt">{cnt}</span>
        </div>"""

    tag_breakdown_html = ""
    if stats["tag_breakdown"]:
        for tag, cnt in stats["tag_breakdown"]:
            active = "tag-pill-active" if tag == tag_filter else ""
            tag_breakdown_html += f'<a href="/admin?tab=audience&tag={tag}" class="tag-pill {active}">{tag} <span class="tag-cnt">{cnt}</span></a>'
    else:
        tag_breakdown_html = '<p class="empty-note">Tags build automatically as fans text in after the next show.</p>'

    fan_profiles_html = ""
    for fan in stats["fan_profiles"]:
        phone_d = fan["phone_number"][-4:]
        mem_e = _esc(fan["fan_memory"] or "")
        loc = fan.get("fan_location") or ""
        loc_html = f'<span class="fan-loc">📍 {_esc(loc)}</span>' if loc else ""
        src = fan.get("source") or "—"
        joined = fan["created_at"].strftime("%Y-%m-%d") if fan.get("created_at") else "—"
        th_q = quote(fan["phone_number"], safe="")
        tags_html = " ".join(
            f'<a href="/admin?tab=audience&tag={t}" class="fan-tag">{_esc(t)}</a>'
            for t in (fan["fan_tags"] or [])
        )
        fan_profiles_html += f"""
        <div class="fan-card">
          <div class="fan-header">
            <span class="fan-phone">…{phone_d}</span>
            <span class="fan-meta">source: {_esc(src)} · joined {joined}</span>
            {loc_html}
            <a class="fan-open-convo" href="/admin?tab=convos&thread={th_q}">Open conversation →</a>
          </div>
          <p class="fan-memory">{mem_e or "<em class='empty-note'>No profile yet</em>"}</p>
          <div class="fan-tags">{tags_html}</div>
        </div>"""

    if not fan_profiles_html:
        fan_profiles_html = '<p class="empty-note">Fan profiles build automatically as fans text in. Check back after the next show.</p>'

    # Conversations: thread view OR inbox
    convos_inner_html = ""
    chart_days = stats["chart_days"]
    mh = stats["messages_last_hour"]

    if tab == "convos":
        if thread_phone:
            phone_d = thread_phone[-4:] if len(thread_phone) >= 4 else thread_phone
            th_enc = quote(thread_phone, safe="")
            export_href = f"/admin/export/thread?thread={th_enc}"
            total = stats["thread_total"]
            n_show = len(stats["thread_rows"])
            end_i = total - thread_page * THREAD_PAGE_SIZE if total else 0
            start_i = (end_i - n_show + 1) if n_show and end_i else 0
            nav_parts = ['<a class="back-inbox" href="/admin?tab=convos">← Inbox</a>']
            if thread_page > 0:
                q = urlencode(
                    {"tab": "convos", "thread": thread_phone, "thread_page": thread_page - 1}
                )
                nav_parts.append(f'<a class="page-link" href="/admin?{q}">Newer messages ↑</a>')
            if start_i > 1:
                q = urlencode(
                    {"tab": "convos", "thread": thread_phone, "thread_page": thread_page + 1}
                )
                nav_parts.append(f'<a class="page-link" href="/admin?{q}">Earlier messages ↓</a>')
            nav_html = " · ".join(nav_parts)
            bubble_html = ""
            for r in stats["thread_rows"]:
                is_fan = r["role"] == "user"
                cls = "bubble-fan" if is_fan else "bubble-bot"
                lbl = "Fan" if is_fan else "Zarna AI"
                ts = r["created_at"].strftime("%m/%d %I:%M %p ET") if r["created_at"] else ""
                bubble_html += f"""
                <div class="bubble-row {cls}">
                  <div class="bubble-meta"><span class="bubble-who">{lbl}</span> · {ts}</div>
                  <div class="bubble-text">{_esc(r["text"])}</div>
                </div>"""
            if not bubble_html:
                bubble_html = '<p class="empty-note">No messages for this number.</p>'
            convos_inner_html = f"""
            <div class="thread-toolbar">
              <div class="thread-title">Conversation <span class="mono">…{phone_d}</span></div>
              <div class="thread-sub">{nav_html}</div>
              <a class="export-btn thread-export" href="{export_href}">⬇ Export this thread (CSV)</a>
            </div>
            <p class="thread-range-note">Messages {start_i}–{end_i} of {total} (oldest → newest in this chunk)</p>
            <div class="bubble-stack card">{bubble_html}</div>
            """
        else:
            q_val = _esc(request.args.get("q", ""))
            ph_val = _esc(inbox_phone_q)
            convos_inner_html = f"""
            <form method="get" action="/admin" class="search-row convo-filters">
              <input type="hidden" name="tab" value="convos">
              <input type="text" name="phone" class="search-input"
                     placeholder="Filter inbox by phone (partial / last 4)…" value="{ph_val}">
              <input type="text" name="q" class="search-input"
                     placeholder="Search message text…" value="{q_val}">
              <button type="submit" class="search-btn">Apply</button>
              {'<a href="/admin?tab=convos" class="search-btn secondary-btn">Clear</a>' if (inbox_phone_q or msg_body_q) else ''}
            </form>
            <p class="inbox-hint">Click a row to open the full conversation. Page size {INBOX_PAGE_SIZE}.</p>
            """
            inbox_body = ""
            for row in stats["inbox_rows"]:
                p = row["phone_number"]
                p4 = p[-4:] if len(p) >= 4 else p
                preview = _esc(row["text"][:120] + ("…" if len(row["text"]) > 120 else ""))
                ts = row["created_at"].strftime("%m/%d %I:%M %p") if row["created_at"] else ""
                role_lbl = "Fan" if row["role"] == "user" else "Bot"
                href = f"/admin?tab=convos&thread={quote(p, safe='')}"
                inbox_body += f"""
                <a href="{href}" class="inbox-row">
                  <span class="inbox-phone mono">…{p4}</span>
                  <span class="inbox-preview">{preview}</span>
                  <span class="inbox-meta"><span class="badge {"badge-fan" if row["role"]=="user" else "badge-bot"}">{role_lbl}</span> {ts}</span>
                </a>"""
            if not inbox_body:
                inbox_body = '<p class="empty-note">No conversations match your filters.</p>'
            next_prev = ""
            if inbox_page > 0 or len(stats["inbox_rows"]) == INBOX_PAGE_SIZE:
                links = []
                if inbox_page > 0:
                    links.append(
                        f'<a class="page-link" href="/admin?{urlencode({"tab": "convos", "phone": inbox_phone_q, "q": msg_body_q, "inbox_page": inbox_page - 1})}">← Newer chats</a>'
                    )
                if len(stats["inbox_rows"]) == INBOX_PAGE_SIZE:
                    links.append(
                        f'<a class="page-link" href="/admin?{urlencode({"tab": "convos", "phone": inbox_phone_q, "q": msg_body_q, "inbox_page": inbox_page + 1})}">Older chats →</a>'
                    )
                next_prev = f'<div class="inbox-pagination">{" · ".join(links)}</div>'
            convos_inner_html += f'<div class="inbox-list card">{inbox_body}</div>{next_prev}'

    filter_banner = ""
    if tag_filter:
        filter_banner = f"""
        <div class="filter-banner">
          <span>Filtering by tag: <strong>{_esc(tag_filter)}</strong> — {len(stats["fan_profiles"])} fan{"s" if len(stats["fan_profiles"]) != 1 else ""}</span>
          <a href="/admin?tab=audience" class="filter-clear">✕ Clear</a>
        </div>"""
    elif tab == "convos" and thread_phone:
        filter_banner = f"""
        <div class="filter-banner">
          <span>Viewing conversation <strong class="mono">…{thread_phone[-4:]}</strong></span>
          <a href="/admin?tab=convos" class="filter-clear">✕ Back to inbox</a>
        </div>"""

    range_pills = _range_links(chart_days)
    health_note = f'<span class="health-pill">{mh} fan msg in last hour</span>'

    from app.ops_metrics import snapshot as ops_snapshot

    ops = ops_snapshot()
    deploy_ref = (os.getenv("RAILWAY_GIT_COMMIT_SHA") or os.getenv("GIT_COMMIT") or "").strip()[:12]
    deploy_disp = deploy_ref if deploy_ref else "—"
    ops_signals_html = f"""
    <div class="card" style="margin-bottom:20px">
      <div class="card-title">Service signals (this worker · resets on deploy)</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:10px">Deploy ref: <code style="color:#e2e8f0">{_esc(deploy_disp)}</code>
        · Active AI replies: <strong>{ops.get("active_ai_replies", 0)}</strong></p>
      <ul style="color:#cbd5e1;font-size:13px;line-height:1.75;list-style:none;padding:0;margin:0">
        <li>SlickText webhook 401: {ops.get("slicktext_webhook_401", 0)}</li>
        <li>Twilio signature fail: {ops.get("twilio_signature_fail", 0)}</li>
        <li>AI / brain errors: {ops.get("ai_reply_error", 0)}</li>
        <li>Dropped (at capacity): {ops.get("ai_reply_capacity_reject", 0)}</li>
      </ul>
      <p style="color:#64748b;font-size:12px;margin-top:10px">Set <code>AI_REPLY_MAX_CONCURRENT</code> (default 16) to tune load.</p>
    </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zarna AI — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: #0a0f1e; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; min-height: 100vh; }}

.header {{ background: linear-gradient(135deg, #7c3aed 0%, #4f46e5 60%, #0891b2 100%); padding: 20px 28px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
.header-left {{ display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }}
.header-logo {{ font-size: 22px; font-weight: 800; color: white; letter-spacing: -0.5px; }}
.header-logo span {{ color: rgba(255,255,255,0.6); font-weight: 400; font-size: 14px; margin-left: 8px; }}
.header-right {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
.refresh-btn {{ background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.25); color: white; border-radius: 8px; padding: 7px 14px; font-size: 13px; cursor: pointer; transition: background 0.2s; }}
.refresh-btn:hover {{ background: rgba(255,255,255,0.25); }}
.refresh-btn.active {{ background: rgba(16,185,129,0.3); border-color: #10b981; }}
.updated-time {{ color: rgba(255,255,255,0.55); font-size: 12px; }}
.health-pill {{ background: rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.2); color: rgba(255,255,255,0.85); border-radius: 20px; padding: 4px 12px; font-size: 12px; }}

.nav-tabs {{ position: sticky; top: 0; z-index: 50; background: #111827; border-bottom: 1px solid #1f2937; padding: 0 28px; display: flex; gap: 4px; flex-wrap: wrap; }}
.nav-tab {{ padding: 14px 20px; font-size: 14px; font-weight: 500; color: #64748b; text-decoration: none; border-bottom: 2px solid transparent; transition: all 0.15s; white-space: nowrap; }}
.nav-tab:hover {{ color: #e2e8f0; }}
.nav-tab.active {{ color: #a78bfa; border-bottom-color: #7c3aed; }}

.container {{ max-width: 1400px; margin: 0 auto; padding: 24px 28px; }}

.stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 24px; }}
.stat-card {{ background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 20px 22px; transition: border-color 0.2s; }}
.stat-card:hover {{ border-color: #374151; }}
.stat-label {{ color: #6b7280; font-size: 12px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 10px; }}
.stat-value {{ font-size: 34px; font-weight: 800; color: white; line-height: 1; margin-bottom: 8px; }}
.stat-value.purple {{ color: #a78bfa; }}
.stat-value.teal {{ color: #2dd4bf; }}
.stat-value.green {{ color: #4ade80; }}
.stat-trend {{ font-size: 12px; }}

.range-toolbar {{ display: flex; align-items: center; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }}
.range-toolbar span {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }}
.range-pill, .range-pill-active {{
  display: inline-block; padding: 6px 12px; border-radius: 8px; font-size: 13px; text-decoration: none;
  background: #1f2937; color: #94a3b8; border: 1px solid #374151;
}}
.range-pill:hover {{ color: #e2e8f0; border-color: #7c3aed; }}
.range-pill-active {{ background: #312e81; color: #c4b5fd; border-color: #7c3aed; }}

.card {{ background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 20px 22px; margin-bottom: 20px; }}
.card-title {{ color: #9ca3af; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 16px; }}
.grid-2 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 14px; margin-bottom: 20px; }}
.grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; margin-bottom: 20px; }}

.top-msg-row {{ display: flex; align-items: center; gap: 10px; padding: 9px 0; border-bottom: 1px solid #1f2937; }}
.top-msg-rank {{ color: #4b5563; font-size: 11px; min-width: 24px; }}
.top-msg-text {{ color: #d1d5db; font-size: 13px; flex: 1; }}
.top-msg-badge {{ background: #312e81; color: #a5b4fc; padding: 2px 9px; border-radius: 10px; font-size: 12px; font-weight: 600; }}

.area-row {{ display: flex; justify-content: space-between; padding: 7px 0; border-bottom: 1px solid #1f2937; font-size: 13px; }}
.area-cnt {{ color: #2dd4bf; font-weight: 600; }}

.tag-pill {{ display: inline-flex; align-items: center; gap: 6px; background: #1e3a5f; color: #93c5fd; padding: 5px 12px; border-radius: 20px; font-size: 13px; text-decoration: none; margin: 4px; transition: background 0.15s; }}
.tag-pill:hover {{ background: #1d4ed8; color: white; }}
.tag-pill-active {{ background: #1d4ed8; color: white; }}
.tag-cnt {{ background: #3b82f6; color: white; border-radius: 10px; padding: 1px 7px; font-size: 11px; }}

.fan-card {{ padding: 14px 0; border-bottom: 1px solid #1f2937; }}
.fan-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; flex-wrap: wrap; }}
.fan-phone {{ color: #6b7280; font-size: 12px; font-family: monospace; }}
.fan-meta {{ color: #64748b; font-size: 11px; }}
.fan-loc {{ color: #fbbf24; font-size: 12px; }}
.fan-open-convo {{ margin-left: auto; font-size: 12px; color: #a78bfa; text-decoration: none; white-space: nowrap; }}
.fan-open-convo:hover {{ text-decoration: underline; }}
.fan-memory {{ color: #d1d5db; font-size: 14px; margin-bottom: 8px; line-height: 1.4; }}
.fan-tags {{ display: flex; flex-wrap: wrap; gap: 4px; }}
.fan-tag {{ background: #1e3a5f; color: #93c5fd; padding: 2px 8px; border-radius: 8px; font-size: 11px; text-decoration: none; }}
.fan-tag:hover {{ background: #1d4ed8; color: white; }}

.search-row {{ display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }}
.convo-filters .search-input {{ flex: 1; min-width: 140px; }}
.search-input {{ flex: 1; background: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 9px 14px; color: #e2e8f0; font-size: 14px; outline: none; }}
.search-input:focus {{ border-color: #7c3aed; }}
.search-btn {{ background: #7c3aed; color: white; border: none; border-radius: 8px; padding: 9px 18px; font-size: 14px; cursor: pointer; white-space: nowrap; }}
.search-btn:hover {{ background: #6d28d9; }}
.secondary-btn {{ background: #374151; text-decoration: none; display: inline-flex; align-items: center; }}
.export-btn {{ display:inline-flex;align-items:center;gap:6px;background:#064e3b;color:#6ee7b7;border:1px solid #065f46;border-radius:8px;padding:9px 18px;font-size:14px;text-decoration:none;transition:background 0.15s; }}
.export-btn:hover {{ background:#065f46;color:white; }}

.inbox-hint {{ color: #64748b; font-size: 13px; margin-bottom: 12px; }}
.inbox-list {{ padding: 0 !important; overflow: hidden; }}
.inbox-row {{
  display: flex; align-items: flex-start; gap: 14px; padding: 14px 18px; border-bottom: 1px solid #1f2937;
  text-decoration: none; color: inherit; transition: background 0.12s;
}}
.inbox-row:hover {{ background: rgba(124,58,237,0.08); }}
.inbox-row:last-child {{ border-bottom: none; }}
.inbox-phone {{ flex: 0 0 auto; color: #a78bfa; font-weight: 600; }}
.inbox-preview {{ flex: 1; color: #d1d5db; font-size: 14px; line-height: 1.35; word-break: break-word; }}
.inbox-meta {{ flex: 0 0 auto; text-align: right; font-size: 12px; color: #6b7280; min-width: 120px; }}
.inbox-pagination {{ margin-top: 14px; }}

.thread-toolbar {{ display: flex; flex-wrap: wrap; align-items: center; gap: 12px; margin-bottom: 10px; }}
.thread-title {{ font-size: 18px; font-weight: 700; color: #f1f5f9; }}
.thread-sub {{ color: #94a3b8; font-size: 13px; flex: 1; }}
.thread-export {{ font-size: 13px; padding: 7px 14px; }}
.thread-range-note {{ color: #64748b; font-size: 12px; margin-bottom: 12px; }}
.back-inbox, .page-link {{ color: #a78bfa; text-decoration: none; }}
.back-inbox:hover, .page-link:hover {{ text-decoration: underline; }}
.mono {{ font-family: ui-monospace, monospace; }}

.bubble-stack {{ padding: 16px 20px !important; max-width: 720px; }}
.bubble-row {{ margin-bottom: 16px; }}
.bubble-row.bubble-fan {{ text-align: left; }}
.bubble-row.bubble-bot {{ text-align: right; }}
.bubble-meta {{ font-size: 11px; color: #64748b; margin-bottom: 4px; }}
.bubble-who {{ font-weight: 600; color: #94a3b8; }}
.bubble-text {{
  display: inline-block; max-width: 85%; text-align: left;
  padding: 10px 14px; border-radius: 12px; font-size: 14px; line-height: 1.45; word-break: break-word;
}}
.bubble-fan .bubble-text {{ background: #312e81; color: #e0e7ff; border-bottom-left-radius: 4px; }}
.bubble-bot .bubble-text {{ background: #064e3b; color: #d1fae5; border-bottom-right-radius: 4px; }}

.badge {{ padding: 2px 8px; border-radius: 8px; font-size: 11px; font-weight: 600; }}
.badge-fan {{ background: #312e81; color: #a5b4fc; }}
.badge-bot {{ background: #064e3b; color: #6ee7b7; }}

.filter-banner {{ background: #1e3a5f; border: 1px solid #2563eb; border-radius: 8px; padding: 10px 18px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; font-size: 13px; color: #93c5fd; flex-wrap: wrap; gap: 8px; }}
.filter-clear {{ color: #6b7280; text-decoration: none; font-size: 13px; }}
.filter-clear:hover {{ color: #e2e8f0; }}

.empty-note {{ color: #6b7280; font-size: 13px; font-style: italic; padding: 8px 0; }}
.tab-content {{ display: none; }}
.tab-content.active {{ display: block; }}

@media (max-width: 768px) {{
  .header {{ padding: 16px; }}
  .header-logo {{ font-size: 18px; }}
  .nav-tab {{ padding: 12px 14px; font-size: 13px; }}
  .container {{ padding: 16px; }}
  .stats-grid {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
  .stat-value {{ font-size: 26px; }}
  .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
  .inbox-row {{ flex-direction: column; gap: 6px; }}
  .inbox-meta {{ text-align: left; }}
  .fan-open-convo {{ margin-left: 0; }}
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

<div class="header">
  <div class="header-left">
    <div class="header-logo">Zarna AI <span>Dashboard</span></div>
    {health_note}
  </div>
  <div class="header-right">
    <button class="refresh-btn" id="autoRefreshBtn" onclick="toggleAutoRefresh()">⟳ Auto-refresh: Off</button>
    <span class="updated-time">Updated: {stats["generated_at"]}</span>
  </div>
</div>

<nav class="nav-tabs">
  <a href="/admin?tab=overview&amp;range={chart_days}" class="nav-tab {'active' if tab == 'overview' else ''}">📊 Overview</a>
  <a href="/admin?tab=audience" class="nav-tab {'active' if tab == 'audience' else ''}">👥 Audience</a>
  <a href="/admin?tab=convos" class="nav-tab {'active' if tab == 'convos' else ''}">💬 Conversations</a>
  <a href="/admin/live-shows" class="nav-tab">🎤 Live shows</a>
</nav>

<div class="container">

  {filter_banner}

  <div class="tab-content {'active' if tab == 'overview' else ''}" id="tab-overview">
    {ops_signals_html}

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

    <div class="range-toolbar">
      <span>Chart window</span>
      {range_pills}
    </div>

    <div class="grid-2">
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Messages Per Day — Last {chart_days} Days</div>
        <canvas id="dayChart" height="110"></canvas>
      </div>
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Activity by Hour (ET) — Same Window</div>
        <canvas id="hourChart" height="110"></canvas>
      </div>
    </div>

    <div style="margin-bottom:20px"></div>

    <div class="grid-3">
      <div class="card" style="grid-column:span 2;margin-bottom:0">
        <div class="card-title">Top Messages from Fans (same window)</div>
        {top_msgs_html}
      </div>
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Top Area Codes</div>
        {area_html}
      </div>
    </div>

  </div>

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

    <div class="card">
      <div class="card-title">Search & Export by Location or Tag</div>
      <form method="get" action="/admin" style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px">
        <input type="hidden" name="tab" value="audience">
        <input type="text" name="tag" class="search-input" style="flex:1;min-width:160px"
               placeholder="Tag: doctor, lawyer, married…"
               value="{tag_filter}">
        <input type="text" name="location" class="search-input" style="flex:1;min-width:160px"
               placeholder="Location: Rhode Island, Boston, Chicago…"
               value="{_esc(request.args.get('location', ''))}">
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

  </div>

  <div class="tab-content {'active' if tab == 'convos' else ''}" id="tab-convos">
    {convos_inner_html}
  </div>

</div>

<script>
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
