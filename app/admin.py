"""
Admin analytics dashboard — password-protected read-only view of all activity.

Access: https://your-railway-url.app/admin
Login: HTTP Basic Auth — username anything, password = ADMIN_PASSWORD env var
"""

import os
from collections import Counter
from datetime import datetime, timezone

from flask import Blueprint, Response, request

admin_bp = Blueprint("admin", __name__)

_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")


def _check_auth():
    """Return True if the request has valid Basic Auth credentials."""
    if not _ADMIN_PASSWORD:
        return True  # no password set — allow all (dev mode)
    auth = request.authorization
    return auth and auth.password == _ADMIN_PASSWORD


def _require_auth():
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="Zarna AI Admin"'},
    )


def _get_db():
    """Get a Postgres connection, or None if not configured."""
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        return None
    import psycopg2
    import psycopg2.extras
    dsn = database_url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def _fetch_stats():
    conn = _get_db()
    if not conn:
        return None

    try:
        with conn.cursor(cursor_factory=__import__("psycopg2.extras", fromlist=["DictCursor"]).DictCursor) as cur:

            # Total unique subscribers
            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
            total_subscribers = cur.fetchone()[0]

            # Total messages (user only)
            cur.execute("SELECT COUNT(*) FROM messages WHERE role = 'user'")
            total_messages = cur.fetchone()[0]

            # Messages today
            cur.execute("SELECT COUNT(*) FROM messages WHERE role = 'user' AND created_at >= NOW() - INTERVAL '24 hours'")
            messages_today = cur.fetchone()[0]

            # Messages this week
            cur.execute("SELECT COUNT(*) FROM messages WHERE role = 'user' AND created_at >= NOW() - INTERVAL '7 days'")
            messages_week = cur.fetchone()[0]

            # Messages per day (last 14 days)
            cur.execute("""
                SELECT DATE(created_at AT TIME ZONE 'America/New_York') as day, COUNT(*) as cnt
                FROM messages WHERE role = 'user' AND created_at >= NOW() - INTERVAL '14 days'
                GROUP BY day ORDER BY day
            """)
            messages_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]

            # Messages by hour of day (last 30 days)
            cur.execute("""
                SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int as hr, COUNT(*) as cnt
                FROM messages WHERE role = 'user' AND created_at >= NOW() - INTERVAL '30 days'
                GROUP BY hr ORDER BY hr
            """)
            rows = cur.fetchall()
            hour_map = {r["hr"]: r["cnt"] for r in rows}
            messages_by_hour = [hour_map.get(h, 0) for h in range(24)]

            # Top 20 most common user messages
            cur.execute("""
                SELECT LOWER(TRIM(text)) as msg, COUNT(*) as cnt
                FROM messages WHERE role = 'user'
                GROUP BY LOWER(TRIM(text))
                ORDER BY cnt DESC LIMIT 20
            """)
            top_messages = [(r["msg"], r["cnt"]) for r in cur.fetchall()]

            # Area code breakdown (top 15)
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

            # Recent 50 conversations
            cur.execute("""
                SELECT phone_number, role, text, created_at
                FROM messages
                ORDER BY created_at DESC
                LIMIT 100
            """)
            raw_messages = cur.fetchall()
            conversations = [
                {
                    "phone": r["phone_number"],
                    "role": r["role"],
                    "text": r["text"],
                    "time": r["created_at"].strftime("%m/%d %I:%M%p") if r["created_at"] else "",
                }
                for r in raw_messages
            ]

        return {
            "total_subscribers": total_subscribers,
            "total_messages": total_messages,
            "messages_today": messages_today,
            "messages_week": messages_week,
            "messages_by_day": messages_by_day,
            "messages_by_hour": messages_by_hour,
            "top_messages": top_messages,
            "top_area_codes": top_area_codes,
            "conversations": conversations,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        }
    finally:
        conn.close()


@admin_bp.route("/admin")
def admin():
    if not _check_auth():
        return _require_auth()

    stats = _fetch_stats()

    if stats is None:
        return "<h2 style='font-family:sans-serif;padding:40px'>No database configured (DATABASE_URL not set).</h2>", 503

    days_labels = [d for d, _ in stats["messages_by_day"]]
    days_data   = [c for _, c in stats["messages_by_day"]]
    hour_data   = stats["messages_by_hour"]
    hour_labels = [f"{h}:00" for h in range(24)]

    top_msg_labels = [m[:40] + ("…" if len(m) > 40 else "") for m, _ in stats["top_messages"]]
    top_msg_data   = [c for _, c in stats["top_messages"]]

    area_labels = [f"({ac})" for ac, _ in stats["top_area_codes"]]
    area_data   = [c for _, c in stats["top_area_codes"]]

    rows_html = ""
    for msg in stats["conversations"]:
        role_color = "#6366f1" if msg["role"] == "user" else "#10b981"
        role_label = "Fan" if msg["role"] == "user" else "Bot"
        phone_display = msg["phone"][-4:] if len(msg["phone"]) >= 4 else msg["phone"]
        text_escaped = msg["text"].replace("<", "&lt;").replace(">", "&gt;")
        rows_html += f"""
        <tr>
            <td style="color:#94a3b8;font-size:12px">...{phone_display}</td>
            <td><span style="background:{role_color};color:white;padding:2px 8px;border-radius:10px;font-size:11px">{role_label}</span></td>
            <td style="max-width:500px;word-wrap:break-word">{text_escaped}</td>
            <td style="color:#94a3b8;font-size:12px;white-space:nowrap">{msg["time"]}</td>
        </tr>"""

    top_msgs_html = ""
    for i, (msg, cnt) in enumerate(stats["top_messages"], 1):
        msg_escaped = msg.replace("<", "&lt;").replace(">", "&gt;")
        top_msgs_html += f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #1e293b">
            <span style="color:#e2e8f0;font-size:14px">#{i} &nbsp;{msg_escaped}</span>
            <span style="background:#6366f1;color:white;padding:2px 10px;border-radius:12px;font-size:13px;font-weight:600;min-width:30px;text-align:center">{cnt}</span>
        </div>"""

    area_html = ""
    for ac, cnt in stats["top_area_codes"]:
        area_html += f"""
        <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1e293b">
            <span style="color:#e2e8f0">({ac})</span>
            <span style="color:#10b981;font-weight:600">{cnt}</span>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zarna AI — Analytics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f172a; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
  .header {{ background: linear-gradient(135deg, #6366f1, #8b5cf6); padding: 24px 32px; display: flex; justify-content: space-between; align-items: center; }}
  .header h1 {{ font-size: 24px; font-weight: 700; color: white; }}
  .header small {{ color: rgba(255,255,255,0.7); font-size: 13px; }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px 32px; }}
  .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 28px; }}
  .stat-card {{ background: #1e293b; border-radius: 12px; padding: 20px 24px; border: 1px solid #334155; }}
  .stat-card .label {{ color: #94a3b8; font-size: 13px; margin-bottom: 8px; }}
  .stat-card .value {{ font-size: 36px; font-weight: 700; color: white; }}
  .stat-card .sub {{ color: #64748b; font-size: 12px; margin-top: 4px; }}
  .charts-grid {{ display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 28px; }}
  .chart-card {{ background: #1e293b; border-radius: 12px; padding: 20px 24px; border: 1px solid #334155; }}
  .chart-card h3 {{ color: #94a3b8; font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }}
  .bottom-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 28px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px 24px; border: 1px solid #334155; }}
  .card h3 {{ color: #94a3b8; font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }}
  .conversations-card {{ background: #1e293b; border-radius: 12px; padding: 20px 24px; border: 1px solid #334155; }}
  .conversations-card h3 {{ color: #94a3b8; font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th {{ color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; padding: 8px 12px; text-align: left; border-bottom: 1px solid #334155; }}
  td {{ padding: 10px 12px; border-bottom: 1px solid #1e293b; vertical-align: top; }}
  tr:hover td {{ background: rgba(99,102,241,0.05); }}
  @media (max-width: 900px) {{
    .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .charts-grid, .bottom-grid {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>Zarna AI — Analytics</h1>
    <small>Live data from production database</small>
  </div>
  <small style="color:rgba(255,255,255,0.6)">Updated: {stats["generated_at"]}</small>
</div>
<div class="container">

  <!-- Key Stats -->
  <div class="stats-grid">
    <div class="stat-card">
      <div class="label">Total Subscribers</div>
      <div class="value">{stats["total_subscribers"]:,}</div>
      <div class="sub">Unique phone numbers</div>
    </div>
    <div class="stat-card">
      <div class="label">Total Messages</div>
      <div class="value">{stats["total_messages"]:,}</div>
      <div class="sub">From fans (all time)</div>
    </div>
    <div class="stat-card">
      <div class="label">Messages Today</div>
      <div class="value" style="color:#6366f1">{stats["messages_today"]:,}</div>
      <div class="sub">Last 24 hours</div>
    </div>
    <div class="stat-card">
      <div class="label">This Week</div>
      <div class="value" style="color:#10b981">{stats["messages_week"]:,}</div>
      <div class="sub">Last 7 days</div>
    </div>
  </div>

  <!-- Charts -->
  <div class="charts-grid">
    <div class="chart-card">
      <h3>Messages Per Day (Last 14 Days)</h3>
      <canvas id="dayChart" height="100"></canvas>
    </div>
    <div class="chart-card">
      <h3>Activity by Hour (ET)</h3>
      <canvas id="hourChart" height="100"></canvas>
    </div>
  </div>

  <!-- Bottom row -->
  <div class="bottom-grid">
    <div class="card" style="grid-column: span 2">
      <h3>Top Messages from Fans</h3>
      {top_msgs_html}
    </div>
    <div class="card">
      <h3>Top Area Codes</h3>
      {area_html}
    </div>
  </div>

  <!-- Recent conversations -->
  <div class="conversations-card">
    <h3>Recent Conversations (Last 100 Messages)</h3>
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

</div>

<script>
const chartDefaults = {{
  plugins: {{ legend: {{ display: false }} }},
  scales: {{
    x: {{ ticks: {{ color: '#64748b', font: {{ size: 11 }} }}, grid: {{ color: '#1e293b' }} }},
    y: {{ ticks: {{ color: '#64748b', font: {{ size: 11 }} }}, grid: {{ color: '#1e293b' }} }}
  }}
}};

new Chart(document.getElementById('dayChart'), {{
  type: 'bar',
  data: {{
    labels: {days_labels},
    datasets: [{{ data: {days_data}, backgroundColor: '#6366f1', borderRadius: 4 }}]
  }},
  options: chartDefaults
}});

new Chart(document.getElementById('hourChart'), {{
  type: 'bar',
  data: {{
    labels: {hour_labels},
    datasets: [{{ data: {hour_data}, backgroundColor: '#10b981', borderRadius: 4 }}]
  }},
  options: chartDefaults
}});
</script>
</body>
</html>"""

    return html
