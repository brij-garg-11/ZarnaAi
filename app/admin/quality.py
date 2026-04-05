"""Quality digest tab — weekly AI analysis reports with Notion integration."""

import json as _json
import subprocess
import sys as _sys

from flask import Response, redirect as _redirect

from app.admin_auth import check_admin_auth, get_db_connection, require_admin_auth_response


def _get_db():
    return get_db_connection()


def _fetch_quality_reports() -> list:
    """Return quality reports newest-first (up to 12)."""
    import psycopg2.extras as _pge
    conn = _get_db()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, created_at, week_start, headline_json, findings_json,
                       notion_page_id, reviewed_at
                FROM ai_quality_reports
                ORDER BY created_at DESC
                LIMIT 12
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def render_quality_tab() -> str:
    """Return inner HTML for the Quality tab (AI weekly digest reports)."""
    reports = _fetch_quality_reports()

    if not reports:
        return """
        <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;
                    padding:48px 40px;text-align:center;color:#6b7280;margin-top:8px;">
          <div style="font-size:36px;margin-bottom:14px">🔍</div>
          <div style="font-size:16px;font-weight:600;color:#9ca3af;margin-bottom:8px">
            No quality reports yet
          </div>
          <div style="font-size:13px;max-width:420px;margin:0 auto;line-height:1.6">
            Run <code style="background:#1f2937;padding:2px 6px;border-radius:4px">
            python scripts/generate_quality_digest.py</code>
            (or wait for the weekly cron) to generate your first digest.
          </div>
        </div>"""

    def _trend_badge(t: str) -> str:
        colors = {"improving": "#4ade80", "declining": "#f87171", "stable": "#fbbf24"}
        icons  = {"improving": "📈", "declining": "📉", "stable": "➡️"}
        c = colors.get(t, "#6b7280")
        return (
            f'<span style="background:{c}22;color:{c};border:1px solid {c}44;'
            f'border-radius:99px;padding:2px 10px;font-size:11px;font-weight:700">'
            f'{icons.get(t,"")} {t.title()}</span>'
        )

    def _sev_badge(s: str) -> str:
        colors = {"high": "#f87171", "medium": "#fbbf24", "low": "#4ade80"}
        c = colors.get(s, "#6b7280")
        return (
            f'<span style="background:{c}22;color:{c};border:1px solid {c}44;'
            f'border-radius:99px;padding:2px 10px;font-size:11px;font-weight:700">'
            f'{s.upper()}</span>'
        )

    rows_html = ""
    for idx, r in enumerate(reports):
        try:
            headline = _json.loads(r["headline_json"] or "{}")
        except Exception:
            headline = {}
        try:
            findings = _json.loads(r["findings_json"] or "{}")
        except Exception:
            findings = {}

        rid      = r["id"]
        ws       = r["week_start"]
        created  = r["created_at"]
        reviewed = r["reviewed_at"]
        notion   = r.get("notion_page_id")

        rr       = headline.get("reply_rate")
        base_rr  = headline.get("baseline_reply_rate")
        scored   = headline.get("scored", 0)
        summary  = findings.get("one_line_summary", "")
        trend    = findings.get("overall_trend", "stable")
        problems = findings.get("problems", [])
        working  = findings.get("whats_working", [])

        rr_str   = f"{rr}%" if rr is not None else "—"
        base_str = f"{base_rr}%" if base_rr is not None else "—"
        created_str = created.strftime("%b %d, %Y") if hasattr(created, "strftime") else str(created)

        reviewed_badge = (
            '<span style="background:#16a34a22;color:#4ade80;border:1px solid #16a34a44;'
            'border-radius:99px;padding:2px 8px;font-size:11px">✓ Reviewed</span>'
            if reviewed else
            '<span style="background:#d9770622;color:#fbbf24;border:1px solid #d9770644;'
            'border-radius:99px;padding:2px 8px;font-size:11px">Pending review</span>'
        )

        notion_link = ""
        if notion:
            url = f"https://notion.so/{notion.replace('-','')}"
            notion_link = (
                f'<a href="{url}" target="_blank" onclick="event.stopPropagation()" '
                f'style="font-size:12px;color:#818cf8;text-decoration:none;margin-left:10px">'
                f'↗ Notion</a>'
            )

        # Stats strip
        rr_color = ("4ade80" if rr and float(rr) >= 60
                    else "fbbf24" if rr and float(rr) >= 40
                    else "f87171" if rr else "6b7280")
        stats_strip = f"""
        <div style="display:flex;gap:24px;flex-wrap:wrap;padding:14px 0 16px;
                    border-bottom:1px solid #1f2937;margin-bottom:18px;">
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:700;color:#e2e8f0">{scored:,}</div>
            <div style="font-size:11px;color:#6b7280">Scored replies</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:700;color:#{rr_color}">{rr_str}</div>
            <div style="font-size:11px;color:#6b7280">Reply rate</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:700;color:#9ca3af">{base_str}</div>
            <div style="font-size:11px;color:#6b7280">Baseline (4wk)</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:700;color:#f87171">{len(problems)}</div>
            <div style="font-size:11px;color:#6b7280">Issues flagged</div>
          </div>
        </div>"""

        # Problems
        problems_html = ""
        for i, p in enumerate(problems, 1):
            sev = p.get("severity", "low")
            problems_html += f"""
            <div style="background:#0f172a;border:1px solid #1f2937;border-radius:8px;
                        padding:14px 16px;margin-bottom:10px;">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
                <span style="font-weight:700;color:#e2e8f0;font-size:13px">
                  Problem {i}: {p.get('title','Untitled')}
                </span>
                {_sev_badge(sev)}
              </div>
              <div style="color:#9ca3af;font-size:12px;margin-bottom:10px;line-height:1.6">
                <strong style="color:#cbd5e1">Evidence:</strong> {p.get('evidence','—')}
              </div>
              <div style="background:#1e3a5f22;border:1px solid #3b82f644;border-radius:6px;
                          padding:10px 14px;color:#93c5fd;font-size:12px;line-height:1.6">
                💡 <strong>Proposed change:</strong> {p.get('proposed_change','—')}
              </div>
            </div>"""

        # What's working
        working_items = "".join(
            f'<li style="color:#9ca3af;font-size:12px;margin-bottom:5px;line-height:1.5">{w}</li>'
            for w in working
        )

        _problems_block = (
            f'<div style="margin-bottom:18px"><div style="font-size:13px;font-weight:600;'
            f'color:#cbd5e1;margin-bottom:10px">🔴 Problems identified</div>{problems_html}</div>'
            if problems_html else ""
        )
        _working_block = (
            '<div style="margin-bottom:14px"><div style="font-size:13px;font-weight:600;'
            "color:#cbd5e1;margin-bottom:8px\">✅ What's working</div>"
            f'<ul style="margin:0;padding-left:18px">{working_items}</ul></div>'
            if working_items else ""
        )
        _review_block = "" if reviewed else (
            f'<div style="padding-top:14px;border-top:1px solid #1f2937;margin-top:6px">'
            f'<form method="POST" action="/admin/quality/{rid}/review">'
            '<button type="submit" style="background:#6366f1;color:#fff;border:none;'
            'border-radius:6px;padding:7px 20px;font-size:13px;font-weight:600;cursor:pointer">'
            '✓ Mark as reviewed</button></form></div>'
        )

        detail_body = f"""
        {stats_strip}
        {_problems_block}
        {_working_block}
        {_review_block}"""

        # Open the most-recent unreviewed digest automatically
        open_attr = "open" if (idx == 0 and not reviewed) else ""

        rows_html += f"""
        <details {open_attr} style="background:#111827;border:1px solid #1f2937;
                  border-radius:12px;margin-bottom:10px;overflow:hidden;">
          <summary style="display:flex;align-items:center;justify-content:space-between;
                          padding:14px 18px;cursor:pointer;list-style:none;
                          user-select:none;gap:10px;flex-wrap:wrap;">
            <div style="display:flex;align-items:center;gap:10px;flex:1;min-width:0;">
              <span style="font-size:14px;font-weight:700;color:#f1f5f9;white-space:nowrap">
                Week of {ws}
              </span>
              <span style="font-size:12px;color:#6b7280">{created_str}</span>
              {notion_link}
            </div>
            <div style="display:flex;align-items:center;gap:8px;flex-shrink:0;">
              {_trend_badge(trend)}
              {reviewed_badge}
              <span style="color:#6b7280;font-size:16px;margin-left:4px">›</span>
            </div>
          </summary>
          <div style="padding:0 18px 18px;">
            {f'<div style="font-size:12px;color:#6b7280;margin-bottom:14px;font-style:italic">{summary}</div>' if summary else ''}
            {detail_body}
          </div>
        </details>"""

    return f"""
<style>
details[open] > summary > div:last-child > span:last-child {{ transform: rotate(90deg); display:inline-block; }}
details > summary::-webkit-details-marker {{ display:none; }}
</style>
    <div style="display:flex;justify-content:space-between;align-items:center;
                margin-bottom:18px;flex-wrap:wrap;gap:10px;">
      <div>
        <div style="font-size:16px;font-weight:700;color:#f1f5f9">AI Quality Digest</div>
        <div style="font-size:12px;color:#6b7280;margin-top:2px">
          Weekly AI analysis — click any digest to read findings and proposed changes
        </div>
      </div>
      <a href="/admin/quality/run" style="background:#6366f1;color:#fff;text-decoration:none;
         border-radius:8px;padding:8px 18px;font-size:13px;font-weight:600;">
        ▶ Run digest now
      </a>
    </div>
    {rows_html}"""


# ── Routes (registered by __init__.py) ────────────────────────────────────────

def register_quality_routes(bp):
    """Attach quality routes to the given blueprint."""

    @bp.route("/admin/quality/<int:report_id>/review", methods=["POST"])
    def quality_mark_reviewed(report_id: int):
        if not check_admin_auth():
            return require_admin_auth_response()
        conn = get_db_connection()
        if not conn:
            return Response("DB not configured", status=503)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE ai_quality_reports SET reviewed_at = NOW() WHERE id = %s",
                        (report_id,),
                    )
            conn.close()
        except Exception as e:
            conn.close()
            return Response(f"Error: {e}", status=500, mimetype="text/plain")
        return _redirect("/admin?tab=quality")

    @bp.route("/admin/quality/run", methods=["GET"])
    def quality_run_digest():
        if not check_admin_auth():
            return require_admin_auth_response()

        def _stream():
            yield "Running quality digest…\n\n"
            try:
                proc = subprocess.Popen(
                    [_sys.executable, "scripts/generate_quality_digest.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                for line in proc.stdout:
                    yield line
                proc.wait()
                yield f"\nExit code: {proc.returncode}\n"
                if proc.returncode == 0:
                    yield "\nDone! Reload the Quality tab to see the new report.\n"
                else:
                    yield "\nScript exited with errors — check logs above.\n"
            except Exception as exc:
                yield f"Error launching script: {exc}\n"

        return Response(_stream(), mimetype="text/plain")
