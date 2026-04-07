"""Insights, Learning, and Impact section rendering for the admin dashboard."""

import json as _json
import os


def _esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _render_impact_section(impact: dict, era: str = "post") -> str:
    """Before/after bot impact banner."""
    if not impact:
        return ""
    pre_list        = impact.get("pre_bot_list", 0)
    post_list       = impact.get("post_bot_list", 0)
    legacy_engaged  = impact.get("legacy_engaged", 0)
    new_engaged     = impact.get("new_engaged", 0)
    legacy_pct      = impact.get("legacy_pct", 0)
    new_pct         = impact.get("new_pct", 0)
    pre_deep_pct         = impact.get("pre_deep_convo_pct", 0)
    post_deep_pct        = impact.get("post_deep_convo_pct", 0)
    pre_deep_fans        = impact.get("pre_deep_convo_fans", 0)
    post_deep_fans       = impact.get("post_deep_convo_fans", 0)
    pre_super_pct        = impact.get("pre_super_deep_pct", 0)
    post_super_pct       = impact.get("post_super_deep_pct", 0)
    pre_super_fans       = impact.get("pre_super_deep_fans", 0)
    post_super_fans      = impact.get("post_super_deep_fans", 0)
    pre_engaging_fans    = impact.get("pre_engaging_fans", 0)
    post_engaging_fans   = impact.get("post_engaging_fans", 0)
    bot_replied          = impact.get("bot_replied_fans", 0)

    # Deep/super deep convos are only meaningful post-bot.
    # Pre-bot data is a cumulative phone-number history (no per-session grouping),
    # so message counts stack across years and produce meaningless 100% figures.
    is_pre        = (era == "pre")
    deep_pct      = post_deep_pct
    deep_fans     = post_deep_fans
    super_pct     = post_super_pct
    super_fans    = post_super_fans
    engaging_fans = post_engaging_fans
    earliest_year   = impact.get("earliest_year", 2022)

    def _bar(pct, color):
        w = min(100, max(0, float(pct or 0)))
        return (
            f'<div style="background:#1f2937;border-radius:4px;height:8px;margin-top:6px;">'
            f'<div style="width:{w}%;background:{color};height:8px;border-radius:4px;'
            f'transition:width .4s;"></div></div>'
        )

    return f"""
    <div style="background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 100%);
                border:1px solid #312e81;border-radius:14px;padding:22px 24px;margin-bottom:22px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;color:#818cf8;
                  text-transform:uppercase;margin-bottom:14px;">
        Bot Impact — Before vs After March 27
      </div>
      <div style="display:grid;grid-template-columns:1fr 1px 1fr 1px 1fr 1px 1fr 1px 1fr;gap:0;align-items:start;">

        <div style="padding-right:20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Legacy subscribers (since {earliest_year})</div>
          <div style="font-size:28px;font-weight:800;color:#f87171;">{legacy_pct}%</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            {legacy_engaged:,} of {pre_list:,} SMS-only fans tried the bot
          </div>
          {_bar(legacy_pct, "#f87171")}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding:0 20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">New subscribers (post-March 27)</div>
          <div style="font-size:28px;font-weight:800;color:#4ade80;">{new_pct}%</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            {new_engaged:,} of {post_list:,} new subs texted the bot
          </div>
          {_bar(new_pct, "#4ade80")}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding:0 20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Deep convos (3+ msgs)</div>
          {"<div style='font-size:22px;font-weight:800;color:#4b5563;'>—</div><div style='font-size:11px;color:#4b5563;margin-top:4px;'>not tracked pre-bot<br>(cumulative history)</div>" if is_pre else f"<div style='font-size:28px;font-weight:800;color:#a78bfa;'>{deep_pct}%</div><div style='font-size:12px;color:#6b7280;margin-top:2px;'>{deep_fans:,} of {engaging_fans:,} fans who replied</div>{_bar(deep_pct, '#a78bfa')}"}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding:0 20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Super deep convos (5+ msgs)</div>
          {"<div style='font-size:22px;font-weight:800;color:#4b5563;'>—</div><div style='font-size:11px;color:#4b5563;margin-top:4px;'>not tracked pre-bot<br>(cumulative history)</div>" if is_pre else f"<div style='font-size:28px;font-weight:800;color:#f472b6;'>{super_pct}%</div><div style='font-size:12px;color:#6b7280;margin-top:2px;'>{super_fans:,} of {engaging_fans:,} fans who replied</div>{_bar(super_pct, '#f472b6')}"}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding-left:20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Bot replied to</div>
          <div style="font-size:28px;font-weight:800;color:#60a5fa;">{bot_replied:,}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            unique fans since launch
          </div>
        </div>

      </div>
      <div style="margin-top:14px;padding-top:12px;border-top:1px solid #1f2937;
                  font-size:11px;color:#4b5563;">
        Goal: pre-bot % stays low, post-bot penetration % and deep convo % grow show over show.
      </div>
    </div>"""


def _render_quality_tab() -> str:
    from app.admin.quality import render_quality_tab
    return render_quality_tab()


def _render_learning_tab(stats: dict) -> str:
    """Return the inner HTML for the ✨ Learning tab."""
    learning_data = stats.get("learning_data", [])

    _INTENT_LABELS = {
        "greeting":  "👋 Greeting",
        "feedback":  "😂 Feedback / Laughs",
        "question":  "❓ Question",
        "personal":  "🙋 Personal / Fan shares",
        "general":   "💬 General",
    }
    _TONE_LABELS = {
        "roast_playful":   "Roast / Playful",
        "warm_supportive": "Warm / Supportive",
        "direct_answer":   "Direct Answer",
        "celebratory":     "Celebratory",
        "sensitive_care":  "Sensitive / Care",
    }

    if not learning_data:
        return """
        <div style="padding:40px;text-align:center;color:#6b7280;">
          <div style="font-size:48px;margin-bottom:16px;">🌱</div>
          <div style="font-size:18px;font-weight:600;color:#e2e8f0;margin-bottom:8px;">No learning data yet</div>
          <div style="font-size:14px;">The bot needs at least 3 scored replies per intent/tone combo before it starts
          injecting examples. Keep running shows — the data builds up quickly.</div>
        </div>"""

    # Group by intent
    by_intent: dict = {}
    for row in learning_data:
        by_intent.setdefault(row["intent"], []).append(row)

    sections_html = ""
    for intent, rows in by_intent.items():
        active_tones = [r for r in rows if r["injected"]]
        inactive_tones = [r for r in rows if not r["injected"]]
        total_scored = sum(r["scored_total"] for r in rows)

        intent_label = _INTENT_LABELS.get(intent, intent.title())

        tone_blocks = ""
        for r in active_tones:
            tone_label = _TONE_LABELS.get(r["tone"], r["tone"])
            examples_html = ""
            for ex in r["examples"]:
                depth = ex["depth"] or 1
                delay = ex["delay"]
                delay_str = f"{delay}s" if delay else "—"
                bar_w = min(100, depth * 20)
                examples_html += f"""
                <div style="background:#111827;border-radius:8px;padding:12px 14px;margin-bottom:8px;">
                  <div style="font-size:13px;color:#e2e8f0;line-height:1.5;margin-bottom:8px;">"{_esc(ex["text"])}"</div>
                  <div style="display:flex;align-items:center;gap:12px;font-size:11px;color:#6b7280;">
                    <span style="color:#6366f1;font-weight:700;">{depth} follow-up{"s" if depth != 1 else ""}</span>
                    <span>reply in {delay_str}</span>
                    <div style="flex:1;height:4px;background:#1f2937;border-radius:2px;">
                      <div style="width:{bar_w}%;height:4px;background:#6366f1;border-radius:2px;"></div>
                    </div>
                  </div>
                </div>"""

            tone_blocks += f"""
            <div style="background:#1f2937;border-radius:10px;padding:16px;margin-bottom:12px;">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
                <span style="font-size:13px;font-weight:600;color:#a78bfa;">{_esc(tone_label)}</span>
                <span style="font-size:11px;background:#6366f1;color:#fff;padding:2px 8px;
                             border-radius:12px;">✓ Active · {r["scored_total"]} scored</span>
              </div>
              {examples_html}
            </div>"""

        inactive_html = ""
        if inactive_tones:
            inactive_names = ", ".join(
                _TONE_LABELS.get(r["tone"], r["tone"]) for r in inactive_tones
            )
            inactive_html = f"""
            <div style="font-size:12px;color:#4b5563;margin-top:4px;">
              ⏳ Not enough data yet for: {_esc(inactive_names)}
            </div>"""

        sections_html += f"""
        <div style="background:#18212f;border:1px solid #1f2937;border-radius:12px;
                    padding:20px;margin-bottom:20px;">
          <div style="display:flex;justify-content:space-between;align-items:center;
                      margin-bottom:16px;">
            <div style="font-size:16px;font-weight:700;color:#e2e8f0;">{intent_label}</div>
            <div style="font-size:12px;color:#6b7280;">{total_scored} total scored replies</div>
          </div>
          {tone_blocks if tone_blocks else
           '<div style="color:#4b5563;font-size:13px;">No active tones yet for this intent.</div>'}
          {inactive_html}
        </div>"""

    injected_count = sum(1 for r in learning_data if r["injected"])
    total_count = len(learning_data)

    return f"""
    <div style="max-width:900px;margin:0 auto;">

      <div style="margin-bottom:24px;">
        <h2 style="color:#e2e8f0;font-size:20px;font-weight:700;margin:0 0 8px;">✨ Bot Learning</h2>
        <p style="color:#94a3b8;font-size:14px;margin:0 0 16px;">
          The bot automatically learns from its most engaging past replies.
          For each intent + tone combination with enough data (≥3 scored replies),
          it injects the top examples into every new prompt — like showing a comedian
          their best bits before they go on stage.
        </p>
        <div style="display:flex;gap:16px;flex-wrap:wrap;">
          <div style="background:#1f2937;border-radius:10px;padding:14px 20px;min-width:140px;">
            <div style="font-size:11px;color:#6b7280;text-transform:uppercase;
                        letter-spacing:.06em;margin-bottom:4px;">Active Combos</div>
            <div style="font-size:28px;font-weight:800;color:#6366f1;">{injected_count}</div>
            <div style="font-size:12px;color:#6b7280;">of {total_count} tracked</div>
          </div>
          <div style="background:#1f2937;border-radius:10px;padding:14px 20px;min-width:140px;">
            <div style="font-size:11px;color:#6b7280;text-transform:uppercase;
                        letter-spacing:.06em;margin-bottom:4px;">How It Works</div>
            <div style="font-size:12px;color:#94a3b8;margin-top:4px;line-height:1.5;">
              Intent + tone detected → top replies fetched (cached 5 min)
              → injected as "what worked" examples → AI learns the pattern
            </div>
          </div>
        </div>
      </div>

      {sections_html}

    </div>"""


def _render_insights_tab(stats: dict, insights_days: int = 30, insights_era: str = "post") -> str:
    """Return the inner HTML for the 🧠 Insights tab."""
    s = stats["insights_summary"]
    scored = stats["insights_scored_total"]

    if not scored:
        if insights_era == "pre":
            empty_msg = (
                "<div style='font-size:16px;font-weight:600;color:#9ca3af;margin-bottom:8px;'>"
                "No AI conversations before March 27</div>"
                "<p style='font-size:13px;max-width:480px;margin:0 auto;line-height:1.6;'>"
                "Before the bot launched, there were no AI-driven conversations — only one-way SMS blasts. "
                "This is the baseline: 0% reply rate, 0 scored replies. Switch to <b>Post-bot</b> to see what the AI changed."
                "</p>"
            )
        else:
            empty_msg = (
                "<div style='font-size:16px;font-weight:600;color:#9ca3af;margin-bottom:8px;'>"
                "No engagement data yet</div>"
                "<p style='font-size:13px;max-width:420px;margin:0 auto;line-height:1.6;'>"
                "Data starts accumulating as fans text in. Come back after your next show."
                "</p>"
            )
        _era_bar = (
            f'<div style="display:flex;gap:8px;justify-content:center;margin-top:16px;">'
            f'<a href="/admin?tab=insights&era=pre" style="padding:6px 18px;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;'
            f'{"background:#f87171;color:#fff;" if insights_era == "pre" else "background:#1f2937;color:#94a3b8;"}">'
            f'Pre-bot</a>'
            f'<a href="/admin?tab=insights&era=post" style="padding:6px 18px;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;'
            f'{"background:#6366f1;color:#fff;" if insights_era == "post" else "background:#1f2937;color:#94a3b8;"}">'
            f'Post-bot</a></div>'
        )
        return f"""
        <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;
                    padding:40px;text-align:center;color:#6b7280;margin-top:8px;">
          <div style="font-size:32px;margin-bottom:12px;">🧠</div>
          {empty_msg}
          {_era_bar}
        </div>"""

    def _pct_color(v):
        if v is None:
            return "#6b7280"
        return "#4ade80" if v >= 60 else ("#fbbf24" if v >= 40 else "#f87171")

    def _drop_color(v):
        if v is None:
            return "#6b7280"
        return "#f87171" if v >= 30 else ("#fbbf24" if v >= 15 else "#4ade80")

    reply_rate = s.get("reply_rate_pct")
    dropoff    = s.get("dropoff_rate_pct")
    delay      = s.get("avg_reply_delay_s")
    avg_len    = s.get("avg_bot_reply_length")

    _era_btn_style = "padding:6px 18px;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;border:none;cursor:pointer;"
    _pre_active  = insights_era == "pre"
    _post_active = insights_era == "post"
    era_toggle_html = (
        f'<a href="/admin?tab=insights&era=pre&days={insights_days}" style="{_era_btn_style}'
        f'{"background:#f87171;color:#fff;" if _pre_active else "background:#1f2937;color:#94a3b8;"}">'
        f'Pre-bot (before Mar 27)</a>'
        f'<a href="/admin?tab=insights&era=post&days={insights_days}" style="{_era_btn_style}'
        f'{"background:#6366f1;color:#fff;" if _post_active else "background:#1f2937;color:#94a3b8;"}">'
        f'Post-bot (after Mar 27)</a>'
    )
    day_picker_html = "".join(
        f'<a href="/admin?tab=insights&era={insights_era}&days={d}" style="'
        f'padding:5px 12px;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;'
        f'{"background:#6366f1;color:#fff;" if d == insights_days else "background:#1f2937;color:#94a3b8;"}'
        f'">{d}d</a>'
        for d in (7, 14, 30)
    ) if not _pre_active else ""
    date_filter_bar = f"""
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:18px;flex-wrap:wrap;">
      <span style="color:#6b7280;font-size:13px;margin-right:4px;">Era:</span>
      {era_toggle_html}
      {"<span style='color:#374151;margin:0 6px;'>|</span><span style='color:#6b7280;font-size:13px;'>Window:</span>" + day_picker_html if day_picker_html else ""}
    </div>"""

    impact_html = _render_impact_section(stats.get("insights_impact", {}), era=insights_era)
    summary_html = impact_html + date_filter_bar + f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px;">
      <div class="stat-card">
        <div class="stat-label">Scored Replies</div>
        <div class="stat-value">{scored:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">{"before Mar 27" if insights_era == "pre" else f"last {insights_days} days"} · excl. unclassified</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Reply Rate</div>
        <div class="stat-value" style="color:{_pct_color(reply_rate)}">{reply_rate if reply_rate is not None else '—'}{'%' if reply_rate is not None else ''}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">fans who texted back · excl. unclassified</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Drop-off Rate</div>
        <div class="stat-value" style="color:{_drop_color(dropoff)}">{dropoff if dropoff is not None else '—'}{'%' if dropoff is not None else ''}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">bot msg then silence · excl. unclassified</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Reply Delay</div>
        <div class="stat-value purple">{int(delay) if delay is not None else '—'}{'s' if delay is not None else ''}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">fan response time</div>
      </div>
    </div>"""

    # Intent breakdown table
    intent_rows_html = ""
    for r in stats["insights_intent"]:
        rr = r.get("reply_rate_pct")
        dr = r.get("dropoff_rate_pct")
        d  = r.get("avg_delay_s")
        intent_rows_html += f"""
        <tr>
          <td style="padding:10px 14px;font-weight:600;color:#e2e8f0;">{_esc(str(r.get("intent","?")).upper())}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">{r.get("total",0):,}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_pct_color(rr)}">{rr if rr is not None else '—'}{'%' if rr is not None else ''}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_drop_color(dr)}">{dr if dr is not None else '—'}{'%' if dr is not None else ''}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">{int(d) if d is not None else '—'}{'s' if d is not None else ''}</td>
        </tr>"""
    if not intent_rows_html:
        intent_rows_html = f'<tr><td colspan="5" style="padding:24px;text-align:center;color:#6b7280;font-style:italic;">No data yet for last {insights_days} days.</td></tr>'

    intent_table = f"""
    <div class="card" style="margin-bottom:20px;padding:0;overflow:hidden;">
      <div style="padding:16px 20px 12px;border-bottom:1px solid #1f2937;">
        <div class="card-title" style="margin:0;">Engagement by Intent — {"Before Mar 27 (Pre-bot)" if insights_era == "pre" else f"Last {insights_days} Days"}</div>
      </div>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="border-bottom:1px solid #1f2937;">
            <th style="padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Intent</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Scored</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Reply Rate ↑</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Drop-off ↓</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Avg Delay</th>
          </tr>
        </thead>
        <tbody>{intent_rows_html}</tbody>
      </table>
    </div>"""

    # Tone breakdown table
    tone_rows_html = ""
    for r in stats["insights_tone"]:
        rr = r.get("reply_rate_pct")
        dr = r.get("dropoff_rate_pct")
        tone_rows_html += f"""
        <tr>
          <td style="padding:10px 14px;font-weight:600;color:#e2e8f0;">{_esc(str(r.get("tone_mode","?"))).title()}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">{r.get("total",0):,}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_pct_color(rr)}">{rr if rr is not None else '—'}{'%' if rr is not None else ''}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_drop_color(dr)}">{dr if dr is not None else '—'}{'%' if dr is not None else ''}</td>
        </tr>"""
    if not tone_rows_html:
        tone_rows_html = '<tr><td colspan="4" style="padding:24px;text-align:center;color:#6b7280;font-style:italic;">No data yet.</td></tr>'

    tone_table = f"""
    <div class="card" style="margin-bottom:20px;padding:0;overflow:hidden;">
      <div style="padding:16px 20px 12px;border-bottom:1px solid #1f2937;">
        <div class="card-title" style="margin:0;">Engagement by Tone — {"Before Mar 27 (Pre-bot)" if insights_era == "pre" else f"Last {insights_days} Days"}</div>
      </div>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="border-bottom:1px solid #1f2937;">
            <th style="padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Tone</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Scored</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Reply Rate ↑</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Drop-off ↓</th>
          </tr>
        </thead>
        <tbody>{tone_rows_html}</tbody>
      </table>
    </div>"""

    # Drop-off trigger list
    dropoff_items_html = ""
    for r in stats["insights_dropoff"]:
        preview = _esc(str(r.get("preview") or ""))
        intent  = _esc(str(r.get("intent") or "—").upper())
        tone    = _esc(str(r.get("tone_mode") or "—"))
        chars   = r.get("reply_length_chars")
        dropoff_items_html += f"""
        <div style="padding:12px 0;border-bottom:1px solid #1f2937;">
          <div style="display:flex;gap:8px;margin-bottom:6px;flex-wrap:wrap;">
            <span style="background:#1f2937;color:#94a3b8;padding:2px 8px;border-radius:8px;font-size:11px;">{intent}</span>
            <span style="background:#1f2937;color:#94a3b8;padding:2px 8px;border-radius:8px;font-size:11px;">{tone}</span>
            {'<span style="background:#1f2937;color:#94a3b8;padding:2px 8px;border-radius:8px;font-size:11px;">' + str(chars) + ' chars</span>' if chars else ''}
          </div>
          <div style="color:#d1d5db;font-size:13px;line-height:1.45;">{preview}</div>
        </div>"""

    if not dropoff_items_html:
        dropoff_items_html = '<p class="empty-note">No drop-off triggers recorded yet. Run the nightly backfill script to score older messages.</p>'

    dropoff_section = f"""
    <div class="card" style="margin-bottom:20px;">
      <div class="card-title">Drop-off Triggers — Last Bot Message Before Fan Went Silent (last 30d)</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:12px;">These are the bot messages that ended in silence — patterns here tell you what to avoid.</p>
      {dropoff_items_html}
    </div>"""

    # Session stats section
    sess = stats.get("insights_session", {})
    total_sess = sess.get("total_sessions") or 0
    avg_msgs   = sess.get("avg_user_msgs")
    max_depth  = sess.get("max_depth")
    came_back  = sess.get("came_back_7d") or 0
    closed_s   = sess.get("closed_sessions") or 0
    ret_7d     = round(came_back / closed_s * 100, 1) if closed_s else None

    session_html = ""
    if total_sess:
        session_html = f"""
        <div class="card" style="margin-bottom:20px;">
          <div class="card-title">Conversation Sessions — Last 30 Days</div>
          <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:4px;">
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">Total Sessions</div>
              <div class="stat-value">{total_sess:,}</div>
            </div>
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">Avg Fan Messages</div>
              <div class="stat-value purple">{avg_msgs if avg_msgs is not None else '—'}</div>
              <div style="color:#64748b;font-size:12px">per session</div>
            </div>
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">Deepest Session</div>
              <div class="stat-value teal">{max_depth if max_depth is not None else '—'}</div>
              <div style="color:#64748b;font-size:12px">total messages</div>
            </div>
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">7-Day Return Rate</div>
              <div class="stat-value" style="color:{_pct_color(ret_7d)}">{ret_7d if ret_7d is not None else '—'}{'%' if ret_7d is not None else ''}</div>
              <div style="color:#64748b;font-size:12px">fans who came back</div>
            </div>
          </div>
          <p style="color:#64748b;font-size:12px;margin-top:12px;">
            Session = contiguous conversation. New session after {_esc(str(os.getenv('SESSION_GAP_HOURS', '24')))}h of silence.
            Run <code style="color:#a5b4fc">python scripts/backfill_silence.py</code> nightly to close stale sessions.
          </p>
        </div>"""

    api_hint = f"""
    <div class="card" style="margin-bottom:0;background:#0d0d1a;border-color:#1a1a3a;">
      <div class="card-title">JSON API — programmatic access</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:10px;">Same data in JSON format, useful for scripts and external tools. All require HTTP Basic Auth (same password).</p>
      <div style="display:flex;flex-direction:column;gap:6px;font-size:12px;">
        <code style="color:#a5b4fc;">/analytics/engagement-summary</code>
        <code style="color:#a5b4fc;">/analytics/intent-breakdown</code>
        <code style="color:#a5b4fc;">/analytics/tone-breakdown</code>
        <code style="color:#a5b4fc;">/analytics/dropoff-triggers</code>
        <code style="color:#a5b4fc;">/analytics/top-bot-replies</code>
        <code style="color:#a5b4fc;">/analytics/reply-length-buckets</code>
      </div>
      <p style="color:#4b5563;font-size:11px;margin-top:10px;">Append <code>?days=7</code> (or 14, 30, 90) to any endpoint to change the window.</p>
    </div>"""

    # ── Blasts section ────────────────────────────────────────────────────
    blasts = stats.get("insights_blasts", [])
    _admin_b64 = __import__('base64').b64encode(f'admin:{os.getenv("ADMIN_PASSWORD","")}'.encode()).decode()

    # Build all blast rows as JSON for client-side tab filtering
    blasts_json = _json.dumps([{
        "id":            b["id"],
        "name":          b.get("name", ""),
        "sent_at_str":   b.get("sent_at_str", ""),
        "sent_count":    b.get("sent_count") or 0,
        "replies_24h":   b.get("replies_24h") or 0,
        "reply_rate_pct":b.get("reply_rate_pct", 0),
        "ctr_pct":       b.get("ctr_pct"),
        "link_clicks":   b.get("link_clicks") or 0,
        "unsub_rate_pct":b.get("unsub_rate_pct"),
        "opt_out_count": b.get("opt_out_count") or 0,
        "blast_category":b.get("blast_category") or "",
    } for b in blasts])

    _cat_colors = {
        "friendly": ("#22d3ee", "#0e7490"),   # cyan
        "sales":    ("#a78bfa", "#6d28d9"),   # purple
        "show":     ("#fb923c", "#c2410c"),   # orange
        "":         ("#4b5563", "#374151"),   # grey (uncategorized)
    }

    blast_section = f"""
    <script>
    const _blastData = {blasts_json};
    const _blastAuth = 'Basic {_admin_b64}';
    const _catLabels = {{ friendly:'💬 Friendly', sales:'🛒 Sales', show:'🎤 Shows', '':'Uncategorized' }};
    const _catColors = {{
      friendly:['#22d3ee','#0e7490'], sales:['#a78bfa','#6d28d9'],
      show:['#fb923c','#c2410c'],    '':['#4b5563','#374151']
    }};

    let _activeBlastTab = 'all';

    function _pctColor(p) {{
      if (p >= 30) return '#22c55e';
      if (p >= 10) return '#eab308';
      if (p >= 5)  return '#f97316';
      return '#ef4444';
    }}

    function renderBlastTable() {{
      const filter = _activeBlastTab;
      const rows = _blastData.filter(b => filter === 'all' || b.blast_category === filter);
      const tbody = document.getElementById('blast-tbody');
      if (!tbody) return;

      // Tab counts
      ['all','friendly','sales','show'].forEach(cat => {{
        const cnt = cat === 'all' ? _blastData.length
                                  : _blastData.filter(b => b.blast_category === cat).length;
        const el = document.getElementById('blast-tab-' + cat);
        if (el) el.querySelector('.btab-cnt').textContent = cnt;
        if (el) {{
          const active = cat === _activeBlastTab;
          el.style.borderBottomColor = active ? (cat === 'all' ? '#818cf8' : (_catColors[cat]||['#818cf8'])[0]) : 'transparent';
          el.style.color = active ? '#e2e8f0' : '#6b7280';
        }}
      }});

      if (!rows.length) {{
        tbody.innerHTML = '<tr><td colspan="8" style="padding:24px;text-align:center;color:#4b5563;font-size:13px;">No blasts in this category yet — assign them using the dropdown on each row.</td></tr>';
        return;
      }}

      tbody.innerHTML = rows.map(b => {{
        const rr = b.reply_rate_pct;
        const rrColor = _pctColor(rr);
        const ctrCell = b.ctr_pct == null ? '—'
          : `<span style="font-weight:700;color:#818cf8">${{b.ctr_pct}}%</span> <span style="color:#4b5563;font-size:11px">(${{b.link_clicks.toLocaleString()}} clicks)</span>`;
        const unsubCell = b.unsub_rate_pct == null ? '—'
          : `<span style="font-weight:700;color:#f87171">${{b.unsub_rate_pct}}%</span> <span style="color:#4b5563;font-size:11px">(${{b.opt_out_count}})</span>`;
        const cat = b.blast_category || '';
        const [cc, cd] = _catColors[cat] || _catColors[''];
        const catLabel = _catLabels[cat] || 'Uncategorized';
        return `<tr id="blast-row-${{b.id}}">
          <td style="padding:10px 14px;">
            <div style="font-weight:600;color:#e2e8f0;margin-bottom:4px;">${{b.name}}</div>
            <select onchange="setBlastCategory(${{b.id}}, this.value)"
              style="background:#1e293b;border:1px solid ${{cd}};color:${{cc}};
                     padding:2px 6px;border-radius:5px;font-size:11px;cursor:pointer;">
              <option value="" ${{cat===''?'selected':''}}>Uncategorized</option>
              <option value="friendly" ${{cat==='friendly'?'selected':''}}>💬 Friendly</option>
              <option value="sales" ${{cat==='sales'?'selected':''}}>🛒 Sales</option>
              <option value="show" ${{cat==='show'?'selected':''}}>🎤 Shows</option>
            </select>
          </td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{b.sent_at_str}}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{b.sent_count.toLocaleString()}}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{b.replies_24h.toLocaleString()}}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:${{rrColor}}">${{rr}}%</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{ctrCell}}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{unsubCell}}</td>
          <td style="padding:10px 14px;text-align:center;">
            <button onclick="deleteBlast(${{b.id}})"
              style="background:transparent;border:1px solid #374151;color:#6b7280;padding:3px 10px;
                     border-radius:6px;font-size:12px;cursor:pointer;"
              onmouseover="this.style.borderColor='#f87171';this.style.color='#f87171'"
              onmouseout="this.style.borderColor='#374151';this.style.color='#6b7280'">
              Delete
            </button>
          </td>
        </tr>`;
      }}).join('');
    }}

    function setBlastCategory(id, cat) {{
      const blast = _blastData.find(b => b.id === id);
      if (blast) blast.blast_category = cat;
      fetch('/admin/actions/set-blast-category/' + id, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json', 'Authorization': _blastAuth }},
        body: JSON.stringify({{ category: cat }}),
      }}).then(r => {{ if (!r.ok) alert('Save failed'); else renderBlastTable(); }});
    }}

    function deleteBlast(id) {{
      if (!confirm('Delete this blast from analytics?')) return;
      fetch('/admin/actions/delete-blast/' + id, {{
        method: 'POST', headers: {{ 'Authorization': _blastAuth }},
      }}).then(r => {{
        if (r.ok) {{ const i = _blastData.findIndex(b => b.id===id); if (i>=0) _blastData.splice(i,1); renderBlastTable(); }}
        else alert('Delete failed');
      }});
    }}

    function submitExternalBlast() {{
      const name = document.getElementById('eb-name').value.trim();
      const date = document.getElementById('eb-date').value;
      const sent = parseInt(document.getElementById('eb-sent').value) || 0;
      const optouts = parseInt(document.getElementById('eb-optouts').value) || 0;
      const clicks = document.getElementById('eb-clicks').value.trim();
      const link_clicks = clicks !== '' ? parseInt(clicks) : null;
      if (!name || !date || !sent) {{ alert('Name, date, and sent count are required.'); return; }}
      fetch('/admin/actions/add-external-blast', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json', 'Authorization': _blastAuth }},
        body: JSON.stringify({{ name, date, sent_count: sent, opt_out_count: optouts, link_clicks }}),
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
        else alert('Error: ' + (d.error || 'unknown'));
      }});
    }}

    document.addEventListener('DOMContentLoaded', renderBlastTable);
    </script>

    <div class="card" style="margin-bottom:20px;padding:0;overflow:hidden;">
      <!-- Header -->
      <div style="padding:16px 20px 0;border-bottom:1px solid #1f2937;">
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px;">
          <div class="card-title" style="margin:0;">📣 Blast Performance</div>
          <span style="font-size:12px;color:#6b7280;">reply rate = subscribers who texted back within 24h</span>
          <button onclick="document.getElementById('add-blast-panel').style.display='block'"
            style="margin-left:auto;background:#1e293b;border:1px solid #374151;color:#94a3b8;
                   padding:5px 12px;border-radius:6px;font-size:12px;cursor:pointer;white-space:nowrap;"
            onmouseover="this.style.borderColor='#4f46e5';this.style.color='#818cf8'"
            onmouseout="this.style.borderColor='#374151';this.style.color='#94a3b8'">
            + Add external blast
          </button>
        </div>
        <!-- Category tabs -->
        <div style="display:flex;gap:0;">
          {"".join(f'''<button id="blast-tab-{cat}" onclick="_activeBlastTab='{cat}';renderBlastTable()"
            style="padding:8px 16px;background:transparent;border:none;border-bottom:2px solid transparent;
                   color:#6b7280;font-size:13px;font-weight:500;cursor:pointer;transition:all .15s;">
            {label} <span class="btab-cnt" style="background:#1e293b;border-radius:10px;
                    padding:1px 7px;font-size:11px;margin-left:4px;">0</span>
          </button>''' for cat, label in [("all","All"), ("friendly","💬 Friendly"), ("sales","🛒 Sales"), ("show","🎤 Shows")])}
        </div>
      </div>

      <!-- Add external blast form -->
      <div id="add-blast-panel" style="display:none;padding:16px 20px;border-bottom:1px solid #1f2937;background:#0f172a;">
        <div style="font-size:13px;color:#94a3b8;margin-bottom:12px;">Add an external blast (e.g. from SlickText)</div>
        <div style="display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr 1fr;gap:10px;align-items:end;">
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Blast Name</label>
            <input id="eb-name" type="text" placeholder="e.g. Zarna Voice Note"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Date Sent</label>
            <input id="eb-date" type="date"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Sent Count</label>
            <input id="eb-sent" type="number" min="0" placeholder="4238"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Opt-outs</label>
            <input id="eb-optouts" type="number" min="0" placeholder="0"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Link Clicks</label>
            <input id="eb-clicks" type="number" min="0" placeholder="0"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div style="display:flex;gap:8px;">
            <button onclick="submitExternalBlast()"
              style="flex:1;background:#4f46e5;border:none;color:#fff;padding:7px 14px;
                     border-radius:6px;font-size:13px;cursor:pointer;font-weight:600;">Add</button>
            <button onclick="document.getElementById('add-blast-panel').style.display='none'"
              style="background:transparent;border:1px solid #374151;color:#6b7280;padding:7px 12px;
                     border-radius:6px;font-size:13px;cursor:pointer;">✕</button>
          </div>
        </div>
      </div>

      <!-- Table -->
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="border-bottom:1px solid #1f2937;">
            <th style="padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Blast</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Date</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Sent</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Replies (24h)</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Reply Rate</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Link CTR</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Unsub Rate</th>
            <th style="padding:10px 14px;"></th>
          </tr>
        </thead>
        <tbody id="blast-tbody"><tr><td colspan="8" style="padding:20px;text-align:center;color:#4b5563;">Loading…</td></tr></tbody>
      </table>
    </div>"""

    return summary_html + intent_table + tone_table + session_html + blast_section + dropoff_section + api_hint

