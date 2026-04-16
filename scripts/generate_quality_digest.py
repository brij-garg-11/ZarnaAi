#!/usr/bin/env python3
"""
Weekly AI Quality Digest — detect engagement problems, propose fixes, save to DB + Notion.

Env (required):
  DATABASE_URL          Production Postgres (same as web service)
  GEMINI_API_KEY        For Gemini analysis

Env (optional — Notion page creation):
  NOTION_TOKEN                   Internal integration secret
  NOTION_DIGEST_PARENT_ID        Page ID of the "AI Quality Digest" parent page in Notion
                                 (share this page with your integration in Notion → Connections)
  NOTION_API_VERSION             Default: 2022-06-28

Run:
  python scripts/generate_quality_digest.py            # analyse last 7 days
  python scripts/generate_quality_digest.py --dry-run  # print report, don't save
  python scripts/generate_quality_digest.py --days 14  # widen window
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("quality_digest")

# ── DB ───────────────────────────────────────────────────────────────────────

def _conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    import psycopg2
    import psycopg2.extras
    c = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    c.autocommit = False
    return c


def ensure_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_quality_reports (
                    id            SERIAL PRIMARY KEY,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    week_start    DATE NOT NULL,
                    headline_json TEXT NOT NULL DEFAULT '{}',
                    findings_json TEXT NOT NULL DEFAULT '[]',
                    notion_page_id TEXT,
                    reviewed_at   TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_quality_reports_week
                ON ai_quality_reports(week_start DESC)
            """)


# ── Analytics queries ─────────────────────────────────────────────────────────

def fetch_analytics(conn, this_start: datetime, this_end: datetime, baseline_start: datetime):
    """
    Returns a dict:
      headline  — overall stats for the current window
      intent    — per-intent stats (this window + baseline)
      tone      — per-tone stats
      length    — reply-rate by length bucket
      silenced  — top 8 bot replies that drove silence this week
      winners   — top 5 replies with the fastest fan responses
    """
    with conn.cursor() as cur:
        # ── Headline ──────────────────────────────────────────────────────────
        cur.execute("""
            SELECT
              COUNT(*)                                                  AS scored,
              ROUND(AVG(did_user_reply::int) * 100, 1)                  AS reply_rate,
              ROUND(
                100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                / NULLIF(COUNT(*), 0), 1
              )                                                         AS dropoff_rate,
              ROUND(AVG(reply_length_chars))                            AS avg_len
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply IS NOT NULL
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
        """, (this_start, this_end))
        headline = dict(cur.fetchone() or {})

        # Baseline reply rate (prior 4 weeks)
        cur.execute("""
            SELECT ROUND(AVG(did_user_reply::int) * 100, 1) AS baseline_reply_rate
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply IS NOT NULL
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
        """, (baseline_start, this_start))
        row = cur.fetchone()
        headline["baseline_reply_rate"] = (row or {}).get("baseline_reply_rate")

        # ── Intent breakdown — this week ──────────────────────────────────────
        cur.execute("""
            SELECT
              COALESCE(intent, 'unknown')                     AS intent,
              COUNT(*)                                        AS total,
              ROUND(AVG(did_user_reply::int) * 100, 1)        AS reply_rate,
              ROUND(
                100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                / NULLIF(COUNT(*), 0), 1
              )                                               AS dropoff_rate
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply IS NOT NULL
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
            GROUP BY COALESCE(intent, 'unknown')
            ORDER BY total DESC
        """, (this_start, this_end))
        intent_this = {r["intent"]: r for r in cur.fetchall()}

        # Intent baseline
        cur.execute("""
            SELECT
              COALESCE(intent, 'unknown')                AS intent,
              ROUND(AVG(did_user_reply::int) * 100, 1)   AS reply_rate
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply IS NOT NULL
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
            GROUP BY COALESCE(intent, 'unknown')
        """, (baseline_start, this_start))
        intent_base = {r["intent"]: r["reply_rate"] for r in cur.fetchall()}

        intent = []
        for k, v in intent_this.items():
            base = intent_base.get(k)
            delta = None
            if v["reply_rate"] is not None and base is not None:
                delta = float(v["reply_rate"]) - float(base)
            intent.append({
                "intent":     k,
                "total":      v["total"],
                "reply_rate": float(v["reply_rate"]) if v["reply_rate"] is not None else None,
                "dropoff_rate": float(v["dropoff_rate"]) if v["dropoff_rate"] is not None else None,
                "baseline_reply_rate": float(base) if base is not None else None,
                "delta_pp":   round(delta, 1) if delta is not None else None,
            })
        intent.sort(key=lambda x: x["total"], reverse=True)

        # ── Tone breakdown ────────────────────────────────────────────────────
        cur.execute("""
            SELECT
              COALESCE(tone_mode, 'unknown')             AS tone,
              COUNT(*)                                   AS total,
              ROUND(AVG(did_user_reply::int) * 100, 1)   AS reply_rate,
              ROUND(
                100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                / NULLIF(COUNT(*), 0), 1
              )                                          AS dropoff_rate
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply IS NOT NULL
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
            GROUP BY COALESCE(tone_mode, 'unknown')
            ORDER BY total DESC
        """, (this_start, this_end))
        tone = [dict(r) for r in cur.fetchall()]

        # ── Reply-rate by length bucket ───────────────────────────────────────
        cur.execute("""
            SELECT
              CASE
                WHEN reply_length_chars <= 60  THEN '≤60'
                WHEN reply_length_chars <= 100 THEN '61-100'
                WHEN reply_length_chars <= 150 THEN '101-150'
                WHEN reply_length_chars <= 200 THEN '151-200'
                ELSE '201+'
              END                                       AS bucket,
              COUNT(*)                                  AS total,
              ROUND(AVG(did_user_reply::int) * 100, 1)  AS reply_rate
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply IS NOT NULL
              AND reply_length_chars IS NOT NULL
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
            GROUP BY 1
            ORDER BY MIN(reply_length_chars)
        """, (this_start, this_end))
        length = [dict(r) for r in cur.fetchall()]

        # ── Top silenced replies (full text for Gemini) ───────────────────────
        cur.execute("""
            SELECT
              LEFT(text, 220)        AS preview,
              intent,
              tone_mode,
              reply_length_chars     AS chars
            FROM messages
            WHERE role = 'assistant'
              AND went_silent_after = TRUE
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
            ORDER BY created_at DESC
            LIMIT 8
        """, (this_start, this_end))
        silenced = [dict(r) for r in cur.fetchall()]

        # ── Best performers (fans replied quickly) ────────────────────────────
        cur.execute("""
            SELECT
              LEFT(text, 180)       AS preview,
              intent,
              tone_mode,
              reply_delay_seconds   AS reply_s,
              reply_length_chars    AS chars
            FROM messages
            WHERE role = 'assistant'
              AND did_user_reply = TRUE
              AND reply_delay_seconds IS NOT NULL
              AND reply_delay_seconds > 0
              AND source IS DISTINCT FROM 'csv_import'
              AND source IS DISTINCT FROM 'blast'
              AND created_at >= %s AND created_at < %s
            ORDER BY reply_delay_seconds
            LIMIT 5
        """, (this_start, this_end))
        winners = [dict(r) for r in cur.fetchall()]

    return dict(
        headline=headline,
        intent=intent,
        tone=tone,
        length=length,
        silenced=silenced,
        winners=winners,
    )


# ── Gemini analysis ───────────────────────────────────────────────────────────

def _gemini_client():
    from google import genai as _genai
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set")
    return _genai.Client(api_key=key)


def _fmt_silenced(silenced: list) -> str:
    lines = []
    for i, r in enumerate(silenced, 1):
        lines.append(
            f"{i}. [{r.get('intent','?')} / {r.get('tone_mode','?')} / {r.get('chars','?')}ch]"
            f" \"{r.get('preview','').strip()}\""
        )
    return "\n".join(lines) if lines else "None this week"


def _fmt_intent(intent: list) -> str:
    lines = []
    for r in intent:
        rr = r.get("reply_rate")
        base = r.get("baseline_reply_rate")
        delta = r.get("delta_pp")
        delta_str = ""
        if delta is not None:
            arrow = "↑" if delta >= 0 else "↓"
            delta_str = f" ({arrow}{abs(delta):.1f}pp vs baseline)"
        lines.append(
            f"  {r['intent']:20s}  n={r['total']:4d}  "
            f"reply={rr if rr is not None else '—'}%  "
            f"dropoff={r.get('dropoff_rate', '—')}%"
            f"{delta_str}"
        )
    return "\n".join(lines) if lines else "No data"


def _fmt_length(length: list) -> str:
    return "\n".join(
        f"  {r['bucket']:10s}  n={r['total']:4d}  reply={r['reply_rate']}%"
        for r in length
    ) if length else "No data"


def build_analysis_prompt(data: dict, week_start: date) -> str:
    h = data["headline"]
    scored = h.get("scored") or 0
    rr = h.get("reply_rate")
    base_rr = h.get("baseline_reply_rate")
    dropoff = h.get("dropoff_rate")
    avg_len = h.get("avg_len")

    rr_str = f"{rr}%" if rr is not None else "—"
    base_str = f"{base_rr}%" if base_rr is not None else "—"
    delta_str = ""
    if rr is not None and base_rr is not None:
        d = float(rr) - float(base_rr)
        arrow = "↑" if d >= 0 else "↓"
        delta_str = f" ({arrow}{abs(d):.1f}pp vs baseline)"

    return f"""You are analyzing engagement data for an AI-powered celebrity fan text assistant.
The assistant texts fans on behalf of a comedian/influencer named Zarna.
Your job: identify the top problems hurting reply rates and propose specific, actionable fixes.

=== Week of {week_start} ===
Scored bot replies this week : {scored:,}
Overall reply rate            : {rr_str}{delta_str}
4-week baseline reply rate    : {base_str}
Drop-off rate (went silent)   : {dropoff if dropoff is not None else '—'}%
Avg bot reply length          : {avg_len if avg_len is not None else '—'} chars

--- Intent breakdown (this week vs baseline) ---
{_fmt_intent(data['intent'])}

--- Reply rate by reply length ---
{_fmt_length(data['length'])}

--- Top 8 replies that drove fan silence (fan never texted back) ---
{_fmt_silenced(data['silenced'])}

--- TASK ---
1. Identify exactly 3 concrete problems hurting reply rates (use the data above as evidence).
   Focus on patterns, not one-off replies.
2. For each problem, propose a SPECIFIC change to how the AI should write replies
   (e.g. length limits, tone instructions, phrase patterns to add/avoid, intent-specific rules).
3. Name 2 things that are working well.

Return ONLY a valid JSON object — no markdown, no preamble — with this exact structure:
{{
  "one_line_summary": "string (≤20 words)",
  "overall_trend": "improving|declining|stable",
  "problems": [
    {{
      "title": "short problem name",
      "evidence": "specific numbers / quotes from the data",
      "proposed_change": "concrete, actionable instruction for the AI voice config",
      "severity": "high|medium|low"
    }}
  ],
  "whats_working": ["string", "string"]
}}"""


def call_gemini(prompt: str) -> dict:
    # Use the same model as the web service (already configured in Railway env)
    model = os.getenv("DIGEST_MODEL", os.getenv("GENERATION_MODEL", "gemini-2.5-flash"))
    client = _gemini_client()
    response = client.models.generate_content(model=model, contents=prompt)
    raw = (response.text or "").strip()
    log.info("Gemini raw response length: %d chars", len(raw))

    # Strip markdown code fences if present
    if "```" in raw:
        # grab content between first ``` and last ```
        parts = raw.split("```")
        # parts[1] is the block after opening fence
        if len(parts) >= 3:
            raw = parts[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

    # Fallback: find first { ... } blob via regex
    if not raw.startswith("{"):
        import re
        m = re.search(r"\{[\s\S]+\}", raw)
        if m:
            raw = m.group(0)

    return json.loads(raw)


# ── Notion page creation ──────────────────────────────────────────────────────

def _notion_headers() -> dict:
    token = os.getenv("NOTION_TOKEN", "")
    version = os.getenv("NOTION_API_VERSION", "2022-06-28")
    if not token:
        raise RuntimeError("NOTION_TOKEN not set")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": version,
    }


def _severity_emoji(s: str) -> str:
    return {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(s, "⚪")


def _trend_emoji(t: str) -> str:
    return {"improving": "📈", "declining": "📉", "stable": "➡️"}.get(t, "")


def _rich_text(content: str) -> list:
    return [{"type": "text", "text": {"content": content}}]


def _heading2(text: str) -> dict:
    return {"object": "block", "type": "heading_2",
            "heading_2": {"rich_text": _rich_text(text)}}


def _heading3(text: str) -> dict:
    return {"object": "block", "type": "heading_3",
            "heading_3": {"rich_text": _rich_text(text)}}


def _para(text: str) -> dict:
    return {"object": "block", "type": "paragraph",
            "paragraph": {"rich_text": _rich_text(text)}}


def _bullet(text: str) -> dict:
    return {"object": "block", "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": _rich_text(text)}}


def _divider() -> dict:
    return {"object": "block", "type": "divider", "divider": {}}


def _callout(text: str, emoji: str = "📌") -> dict:
    return {
        "object": "block",
        "type": "callout",
        "callout": {
            "rich_text": _rich_text(text),
            "icon": {"type": "emoji", "emoji": emoji},
            "color": "gray_background",
        },
    }


def build_notion_blocks(
    week_start: date,
    headline: dict,
    findings: dict,
    data: dict,
) -> list:
    """Build Notion page blocks for the digest."""
    h = headline
    rr = h.get("reply_rate")
    base = h.get("baseline_reply_rate")
    trend = findings.get("overall_trend", "stable")
    summary = findings.get("one_line_summary", "")
    problems = findings.get("problems", [])
    working = findings.get("whats_working", [])

    rr_str = f"{rr}%" if rr is not None else "—"
    base_str = f"{base}%" if base is not None else "—"

    blocks: list = []

    # Banner
    blocks.append(_callout(
        f"{_trend_emoji(trend)}  Week of {week_start}  —  {summary}",
        _trend_emoji(trend) or "📊",
    ))
    blocks.append(_divider())

    # Headlines
    blocks.append(_heading2("📊 This Week at a Glance"))
    blocks.append(_bullet(f"Scored bot replies: {h.get('scored', 0):,}"))
    blocks.append(_bullet(f"Overall reply rate: {rr_str}  (4-week baseline: {base_str})"))
    blocks.append(_bullet(f"Drop-off rate (went silent): {h.get('dropoff_rate', '—')}%"))
    blocks.append(_bullet(f"Avg reply length: {h.get('avg_len', '—')} characters"))
    blocks.append(_divider())

    # Intent breakdown
    if data.get("intent"):
        blocks.append(_heading2("🎯 Intent Breakdown"))
        for r in data["intent"]:
            delta = r.get("delta_pp")
            arrow = ""
            if delta is not None:
                arrow = f"  ({'↑' if delta >= 0 else '↓'}{abs(delta):.1f}pp)" if delta != 0 else "  (→ flat)"
            blocks.append(_bullet(
                f"{r['intent']:20s}  n={r['total']}  "
                f"reply={r.get('reply_rate', '—')}%  "
                f"dropoff={r.get('dropoff_rate', '—')}%{arrow}"
            ))
        blocks.append(_divider())

    # Length buckets
    if data.get("length"):
        blocks.append(_heading2("📏 Reply Rate by Length"))
        for r in data["length"]:
            blocks.append(_bullet(
                f"{r['bucket']:10s}  n={r['total']}  reply={r['reply_rate']}%"
            ))
        blocks.append(_divider())

    # Problems
    blocks.append(_heading2("🔴 Problems Identified"))
    if not problems:
        blocks.append(_para("No significant problems detected this week."))
    for i, p in enumerate(problems, 1):
        sev_emoji = _severity_emoji(p.get("severity", "low"))
        blocks.append(_heading3(f"{sev_emoji} Problem {i}: {p.get('title', 'Untitled')}"))
        blocks.append(_para(f"Evidence: {p.get('evidence', '—')}"))
        blocks.append(_callout(
            f"Proposed change: {p.get('proposed_change', '—')}",
            "💡",
        ))
    blocks.append(_divider())

    # What's working
    blocks.append(_heading2("✅ What's Working"))
    for w in working:
        blocks.append(_bullet(w))
    blocks.append(_divider())

    # Raw silenced replies
    if data.get("silenced"):
        blocks.append(_heading2("🔇 Replies That Drove Silence (sample)"))
        for r in data["silenced"][:6]:
            preview = (r.get("preview") or "").strip()
            meta = f"[{r.get('intent','?')} / {r.get('tone_mode','?')} / {r.get('chars','?')}ch]"
            blocks.append(_bullet(f"{meta}  \"{preview}\""))
        blocks.append(_divider())

    # Approval callout
    blocks.append(_callout(
        "⬛ Mark as reviewed in the admin dashboard: /admin?tab=quality",
        "✅",
    ))

    return blocks


def create_notion_page(week_start: date, headline: dict, findings: dict, data: dict) -> str | None:
    parent_id = os.getenv("NOTION_DIGEST_PARENT_ID", "")
    if not parent_id:
        log.warning("NOTION_DIGEST_PARENT_ID not set — skipping Notion page")
        return None

    import urllib.request

    title = f"AI Quality Digest — Week of {week_start}"
    blocks = build_notion_blocks(week_start, headline, findings, data)

    # Notion API limits: 100 blocks per request
    first_batch = blocks[:100]
    rest = blocks[100:]

    payload = {
        "parent": {"type": "page_id", "page_id": parent_id},
        "properties": {
            "title": {"title": _rich_text(title)},
        },
        "children": first_batch,
    }

    headers = _notion_headers()
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        "https://api.notion.com/v1/pages",
        data=body,
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read().decode())

    page_id = result.get("id")
    if not page_id:
        log.error("Notion page creation returned no ID: %s", result)
        return None

    # Append remaining blocks if any
    if rest:
        for i in range(0, len(rest), 100):
            chunk = rest[i:i + 100]
            patch_payload = {"children": chunk}
            patch_body = json.dumps(patch_payload).encode()
            patch_req = urllib.request.Request(
                f"https://api.notion.com/v1/blocks/{page_id}/children",
                data=patch_body,
                headers=headers,
                method="PATCH",
            )
            with urllib.request.urlopen(patch_req):
                pass

    log.info("Created Notion page: %s", page_id)
    return page_id


# ── DB save ───────────────────────────────────────────────────────────────────

def _json_serial(obj):
    """JSON serializer that handles Decimal and date types from psycopg2."""
    import decimal
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _to_json(obj) -> str:
    return json.dumps(obj, default=_json_serial)


def save_report(conn, week_start: date, headline: dict, findings: dict, notion_page_id: str | None):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_quality_reports
                  (week_start, headline_json, findings_json, notion_page_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (
                week_start,
                _to_json(headline),
                _to_json(findings),
                notion_page_id,
            ))
            row = cur.fetchone()
            return (row or {}).get("id")


# ── Auto-winner promotion ─────────────────────────────────────────────────────

# Intents that produce sell links — never promote these into the corpus.
_SELL_INTENTS = frozenset({"show", "book", "clip", "podcast", "merch"})

# Rolling window: keep this many auto-snapshot batches active at once.
# Older batches are deactivated so the corpus stays lean.
_AUTO_SNAPSHOTS_TO_KEEP = 3

# Quality bar: fan replied within this many seconds (genuine fast engagement).
_MAX_REPLY_SECONDS = 300

# Per intent+tone combo, take at most this many winners per week.
_MAX_PER_COMBO = 2


def fetch_winners_for_corpus(
    conn,
    this_start: datetime,
    this_end: datetime,
) -> list[dict]:
    """
    Return the top-performing bot replies from this window that are candidates
    for the winning examples corpus.

    Selection criteria (all must hold):
      - Fan replied (did_user_reply = TRUE)
      - Did NOT drive silence after (went_silent_after IS NOT TRUE)
      - Fan replied within _MAX_REPLY_SECONDS seconds (genuine engagement)
      - Reply is 40–250 chars (not a stub, not a wall of text)
      - Intent is not a sell intent (no ticket/book/merch links)
      - Not already in the corpus (deduped by source_msg_id)
      - Not from a blast send

    Returns up to _MAX_PER_COMBO entries per (intent, tone_mode) combo,
    ranked by conversation depth (msgs_after_this DESC) then reply speed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    m.id                                        AS source_msg_id,
                    m.text,
                    m.intent,
                    COALESCE(m.tone_mode, 'unknown')            AS tone_mode,
                    m.reply_delay_seconds,
                    COALESCE(m.msgs_after_this, 0)              AS depth,
                    RANK() OVER (
                        PARTITION BY m.intent, COALESCE(m.tone_mode, 'unknown')
                        ORDER BY COALESCE(m.msgs_after_this, 0) DESC,
                                 m.reply_delay_seconds ASC
                    )                                           AS rk
                FROM messages m
                WHERE m.role                = 'assistant'
                  AND m.did_user_reply      = TRUE
                  AND (m.went_silent_after IS DISTINCT FROM TRUE)
                  AND m.msg_source         IS DISTINCT FROM 'blast'
                  AND m.reply_delay_seconds BETWEEN 5 AND %s
                  AND m.reply_length_chars  BETWEEN 40 AND 250
                  AND m.intent              IS NOT NULL
                  AND m.intent              NOT IN %s
                  AND m.text NOT LIKE '%%zarnagarg.com%%'
                  AND m.text NOT LIKE '%%amazon.com%%'
                  AND m.text NOT LIKE '%%youtube.com%%'
                  AND m.text NOT LIKE '%%shopmy.us%%'
                  AND m.created_at          >= %s
                  AND m.created_at          <  %s
                  AND m.id NOT IN (
                      SELECT source_msg_id
                      FROM   winning_examples_corpus
                      WHERE  source_msg_id IS NOT NULL
                  )
            )
            SELECT source_msg_id, text, intent, tone_mode, reply_delay_seconds, depth
            FROM   ranked
            WHERE  rk <= %s
            ORDER  BY intent, tone_mode, rk
            """,
            (
                _MAX_REPLY_SECONDS,
                tuple(_SELL_INTENTS),
                this_start,
                this_end,
                _MAX_PER_COMBO,
            ),
        )
        rows = cur.fetchall()

    return [dict(r) for r in rows]


def auto_promote_winners(
    conn,
    week_start: "date",
    winners: list[dict],
    dry_run: bool = False,
    creator_slug: str = "zarna",
) -> int:
    """
    Insert winners into winning_examples_corpus under a dated snapshot tag,
    then deactivate old auto-snapshot batches beyond the rolling window.

    Returns the number of rows inserted (0 on dry-run).
    """
    if not winners:
        log.info("auto_promote_winners: no winners to promote this week")
        return 0

    snapshot_tag = f"auto-{week_start}"
    log.info(
        "auto_promote_winners: promoting %d winners → snapshot '%s'",
        len(winners), snapshot_tag,
    )

    if dry_run:
        for w in winners:
            log.info(
                "  [DRY RUN] would promote: intent=%s tone=%s depth=%s delay=%ss  \"%s\"",
                w["intent"], w["tone_mode"], w["depth"], w["reply_delay_seconds"],
                w["text"][:80],
            )
        return 0

    with conn:
        with conn.cursor() as cur:
            # Insert new winners
            inserted = 0
            for w in winners:
                cur.execute(
                    """
                    INSERT INTO winning_examples_corpus
                        (creator_slug, intent, tone_mode, text, snapshot_tag,
                         is_active, source_msg_id)
                    VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        creator_slug,
                        w["intent"],
                        w["tone_mode"],
                        w["text"],
                        snapshot_tag,
                        w["source_msg_id"],
                    ),
                )
                inserted += cur.rowcount

            # Record the snapshot
            cur.execute(
                """
                INSERT INTO winning_examples_snapshots (tag, notes, example_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (tag) DO UPDATE
                    SET example_count = EXCLUDED.example_count
                """,
                (
                    snapshot_tag,
                    f"Auto-promoted from digest run for week of {week_start}",
                    inserted,
                ),
            )

            # Deactivate auto-snapshots outside the rolling window.
            # Find all auto-* snapshot tags ordered by creation date, keep the
            # most recent _AUTO_SNAPSHOTS_TO_KEEP active.
            cur.execute(
                """
                UPDATE winning_examples_corpus
                SET    is_active = FALSE
                WHERE  creator_slug  = %s
                  AND  snapshot_tag  LIKE 'auto-%%'
                  AND  snapshot_tag NOT IN (
                      SELECT tag
                      FROM   winning_examples_snapshots
                      WHERE  tag LIKE 'auto-%%'
                        AND  rolled_back_at IS NULL
                      ORDER  BY created_at DESC
                      LIMIT  %s
                  )
                """,
                (creator_slug, _AUTO_SNAPSHOTS_TO_KEEP),
            )
            deactivated = cur.rowcount

    log.info(
        "auto_promote_winners: inserted=%d deactivated_old=%d snapshot='%s'",
        inserted, deactivated, snapshot_tag,
    )
    return inserted


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Generate weekly AI quality digest")
    ap.add_argument("--days", type=int, default=7, help="Analysis window in days (default 7)")
    ap.add_argument("--dry-run", action="store_true", help="Print report without saving to DB/Notion")
    args = ap.parse_args()

    now = datetime.now(tz=timezone.utc)
    this_end = now
    this_start = now - timedelta(days=args.days)
    baseline_start = now - timedelta(days=args.days + 28)
    week_start = this_start.date()

    log.info("Analysing %s → %s  (baseline from %s)", this_start.date(), this_end.date(), baseline_start.date())

    conn = _conn()
    if not conn:
        log.error("DATABASE_URL not set — aborting")
        sys.exit(1)

    ensure_table(conn)
    data = fetch_analytics(conn, this_start, this_end, baseline_start)

    scored = (data["headline"].get("scored") or 0)
    log.info("Fetched analytics — %d scored replies this period", scored)

    if scored < 10:
        log.warning("Fewer than 10 scored replies (%d) — digest may be low quality", scored)

    # Call Gemini
    log.info("Calling Gemini for analysis …")
    prompt = build_analysis_prompt(data, week_start)
    try:
        findings = call_gemini(prompt)
    except Exception as e:
        log.error("Gemini call failed: %s", e)
        findings = {
            "one_line_summary": "Analysis unavailable (Gemini error)",
            "overall_trend": "stable",
            "problems": [],
            "whats_working": [],
        }

    log.info("Findings: %s", findings.get("one_line_summary"))
    for p in findings.get("problems", []):
        log.info("  [%s] %s — %s", p.get("severity","?"), p.get("title",""), p.get("proposed_change",""))

    # Auto-promote this week's winners into the corpus (runs before dry-run print
    # so the dry-run output shows what would be promoted).
    log.info("Fetching winners for corpus promotion …")
    winners = fetch_winners_for_corpus(conn, this_start, this_end)
    log.info("Found %d winner candidates for corpus", len(winners))
    promoted = auto_promote_winners(conn, week_start, winners, dry_run=args.dry_run)

    if args.dry_run:
        print("\n=== DRY RUN — NOT SAVED ===")
        print(json.dumps({"headline": data["headline"], "findings": findings}, indent=2, default=_json_serial))
        print(f"\n[Auto-promote] Would have promoted {len(winners)} winners (dry-run, nothing inserted)")
        conn.close()
        return

    log.info("Auto-promoted %d winners into corpus", promoted)

    # Create Notion page
    notion_page_id = None
    try:
        notion_page_id = create_notion_page(week_start, data["headline"], findings, data)
    except Exception as e:
        log.warning("Notion page creation failed: %s", e)

    # Save to DB
    report_id = save_report(conn, week_start, data["headline"], findings, notion_page_id)
    conn.close()
    log.info("Saved quality report id=%s notion_page=%s promoted_winners=%d", report_id, notion_page_id, promoted)


if __name__ == "__main__":
    main()
