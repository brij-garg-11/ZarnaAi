#!/usr/bin/env python3
"""
Weekly SMB AI Quality Digest — per-tenant engagement analysis, auto-improvement, and opt-out alerts.

Runs every Monday.  For each registered SMB tenant it:

  1. Ensures engagement-tracking schema exists (idempotent).
  2. Scores recent smb_messages (did subscriber reply? how fast? went silent?).
  3. Fetches analytics: reply rate, silence rate, recent opt-outs + their last conversation.
  4. Calls Gemini to identify problems and propose fixes (adapted for business SMS, not fan messaging).
  5. Auto-promotes the week's top-performing bot replies into smb_winning_examples so
     future bot replies get better automatically.
  6. Texts a concise digest to the tenant owner (via Twilio).
  7. Saves the full report to smb_quality_reports for the operator portal.

Env (required):
  DATABASE_URL         Production Postgres (same as web service)
  GEMINI_API_KEY       For analysis

Env (optional):
  TWILIO_ACCOUNT_SID  }  Required to send SMS digest to tenant owners
  TWILIO_AUTH_TOKEN   }
  SMB_DIGEST_DAYS        Analysis window in days (default 7)
  SMB_DIGEST_TENANT      Run for one tenant only (slug), default = all

Run:
  python scripts/generate_smb_quality_digest.py
  python scripts/generate_smb_quality_digest.py --dry-run
  python scripts/generate_smb_quality_digest.py --tenant west_side_comedy
  python scripts/generate_smb_quality_digest.py --days 14
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("smb_quality_digest")

# Add project root to path so app imports work
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def _conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    import psycopg2
    c = psycopg2.connect(url)
    c.autocommit = False
    return c


# ---------------------------------------------------------------------------
# Tenant loader (from creator_config/)
# ---------------------------------------------------------------------------

def _load_all_tenants() -> list:
    """Load all SMB tenants from creator_config/*.json."""
    from app.smb.tenants import TenantRegistry, _CONFIG_DIR
    reg = TenantRegistry()
    return reg.all_tenants()


# ---------------------------------------------------------------------------
# Notion helpers — shared primitives
# ---------------------------------------------------------------------------

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


def _rich_text(content: str) -> list:
    return [{"type": "text", "text": {"content": content[:2000]}}]


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


def _severity_emoji(s: str) -> str:
    return {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(s, "⚪")


def _trend_emoji(t: str) -> str:
    return {"improving": "📈", "declining": "📉", "stable": "➡️",
            "insufficient_data": "📊"}.get(t, "📊")


# ---------------------------------------------------------------------------
# Notion page builder — SMB variant
# ---------------------------------------------------------------------------

def build_smb_notion_blocks(
    tenant_display_name: str,
    business_type: str,
    week_start: date,
    data: dict,
    findings: dict,
    promoted_count: int,
) -> list:
    h = data["headline"]
    rr = h.get("reply_rate")
    base = h.get("baseline_reply_rate")
    silence = h.get("silence_rate")
    trend = findings.get("overall_trend", "stable")
    summary = findings.get("one_line_summary", "")
    problems = findings.get("problems", [])
    working = findings.get("whats_working", [])
    opt_outs = data.get("opt_outs", [])
    silenced = data.get("silenced", [])
    winners = data.get("winners", [])

    rr_str = f"{rr}%" if rr is not None else "—"
    base_str = f"{base}%" if base is not None else "—"

    blocks: list = []

    # Banner callout
    blocks.append(_callout(
        f"{_trend_emoji(trend)}  {tenant_display_name} — Week of {week_start}  |  {summary}",
        _trend_emoji(trend) or "📊",
    ))
    blocks.append(_divider())

    # At-a-glance
    blocks.append(_heading2("📊 This Week at a Glance"))
    blocks.append(_bullet(f"Business: {tenant_display_name} ({business_type})"))
    blocks.append(_bullet(f"Scored bot replies: {h.get('scored', 0):,}"))
    blocks.append(_bullet(f"Subscriber reply rate: {rr_str}  (4-week baseline: {base_str})"))
    blocks.append(_bullet(f"Silence rate (no reply): {silence if silence is not None else '—'}%"))
    blocks.append(_bullet(f"Avg bot reply length: {h.get('avg_len', '—')} chars"))
    blocks.append(_bullet(f"Auto-promoted winning examples this week: {promoted_count}"))
    blocks.append(_divider())

    # Opt-outs — most urgent section
    if opt_outs:
        blocks.append(_heading2(f"🚨 Opt-outs This Week ({len(opt_outs)})"))
        blocks.append(_callout(
            "These subscribers texted STOP this week. Review their last conversation to understand why.",
            "⚠️",
        ))
        for o in opt_outs:
            blocks.append(_para(
                f"Subscriber ...{o['phone_suffix']}  stopped at {o.get('stopped_at', '?')}"
            ))
            for m in o.get("last_messages", []):
                role_label = "Bot" if m["role"] == "assistant" else "Subscriber"
                blocks.append(_bullet(f"{role_label}: {m['body']}"))
        blocks.append(_divider())
    else:
        blocks.append(_callout("✅ No opt-outs this week!", "🎉"))
        blocks.append(_divider())

    # Problems
    blocks.append(_heading2("🔴 Problems Identified"))
    if not problems:
        blocks.append(_para("No significant problems detected this week — or not enough data yet."))
    for i, p in enumerate(problems, 1):
        sev_emoji = _severity_emoji(p.get("severity", "low"))
        blocks.append(_heading3(f"{sev_emoji} Problem {i}: {p.get('title', 'Untitled')}"))
        blocks.append(_para(f"Evidence: {p.get('evidence', '—')}"))
        blocks.append(_callout(
            f"Proposed fix: {p.get('proposed_change', '—')}",
            "💡",
        ))
    blocks.append(_divider())

    # What's working
    blocks.append(_heading2("✅ What's Working"))
    if working:
        for w in working:
            blocks.append(_bullet(w))
    else:
        blocks.append(_para("Not enough data to identify patterns yet."))
    blocks.append(_divider())

    # Silenced replies sample
    if silenced:
        blocks.append(_heading2("🔇 Replies That Drove Silence"))
        for r in silenced[:6]:
            preview = (r.get("preview") or "").strip()
            meta = f"[{r.get('chars', '?')}ch]"
            blocks.append(_bullet(f"{meta}  \"{preview}\""))
        blocks.append(_divider())

    # Winning examples promoted
    if winners:
        blocks.append(_heading2("🏆 Top Replies Auto-Promoted This Week"))
        blocks.append(_para(
            "These bot replies got the fastest subscriber responses and are now "
            "automatically used as examples to improve future replies."
        ))
        for w in winners[:5]:
            delay = w.get("reply_s") or w.get("reply_delay_seconds")
            delay_str = f"  (subscriber replied in {delay}s)" if delay else ""
            blocks.append(_bullet(f"\"{(w.get('preview') or w.get('text',''))}\"{delay_str}"))
        blocks.append(_divider())

    blocks.append(_callout(
        "Review full report at your operator portal. Next digest: next Monday.",
        "📋",
    ))

    return blocks


def _notion_post(url: str, payload: dict) -> dict:
    import urllib.request
    headers = _notion_headers()
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def _notion_patch_children(page_id: str, blocks: list) -> None:
    import urllib.request
    headers = _notion_headers()
    payload = {"children": blocks}
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"https://api.notion.com/v1/blocks/{page_id}/children",
        data=body, headers=headers, method="PATCH",
    )
    with urllib.request.urlopen(req):
        pass


def create_smb_notion_page(
    tenant_display_name: str,
    business_type: str,
    week_start: date,
    data: dict,
    findings: dict,
    promoted_count: int,
) -> str | None:
    """
    Create (or skip if NOTION vars not set) a Notion page for this tenant's weekly digest.

    Parent page: SMB_NOTION_DIGEST_PARENT_ID  (one shared parent for all tenants)
    Page title:  "{Tenant Name} — SMB Digest — Week of {date}"

    Returns the Notion page ID, or None if Notion is not configured.
    """
    parent_id = os.getenv("SMB_NOTION_DIGEST_PARENT_ID", "")
    if not parent_id:
        log.info("Notion: SMB_NOTION_DIGEST_PARENT_ID not set — skipping Notion page")
        return None
    if not os.getenv("NOTION_TOKEN", ""):
        log.info("Notion: NOTION_TOKEN not set — skipping Notion page")
        return None

    title = f"{tenant_display_name} — SMB Digest — Week of {week_start}"
    blocks = build_smb_notion_blocks(
        tenant_display_name, business_type, week_start, data, findings, promoted_count,
    )

    first_batch = blocks[:100]
    rest = blocks[100:]

    payload = {
        "parent": {"type": "page_id", "page_id": parent_id},
        "properties": {
            "title": {"title": _rich_text(title)},
        },
        "children": first_batch,
    }

    try:
        result = _notion_post("https://api.notion.com/v1/pages", payload)
        page_id = result.get("id")
        if not page_id:
            log.error("Notion: page creation returned no ID: %s", result)
            return None
        for i in range(0, len(rest), 100):
            _notion_patch_children(page_id, rest[i:i + 100])
        log.info("Notion: created page '%s' (id=%s)", title, page_id)
        return page_id
    except Exception:
        log.exception("Notion: failed to create page for %s", tenant_display_name)
        return None


# ---------------------------------------------------------------------------
# Gemini analysis — business-voice variant
# ---------------------------------------------------------------------------

def build_smb_analysis_prompt(tenant_slug: str, display_name: str, business_type: str, data: dict, week_start: date) -> str:
    h = data["headline"]
    scored = h.get("scored") or 0
    rr = h.get("reply_rate")
    base_rr = h.get("baseline_reply_rate")
    silence = h.get("silence_rate")
    avg_len = h.get("avg_len")

    rr_str = f"{rr}%" if rr is not None else "—"
    base_str = f"{base_rr}%" if base_rr is not None else "—"
    delta_str = ""
    if rr is not None and base_rr is not None:
        d = float(rr) - float(base_rr)
        arrow = "↑" if d >= 0 else "↓"
        delta_str = f" ({arrow}{abs(d):.1f}pp vs baseline)"

    silenced_text = "\n".join(
        f"{i+1}. [{r.get('chars','?')}ch] \"{(r.get('preview') or '').strip()}\""
        for i, r in enumerate(data.get("silenced", []))
    ) or "None this week"

    opt_outs_text = ""
    for o in data.get("opt_outs", []):
        opt_outs_text += f"\n  Subscriber ...{o['phone_suffix']} stopped at {o['stopped_at']}\n"
        for m in o.get("last_messages", []):
            role_label = "Bot" if m["role"] == "assistant" else "Subscriber"
            opt_outs_text += f"    {role_label}: {m['body']}\n"
    if not opt_outs_text.strip():
        opt_outs_text = "  None this week — great!"

    return f"""You are analyzing the SMS AI quality for {display_name}, a {business_type}.
The AI assistant texts their subscribers on their behalf to answer questions, share updates, and drive engagement.
Your job: identify the top problems hurting subscriber engagement and propose specific, actionable fixes.

=== Week of {week_start} ({display_name}) ===
Scored bot replies this week  : {scored:,}
Overall subscriber reply rate : {rr_str}{delta_str}
4-week baseline reply rate    : {base_str}
Silence rate (subscriber gone): {silence if silence is not None else '—'}%
Avg bot reply length          : {avg_len if avg_len is not None else '—'} chars

--- Top 8 bot replies that caused subscriber silence ---
{silenced_text}

--- Recent opt-outs (STOP) + their last conversation ---
{opt_outs_text}

--- TASK ---
1. Identify exactly 3 concrete problems hurting subscriber engagement.
   If there were opt-outs, identify what likely triggered them.
2. For each problem, propose a SPECIFIC change to how the AI should write replies.
   (e.g. "too long — cap at 2 sentences", "too formal — use casual contractions",
   "asks questions back too often", "doesn't offer next steps").
3. Name 2 things that are working well (or note if there is not enough data yet).

Return ONLY a valid JSON object — no markdown, no preamble — with this exact structure:
{{
  "one_line_summary": "string (≤20 words)",
  "overall_trend": "improving|declining|stable|insufficient_data",
  "problems": [
    {{
      "title": "short problem name",
      "evidence": "specific quotes or numbers from the data",
      "proposed_change": "concrete, actionable instruction for the AI prompt or tone config",
      "severity": "high|medium|low"
    }}
  ],
  "whats_working": ["string", "string"]
}}"""


def call_gemini(prompt: str) -> dict:
    from google import genai as _genai
    model = os.getenv("DIGEST_MODEL", os.getenv("GENERATION_MODEL", "gemini-2.5-flash"))
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set")
    client = _genai.Client(api_key=key)
    response = client.models.generate_content(model=model, contents=prompt)
    raw = (response.text or "").strip()

    if "```" in raw:
        parts = raw.split("```")
        if len(parts) >= 3:
            raw = parts[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

    if not raw.startswith("{"):
        import re
        m = re.search(r"\{[\s\S]+\}", raw)
        if m:
            raw = m.group(0)

    return json.loads(raw)


# ---------------------------------------------------------------------------
# SMS delivery — send digest to tenant owner
# ---------------------------------------------------------------------------

def _send_owner_digest_sms(tenant, digest_text: str) -> bool:
    """Send a short digest SMS to the business owner. Returns True on success."""
    if not tenant.owner_phone:
        log.info("SMB digest: no owner_phone for %s — skipping SMS", tenant.slug)
        return False
    if not tenant.sms_number:
        log.info("SMB digest: no sms_number for %s — skipping SMS", tenant.slug)
        return False

    sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    token = os.getenv("TWILIO_AUTH_TOKEN", "")
    if not sid or not token:
        log.warning("SMB digest: TWILIO_ACCOUNT_SID/AUTH_TOKEN not set — skipping owner SMS")
        return False

    try:
        from twilio.rest import Client
        Client(sid, token).messages.create(
            to=tenant.owner_phone,
            from_=tenant.sms_number,
            body=digest_text,
        )
        log.info("SMB digest SMS sent to owner (tenant=%s)", tenant.slug)
        return True
    except Exception:
        log.exception("SMB digest: failed to send owner SMS (tenant=%s)", tenant.slug)
        return False


def _format_owner_sms(tenant_name: str, week_start: date, headline: dict, findings: dict, opt_outs: list) -> str:
    """
    Format a brief SMS digest for the tenant owner.
    Keeps it tight — max ~3 sentences so it's readable as a text.
    """
    scored = headline.get("scored") or 0
    rr = headline.get("reply_rate")
    base = headline.get("baseline_reply_rate")
    trend = findings.get("overall_trend", "stable")
    summary = findings.get("one_line_summary", "")

    rr_str = f"{rr}%" if rr is not None else "—"
    base_str = f" (baseline {base}%)" if base is not None else ""

    trend_emoji = {"improving": "📈", "declining": "📉", "stable": "➡️"}.get(trend, "📊")
    opt_out_note = f" {len(opt_outs)} opted out this week." if opt_outs else ""

    # Top problem
    problems = findings.get("problems", [])
    problem_note = ""
    if problems:
        top = problems[0]
        problem_note = f" Top issue: {top.get('title','')}."

    lines = [
        f"{trend_emoji} {tenant_name} Weekly AI Digest — week of {week_start}",
        f"Reply rate: {rr_str}{base_str}. {scored} bot replies scored.{opt_out_note}",
        summary + problem_note if (summary or problem_note) else "",
        "Full report at your operator portal. Reply STOP to unsubscribe from digests.",
    ]
    return "\n".join(l for l in lines if l.strip())


# ---------------------------------------------------------------------------
# Per-tenant digest run
# ---------------------------------------------------------------------------

def run_tenant_digest(
    conn,
    tenant,
    this_start: datetime,
    this_end: datetime,
    baseline_start: datetime,
    week_start: date,
    dry_run: bool = False,
) -> dict:
    """
    Run the full digest pipeline for a single tenant. Returns findings dict.
    """
    from app.smb import storage as smb_storage

    log.info("=== SMB Digest: %s (slug=%s) ===", tenant.display_name, tenant.slug)

    # 1. Score recent messages
    log.info("[%s] Scoring recent messages ...", tenant.slug)
    scored_count = smb_storage.score_smb_messages(conn, tenant.slug, silence_cutoff_hours=4)
    log.info("[%s] Scored %d message(s)", tenant.slug, scored_count)

    # 2. Fetch analytics
    data = smb_storage.fetch_smb_analytics(
        conn, tenant.slug, this_start, this_end, baseline_start
    )
    h = data["headline"]
    log.info(
        "[%s] Analytics: scored=%s reply_rate=%s%% silence=%s%% opt_outs=%d",
        tenant.slug,
        h.get("scored", 0),
        h.get("reply_rate", "—"),
        h.get("silence_rate", "—"),
        len(data.get("opt_outs", [])),
    )

    if data.get("opt_outs"):
        log.warning(
            "[%s] OPT-OUTS THIS WEEK (%d):",
            tenant.slug, len(data["opt_outs"]),
        )
        for o in data["opt_outs"]:
            log.warning(
                "  ...%s stopped at %s. Last messages:",
                o["phone_suffix"], o["stopped_at"],
            )
            for m in o.get("last_messages", []):
                role_label = "Bot" if m["role"] == "assistant" else "Sub"
                log.warning("    %s: %s", role_label, m["body"][:100])

    # 3. Gemini analysis
    if (h.get("scored") or 0) < 3 and not data.get("opt_outs"):
        log.info("[%s] Not enough data for analysis (%d scored) — skipping Gemini", tenant.slug, h.get("scored", 0))
        findings = {
            "one_line_summary": "Insufficient data this week",
            "overall_trend": "insufficient_data",
            "problems": [],
            "whats_working": [],
        }
    else:
        log.info("[%s] Calling Gemini for analysis ...", tenant.slug)
        prompt = build_smb_analysis_prompt(
            tenant.slug, tenant.display_name, tenant.business_type, data, week_start
        )
        try:
            findings = call_gemini(prompt)
            log.info("[%s] Findings: %s", tenant.slug, findings.get("one_line_summary"))
            for p in findings.get("problems", []):
                log.info(
                    "  [%s] %s — %s",
                    p.get("severity", "?"), p.get("title", ""), p.get("proposed_change", ""),
                )
        except Exception as e:
            log.error("[%s] Gemini call failed: %s", tenant.slug, e)
            findings = {
                "one_line_summary": "Analysis unavailable (Gemini error)",
                "overall_trend": "stable",
                "problems": [],
                "whats_working": [],
            }

    # 4. Auto-promote winners
    log.info("[%s] Fetching winning examples ...", tenant.slug)
    winners = smb_storage.fetch_smb_winners(conn, tenant.slug, this_start, this_end)
    log.info("[%s] Found %d winner candidate(s)", tenant.slug, len(winners))
    promoted = smb_storage.save_smb_winners(conn, tenant.slug, week_start, winners, dry_run=dry_run)
    if not dry_run:
        log.info("[%s] Auto-promoted %d winner(s)", tenant.slug, promoted)

    if dry_run:
        print(f"\n=== DRY RUN [{tenant.slug}] ===")
        print(json.dumps({"headline": h, "findings": findings, "winners_found": len(winners)}, indent=2, default=str))
        return findings

    # 5. Notion page
    notion_page_id = None
    try:
        notion_page_id = create_smb_notion_page(
            tenant.display_name,
            tenant.business_type,
            week_start,
            data,
            findings,
            promoted,
        )
    except Exception:
        log.exception("[%s] Notion page creation failed — continuing", tenant.slug)

    # 6. Save report to DB
    smb_storage.save_smb_quality_report(conn, tenant.slug, week_start, h, findings)
    log.info("[%s] Report saved (notion_page_id=%s)", tenant.slug, notion_page_id or "none")

    # 7. Send SMS digest to owner
    sms_text = _format_owner_sms(tenant.display_name, week_start, h, findings, data.get("opt_outs", []))
    _send_owner_digest_sms(tenant, sms_text)

    return findings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="SMB weekly AI quality digest")
    ap.add_argument("--days",     type=int, default=int(os.getenv("SMB_DIGEST_DAYS", "7")))
    ap.add_argument("--tenant",   default=os.getenv("SMB_DIGEST_TENANT", ""),
                    help="Slug of a single tenant to run (default: all)")
    ap.add_argument("--dry-run",  action="store_true", help="Print report without saving or SMS")
    args = ap.parse_args()

    now          = datetime.now(tz=timezone.utc)
    this_end     = now
    this_start   = now - timedelta(days=args.days)
    baseline_start = now - timedelta(days=args.days + 28)
    week_start   = this_start.date()

    log.info("SMB digest run: %s → %s (baseline from %s)",
             this_start.date(), this_end.date(), baseline_start.date())

    conn = _conn()
    if not conn:
        log.error("DATABASE_URL not set — aborting")
        sys.exit(1)

    # Ensure schema
    from app.smb import storage as smb_storage
    smb_storage.ensure_smb_engagement_schema(conn)
    log.info("Schema ensured")

    # Load tenants
    tenants = _load_all_tenants()
    if args.tenant:
        tenants = [t for t in tenants if t.slug == args.tenant]
        if not tenants:
            log.error("No tenant found with slug=%r", args.tenant)
            sys.exit(1)

    log.info("Running digest for %d tenant(s): %s",
             len(tenants), ", ".join(t.slug for t in tenants))

    for tenant in tenants:
        try:
            run_tenant_digest(
                conn, tenant,
                this_start, this_end, baseline_start, week_start,
                dry_run=args.dry_run,
            )
        except Exception:
            log.exception("SMB digest failed for tenant=%s — continuing with next tenant", tenant.slug)

    conn.close()
    log.info("SMB digest complete")


if __name__ == "__main__":
    main()
