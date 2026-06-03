#!/usr/bin/env python3
"""
Weekly Client Summary — assembles a per-client operational snapshot and writes
it to the operator DB + optionally a Notion page.

This is an INTERNAL operator tool. Clients never see this report.

Env (required):
  OPERATOR_DATABASE_URL   Operator Postgres
  DATABASE_URL            Per-client Postgres (for message/contact counts)

Env (optional — Notion):
  NOTION_TOKEN                    Internal integration secret
  NOTION_SUMMARY_PARENT_ID        Page ID of the "Weekly Client Summaries" parent in Notion
  NOTION_API_VERSION              Default: 2022-06-28

Run:
  python scripts/generate_client_summary.py --slug zarna
  python scripts/generate_client_summary.py --slug zarna --week 2026-05-05
  python scripts/generate_client_summary.py --slug zarna --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("client_summary")

# ── DB helpers ───────────────────────────────────────────────────────────────


def _operator_conn():
    url = os.getenv("OPERATOR_DATABASE_URL", "")
    if not url:
        log.error("OPERATOR_DATABASE_URL not set")
        return None
    import psycopg2
    import psycopg2.extras
    c = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    c.autocommit = False
    return c


def _client_conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        log.warning("DATABASE_URL not set — skipping fan/message counts")
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
                CREATE TABLE IF NOT EXISTS client_weekly_summaries (
                    id               BIGSERIAL PRIMARY KEY,
                    creator_slug     TEXT NOT NULL,
                    week_start       DATE NOT NULL,
                    headline_json    JSONB,
                    notion_page_id   TEXT,
                    generated_at     TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(creator_slug, week_start)
                )
            """)


# ── Data assembly ─────────────────────────────────────────────────────────────


def fetch_message_stats(client_conn, slug: str, week_start: date, week_end: date) -> dict:
    """Message volume and new subscribers from the per-client DB."""
    if not client_conn:
        return {"message_count": None, "new_subscribers": None}
    try:
        with client_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS message_count
                FROM   messages m
                JOIN   contacts c ON c.phone_number = m.phone_number
                WHERE  c.creator_slug = %s
                  AND  m.created_at >= %s
                  AND  m.created_at <  %s
                """,
                (slug, week_start, week_end),
            )
            row = cur.fetchone()
            message_count = int(row["message_count"]) if row else 0

            cur.execute(
                """
                SELECT COUNT(*) AS new_subs
                FROM   contacts
                WHERE  creator_slug = %s
                  AND  created_at >= %s
                  AND  created_at <  %s
                """,
                (slug, week_start, week_end),
            )
            row = cur.fetchone()
            new_subscribers = int(row["new_subs"]) if row else 0

        return {"message_count": message_count, "new_subscribers": new_subscribers}
    except Exception as e:
        log.warning("fetch_message_stats failed: %s", e)
        return {"message_count": None, "new_subscribers": None}


def fetch_cost_stats(op_conn, slug: str, week_start: date, week_end: date) -> dict:
    """AI + SMS costs for the week from the operator DB."""
    try:
        with op_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(cost_usd), 0) AS ai_cost
                FROM   ai_cost_log
                WHERE  creator_slug = %s
                  AND  log_date >= %s
                  AND  log_date <  %s
                """,
                (slug, week_start, week_end),
            )
            row = cur.fetchone()
            ai_cost = float(row["ai_cost"]) if row else 0.0

            cur.execute(
                """
                SELECT COALESCE(SUM(inbound_cost_usd + outbound_cost_usd), 0) AS sms_cost
                FROM   sms_cost_log
                WHERE  creator_slug = %s
                  AND  log_date >= %s
                  AND  log_date <  %s
                """,
                (slug, week_start, week_end),
            )
            row = cur.fetchone()
            sms_cost = float(row["sms_cost"]) if row else 0.0

        return {"ai_cost_usd": round(ai_cost, 4), "sms_cost_usd": round(sms_cost, 4)}
    except Exception as e:
        log.warning("fetch_cost_stats failed: %s", e)
        return {"ai_cost_usd": None, "sms_cost_usd": None}


def fetch_alert_stats(op_conn, slug: str, week_start: date, week_end: date) -> dict:
    """Count of alerts fired this week from the operator DB."""
    try:
        with op_conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE severity = 'error')   AS errors,
                    COUNT(*) FILTER (WHERE severity = 'warning') AS warnings
                FROM   client_alerts
                WHERE  creator_slug = %s
                  AND  occurred_at >= %s
                  AND  occurred_at <  %s
                """,
                (slug, week_start, week_end),
            )
            row = cur.fetchone()
        return {
            "alert_total": int(row["total"]) if row else 0,
            "alert_errors": int(row["errors"]) if row else 0,
            "alert_warnings": int(row["warnings"]) if row else 0,
        }
    except Exception as e:
        log.warning("fetch_alert_stats failed: %s", e)
        return {"alert_total": None, "alert_errors": None, "alert_warnings": None}


def fetch_quality_score(op_conn, slug: str, week_start: date) -> dict:
    """Latest quality digest score for this week (if one exists)."""
    try:
        with op_conn.cursor() as cur:
            cur.execute(
                """
                SELECT headline_json, findings_json
                FROM   ai_quality_reports
                WHERE  week_start = %s
                ORDER  BY created_at DESC
                LIMIT  1
                """,
                (week_start,),
            )
            row = cur.fetchone()
        if not row:
            return {"quality_headline": None}
        try:
            headline = json.loads(row["headline_json"]) if isinstance(row["headline_json"], str) else row["headline_json"]
            summary = headline.get("one_line_summary") or headline.get("overall_trend")
        except Exception:
            summary = None
        return {"quality_headline": summary}
    except Exception as e:
        log.warning("fetch_quality_score failed: %s", e)
        return {"quality_headline": None}


# ── Notion ───────────────────────────────────────────────────────────────────


def _notion_headers():
    token = os.getenv("NOTION_TOKEN", "")
    version = os.getenv("NOTION_API_VERSION", "2022-06-28")
    if not token:
        return None
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": version,
    }


def create_notion_page(slug: str, week_start: date, headline: dict) -> str | None:
    parent_id = os.getenv("NOTION_SUMMARY_PARENT_ID", "")
    headers = _notion_headers()
    if not parent_id or not headers:
        log.info("Notion not configured — skipping page creation")
        return None

    import urllib.request

    msg_count = headline.get("message_count")
    new_subs = headline.get("new_subscribers")
    ai_cost = headline.get("ai_cost_usd")
    sms_cost = headline.get("sms_cost_usd")
    alert_errors = headline.get("alert_errors", 0)
    quality = headline.get("quality_headline") or "No quality digest this week"

    bullets = []
    if msg_count is not None:
        bullets.append(f"Messages: {msg_count:,}")
    if new_subs is not None:
        bullets.append(f"New subscribers: {new_subs:,}")
    if ai_cost is not None:
        bullets.append(f"AI cost: ${ai_cost:.4f}")
    if sms_cost is not None:
        bullets.append(f"SMS cost: ${sms_cost:.4f}")
    bullets.append(f"Errors this week: {alert_errors}")
    bullets.append(f"Quality: {quality}")

    def _text(s):
        return [{"type": "text", "text": {"content": s}}]

    children = [
        {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": _text(b)},
        }
        for b in bullets
    ]

    payload = json.dumps({
        "parent": {"page_id": parent_id},
        "properties": {
            "title": {"title": _text(f"{slug} — week of {week_start}")}
        },
        "children": children,
    }).encode()

    req = urllib.request.Request(
        "https://api.notion.com/v1/pages",
        data=payload,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            page_id = data.get("id")
            log.info("Notion page created: %s", page_id)
            return page_id
    except Exception as e:
        log.warning("Notion page creation failed: %s", e)
        return None


# ── Save to DB ────────────────────────────────────────────────────────────────


def save_summary(op_conn, slug: str, week_start: date, headline: dict, notion_page_id: str | None):
    with op_conn:
        with op_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO client_weekly_summaries
                    (creator_slug, week_start, headline_json, notion_page_id, generated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (creator_slug, week_start)
                DO UPDATE SET
                    headline_json  = EXCLUDED.headline_json,
                    notion_page_id = EXCLUDED.notion_page_id,
                    generated_at   = NOW()
                """,
                (slug, week_start, json.dumps(headline), notion_page_id),
            )


# ── Entry point ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Generate weekly client summary")
    parser.add_argument("--slug", required=True, help="Creator slug (e.g. zarna)")
    parser.add_argument(
        "--week",
        help="Week start date YYYY-MM-DD (default: last Monday)",
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print report but do not write to DB or Notion",
    )
    args = parser.parse_args()

    if args.week:
        week_start = date.fromisoformat(args.week)
    else:
        today = date.today()
        week_start = today - timedelta(days=today.weekday() + 7)  # last Monday

    week_end = week_start + timedelta(days=7)
    slug = args.slug.strip().lower()

    log.info("Generating weekly summary for slug=%s week=%s..%s", slug, week_start, week_end)

    op_conn = _operator_conn()
    if not op_conn:
        log.error("Cannot connect to operator DB — aborting")
        sys.exit(1)

    client_conn = _client_conn()

    ensure_table(op_conn)

    headline: dict = {"slug": slug, "week_start": str(week_start), "week_end": str(week_end)}
    headline.update(fetch_message_stats(client_conn, slug, week_start, week_end))
    headline.update(fetch_cost_stats(op_conn, slug, week_start, week_end))
    headline.update(fetch_alert_stats(op_conn, slug, week_start, week_end))
    headline.update(fetch_quality_score(op_conn, slug, week_start))

    log.info("Summary: %s", json.dumps(headline, indent=2))

    if args.dry_run:
        log.info("Dry run — not writing to DB or Notion")
        return

    notion_page_id = create_notion_page(slug, week_start, headline)
    save_summary(op_conn, slug, week_start, headline, notion_page_id)
    log.info("Summary saved for slug=%s week=%s", slug, week_start)

    if client_conn:
        client_conn.close()
    op_conn.close()


if __name__ == "__main__":
    main()
