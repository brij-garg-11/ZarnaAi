#!/usr/bin/env python3
"""
One-time backfill of the Monthly Cost History database inside each client's Notion page.

Queries Postgres month-by-month from the first message to today and writes a row
for each month into the embedded "📅 Monthly Cost History" database.

Run:
  python scripts/backfill_notion_monthly_history.py
  python scripts/backfill_notion_monthly_history.py --dry-run
  python scripts/backfill_notion_monthly_history.py --slug zarna  # one client only
"""
import argparse
import logging
import os
import sys
from datetime import date
from dateutil.relativedelta import relativedelta

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "operator"))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AI_COST_PER_MSG  = 0.004
SMS_COST_PER_MSG = 0.0079
PHONE_RENTAL     = 1.15


def _months_between(start: date, end: date) -> list[date]:
    """Return list of first-of-month dates from start to end inclusive."""
    months = []
    cur = start.replace(day=1)
    while cur <= end.replace(day=1):
        months.append(cur)
        cur += relativedelta(months=1)
    return months


def _get_monthly_data(cur, slug: str, account_type: str, month_start: date) -> dict:
    """Pull all metrics for a given month from Postgres."""
    import psycopg2.extras
    month_end_sql = "DATE_TRUNC('month', %s::date) + INTERVAL '1 month'"

    # Messages this month
    if account_type == "performer":
        cur.execute(
            """SELECT COUNT(*) AS cnt FROM messages m
               JOIN contacts c ON c.phone_number = m.phone_number
               WHERE c.creator_slug = %s
                 AND m.created_at >= DATE_TRUNC('month', %s::date)
                 AND m.created_at < """ + month_end_sql,
            (slug, month_start, month_start),
        )
    else:
        cur.execute(
            """SELECT COUNT(*) AS cnt FROM smb_messages
               WHERE tenant_slug = %s
                 AND created_at >= DATE_TRUNC('month', %s::date)
                 AND created_at < """ + month_end_sql,
            (slug, month_start, month_start),
        )
    msgs = cur.fetchone()[0]

    # AI replies + cost (hybrid)
    if account_type == "performer":
        cur.execute(
            """SELECT
                  COUNT(*) AS total_cnt,
                  COUNT(*) FILTER (WHERE m.ai_cost_usd IS NOT NULL) AS tracked_cnt,
                  COALESCE(SUM(m.ai_cost_usd), 0) AS exact_cost
               FROM messages m
               JOIN contacts c ON c.phone_number = m.phone_number
               WHERE c.creator_slug = %s AND m.role = 'assistant'
                 AND m.created_at >= DATE_TRUNC('month', %s::date)
                 AND m.created_at < """ + month_end_sql,
            (slug, month_start, month_start),
        )
        row = cur.fetchone()
        ai_replies   = row[0]
        untracked    = row[0] - row[1]
        ai_cost      = round(float(row[2]) + untracked * AI_COST_PER_MSG, 4)
        ai_exact     = (untracked == 0)
    else:
        ai_replies = msgs
        ai_cost    = round(msgs * AI_COST_PER_MSG, 4)
        ai_exact   = False

    # SMS cost (exact if in sms_cost_log, else estimate)
    cur.execute(
        """SELECT COALESCE(SUM(inbound_cost_usd + outbound_cost_usd), -1) AS sms_cost
           FROM sms_cost_log
           WHERE creator_slug = %s
             AND log_date >= DATE_TRUNC('month', %s::date)
             AND log_date < """ + month_end_sql,
        (slug, month_start, month_start),
    )
    sms_row  = cur.fetchone()[0]
    sms_cost = round(float(sms_row), 4) if sms_row >= 0 else round(msgs * SMS_COST_PER_MSG, 4)
    sms_exact = sms_row >= 0

    # Blasts + fans reached
    cur.execute(
        """SELECT COUNT(*) AS blasts, COALESCE(SUM(sent_count), 0) AS fans
           FROM blast_drafts
           WHERE status = 'sent'
             AND sent_at >= DATE_TRUNC('month', %s::date)
             AND sent_at < """ + month_end_sql,
        (month_start, month_start),
    )
    brow         = cur.fetchone()
    blasts       = int(brow[0] or 0)
    fans_reached = int(brow[1] or 0)

    return {
        "msgs":        msgs,
        "ai_replies":  ai_replies,
        "ai_cost":     ai_cost,
        "sms_cost":    sms_cost,
        "total_cost":  round(PHONE_RENTAL + ai_cost + sms_cost, 2),
        "blasts":      blasts,
        "fans":        fans_reached,
        "cost_exact":  ai_exact and sms_exact,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--slug", help="Only backfill this client slug")
    args = parser.parse_args()

    import psycopg2
    import psycopg2.extras
    db_url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    conn   = psycopg2.connect(db_url)
    conn.autocommit = True

    from app.notion_crm import (
        PERFORMERS_DB_ID, BUSINESSES_DB_ID,
        _find_page_by_slug, sync_monthly_cost_row,
    )

    # Load all active clients
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if args.slug:
            cur.execute(
                "SELECT DISTINCT creator_slug, account_type FROM operator_users "
                "WHERE creator_slug=%s AND is_active=TRUE LIMIT 1",
                (args.slug,),
            )
        else:
            cur.execute(
                "SELECT DISTINCT creator_slug, account_type FROM operator_users "
                "WHERE creator_slug IS NOT NULL AND is_active=TRUE"
            )
        clients = cur.fetchall()

    logger.info("Backfilling %d client(s)%s", len(clients), " (DRY RUN)" if args.dry_run else "")

    for client in clients:
        slug         = client["creator_slug"]
        account_type = client["account_type"]

        with conn.cursor() as cur:
            # Find first message date to know how far back to go
            if account_type == "performer":
                cur.execute(
                    """SELECT MIN(m.created_at)::date FROM messages m
                       JOIN contacts c ON c.phone_number=m.phone_number
                       WHERE c.creator_slug=%s""",
                    (slug,),
                )
            else:
                cur.execute(
                    "SELECT MIN(created_at)::date FROM smb_messages WHERE tenant_slug=%s",
                    (slug,),
                )
            row = cur.fetchone()
            first_date = row[0] if row and row[0] else date.today()

            db_id  = PERFORMERS_DB_ID if account_type == "performer" else BUSINESSES_DB_ID
            page_id = _find_page_by_slug(db_id, slug)
            if not page_id:
                logger.warning("  ✗ %s — no Notion page, skipping", slug)
                continue

        months = _months_between(first_date, date.today())
        logger.info("  %s (%s): %d months to sync", slug, account_type, len(months))

        for month_start in months:
            month_label = month_start.strftime("%B %Y")
            month_key   = month_start.strftime("%Y-%m")
            with conn.cursor() as cur:
                data = _get_monthly_data(cur, slug, account_type, month_start)

            if args.dry_run:
                logger.info("    [DRY] %s | msgs=%d ai=$%.4f sms=$%.4f total=$%.2f blasts=%d fans=%d exact=%s",
                            month_label, data["msgs"], data["ai_cost"], data["sms_cost"],
                            data["total_cost"], data["blasts"], data["fans"], data["cost_exact"])
                continue

            # Monthly fee from Notion for margin calc
            monthly_fee = 0.0
            try:
                import requests
                from app.notion_crm import _headers, NOTION_API
                resp = requests.get(f"{NOTION_API}/pages/{page_id}", headers=_headers(), timeout=10)
                fee_prop = resp.json().get("properties", {}).get("Monthly Fee ($)", {}).get("number")
                if fee_prop is not None:
                    monthly_fee = float(fee_prop)
            except Exception:
                pass

            net_margin = round(monthly_fee - data["total_cost"], 2)

            ok = sync_monthly_cost_row(
                page_id      = page_id,
                month_label  = month_label,
                month_key    = month_key,
                messages     = data["msgs"],
                ai_replies   = data["ai_replies"],
                ai_cost      = data["ai_cost"],
                sms_cost     = data["sms_cost"],
                total_cost   = data["total_cost"],
                net_margin   = net_margin,
                blasts       = data["blasts"],
                fans_reached = data["fans"],
                cost_exact   = data["cost_exact"],
                db_conn      = conn,
            )
            status = "✓" if ok else "✗"
            logger.info("    %s %s | total=$%.2f", status, month_label, data["total_cost"])

    conn.close()
    logger.info("Backfill complete.")


if __name__ == "__main__":
    main()
