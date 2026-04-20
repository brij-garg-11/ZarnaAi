#!/usr/bin/env python3
"""
Daily sync of all customer metrics and cost estimates → Notion CRM databases.

Updates every row in the Performers and Businesses databases with:
  - Subscriber count
  - Total messages / messages this month
  - Estimated AI cost, SMS cost, phone rental
  - Net margin (Monthly Fee - Total Cost)

Run:
  python scripts/sync_crm_to_notion.py

Env (required):
  NOTION_TOKEN              Internal integration secret
  DATABASE_URL              Production Postgres
  NOTION_PERFORMERS_DB_ID   (optional — defaults to hardcoded value)
  NOTION_BUSINESSES_DB_ID   (optional — defaults to hardcoded value)
"""

import logging
import os
import sys

# Allow importing from operator/app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    import psycopg2
    import psycopg2.extras

    db_url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    if not db_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    conn = psycopg2.connect(db_url)

    # Import after path setup
    from operator.app.notion_crm import sync_customer_costs

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT creator_slug, account_type, email
            FROM operator_users
            WHERE creator_slug IS NOT NULL
              AND account_type IS NOT NULL
              AND is_active = TRUE
            ORDER BY created_at DESC
        """)
        customers = cur.fetchall()

    logger.info("sync_crm_to_notion: found %d customers to sync", len(customers))

    ok = 0
    fail = 0
    for row in customers:
        slug         = row["creator_slug"]
        account_type = row["account_type"]
        try:
            result = sync_customer_costs(slug, account_type, conn)
            if result:
                logger.info("  ✓ %s (%s)", slug, account_type)
                ok += 1
            else:
                logger.warning("  ✗ %s — no Notion page found, skipping", slug)
                fail += 1
        except Exception:
            logger.exception("  ✗ %s — unexpected error", slug)
            fail += 1

    conn.close()
    logger.info("sync_crm_to_notion: done — %d ok, %d failed/skipped", ok, fail)


if __name__ == "__main__":
    main()
