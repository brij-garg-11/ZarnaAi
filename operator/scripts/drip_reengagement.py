#!/usr/bin/env python3
"""
Drip re-engagement cron.

Finds fans who haven't sent a message in N days (default: 30) and queues
a single targeted re-engagement blast so Zarna pops back into their inbox.

The script does NOT send directly — it inserts a blast_drafts row with
status='queued_drip' (actually 'draft' so it can be reviewed) and logs
what it would send. Edit DRIP_MESSAGE below to customise the text.

Run on Railway as a scheduled cron (e.g. weekly, Sunday morning):
    python -u operator/scripts/drip_reengagement.py

Or locally:
    DATABASE_URL="..." python -u operator/scripts/drip_reengagement.py

Flags:
    --days N          Silence threshold in days (default: 30)
    --dry-run         Print fans + counts without inserting
    --send            Actually create a blast draft (default: dry-run)
    --creator SLUG    Restrict to one creator slug (default: all slugs)
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    logger.error("DATABASE_URL not set.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ── Drip message copy ────────────────────────────────────────────────────────
# {{name}} will be resolved per-fan at send time if fan_name is populated.
DRIP_MESSAGE = (
    "Hey {{name}}! Zarna here — haven't heard from you in a while 👋 "
    "What have you been up to? Reply anytime!"
)
# ─────────────────────────────────────────────────────────────────────────────


def get_silent_fans(conn, days: int, creator_slug: str | None) -> list[dict]:
    """Return fans who have not sent a message in the last `days` days."""
    slug_clause = "AND c.creator_slug = %s" if creator_slug else ""
    params: list = [days]
    if creator_slug:
        params.append(creator_slug)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT c.phone_number,
                   COALESCE(c.fan_name, '')     AS fan_name,
                   COALESCE(c.fan_location, '') AS fan_location,
                   COALESCE(c.fan_tier, '')     AS fan_tier,
                   c.creator_slug,
                   MAX(m.created_at)            AS last_message_at
            FROM   contacts c
            JOIN   messages m ON m.phone_number = c.phone_number
                              AND m.role = 'user'
            WHERE  c.phone_number NOT LIKE 'whatsapp:%%'
            {slug_clause}
            GROUP BY c.phone_number, c.fan_name, c.fan_location, c.fan_tier, c.creator_slug
            HAVING MAX(m.created_at) < NOW() - INTERVAL '%s days'
            ORDER BY last_message_at ASC
            """,
            params + [days],
        )
        return [dict(r) for r in cur.fetchall()]


def get_optouts(conn) -> set:
    """Return phone numbers that have opted out."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT phone_number FROM optouts")
            return {r[0] for r in cur.fetchall()}
    except Exception:
        return set()


def create_drip_blast_draft(
    conn,
    creator_slug: str,
    message: str,
    fan_count: int,
    created_by: str = "drip-cron",
) -> int:
    """Insert a blast_drafts row targeting dormant/lurker fans for this slug."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    name = f"[Drip] Re-engagement {today}"
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO blast_drafts
                  (name, body, channel, audience_type, audience_filter,
                   audience_sample_pct, status, created_by, creator_slug,
                   blast_context_note)
                VALUES (%s, %s, 'twilio', 'all', '',
                        100, 'draft', %s, %s,
                        'Auto-generated drip re-engagement blast. Review before sending.')
                RETURNING id
                """,
                (name, message, created_by, creator_slug or None),
            )
            return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="Silence threshold in days")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Print without creating drafts (default)")
    parser.add_argument("--send", action="store_true",
                        help="Actually create blast drafts (overrides --dry-run)")
    parser.add_argument("--creator", type=str, default=None,
                        help="Restrict to one creator slug")
    args = parser.parse_args()

    dry_run = not args.send

    logger.info("=== Drip Re-engagement Cron ===")
    logger.info("  silence threshold: %d days", args.days)
    logger.info("  mode: %s", "DRY RUN" if dry_run else "LIVE — will create blast drafts")
    if args.creator:
        logger.info("  creator filter: %s", args.creator)

    conn = psycopg2.connect(DSN)
    try:
        optouts = get_optouts(conn)
        fans = get_silent_fans(conn, args.days, args.creator)
    finally:
        conn.close()

    # Filter optouts
    fans = [f for f in fans if f["phone_number"] not in optouts]

    if not fans:
        logger.info("No silent fans found — nothing to do.")
        return

    # Group by creator_slug
    by_slug: dict[str, list] = {}
    for fan in fans:
        slug = fan["creator_slug"] or "zarna"
        by_slug.setdefault(slug, []).append(fan)

    logger.info("Found %d silent fans across %d creator(s):", len(fans), len(by_slug))
    for slug, slug_fans in by_slug.items():
        tiers = {}
        for f in slug_fans:
            tiers[f["fan_tier"] or "unscored"] = tiers.get(f["fan_tier"] or "unscored", 0) + 1
        tier_summary = "  ".join(f"{t}: {n}" for t, n in sorted(tiers.items()))
        logger.info("  %s  →  %d fans  (%s)", slug, len(slug_fans), tier_summary)

    if dry_run:
        logger.info("\nDRY RUN — no drafts created.")
        logger.info("Re-run with --send to create blast drafts for review in the operator dashboard.")
        return

    # Create one draft per creator slug
    conn = psycopg2.connect(DSN)
    try:
        for slug, slug_fans in by_slug.items():
            draft_id = create_drip_blast_draft(
                conn,
                creator_slug=slug,
                message=DRIP_MESSAGE,
                fan_count=len(slug_fans),
            )
            logger.info(
                "  ✅ Created draft #%d for %s (%d fans) — review at /operator/blast/%d",
                draft_id, slug, len(slug_fans), draft_id,
            )
    finally:
        conn.close()

    logger.info("=== Done. Review drafts in the operator dashboard before sending. ===")


if __name__ == "__main__":
    main()
