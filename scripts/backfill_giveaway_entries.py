#!/usr/bin/env python3
"""
Backfill giveaway entries from messages already in the database.

The live entry hook (app/giveaway/entry.py) only records fans who text the
keyword AFTER it's deployed. If a campaign's window opened before deploy — or
you create a campaign for a word fans have already been texting — run this to
scan the messages table for the keyword within the campaign window and insert
the matching fans as entries. Idempotent: re-running never double-enters
(UNIQUE (campaign_id, phone_number) + ON CONFLICT DO NOTHING).

Usage:
  python scripts/backfill_giveaway_entries.py --campaign-id 3
  python scripts/backfill_giveaway_entries.py --campaign-id 3 --dry-run

Env: DATABASE_URL
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv

load_dotenv()


def _connect():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        sys.exit("DATABASE_URL not set")
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign-id", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true", help="show matches, insert nothing")
    args = ap.parse_args()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT label, keyword, starts_at, ends_at, creator_slug "
                "FROM giveaway_campaigns WHERE id = %s",
                (args.campaign_id,),
            )
            row = cur.fetchone()
            if not row:
                sys.exit(f"No campaign with id={args.campaign_id}")
            label, keyword, starts_at, ends_at, slug = row
            print(f"Campaign #{args.campaign_id}: {label!r}  keyword={keyword!r}")
            print(f"  window: {starts_at} → {ends_at}  slug={slug}")

            # Earliest matching inbound per phone within the window.
            cur.execute(
                """
                SELECT DISTINCT ON (phone_number) phone_number, id
                FROM messages
                WHERE role = 'user'
                  AND text ILIKE %s
                  AND (%s::timestamptz IS NULL OR created_at >= %s)
                  AND (%s::timestamptz IS NULL OR created_at <= %s)
                ORDER BY phone_number, created_at ASC
                """,
                (f"%{keyword}%", starts_at, starts_at, ends_at, ends_at),
            )
            matches = cur.fetchall()

        print(f"  found {len(matches)} distinct fans matching the keyword in-window")
        if args.dry_run:
            for phone, mid in matches[:50]:
                print(f"    ...{phone[-4:]}  (msg {mid})")
            if len(matches) > 50:
                print(f"    … and {len(matches) - 50} more")
            print("  (dry run — nothing inserted)")
            return 0

        inserted = 0
        with conn:
            with conn.cursor() as cur:
                for phone, mid in matches:
                    cur.execute(
                        """
                        INSERT INTO giveaway_entries
                            (campaign_id, phone_number, message_id, source, creator_slug)
                        VALUES (%s, %s, %s, 'backfill', %s)
                        ON CONFLICT (campaign_id, phone_number) DO NOTHING
                        RETURNING id
                        """,
                        (args.campaign_id, phone, mid, slug),
                    )
                    if cur.fetchone():
                        inserted += 1
        print(f"  inserted {inserted} new entries ({len(matches) - inserted} already present)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
