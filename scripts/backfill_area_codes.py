"""
Backfill ``contacts.area_codes`` from each contact's phone number.

WHY
---
Area-code blast targeting (e.g. "everyone in NY/NJ") needs every contact tagged
with the NANP area code (NPA) of their number. New/updated contacts are tagged
automatically on write, but the existing ~4,500 Zarna subscribers predate the
column, so this one-time pass fills them in.

Only contacts whose ``area_codes`` is still empty are touched, so any manual
area-code additions made by the operator are preserved. Re-runnable and additive.

ROLLBACK
--------
    UPDATE contacts SET area_codes = '{}';   -- clears ALL (manual ones too)

USAGE
-----
    python scripts/backfill_area_codes.py --dry-run
    python scripts/backfill_area_codes.py
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

load_dotenv()

from app.area_codes import area_code_from_phone


def _dsn(database_url: str) -> str:
    return database_url.replace("postgres://", "postgresql://", 1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill contacts.area_codes from phone numbers.")
    parser.add_argument("--dry-run", action="store_true", help="Compute without writing.")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        print("DATABASE_URL not set.")
        return 1

    import psycopg2
    import psycopg2.extras

    print("=" * 64)
    print("  Backfill contacts.area_codes from phone numbers")
    print("=" * 64)
    print(f"  Mode: {'dry-run' if args.dry_run else 'WRITE'}")

    conn = psycopg2.connect(_dsn(database_url))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT phone_number FROM contacts WHERE area_codes = '{}'")
            rows = [r[0] for r in cur.fetchall()]

        updates: list[tuple[list[str], str]] = []
        unmatched = 0
        dist: Counter = Counter()
        for phone in rows:
            npa = area_code_from_phone(phone)
            if npa:
                updates.append(([npa], phone))
                dist[npa] += 1
            else:
                unmatched += 1

        print(f"\n  contacts with empty area_codes : {len(rows):,}")
        print(f"  derivable (will update)        : {len(updates):,}")
        print(f"  unmatched (non-NANP / short)   : {unmatched:,}")
        if dist:
            print("\n  top area codes found:")
            for code, cnt in dist.most_common(15):
                print(f"    {code}: {cnt:,}")

        updated = 0
        if not args.dry_run and updates:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    "UPDATE contacts SET area_codes = %s WHERE phone_number = %s AND area_codes = '{}'",
                    updates,
                    page_size=1000,
                )
                updated = len(updates)
            conn.commit()

        print("\n" + "=" * 64)
        print(f"  {'DRY RUN (no writes)' if args.dry_run else 'WRITE COMPLETE'}")
        print(f"  Rows updated: {updated:,}")
        print("=" * 64)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
