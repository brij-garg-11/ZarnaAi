"""
Import SlickText subscribers into the Postgres contacts table.

Defaults to the textwords configured by `SLICKTEXT_CONTACT_TEXTWORDS`.

Usage:
    DATABASE_URL="postgresql://..." python3 scripts/import_slicktext_subscribers.py
    python3 scripts/import_slicktext_subscribers.py --dry-run
    python3 scripts/import_slicktext_subscribers.py --textwords "3185378:zarna,4633842:hello"
"""

import argparse
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

load_dotenv()

from app.config import SLICKTEXT_CONTACT_TEXTWORDS
from app.messaging.slicktext_contacts import (
    fetch_unique_contacts,
    parse_textword_config,
    sync_contacts_to_postgres,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import SlickText subscribers into contacts.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and compare contacts without modifying Postgres.",
    )
    parser.add_argument(
        "--textwords",
        default=SLICKTEXT_CONTACT_TEXTWORDS,
        help="Comma-separated `id[:label]` list. Defaults to SLICKTEXT_CONTACT_TEXTWORDS.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    database_url = os.getenv("DATABASE_URL", "")
    public_key = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
    private_key = os.getenv("SLICKTEXT_PRIVATE_KEY", "")

    if not database_url:
        print("DATABASE_URL not set.")
        return 1
    if not public_key or not private_key:
        print("SLICKTEXT_PUBLIC_KEY / SLICKTEXT_PRIVATE_KEY not set.")
        return 1

    try:
        textwords = parse_textword_config(args.textwords)
    except ValueError as exc:
        print(f"Invalid SLICKTEXT_CONTACT_TEXTWORDS value: {exc}")
        return 1

    print("=" * 60)
    print("  SlickText → Postgres Subscriber Sync")
    print("=" * 60)
    print(f"  Mode        : {'dry-run' if args.dry_run else 'write'}")
    print(f"  Textwords   : {', '.join(f'{label}:{textword_id}' for textword_id, label in textwords)}")

    contacts, fetch_stats = fetch_unique_contacts(
        public_key=public_key,
        private_key=private_key,
        textwords=textwords,
    )
    print(f"\nFetched rows           : {fetch_stats.fetched:,}")
    print(f"Unique phone numbers   : {fetch_stats.unique:,}")
    print(f"With subscribedDate    : {fetch_stats.with_dates:,}")

    sync_stats = sync_contacts_to_postgres(
        database_url=database_url,
        contacts=contacts,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print(f"{'Dry run complete' if args.dry_run else 'Sync complete'}")
    print(f"Inserted              : {sync_stats.inserted:,}")
    print(f"Updated               : {sync_stats.updated:,}")
    print(f"Skipped               : {sync_stats.skipped:,}")
    print(f"Total contacts after  : {sync_stats.total_contacts_after:,}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
