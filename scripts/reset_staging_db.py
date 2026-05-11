"""
Wipe all auto-seeded test data from the staging databases and re-seed.

Use this when:
  - The staging DB has gotten messy from manual testing and you want a
    clean slate for the next round
  - You want to test the signup flow from scratch (delete operator users,
    let them re-sign-up at /signup)
  - The seed script schema has changed and you want to apply it fresh

What gets WIPED
===============

Operator DB:
  - bot_configs               WHERE creator_slug IN seeded slugs
  - team_members              WHERE tenant_slug IN seeded slugs
  - smb_bot_config            WHERE tenant_slug IN seeded slugs
  - operator_credit_usage     WHERE creator_slug IN seeded slugs
  - credit_events             WHERE creator_slug IN seeded slugs (best effort)
  - operator_users            WHERE email IN seeded emails

Main app DB:
  - messages                  WHERE phone_number IN seeded fan phones
  - contacts                  WHERE source = 'staging-seed'
                              OR creator_slug IN seeded slugs

What is PRESERVED
=================

  - Any operator_users / contacts / messages that were NOT created by the
    seed script (e.g. fans you added via the /api/admin/staging/add-test-fan
    endpoint, conversations from real Twilio inbound texts to the staging
    number)
  - Stripe customer records
  - Any data on prod (this script only ever talks to the staging URLs)

After wipe, runs scripts/seed_staging_db.py to re-populate.

Usage
=====

  STAGING_MAIN_DB_URL='<DATABASE_PUBLIC_URL from main-app Postgres>' \
  STAGING_OPERATOR_DB_URL='<DATABASE_PUBLIC_URL from operator Postgres>' \
  STAGING_OWNER_PASSWORD='<bootstrap password from 1Password>' \
  python scripts/reset_staging_db.py

Add --no-reseed to wipe without re-seeding (rare).
Add --keep-real-fans to also preserve fans NOT tagged 'staging-seed'.
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2

# Reuse the seed config so we never drift between wipe + re-seed.
from seed_staging_db import (  # noqa: E402  (script-style import)
    CREATORS,
    FAN_PREFIX_BY_SLUG,
    MAGIC_NUMBERS,
    main as run_seed,
)


SEEDED_SLUGS = [c["slug"] for c in CREATORS]
SEEDED_EMAILS = [c["email"] for c in CREATORS]


def wipe_main_db(conn, keep_real_fans: bool) -> None:
    """Wipe seeded fans + their messages from the main app DB."""
    print("\n=== Wiping main app DB ===")
    with conn, conn.cursor() as cur:
        # Build the phone-number set: anything tagged 'staging-seed' is fair
        # game; otherwise also remove rows with creator_slug in the seed list.
        if keep_real_fans:
            cur.execute(
                "SELECT phone_number FROM contacts WHERE source = 'staging-seed'"
            )
        else:
            cur.execute(
                """
                SELECT phone_number FROM contacts
                WHERE source = 'staging-seed'
                   OR creator_slug = ANY(%s)
                """,
                (SEEDED_SLUGS,),
            )
        phones = [r[0] for r in cur.fetchall()]
        print(f"  Identified {len(phones)} seeded fan phone numbers")

        if phones:
            cur.execute(
                "DELETE FROM messages WHERE phone_number = ANY(%s)", (phones,)
            )
            print(f"  Deleted {cur.rowcount} message rows")
            cur.execute(
                "DELETE FROM contacts WHERE phone_number = ANY(%s)", (phones,)
            )
            print(f"  Deleted {cur.rowcount} contact rows")


def wipe_operator_db(conn) -> None:
    """Wipe seeded operator users + their bot configs + team rows + credit rows."""
    print("\n=== Wiping operator DB ===")
    with conn, conn.cursor() as cur:
        # bot_configs
        cur.execute(
            "DELETE FROM bot_configs WHERE creator_slug = ANY(%s)",
            (SEEDED_SLUGS,),
        )
        print(f"  Deleted {cur.rowcount} bot_configs rows")

        # team_members
        cur.execute(
            "DELETE FROM team_members WHERE tenant_slug = ANY(%s)",
            (SEEDED_SLUGS,),
        )
        print(f"  Deleted {cur.rowcount} team_members rows")

        # smb_bot_config (only business creators have rows)
        cur.execute(
            "DELETE FROM smb_bot_config WHERE tenant_slug = ANY(%s)",
            (SEEDED_SLUGS,),
        )
        print(f"  Deleted {cur.rowcount} smb_bot_config rows")

        # operator_credit_usage
        cur.execute(
            "DELETE FROM operator_credit_usage WHERE creator_slug = ANY(%s)",
            (SEEDED_SLUGS,),
        )
        print(f"  Deleted {cur.rowcount} operator_credit_usage rows")

        # credit_events  (best effort — table only exists on certain branches)
        try:
            cur.execute(
                "DELETE FROM credit_events WHERE creator_slug = ANY(%s)",
                (SEEDED_SLUGS,),
            )
            print(f"  Deleted {cur.rowcount} credit_events rows")
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            print("  credit_events table not present — skipped")

        # operator_users LAST (fk references from the above)
        cur.execute(
            "DELETE FROM operator_users WHERE email = ANY(%s)",
            (SEEDED_EMAILS,),
        )
        print(f"  Deleted {cur.rowcount} operator_users rows")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset staging DB to a clean seeded state.")
    parser.add_argument("--no-reseed", action="store_true",
                        help="Wipe but skip the re-seed step")
    parser.add_argument("--keep-real-fans", action="store_true",
                        help="Preserve any fan rows NOT tagged 'staging-seed'")
    args = parser.parse_args()

    main_url = os.environ.get("STAGING_MAIN_DB_URL")
    # Default to main URL — same as the seed script. See its docstring.
    op_url = os.environ.get("STAGING_OPERATOR_DB_URL") or main_url
    if not main_url:
        print("ERROR: STAGING_MAIN_DB_URL must be set.", file=sys.stderr)
        return 2

    print(f"Reset target: {len(SEEDED_SLUGS)} seeded creators ({', '.join(SEEDED_SLUGS)})")
    print(f"Keep-real-fans: {args.keep_real_fans}")
    print(f"Re-seed after wipe: {not args.no_reseed}")

    same_db = (op_url == main_url)
    op_conn = psycopg2.connect(op_url)
    main_conn = op_conn if same_db else psycopg2.connect(main_url)
    try:
        wipe_main_db(main_conn, keep_real_fans=args.keep_real_fans)
        wipe_operator_db(op_conn)
    finally:
        op_conn.close()
        if not same_db:
            main_conn.close()

    if args.no_reseed:
        print("\nWipe complete. Skipping re-seed (--no-reseed).")
        return 0

    print("\nRe-seeding...")
    return run_seed()


if __name__ == "__main__":
    sys.exit(main())
