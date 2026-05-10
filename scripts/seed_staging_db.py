"""
Seed the staging databases with the bare minimum to exercise:
  - SMS inbound + outbound on the staging Twilio number
  - Credit gates (trial tier with low remaining credits, so blasts hit the limit fast)
  - Multi-fan blast targeting (a handful of test fans pre-opted-in)

This script connects to BOTH staging Postgres databases (main app + operator)
using the *public* viaduct URLs so it can run from anywhere — the internal
.railway.internal hostnames only resolve from inside Railway.

Usage (from repo root, after `pip install psycopg2-binary bcrypt`):

  STAGING_MAIN_DB_URL='postgresql://postgres:...@viaduct.proxy.rlwy.net:46568/railway' \
  STAGING_OPERATOR_DB_URL='postgresql://postgres:...@viaduct.proxy.rlwy.net:47900/railway' \
  python scripts/seed_staging_db.py

Re-run is safe — every insert uses ON CONFLICT DO NOTHING / DO UPDATE.
"""

from __future__ import annotations

import os
import sys

import bcrypt
import psycopg2

CREATOR_SLUG = "brij-test"

OPERATOR_OWNER_EMAIL = "brijgarg286@gmail.com"

# The bootstrap password is read from STAGING_OWNER_PASSWORD env var so it
# never lives in git. Pull the actual value from 1Password ("Zarna Staging —
# operator login") and `export STAGING_OWNER_PASSWORD=...` before running.
OPERATOR_OWNER_PASSWORD = os.environ.get("STAGING_OWNER_PASSWORD")
TRIAL_CREDITS = 10

TEST_FANS = [
    {
        "phone": "+15005550006",
        "fan_name": "Test Fan Alice (Twilio magic — accepts SMS)",
        "fan_location": "San Francisco, CA",
        "fan_tags": ["test", "vip"],
        "fan_memory": "Loves the brij-test bot. Has been a fan since the staging environment came online.",
    },
    {
        "phone": "+15005550008",
        "fan_name": "Test Fan Bob (Twilio magic — invalid for testing failures)",
        "fan_location": "Austin, TX",
        "fan_tags": ["test", "active"],
        "fan_memory": "Texts back rarely but reads everything.",
    },
    {
        "phone": "+15005550010",
        "fan_name": "Test Fan Carol (Twilio magic — unavailable, tests retry)",
        "fan_location": "New York, NY",
        "fan_tags": ["test", "engaged"],
        "fan_memory": "Replies with multi-question messages. Used to test long replies.",
    },
    {
        "phone": "+15005550003",
        "fan_name": "Test Fan Dave (Twilio magic — international forbidden)",
        "fan_location": "Seattle, WA",
        "fan_tags": ["test"],
        "fan_memory": "Quiet fan; used to test cold contact follow-ups.",
    },
]


def connect(url: str):
    return psycopg2.connect(url)


def seed_operator(conn) -> None:
    print("\n=== Operator DB seed ===")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM operator_users WHERE email = %s", (OPERATOR_OWNER_EMAIL,))
        existed = cur.fetchone() is not None

        password_hash = bcrypt.hashpw(
            OPERATOR_OWNER_PASSWORD.encode(), bcrypt.gensalt()
        ).decode()

        cur.execute(
            """
            INSERT INTO operator_users (
                email, name, password_hash, is_active, is_owner,
                creator_slug, account_type, plan_tier, trial_credits_remaining,
                trial_started_at
            )
            VALUES (%s, %s, %s, TRUE, TRUE, %s, 'performer', 'trial', %s, NOW())
            ON CONFLICT (email) DO UPDATE SET
                creator_slug = EXCLUDED.creator_slug,
                plan_tier = EXCLUDED.plan_tier,
                trial_credits_remaining = EXCLUDED.trial_credits_remaining,
                is_active = TRUE,
                is_owner = TRUE
            """,
            (
                OPERATOR_OWNER_EMAIL,
                "Brij (staging owner)",
                password_hash,
                CREATOR_SLUG,
                TRIAL_CREDITS,
            ),
        )

        action = "updated" if existed else "created"
        print(f"  Operator user {action}: {OPERATOR_OWNER_EMAIL} (plan=trial, credits={TRIAL_CREDITS})")


def seed_main_app(conn) -> None:
    print("\n=== Main app DB seed ===")
    with conn, conn.cursor() as cur:
        for fan in TEST_FANS:
            cur.execute(
                """
                INSERT INTO contacts (
                    phone_number, source, creator_slug,
                    fan_name, fan_location, fan_tags, fan_memory
                )
                VALUES (%s, 'staging-seed', %s, %s, %s, %s, %s)
                ON CONFLICT (phone_number) DO UPDATE SET
                    creator_slug = EXCLUDED.creator_slug,
                    fan_name     = EXCLUDED.fan_name,
                    fan_location = EXCLUDED.fan_location,
                    fan_tags     = EXCLUDED.fan_tags,
                    fan_memory   = EXCLUDED.fan_memory
                """,
                (
                    fan["phone"],
                    CREATOR_SLUG,
                    fan["fan_name"],
                    fan["fan_location"],
                    fan["fan_tags"],
                    fan["fan_memory"],
                ),
            )
            print(f"  Fan upserted: {fan['phone']:<15s} | {fan['fan_name']}")

        cur.execute(
            "SELECT COUNT(*) FROM contacts WHERE creator_slug = %s",
            (CREATOR_SLUG,),
        )
        total = cur.fetchone()[0]
        print(f"  Total fans for creator '{CREATOR_SLUG}': {total}")


def main() -> int:
    main_url = os.environ.get("STAGING_MAIN_DB_URL")
    op_url = os.environ.get("STAGING_OPERATOR_DB_URL")
    if not main_url or not op_url:
        print(
            "ERROR: Both STAGING_MAIN_DB_URL and STAGING_OPERATOR_DB_URL must be set.\n"
            "Pull them from Railway (Postgres service → Variables → DATABASE_PUBLIC_URL).",
            file=sys.stderr,
        )
        return 2

    if not OPERATOR_OWNER_PASSWORD:
        print(
            "ERROR: STAGING_OWNER_PASSWORD env var must be set.\n"
            "Pull from 1Password ('Zarna Staging — operator login').",
            file=sys.stderr,
        )
        return 2

    print("Connecting to staging databases...")
    op_conn = connect(op_url)
    main_conn = connect(main_url)
    try:
        seed_operator(op_conn)
        seed_main_app(main_conn)
    finally:
        op_conn.close()
        main_conn.close()

    print("\nDone. Login at the operator dashboard with:")
    print(f"  email:    {OPERATOR_OWNER_EMAIL}")
    print(f"  password: <STAGING_OWNER_PASSWORD env var>")
    print(f"  credits:  {TRIAL_CREDITS} on the 'trial' plan (will hard-stop after that)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
