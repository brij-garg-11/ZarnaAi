"""
Pre-seed a campaign message into conversation history for subscribers.

This inserts one "assistant" row per subscriber so the bot knows the
context of what was asked in the outbound blast, and can respond
intelligently when fans reply.

Usage — test on one number first:
    DATABASE_URL="..." python3 scripts/preseed_campaign.py --test +16467244908

Usage — run for all subscribers once test confirms it works:
    DATABASE_URL="..." python3 scripts/preseed_campaign.py --all

The campaign message is defined in CAMPAIGN_MESSAGE below.
Update it before each new campaign.
"""

import argparse
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌  DATABASE_URL not set.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ── UPDATE THIS BEFORE EACH CAMPAIGN ──────────────────────────────────────────
CAMPAIGN_MESSAGE = "Pop Quiz: Who is Zarna's enemy #1? 😤 Reply back!"
# ──────────────────────────────────────────────────────────────────────────────


def preseed(phone_numbers: list):
    """
    Bulk-insert the campaign message for all given numbers in two SQL statements.
    Safe to re-run — skips any number that already has this exact message.
    """
    conn = psycopg2.connect(DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                # Find which numbers already have this message seeded
                cur.execute("""
                    SELECT DISTINCT phone_number FROM messages
                    WHERE role = 'assistant' AND text = %s
                """, (CAMPAIGN_MESSAGE,))
                already_seeded = {r[0] for r in cur.fetchall()}

                to_insert = [n for n in phone_numbers if n not in already_seeded]

                if to_insert:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO messages (phone_number, role, text, source) VALUES %s",
                        [(n, "assistant", CAMPAIGN_MESSAGE, "blast") for n in to_insert],
                        page_size=500,
                    )

        return len(to_insert), len(already_seeded)
    finally:
        conn.close()


def get_all_subscribers():
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT phone_number FROM contacts ORDER BY created_at")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", metavar="PHONE", help="Seed one phone number only (e.g. +16467244908)")
    group.add_argument("--all",  action="store_true", help="Seed all subscribers in the database")
    args = parser.parse_args()

    print(f"\nCampaign message:\n  \"{CAMPAIGN_MESSAGE}\"\n")

    if args.test:
        number = args.test.strip()
        if not number.startswith("+"):
            number = "+1" + number.lstrip("1")
        print(f"🧪  Test mode — seeding 1 number: {number}")
        inserted, skipped = preseed([number])  # skipped count = already done
        if inserted:
            print(f"✅  Seeded! Now text {number} with your reply to test the bot's response.")
        else:
            print(f"⚠️   Already seeded (or message exists). Check the messages table.")

    else:
        subscribers = get_all_subscribers()
        total = len(subscribers)
        print(f"📋  Found {total:,} subscribers in database")
        print(f"⚡  Seeding campaign context for all of them…\n")

        inserted, skipped = preseed(subscribers)

        print(f"✅  Done!")
        print(f"   Seeded  : {inserted:,}")
        print(f"   Skipped : {skipped:,} (already seeded)")
        print(f"\nYou're ready to send the blast from SlickText.")


if __name__ == "__main__":
    main()
