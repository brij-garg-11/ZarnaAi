"""
Migrate GWG contacts from the legacy `contacts` table into `smb_subscribers`.

These subscribers originally opted in via Twilio (855 number) for Grades with Gargs.
We're migrating them to the SMB platform on the 212 number as active subscribers,
skipping onboarding (onboarding_step = -1 = completed).

Run once in Railway's shell:
    python scripts/migrate_gwg_contacts.py

Safe to run multiple times — uses INSERT ... ON CONFLICT DO NOTHING.
"""

import os
import sys

import psycopg2
import psycopg2.extras

TENANT_SLUG = "grades_with_gargs"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: DATABASE_URL env var not set.")
    sys.exit(1)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Count existing contacts
            cur.execute("SELECT COUNT(*) FROM contacts")
            total = cur.fetchone()[0]
            print(f"Found {total} contact(s) in the contacts table.")

            if total == 0:
                print("Nothing to migrate.")
                return

            # Preview
            cur.execute("SELECT phone_number, source, created_at FROM contacts ORDER BY created_at")
            rows = cur.fetchall()
            print("\nContacts to migrate:")
            for r in rows:
                print(f"  {r['phone_number']}  source={r['source']}  joined={r['created_at']}")

            confirm = input(f"\nMigrate all {total} contact(s) to smb_subscribers as '{TENANT_SLUG}'? [y/N] ").strip().lower()
            if confirm != "y":
                print("Aborted.")
                return

            # Insert into smb_subscribers — skip duplicates
            inserted = 0
            skipped = 0
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO smb_subscribers
                        (phone_number, tenant_slug, status, onboarding_step, created_at, updated_at)
                    VALUES (%s, %s, 'active', -1, %s, NOW())
                    ON CONFLICT (phone_number, tenant_slug) DO NOTHING
                    """,
                    (r["phone_number"], TENANT_SLUG, r["created_at"]),
                )
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

            conn.commit()
            print(f"\nDone. Inserted: {inserted}  Skipped (already existed): {skipped}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
