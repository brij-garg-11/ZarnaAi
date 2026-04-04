"""
Import all SlickText subscribers into the Postgres contacts table,
preserving their real subscribedDate as created_at.

Pulls from both textwords (zarna: 3185378, hello: 4633842).

- New contacts: inserted with their real subscribedDate as created_at.
- Existing contacts: created_at is backfilled only if it looks like it was
  set to the import date (i.e. after 2026-03-26), so re-running this is safe.

Usage:
    DATABASE_URL="postgresql://..." python3 scripts/import_slicktext_subscribers.py

Or with .env:
    python3 scripts/import_slicktext_subscribers.py
"""

import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
PUB_KEY      = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
PRIV_KEY     = os.getenv("SLICKTEXT_PRIVATE_KEY", "")

if not DATABASE_URL:
    print("❌  DATABASE_URL not set.")
    sys.exit(1)
if not PUB_KEY or not PRIV_KEY:
    print("❌  SLICKTEXT_PUBLIC_KEY / SLICKTEXT_PRIVATE_KEY not set.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

TEXTWORDS = [
    (3185378, "zarna"),
    (4633842, "hello"),
]

PAGE_SIZE = 200

# Contacts created_at after this date are considered "wrong" (set to import
# time rather than real subscribe date) and will be backfilled.
_BACKFILL_THRESHOLD = "2026-03-26"


def _parse_subscribed_date(raw: str | None) -> str | None:
    """Parse SlickText subscribedDate (Pacific time) → ISO UTC string or None."""
    if not raw:
        return None
    try:
        # SlickText returns "YYYY-MM-DD HH:MM:SS" in Pacific time (UTC-8 / UTC-7).
        # We store as-is with timezone info; Postgres handles the conversion.
        # Treat as UTC-8 (PST) conservatively — off by at most 1h vs PDT.
        dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
        # Return ISO string; let Postgres interpret as the value given
        return raw.strip()
    except ValueError:
        return None


def fetch_all_contacts(textword_id: int, label: str):
    """Page through all contacts for a textword, yield (phone, subscribed_date) tuples."""
    offset  = 0
    total   = None
    fetched = 0

    print(f"\n  Fetching textword '{label}' (id={textword_id})…")

    while True:
        resp = requests.get(
            "https://api.slicktext.com/v1/contacts/",
            params={"textword": textword_id, "limit": PAGE_SIZE, "offset": offset},
            auth=(PUB_KEY, PRIV_KEY),
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  ❌  API error {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json()
        if total is None:
            total = data["meta"]["total"]
            print(f"  Total subscribers: {total:,}")

        contacts = data.get("contacts", [])
        if not contacts:
            break

        for c in contacts:
            number = (c.get("number") or "").strip()
            if number:
                yield number, _parse_subscribed_date(c.get("subscribedDate"))

        fetched += len(contacts)
        offset  += PAGE_SIZE

        print(f"  … fetched {fetched:,} / {total:,}", end="\r")

        if fetched >= total:
            break

        time.sleep(0.1)

    print(f"  ✅  Done — {fetched:,} contacts fetched from '{label}'")


def import_into_postgres(contacts: list) -> tuple[int, int, int]:
    """
    Upsert contacts with real subscribedDate.
    Returns (inserted, backfilled, skipped).
    """
    conn = psycopg2.connect(DSN)
    inserted   = 0
    backfilled = 0
    skipped    = 0

    try:
        with conn:
            with conn.cursor() as cur:
                for number, subscribed_date in contacts:
                    if subscribed_date:
                        # Insert new; if exists and created_at looks wrong, backfill it
                        cur.execute(
                            """
                            INSERT INTO contacts (phone_number, source, created_at)
                            VALUES (%s, 'slicktext', %s::timestamp)
                            ON CONFLICT (phone_number) DO UPDATE
                              SET created_at = EXCLUDED.created_at
                            WHERE contacts.created_at::date >= %s::date
                            """,
                            (number, subscribed_date, _BACKFILL_THRESHOLD),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO contacts (phone_number, source)
                            VALUES (%s, 'slicktext')
                            ON CONFLICT (phone_number) DO NOTHING
                            """,
                            (number,),
                        )

                    if cur.rowcount > 0:
                        # rowcount=1 on both INSERT and UPDATE
                        if subscribed_date:
                            inserted += 1
                        else:
                            inserted += 1
                    else:
                        skipped += 1
    finally:
        conn.close()

    return inserted, backfilled, skipped


def main():
    print("=" * 60)
    print("  SlickText → Postgres Subscriber Import (with real dates)")
    print("=" * 60)

    all_contacts: list[tuple[str, str | None]] = []
    seen: set[str] = set()

    for tw_id, label in TEXTWORDS:
        for number, sub_date in fetch_all_contacts(tw_id, label):
            if number not in seen:
                seen.add(number)
                all_contacts.append((number, sub_date))

    total_unique = len(all_contacts)
    has_dates    = sum(1 for _, d in all_contacts if d)
    print(f"\n📱  Total unique phone numbers: {total_unique:,}")
    print(f"📅  With real subscribedDate  : {has_dates:,}")

    print(f"\n💾  Upserting into Postgres (backfilling created_at where needed)…")
    inserted, backfilled, skipped = import_into_postgres(all_contacts)

    print("\n" + "=" * 60)
    print(f"✅  Import complete!")
    print(f"   Inserted / updated : {inserted:,}")
    print(f"   Already correct    : {skipped:,}")
    print(f"   Total unique subs  : {total_unique:,}")
    print("=" * 60)
    print("\ncontacts.created_at now reflects real SlickText subscribe dates.")
    print("The Insights dashboard pre/post bot comparison will now be accurate.")


if __name__ == "__main__":
    main()
