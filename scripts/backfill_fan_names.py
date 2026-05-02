"""
Backfill fan_name column from existing fan_memory paragraphs.

Reads every contact that has a fan_memory but an empty fan_name,
asks Gemini to extract the first name, and writes it back.

Usage:
    DATABASE_URL="postgresql://..." GEMINI_API_KEY="..." python3 scripts/backfill_fan_names.py

Dry-run (print but don't write):
    ... python3 scripts/backfill_fan_names.py --dry-run
"""

import os
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌  DATABASE_URL not set.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Lightweight regex extraction — avoids an API call for obvious patterns.
import re
_NAME_PATTERNS = [
    re.compile(r"Fan(?:'s name)? is (?:named )?([A-Z][a-z]{1,30})\b", re.IGNORECASE),
    re.compile(r"named? ([A-Z][a-z]{1,30})\b", re.IGNORECASE),
    re.compile(r"\bname(?:d)? ([A-Z][a-z]{1,30})\b", re.IGNORECASE),
]


def _extract_name_regex(memory: str) -> str:
    for pat in _NAME_PATTERNS:
        m = pat.search(memory)
        if m:
            return m.group(1).strip()
    return ""


def get_fans_needing_name(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT phone_number, fan_memory
            FROM   contacts
            WHERE  (fan_name IS NULL OR fan_name = '')
              AND  fan_memory IS NOT NULL
              AND  fan_memory <> ''
            ORDER BY created_at DESC
        """)
        return [(r["phone_number"], r["fan_memory"]) for r in cur.fetchall()]


def save_name(conn, phone_number: str, name: str) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE contacts SET fan_name = %s WHERE phone_number = %s",
                (name[:80], phone_number),
            )


def process_fan(phone_number: str, fan_memory: str, dry_run: bool) -> tuple:
    """Return (phone, name_found, method)."""
    # Try regex first — free and instant
    name = _extract_name_regex(fan_memory)
    method = "regex"

    if not name:
        # Fall back to Gemini for trickier phrasing
        try:
            from google import genai
            from app.config import GEMINI_API_KEY, INTENT_MODEL
            client = genai.Client(api_key=GEMINI_API_KEY)
            prompt = (
                f"Extract the fan's first name from this profile snippet. "
                f"Reply with ONLY the first name (e.g. 'Priya') or an empty string if no name is present.\n\n"
                f"Profile: {fan_memory[:300]}"
            )
            resp = client.models.generate_content(model=INTENT_MODEL, contents=prompt)
            raw = (resp.text or "").strip().strip('"').strip("'")
            if raw and len(raw.split()) == 1 and raw.isalpha():
                name = raw[:80]
                method = "gemini"
        except Exception as e:
            return phone_number, "", f"error: {e}"

    if not name:
        return phone_number, "", "no-name"

    if not dry_run:
        conn = psycopg2.connect(DSN)
        try:
            save_name(conn, phone_number, name)
        finally:
            conn.close()

    return phone_number, name, method


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    args = parser.parse_args()

    print("🔍  Connecting to database…")
    conn = psycopg2.connect(DSN)
    fans = get_fans_needing_name(conn)
    conn.close()

    total = len(fans)
    if total == 0:
        print("✅  All fans already have names (or no memory to extract from). Nothing to do.")
        return

    print(f"📋  Found {total} fans with memory but no name")
    if args.dry_run:
        print("🔍  DRY RUN — no writes will be made")
    print(f"⏳  Processing…\n{'─' * 60}")

    found = 0
    skipped = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(process_fan, phone, memory, args.dry_run): phone
            for phone, memory in fans
        }
        for future in as_completed(futures):
            phone, name, method = future.result()
            short = f"...{phone[-4:]}"
            if name:
                found += 1
                tag = "[DRY RUN] " if args.dry_run else ""
                print(f"  ✅ {tag}{short}  →  {name}  ({method})")
            elif "error" in method:
                errors += 1
                print(f"  ❌ {short}  —  {method}")
            else:
                skipped += 1

    print(f"\n{'─' * 60}")
    print(f"✅  Done!  {found} names extracted, {skipped} fans with no name in memory, {errors} errors")
    if args.dry_run:
        print("   (DRY RUN — nothing was written)")
    else:
        print("   fan_name column is now populated. Blasts using {{name}} will personalise immediately.")


if __name__ == "__main__":
    main()
