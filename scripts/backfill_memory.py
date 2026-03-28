"""
Backfill fan memory profiles from existing conversation history.

This script ONLY reads from and writes to the Postgres database.
It does NOT send any SMS messages or trigger any webhooks.

Usage:
    DATABASE_URL="postgresql://..." python3 scripts/backfill_memory.py

Or add DATABASE_URL to .env temporarily and run:
    python3 scripts/backfill_memory.py
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

import psycopg2
import psycopg2.extras

# Must have DB + Gemini keys
DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌  DATABASE_URL not set. Get it from Railway → Postgres → Variables tab.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Import the memory extractor (uses GEMINI_API_KEY from env)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.brain.memory import extract_memory


def get_fans_to_backfill(conn):
    """Return fans who have messages but no memory profile yet."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT c.phone_number
            FROM contacts c
            WHERE (c.fan_memory IS NULL OR c.fan_memory = '')
              AND EXISTS (
                  SELECT 1 FROM messages m
                  WHERE m.phone_number = c.phone_number
                    AND m.role = 'user'
              )
            ORDER BY c.created_at DESC
        """)
        return [r["phone_number"] for r in cur.fetchall()]


def get_fan_messages(conn, phone_number):
    """Return all user messages for a fan, oldest first."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT text FROM messages
            WHERE phone_number = %s AND role = 'user'
            ORDER BY created_at ASC
        """, (phone_number,))
        return [r["text"] for r in cur.fetchall()]


def save_profile(conn, phone_number, memory, tags, location):
    """Write memory profile back — no messages sent, pure DB write."""
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE contacts
                SET fan_memory   = %s,
                    fan_tags     = %s,
                    fan_location = COALESCE(NULLIF(%s, ''), fan_location)
                WHERE phone_number = %s
            """, (memory[:400], tags, location[:100] if location else "", phone_number))


def process_fan(phone_number):
    """Process one fan — extract memory from all their messages combined."""
    conn = psycopg2.connect(DSN)
    try:
        messages = get_fan_messages(conn, phone_number)
        if not messages:
            return phone_number, "skipped (no messages)", None, [], ""

        # Combine all messages into one context string for a single extraction call
        combined = " | ".join(messages)

        memory, tags, location, minor = extract_memory("", combined)

        if minor:
            return phone_number, "skipped (minor detected)", "", [], ""

        if memory or tags or location:
            save_profile(conn, phone_number, memory, tags, location)
            return phone_number, "ok", memory, tags, location
        else:
            return phone_number, "no info extracted", "", [], ""
    except Exception as e:
        return phone_number, f"error: {e}", "", [], ""
    finally:
        conn.close()


def main():
    print("🔍  Connecting to database…")
    conn = psycopg2.connect(DSN)
    fans = get_fans_to_backfill(conn)
    conn.close()

    total = len(fans)
    if total == 0:
        print("✅  All fans already have memory profiles. Nothing to do.")
        return

    print(f"📋  Found {total} fans to backfill")
    print(f"⏳  Estimated time: ~{round(total / 10 * 1.5)} seconds (10 parallel calls)\n")
    print("─" * 60)

    done = 0
    profiled = 0
    skipped = 0
    errors = 0

    # Process 10 fans at a time — fast enough, won't hammer Gemini quota
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(process_fan, phone): phone for phone in fans}
        for future in as_completed(futures):
            phone, status, memory, tags, location = future.result()
            done += 1
            short_phone = f"...{phone[-4:]}"

            if status == "ok":
                profiled += 1
                loc_str = f" 📍{location}" if location else ""
                tag_str = f" [{', '.join(tags[:3])}{'…' if len(tags) > 3 else ''}]" if tags else ""
                print(f"  ✅ {short_phone}{loc_str}{tag_str}  —  {memory[:60]}{'…' if len(memory) > 60 else ''}")
            elif "no info" in status:
                skipped += 1
                print(f"  ○  {short_phone}  —  no personal info shared")
            elif "skipped" in status:
                skipped += 1
            else:
                errors += 1
                print(f"  ❌ {short_phone}  —  {status}")

            # Print progress every 25 fans
            if done % 25 == 0:
                print(f"\n  [{done}/{total}] processed so far…\n")

    print("\n" + "─" * 60)
    print(f"✅  Done! {profiled} profiles built, {skipped} skipped (no info), {errors} errors")
    print(f"🎉  Your Audience tab will now show fan profiles and tags.")


if __name__ == "__main__":
    main()
