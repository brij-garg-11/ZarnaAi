"""
Import pre-bot SlickText chat history (CSV) into the messages table.

Only imports messages sent BEFORE the bot launch date (2026-03-27).
Post-launch messages are already in the DB from the live bot.

After inserting, computes did_user_reply / went_silent_after / reply_delay_seconds
for all imported rows so Insights analytics work correctly.

Usage:
    python3 scripts/import_chat_transcripts.py /path/to/Chat-Transcripts.csv

Or upload the CSV via the admin endpoint:
    POST /admin/actions/import-chat-transcripts
"""

import csv
import os
import sys
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌  DATABASE_URL not set.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

ZARNA_NUMBER  = "+18775532629"
BOT_LAUNCH    = datetime(2026, 3, 27, tzinfo=timezone.utc)
REPLY_WINDOW  = timedelta(hours=48)   # max time window to count a reply


# ── Timestamp parsing ────────────────────────────────────────────────────────

_TZ_OFFSETS = {"EDT": -4, "EST": -5, "PDT": -7, "PST": -8, "UTC": 0}

def parse_sent(raw: str) -> datetime | None:
    """Parse 'YYYY-MM-DD HH:MM:SS TZ' → UTC datetime."""
    raw = raw.strip()
    if not raw:
        return None
    parts = raw.rsplit(" ", 1)
    if len(parts) == 2:
        dt_str, tz_abbr = parts
        offset_h = _TZ_OFFSETS.get(tz_abbr.upper(), -5)
    else:
        dt_str = parts[0]
        offset_h = -5  # assume EST
    try:
        naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return naive.replace(tzinfo=timezone(timedelta(hours=offset_h)))
    except ValueError:
        return None


# ── DB helpers ───────────────────────────────────────────────────────────────

def ensure_source_column(conn):
    """Add source column if missing (no DEFAULT to avoid table lock)."""
    with conn.cursor() as cur:
        # No DEFAULT clause — avoids full table rewrite lock on large tables
        cur.execute(
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS source TEXT"
        )
    conn.commit()


def load_csv(path: str) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            dt = parse_sent(r.get("Sent", ""))
            if not dt:
                continue
            # Only pre-bot messages
            if dt >= BOT_LAUNCH:
                continue

            from_num = (r.get("From") or "").strip()
            to_num   = (r.get("To")   or "").strip()
            body     = (r.get("Body") or "").strip()
            if not body:
                continue  # skip empty / media-only

            if from_num == ZARNA_NUMBER:
                role         = "assistant"
                phone_number = to_num
            else:
                role         = "user"
                phone_number = from_num

            if not phone_number:
                continue

            rows.append({
                "phone_number": phone_number,
                "role":         role,
                "text":         body,
                "created_at":   dt,
            })

    return rows


def insert_rows(conn, rows: list[dict]) -> int:
    """Bulk-insert all rows in one statement, then count what landed."""
    import psycopg2.extras as _extras
    tuples = [(r["phone_number"], r["role"], r["text"], r["created_at"]) for r in rows]
    with conn.cursor() as cur:
        # Temp table to stage rows, then insert-select to skip dupes
        cur.execute(
            "CREATE TEMP TABLE _csv_stage "
            "(phone_number TEXT, role TEXT, text TEXT, created_at TIMESTAMPTZ) ON COMMIT DROP"
        )
        _extras.execute_values(
            cur,
            "INSERT INTO _csv_stage (phone_number, role, text, created_at) VALUES %s",
            tuples,
            page_size=500,
        )
        cur.execute(
            """
            INSERT INTO messages (phone_number, role, text, created_at, source)
            SELECT phone_number, role, text, created_at, 'csv_import'
            FROM _csv_stage
            ON CONFLICT DO NOTHING
            """
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def score_imported_rows(conn) -> int:
    """
    For each csv_import assistant message, compute:
      - did_user_reply      (bool)
      - reply_delay_seconds (int, seconds until next user msg from same fan)
      - went_silent_after   (bool, no user reply within REPLY_WINDOW)
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE messages AS m
            SET
              did_user_reply = EXISTS (
                SELECT 1 FROM messages m2
                WHERE m2.phone_number = m.phone_number
                  AND m2.role = 'user'
                  AND m2.source = 'csv_import'
                  AND m2.created_at > m.created_at
                  AND m2.created_at <= m.created_at + INTERVAL '{int(REPLY_WINDOW.total_seconds())} seconds'
              ),
              reply_delay_seconds = (
                SELECT EXTRACT(EPOCH FROM (m2.created_at - m.created_at))::int
                FROM messages m2
                WHERE m2.phone_number = m.phone_number
                  AND m2.role = 'user'
                  AND m2.source = 'csv_import'
                  AND m2.created_at > m.created_at
                ORDER BY m2.created_at
                LIMIT 1
              ),
              went_silent_after = NOT EXISTS (
                SELECT 1 FROM messages m2
                WHERE m2.phone_number = m.phone_number
                  AND m2.role = 'user'
                  AND m2.source = 'csv_import'
                  AND m2.created_at > m.created_at
                  AND m2.created_at <= m.created_at + INTERVAL '{int(REPLY_WINDOW.total_seconds())} seconds'
              )
            WHERE m.role = 'assistant'
              AND m.source = 'csv_import'
              AND m.did_user_reply IS NULL
            """
        )
        updated = cur.rowcount
    conn.commit()
    return updated


# ── Main ─────────────────────────────────────────────────────────────────────

def main(csv_path: str):
    print("=" * 60)
    print("  SlickText Chat CSV → Postgres Import")
    print("=" * 60)

    print(f"\n📂  Reading {csv_path} …")
    rows = load_csv(csv_path)

    incoming = sum(1 for r in rows if r["role"] == "user")
    outgoing = sum(1 for r in rows if r["role"] == "assistant")
    fans     = len({r["phone_number"] for r in rows if r["role"] == "user"})
    print(f"  Pre-bot rows found : {len(rows):,}")
    print(f"  Incoming (fans)    : {incoming:,}  from {fans:,} unique fans")
    print(f"  Outgoing (Zarna)   : {outgoing:,}")

    conn = psycopg2.connect(DSN, connect_timeout=15)
    try:
        print("\n🔧  Ensuring source column exists …")
        ensure_source_column(conn)

        print("💾  Inserting rows …")
        inserted = insert_rows(conn, rows)
        print(f"  Inserted : {inserted:,}  (skipped {len(rows) - inserted:,} exact dupes)")

        print("📊  Scoring reply metrics for imported assistant messages …")
        scored = score_imported_rows(conn)
        print(f"  Scored   : {scored:,} outgoing messages")
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("✅  Import complete!")
    print("   Pre-bot toggle on Insights will now show real reply rates.")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/import_chat_transcripts.py <path-to-csv>")
        sys.exit(1)
    main(sys.argv[1])
