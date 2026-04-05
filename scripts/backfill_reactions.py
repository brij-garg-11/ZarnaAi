#!/usr/bin/env python3
"""
One-time backfill: recover dropped reactions from Railway deploy logs.

Railway logs every ignored reaction/emoji as:
  INFO:app.messaging.slicktext_adapter:Ignoring reaction from +1XXX: Loved "..."
  INFO:app.messaging.slicktext_adapter:Ignoring emoji-only message from +1XXX: ❤️

This script parses those lines from a log file (or stdin), inserts them into
the messages table with source='reaction', and scores the previous bot reply
so did_user_reply gets set correctly.

Usage:
  # Pipe a downloaded log file:
  python scripts/backfill_reactions.py reactions_log.txt

  # Or pipe from stdin:
  cat reactions_log.txt | python scripts/backfill_reactions.py

Safe to re-run — uses ON CONFLICT DO NOTHING on (phone_number, created_at, source).
"""

import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import List, Tuple

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger(__name__)

# Matches Railway log lines for ignored reactions and emoji-only messages.
# Railway log lines look like:
#   2026-04-04T21:30:15.123Z INFO:app.messaging.slicktext_adapter:Ignoring reaction from +14075551234: Loved "..."
#   2026-04-04T21:30:15.123Z INFO:app.messaging.slicktext_adapter:Ignoring emoji-only message from +14075551234: ❤️
_REACTION_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)?\s*"
    r"INFO:app\.messaging\.slicktext_adapter:"
    r"Ignoring (?:reaction|emoji-only message) from (?P<phone>\+\d+): (?P<body>.+)"
)


def parse_log(text: str) -> List[Tuple[str, str, datetime]]:
    """
    Returns list of (phone, body, timestamp) tuples parsed from log text.
    Deduplicates by (phone, body) — keeps earliest occurrence.
    """
    seen = set()
    results = []
    for line in text.splitlines():
        m = _REACTION_RE.search(line)
        if not m:
            continue
        phone = m.group("phone").strip()
        body  = m.group("body").strip()[:500]
        ts_str = m.group("ts")
        if ts_str:
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except ValueError:
                    ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        key = (phone, body)
        if key not in seen:
            seen.add(key)
            results.append((phone, body, ts))

    return results


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        _logger.error("DATABASE_URL not set.")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def backfill(records: List[Tuple[str, str, datetime]]) -> Tuple[int, int]:
    """
    Insert reaction rows and score previous bot replies.
    Returns (inserted, skipped) counts.
    """
    if not records:
        _logger.info("No reaction records found in log — nothing to insert.")
        return 0, 0

    conn = _get_conn()
    inserted = 0
    skipped  = 0

    try:
        with conn.cursor() as cur:
            for phone, body, ts in records:
                # Insert reaction message — skip if exact duplicate already exists
                cur.execute(
                    """
                    INSERT INTO messages (phone_number, role, text, source, created_at)
                    VALUES (%s, 'user', %s, 'reaction', %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (phone, body, ts),
                )
                if cur.rowcount > 0:
                    inserted += 1
                    # Score the most recent unscored bot reply for this fan
                    cur.execute(
                        """
                        UPDATE messages
                        SET did_user_reply      = TRUE,
                            reply_delay_seconds = GREATEST(
                                0,
                                EXTRACT(EPOCH FROM (%s - created_at))::INT
                            )
                        WHERE id = (
                            SELECT id FROM messages
                            WHERE phone_number   = %s
                              AND role           = 'assistant'
                              AND did_user_reply IS NULL
                            ORDER BY created_at DESC
                            LIMIT 1
                        )
                        """,
                        (ts, phone),
                    )
                else:
                    skipped += 1

        conn.commit()
    finally:
        conn.close()

    return inserted, skipped


def main():
    if len(sys.argv) > 1:
        path = sys.argv[1]
        _logger.info("Reading log from file: %s", path)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
    else:
        _logger.info("Reading log from stdin...")
        text = sys.stdin.read()

    records = parse_log(text)
    _logger.info("Parsed %d unique reaction events from log", len(records))

    for phone, body, ts in records:
        _logger.info("  %s  ...%s  %s", ts.strftime("%H:%M:%S"), phone[-4:], body[:60])

    if not records:
        _logger.info("Nothing to do.")
        return

    confirm = input(f"\nInsert {len(records)} reaction(s) into DB? [y/N] ").strip().lower()
    if confirm != "y":
        _logger.info("Aborted.")
        return

    inserted, skipped = backfill(records)
    _logger.info("Done — inserted: %d  already existed: %d", inserted, skipped)


if __name__ == "__main__":
    main()
