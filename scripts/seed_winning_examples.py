#!/usr/bin/env python3
"""
Pillar 1, Step 3 — Cold-start fix: seed the winning_examples_corpus.

Seeds the corpus from the top N highest-scoring historical assistant messages
so the bot has high-quality examples from day one, even before it has built up
enough organic engagement data.

Usage:
    # Seed from top 200 historical replies (default):
    python scripts/seed_winning_examples.py

    # Seed with a custom snapshot tag and count:
    python scripts/seed_winning_examples.py --tag 2026-04-15 --limit 300

    # Dry-run — show what would be seeded without writing:
    python scripts/seed_winning_examples.py --dry-run

    # Roll back a specific snapshot (deactivates those examples):
    python scripts/seed_winning_examples.py --rollback 2026-04-15

    # List all snapshots:
    python scripts/seed_winning_examples.py --list-snapshots
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_LIMIT = 200
DEFAULT_MIN_CHARS = 40
DEFAULT_MAX_CHARS = 380


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        log.error("DATABASE_URL not set")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def _ensure_corpus_tables(conn):
    """Create corpus tables if they don't exist (idempotent)."""
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS winning_examples_corpus (
                    id            BIGSERIAL PRIMARY KEY,
                    creator_slug  TEXT        NOT NULL DEFAULT 'zarna',
                    intent        TEXT        NOT NULL,
                    tone_mode     TEXT        NOT NULL,
                    text          TEXT        NOT NULL,
                    snapshot_tag  TEXT        NOT NULL DEFAULT 'manual',
                    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
                    source_msg_id BIGINT,
                    created_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wec_creator_intent_tone
                    ON winning_examples_corpus(creator_slug, intent, tone_mode)
                    WHERE is_active = TRUE
            """)
            cur.execute("""
                ALTER TABLE winning_examples_corpus
                ADD COLUMN IF NOT EXISTS creator_slug TEXT NOT NULL DEFAULT 'zarna'
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS winning_examples_snapshots (
                    id             SERIAL PRIMARY KEY,
                    tag            TEXT UNIQUE NOT NULL,
                    creator_slug   TEXT        NOT NULL DEFAULT 'zarna',
                    notes          TEXT        DEFAULT '',
                    example_count  INT         DEFAULT 0,
                    created_at     TIMESTAMPTZ DEFAULT NOW(),
                    rolled_back_at TIMESTAMPTZ
                )
            """)


def list_snapshots(conn, creator_slug: str):
    """Print all corpus snapshots for a given creator."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tag, example_count, notes, created_at, rolled_back_at
            FROM   winning_examples_snapshots
            WHERE  creator_slug = %s
            ORDER  BY created_at DESC
        """, (creator_slug,))
        rows = cur.fetchall()
    if not rows:
        print(f"No snapshots found for creator '{creator_slug}'.")
        return
    print(f"\nSnapshots for creator: {creator_slug}")
    print(f"{'TAG':<24} {'COUNT':>6}  {'STATUS':<12}  CREATED")
    print("-" * 68)
    for tag, count, notes, created_at, rolled_back in rows:
        status = "ROLLED BACK" if rolled_back else "active"
        print(f"{tag:<24} {count or 0:>6}  {status:<12}  {created_at.strftime('%Y-%m-%d %H:%M UTC')}")
        if notes:
            print(f"  notes: {notes}")
    print()


def rollback_snapshot(conn, tag: str, creator_slug: str):
    """Deactivate all corpus examples from a specific snapshot tag for a creator."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE winning_examples_corpus SET is_active = FALSE "
                "WHERE snapshot_tag = %s AND creator_slug = %s",
                (tag, creator_slug),
            )
            deactivated = cur.rowcount
            cur.execute(
                "UPDATE winning_examples_snapshots SET rolled_back_at = NOW() "
                "WHERE tag = %s AND creator_slug = %s",
                (tag, creator_slug),
            )
    log.info("rollback: deactivated %d examples for creator='%s' snapshot='%s'",
             deactivated, creator_slug, tag)
    if deactivated == 0:
        log.warning("No examples found for creator='%s' snapshot='%s' — nothing changed",
                    creator_slug, tag)


def seed(conn, creator_slug: str, tag: str, limit: int, dry_run: bool, notes: str = "") -> int:
    """
    Pull the top `limit` assistant messages ranked by engagement quality and
    insert them into winning_examples_corpus under `tag`.

    Ranking logic mirrors the quality digest — prioritises replies with high
    msgs_after_this (kept the conversation going) and fast reply_delay_seconds
    (fan was eager), filtered to conversational intent/tone combos only.

    Returns the number of examples inserted (0 in dry-run).
    """
    # Skip structured intents (show/book/podcast/clip) — those are route-based,
    # not style-based, so they don't benefit from style examples.
    SKIP_INTENTS = ("show", "book", "podcast", "clip", "merch")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, intent, tone_mode, text
            FROM   messages
            WHERE  role               = 'assistant'
              AND  intent             IS NOT NULL
              AND  tone_mode          IS NOT NULL
              AND  intent             NOT IN ({','.join('%s' for _ in SKIP_INTENTS)})
              AND  did_user_reply     = TRUE
              AND  msgs_after_this    >= 1
              AND  msg_source IS DISTINCT FROM 'blast'
              AND  reply_length_chars BETWEEN %s AND %s
              AND  source IS DISTINCT FROM 'blast'
              AND  text NOT LIKE '%%zarnagarg.com%%'
              AND  text NOT LIKE '%%amazon.com%%'
              AND  text NOT LIKE '%%youtube.com%%'
            ORDER BY COALESCE(msgs_after_this, 1) DESC,
                     reply_delay_seconds ASC NULLS LAST
            LIMIT %s
            """,
            (*SKIP_INTENTS, DEFAULT_MIN_CHARS, DEFAULT_MAX_CHARS, limit),
        )
        candidates = cur.fetchall()

    log.info("seed: found %d candidate messages to seed", len(candidates))

    if dry_run:
        intent_counts: dict = {}
        for _, intent, tone, _ in candidates:
            key = f"{intent}/{tone}"
            intent_counts[key] = intent_counts.get(key, 0) + 1
        log.info("[DRY RUN] creator='%s' would insert %d examples under tag '%s'",
                 creator_slug, len(candidates), tag)
        for combo, cnt in sorted(intent_counts.items(), key=lambda x: -x[1])[:10]:
            log.info("  %-30s %d examples", combo, cnt)
        return 0

    if not candidates:
        log.warning("seed: no qualifying messages found — corpus not seeded")
        return 0

    # Check if this tag already exists for this creator
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM winning_examples_corpus "
            "WHERE snapshot_tag = %s AND creator_slug = %s",
            (tag, creator_slug),
        )
        existing = cur.fetchone()[0]
    if existing:
        log.warning(
            "Snapshot '%s' for creator '%s' already has %d examples. "
            "Use --rollback first to replace it.",
            tag, creator_slug, existing,
        )
        sys.exit(1)

    rows_to_insert = [
        (creator_slug, intent, tone_mode, text, tag, msg_id)
        for msg_id, intent, tone_mode, text in candidates
    ]

    from psycopg2.extras import execute_values
    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO winning_examples_corpus
                    (creator_slug, intent, tone_mode, text, snapshot_tag, source_msg_id)
                VALUES %s
                """,
                rows_to_insert,
                page_size=len(rows_to_insert),
            )
            inserted = cur.rowcount

        cur_snap = conn.cursor()
        cur_snap.execute(
            """
            INSERT INTO winning_examples_snapshots (tag, creator_slug, notes, example_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tag) DO UPDATE
               SET example_count = EXCLUDED.example_count,
                   notes         = EXCLUDED.notes
            """,
            (tag, creator_slug, notes, inserted),
        )
        cur_snap.close()

    log.info("seed: inserted %d examples for creator='%s' snapshot='%s'",
             inserted, creator_slug, tag)
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Seed or roll back the winning examples corpus")
    parser.add_argument("--creator",  default="zarna",
                        help="Creator slug — REQUIRED for non-Zarna creators (default: zarna)")
    parser.add_argument("--tag",      default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Snapshot tag (default: today's date)")
    parser.add_argument("--limit",    type=int, default=DEFAULT_LIMIT,
                        help=f"Max examples to seed (default: {DEFAULT_LIMIT})")
    parser.add_argument("--notes",    default="", help="Optional notes stored with the snapshot")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Show what would be seeded without writing")
    parser.add_argument("--rollback", metavar="TAG",
                        help="Deactivate all corpus examples for this snapshot tag")
    parser.add_argument("--list-snapshots", action="store_true",
                        help="List all snapshots and exit")
    args = parser.parse_args()

    conn = _get_conn()
    _ensure_corpus_tables(conn)
    try:
        if args.list_snapshots:
            list_snapshots(conn, creator_slug=args.creator)
            return

        if args.rollback:
            rollback_snapshot(conn, tag=args.rollback, creator_slug=args.creator)
            return

        inserted = seed(conn, creator_slug=args.creator, tag=args.tag,
                        limit=args.limit, dry_run=args.dry_run, notes=args.notes)
        if not args.dry_run:
            log.info("Done. Snapshot '%s' is now active for creator='%s' (%d examples).",
                     args.tag, args.creator, inserted)
            log.info("To roll back: python scripts/seed_winning_examples.py --creator %s --rollback %s",
                     args.creator, args.tag)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
