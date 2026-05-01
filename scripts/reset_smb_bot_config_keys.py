"""
Clear stale per-key overrides from smb_bot_config so the values fall back to
the canonical defaults in operator/app/business_configs/<slug>.json.

Why this exists:
  GET /api/bot-data merges file defaults with DB overrides from
  smb_bot_config.config_json (DB wins). When someone (or an early bug)
  writes a wrong value via MyBot, the DB override sticks across deploys.
  Deleting just those keys from the JSONB blob lets the file values take
  effect again on the next read — no need to wipe the whole row.

Usage (from repo root):
  # Dry run (default) — shows what would change for the canonical WSCC reset
  python scripts/reset_smb_bot_config_keys.py

  # Actually apply the WSCC reset
  python scripts/reset_smb_bot_config_keys.py --apply

  # Custom tenant + keys
  python scripts/reset_smb_bot_config_keys.py \
      --slug west_side_comedy \
      --keys welcome_message,signup_question \
      --apply

Requires:
  DATABASE_URL pointing at the operator's Postgres.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Iterable

import psycopg2
import psycopg2.extras


# ── Defaults ─────────────────────────────────────────────────────────────────
# Out-of-the-box this script targets the WSCC tenant and the two keys we
# verified were stale (welcome_message + signup_question). Override with
# CLI flags for any other tenant.
DEFAULT_SLUG = "west_side_comedy"
DEFAULT_KEYS = ("welcome_message", "signup_question")


def fetch_overrides(cur, slug: str) -> dict | None:
    cur.execute(
        "SELECT config_json FROM smb_bot_config WHERE tenant_slug = %s",
        (slug,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(row["config_json"] or {})


def reset_keys(slug: str, keys: Iterable[str], apply: bool) -> int:
    """Returns the number of keys actually removed (0 if dry-run or no-op)."""
    keys = [k.strip() for k in keys if k and k.strip()]
    if not keys:
        print("No keys provided — nothing to do.")
        return 0

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.exit("DATABASE_URL is required")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            before = fetch_overrides(cur, slug)
            if before is None:
                print(f"No smb_bot_config row for tenant_slug={slug!r}. Nothing to do.")
                return 0

            present = [k for k in keys if k in before]
            absent = [k for k in keys if k not in before]

            print(f"Tenant: {slug}")
            print(f"Existing override keys: {sorted(before.keys())}")
            if absent:
                print(f"Skipping (not present): {absent}")
            if not present:
                print("None of the requested keys are present in DB overrides — file defaults already win.")
                return 0

            print("\nWill clear these keys (DB → file fallback):")
            for k in present:
                old_val = before.get(k)
                preview = (old_val[:80] + "…") if isinstance(old_val, str) and len(old_val) > 80 else old_val
                print(f"  - {k}: {preview!r}")

            if not apply:
                print("\n[dry run] Re-run with --apply to commit.")
                return 0

            # JSONB minus operator strips top-level keys. We chain it once
            # per key so a single statement clears all of them atomically.
            #   config_json - 'k1' - 'k2' - 'k3'
            minus_chain = " ".join(f"- %s" for _ in present)
            sql = (
                "UPDATE smb_bot_config "
                f"SET config_json = config_json {minus_chain}, updated_at = NOW() "
                "WHERE tenant_slug = %s"
            )
            cur.execute(sql, (*present, slug))

            after = fetch_overrides(cur, slug) or {}
            print("\nNew override keys:", sorted(after.keys()))
            print(f"Cleared {len(present)} key(s).")
            return len(present)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--slug", default=DEFAULT_SLUG, help=f"tenant_slug (default: {DEFAULT_SLUG})")
    parser.add_argument(
        "--keys",
        default=",".join(DEFAULT_KEYS),
        help=f"comma-separated keys to clear (default: {','.join(DEFAULT_KEYS)})",
    )
    parser.add_argument("--apply", action="store_true", help="commit changes (default is dry-run)")
    args = parser.parse_args()

    keys = args.keys.split(",")
    reset_keys(args.slug, keys, apply=args.apply)


if __name__ == "__main__":
    main()
