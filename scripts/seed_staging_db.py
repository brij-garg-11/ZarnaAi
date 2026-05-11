"""
Seed the staging database with rich test data so anyone can poke at the
staging dashboard immediately and have populated UI to look at.

NOTE: as of 2026-05-10 the operator and main app share ONE Postgres in
staging (matches prod architecture). STAGING_OPERATOR_DB_URL is now
optional — if unset, the script reuses STAGING_MAIN_DB_URL for both.

What gets seeded
================

Three creator accounts, each with its own owner login + 20 fake fans + a
handful of seed conversations so the inbox isn't empty:

  brij-test       (performer)  brijgarg286@gmail.com
  alice-test      (performer)  alice-test@staging.zar.bot
  westside-test   (business)   westside-test@staging.zar.bot

Each creator is seeded as 'trial' tier with 50 credits so anyone can:
  - Send blasts and watch credits decrement
  - Hit the credit gate (set to 50 instead of the original 10 so a single
    blast to all 20 fans doesn't immediately wipe the account)
  - Buy a Stripe test booster pack to top up

Twilio number caveats
=====================

Inbound SMS routing is single-tenant (CREATOR_SLUG env var on the main app
service). Today only brij-test has a real Twilio number wired
(+15732290656). alice-test gets its own dedicated main-app + number once
the Phase E provisioning runs. westside-test currently rides the brij-test
number — text it the keyword "WESTSIDETEST" to switch personas if/when we
build keyword routing.

Fake fan numbers
================

Each fan uses one of:
  - Twilio MAGIC numbers (+1500 555 0006 / 0008 / 0010 / 0003) — useful for
    testing credit deduction without delivery cost. Sending to these from
    a real subaccount FAILS at the Twilio API (they only work with test
    creds), but the failure happens AFTER the credit math runs locally.
  - Documentation-reserved fake numbers in +1 555 0100–0199 — these look
    like real phone numbers in the UI but Twilio rejects pre-billing.

NEITHER will deliver to a real phone. Use the
POST /api/admin/staging/add-test-fan endpoint to add YOUR phone if you
want real SMS delivery.

Usage
=====

  STAGING_MAIN_DB_URL='<DATABASE_PUBLIC_URL from main-app Postgres>' \
  STAGING_OWNER_PASSWORD='<bootstrap password from 1Password>' \
  python scripts/seed_staging_db.py

  # Or, if you've kept the legacy split-DB layout:
  STAGING_MAIN_DB_URL='...' STAGING_OPERATOR_DB_URL='...' \
  STAGING_OWNER_PASSWORD='...' python scripts/seed_staging_db.py

Re-run is safe — every insert uses ON CONFLICT DO NOTHING / DO UPDATE.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List

import psycopg2

# Match the app's password hashing (werkzeug PBKDF2). DON'T use bcrypt here —
# the operator's auth uses werkzeug.security.check_password_hash which doesn't
# understand bcrypt-format hashes, so seeded users won't be able to log in.
from werkzeug.security import generate_password_hash

# ---------------------------------------------------------------------------
# Test creators
# ---------------------------------------------------------------------------

OPERATOR_OWNER_PASSWORD = os.environ.get("STAGING_OWNER_PASSWORD")
TRIAL_CREDITS = 50  # raised from 10 so one blast to all 20 fans doesn't wipe it

CREATORS: List[Dict[str, Any]] = [
    {
        "slug": "brij-test",
        "email": "brijgarg286@gmail.com",
        "name": "Brij (staging owner)",
        "account_type": "performer",
        "display_name": "Brij Test Bot",
        "is_owner": True,
    },
    {
        "slug": "alice-test",
        "email": "alice-test@staging.zar.bot",
        "name": "Alice Test (staging performer)",
        "account_type": "performer",
        "display_name": "Alice Test (Performer)",
        "is_owner": False,
    },
    {
        "slug": "westside-test",
        "email": "westside-test@staging.zar.bot",
        "name": "Westside Test Diner (staging business)",
        "account_type": "business",
        "display_name": "Westside Test Diner",
        "is_owner": False,
    },
]

# ---------------------------------------------------------------------------
# Fake fan generator — same set per creator, prefixed by creator initials so
# numbers don't collide across creators (contacts.phone_number is a global PK)
# ---------------------------------------------------------------------------

# Twilio magic numbers (work with test creds only — fail on real subaccount,
# but the credit math runs first so they're useful for credit testing)
MAGIC_NUMBERS = [
    "+15005550006",  # accepts SMS
    "+15005550008",  # invalid
    "+15005550010",  # unavailable
    "+15005550003",  # international forbidden
]

FAKE_NAMES = [
    "Test Fan Alice", "Test Fan Bob", "Test Fan Carol", "Test Fan Dave",
    "Test Fan Eve", "Test Fan Frank", "Test Fan Grace", "Test Fan Henry",
    "Test Fan Iris", "Test Fan Jack", "Test Fan Kim", "Test Fan Liam",
    "Test Fan Mia", "Test Fan Noah", "Test Fan Olivia", "Test Fan Pete",
    "Test Fan Quinn", "Test Fan Riley", "Test Fan Sam", "Test Fan Tess",
]
FAKE_LOCATIONS = [
    "San Francisco, CA", "Austin, TX", "New York, NY", "Seattle, WA",
    "Chicago, IL", "Denver, CO", "Miami, FL", "Portland, OR",
    "Boston, MA", "Atlanta, GA",
]

# Deterministic per-creator fan offset so phone numbers don't collide.
# brij-test     fans use +1 555 0100 0XXX
# alice-test    fans use +1 555 0101 0XXX
# westside-test fans use +1 555 0102 0XXX
FAN_PREFIX_BY_SLUG = {
    "brij-test": "+15550100",
    "alice-test": "+15550101",
    "westside-test": "+15550102",
}


def fans_for(slug: str) -> List[Dict[str, Any]]:
    """20 fake fans for a creator: 4 magic numbers + 16 reserved-fake numbers."""
    prefix = FAN_PREFIX_BY_SLUG[slug]
    fans: List[Dict[str, Any]] = []

    # 4 magic numbers — one per creator slug, qualified so they don't collide
    # across slugs. We use the magic prefix for the leading digits so credit
    # math treats them like Twilio test numbers, but suffix-rotate by slug so
    # the contacts.phone_number PK is unique per row.
    for i, mn in enumerate(MAGIC_NUMBERS):
        # mutate last digit by slug position so PK is unique across creators
        slug_offset = list(FAN_PREFIX_BY_SLUG).index(slug)
        suffix = int(mn[-1]) + (10 * slug_offset)
        # Keep the +15005550XYZ prefix shape; just bump last 3 digits
        unique = mn[:-3] + f"{(int(mn[-3:]) + slug_offset):03d}"
        fans.append({
            "phone": unique,
            "fan_name": f"{FAKE_NAMES[i]} (Twilio magic — credit-logic test only)",
            "fan_location": FAKE_LOCATIONS[i % len(FAKE_LOCATIONS)],
            "fan_tags": ["test", "magic"],
            "fan_memory": f"Magic Twilio test number for {slug}. Will not deliver real SMS.",
        })
    # 16 reserved-fake numbers — look like real phones but Twilio won't deliver
    for i in range(16):
        idx = i + 4  # so name doesn't collide with magic-number names
        fans.append({
            "phone": f"{prefix}{i:04d}",
            "fan_name": f"{FAKE_NAMES[idx]} (fake — staging seed)",
            "fan_location": FAKE_LOCATIONS[idx % len(FAKE_LOCATIONS)],
            "fan_tags": ["test", "seed"],
            "fan_memory": f"Seeded fake fan for {slug}. No real phone behind this number.",
        })
    return fans


SEED_CONVERSATIONS = [
    # (offset_index, role, text)  applied to the FIRST fan of each creator
    (0, "user", "hey is this a real bot?"),
    (0, "assistant", "Hey! Yep — I'm a staging test bot. Anything you say here is just for engineering tests."),
    (0, "user", "cool, what can you do?"),
    (0, "assistant", "Send blasts, reply to texts, track credits. Mostly I exist so the team can test things without bothering real fans."),
    # second fan
    (1, "user", "yo"),
    (1, "assistant", "Hey! Reply STOP if you want me to stop messaging you."),
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def connect(url: str):
    return psycopg2.connect(url)


def upsert_operator_user(cur, creator: Dict[str, Any], password_hash: str) -> int:
    cur.execute(
        """
        INSERT INTO operator_users (
            email, name, password_hash, is_active, is_owner,
            creator_slug, account_type, plan_tier, trial_credits_remaining,
            trial_started_at
        )
        VALUES (%s, %s, %s, TRUE, %s, %s, %s, 'trial', %s, NOW())
        ON CONFLICT (email) DO UPDATE SET
            password_hash           = EXCLUDED.password_hash,
            creator_slug            = EXCLUDED.creator_slug,
            account_type            = EXCLUDED.account_type,
            plan_tier               = EXCLUDED.plan_tier,
            trial_credits_remaining = EXCLUDED.trial_credits_remaining,
            is_active               = TRUE,
            is_owner                = EXCLUDED.is_owner
        RETURNING id
        """,
        (
            creator["email"],
            creator["name"],
            password_hash,
            creator["is_owner"],
            creator["slug"],
            creator["account_type"],
            TRIAL_CREDITS,
        ),
    )
    return cur.fetchone()[0]


def upsert_bot_config(cur, user_id: int, creator: Dict[str, Any]) -> None:
    config_json = {
        "display_name": creator["display_name"],
        "bio": f"Staging test {creator['account_type']} — auto-seeded.",
        "tone": "casual",
        "website_url": "",
        "podcast_url": "",
        "media_urls": [],
        "extra_context": "Seeded by scripts/seed_staging_db.py — safe to wipe and re-seed.",
    }
    cur.execute(
        """
        INSERT INTO bot_configs
            (operator_user_id, creator_slug, account_type, config_json, status)
        VALUES (%s, %s, %s, %s::jsonb, 'submitted')
        ON CONFLICT (creator_slug) DO UPDATE SET
            config_json   = EXCLUDED.config_json,
            account_type  = EXCLUDED.account_type,
            status        = 'submitted',
            updated_at    = NOW()
        """,
        (user_id, creator["slug"], creator["account_type"], json.dumps(config_json)),
    )


def upsert_team_member(cur, user_id: int, slug: str) -> None:
    cur.execute(
        """
        INSERT INTO team_members (tenant_slug, user_id, role, invited_at, accepted_at)
        VALUES (%s, %s, 'owner', NOW(), NOW())
        ON CONFLICT (tenant_slug, user_id) DO UPDATE SET role='owner'
        """,
        (slug, user_id),
    )


def upsert_smb_bot_config(cur, slug: str, display_name: str) -> None:
    seed = json.dumps({
        "display_name": display_name,
        "tone": "casual",
        "website": "",
        "welcome_message": "",
        "signup_question": "",
        "outreach_invite_message": "",
    })
    cur.execute(
        """
        INSERT INTO smb_bot_config (tenant_slug, config_json, updated_at)
        VALUES (%s, %s::jsonb, NOW())
        ON CONFLICT (tenant_slug) DO NOTHING
        """,
        (slug, seed),
    )


def upsert_credit_usage(cur, user_id: int, slug: str) -> None:
    cur.execute(
        """
        INSERT INTO operator_credit_usage
            (operator_user_id, creator_slug, period_start,
             credits_used, credits_included)
        VALUES (%s, %s, CURRENT_DATE, 0, %s)
        ON CONFLICT (operator_user_id, period_start) DO NOTHING
        """,
        (user_id, slug, TRIAL_CREDITS),
    )


def upsert_fan(cur, fan: Dict[str, Any], slug: str) -> None:
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
            fan["phone"], slug, fan["fan_name"], fan["fan_location"],
            fan["fan_tags"], fan["fan_memory"],
        ),
    )


def seed_conversation(cur, fan_phone: str, role: str, text: str, slug: str) -> None:
    """Idempotent: one row per (phone, role, text, slug) so re-runs don't duplicate."""
    cur.execute(
        """
        INSERT INTO messages (phone_number, role, text, creator_slug, msg_source)
        SELECT %s, %s, %s, %s, 'staging-seed'
        WHERE NOT EXISTS (
            SELECT 1 FROM messages
            WHERE phone_number=%s AND role=%s AND text=%s AND creator_slug=%s
        )
        """,
        (fan_phone, role, text, slug, fan_phone, role, text, slug),
    )


# ---------------------------------------------------------------------------
# Top-level seed flow
# ---------------------------------------------------------------------------

def seed_operator_db(conn) -> Dict[str, int]:
    """Seed all operator-side rows. Returns {slug: user_id} mapping."""
    print("\n=== Operator DB seed ===")
    password_hash = generate_password_hash(OPERATOR_OWNER_PASSWORD)

    user_ids: Dict[str, int] = {}
    with conn, conn.cursor() as cur:
        for c in CREATORS:
            uid = upsert_operator_user(cur, c, password_hash)
            user_ids[c["slug"]] = uid
            upsert_bot_config(cur, uid, c)
            upsert_team_member(cur, uid, c["slug"])
            if c["account_type"] == "business":
                upsert_smb_bot_config(cur, c["slug"], c["display_name"])
            upsert_credit_usage(cur, uid, c["slug"])
            print(
                f"  Operator user upserted: {c['email']:<40s} "
                f"slug={c['slug']:<14s} type={c['account_type']:<9s} credits={TRIAL_CREDITS}"
            )
    return user_ids


def seed_main_db(conn) -> None:
    """Seed all main-app fan rows + a few seed conversations."""
    print("\n=== Main app DB seed ===")
    with conn, conn.cursor() as cur:
        for c in CREATORS:
            fans = fans_for(c["slug"])
            for fan in fans:
                upsert_fan(cur, fan, c["slug"])
            print(f"  Fans seeded for slug={c['slug']:<14s}: {len(fans)}")

            # Seed sample inbox conversations on the FIRST 2 fans of each creator
            for offset, role, text in SEED_CONVERSATIONS:
                if offset < len(fans):
                    seed_conversation(cur, fans[offset]["phone"], role, text, c["slug"])
            print(f"  Sample conversations seeded for first 2 fans of {c['slug']}")

        cur.execute(
            "SELECT creator_slug, COUNT(*) FROM contacts GROUP BY creator_slug ORDER BY creator_slug"
        )
        for slug, count in cur.fetchall():
            print(f"  Total fans for '{slug}': {count}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    main_url = os.environ.get("STAGING_MAIN_DB_URL")
    # Default to the main URL — operator and main app share one Postgres
    # in the current staging layout. Override via STAGING_OPERATOR_DB_URL
    # only if you've kept the deprecated split-DB layout.
    op_url = os.environ.get("STAGING_OPERATOR_DB_URL") or main_url
    if not main_url:
        print(
            "ERROR: STAGING_MAIN_DB_URL must be set.\n"
            "Pull from Railway (Postgres service → Variables → DATABASE_PUBLIC_URL).",
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

    same_db = (op_url == main_url)
    print(f"Connecting to staging database{'s' if not same_db else ''}...")
    if same_db:
        print("  (single shared Postgres — matches prod architecture)")
    op_conn = connect(op_url)
    main_conn = op_conn if same_db else connect(main_url)
    try:
        seed_operator_db(op_conn)
        seed_main_db(main_conn)
    finally:
        op_conn.close()
        if not same_db:
            main_conn.close()

    print("\n" + "=" * 70)
    print("Done. Login at https://zar-chat-magic.lovable.app/login with:")
    print("=" * 70)
    for c in CREATORS:
        print(f"  {c['email']:<40s} | slug={c['slug']:<14s} | type={c['account_type']}")
    print(f"  password: <STAGING_OWNER_PASSWORD env var> (same for all 3)")
    print(f"  credits:  {TRIAL_CREDITS} on the 'trial' plan")
    return 0


if __name__ == "__main__":
    sys.exit(main())
