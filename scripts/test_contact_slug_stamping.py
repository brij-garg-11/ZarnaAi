"""
Regression test for Bug #4 — contacts.creator_slug stamping.

Before the fix, every PostgresStorage().save_contact() call would stamp
creator_slug = process-global env var (default 'zarna'), regardless of
which creator's brain made the call. That corrupts contacts.creator_slug
silently, which then poisons get_top_performing_replies() because its
JOIN relies on that column to route organic examples per creator.

What this test does:
  1. Spin up PostgresStorage once with creator_slug='marcus_cole'
  2. save_contact() on a fresh fake phone → assert stamped 'marcus_cole'
  3. Spin up another PostgresStorage with creator_slug='zarna' in same process
  4. save_contact() on a different fresh phone → assert stamped 'zarna'
  5. create_brain(slug='marcus_cole') + incoming message → verify the
     fan row is stamped 'marcus_cole' end-to-end
  6. create_brain(slug='zarna') + incoming message → verify stamped 'zarna'
  7. Clean up all test rows
"""
from __future__ import annotations
import os, sys, time
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("MULTI_MODEL_REPLY", "off")

import psycopg2
from app.storage.postgres import PostgresStorage

DSN = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)

TEST_PHONES = [
    "+15550099001",  # marcus direct storage
    "+15550099002",  # zarna direct storage
    "+15550099003",  # marcus via full brain path
    "+15550099004",  # zarna via full brain path
]

def cleanup():
    """Wipe test rows from contacts + messages so re-runs are idempotent."""
    conn = psycopg2.connect(DSN); cur = conn.cursor()
    for phone in TEST_PHONES:
        for tbl in ("messages", "contacts"):
            try:
                cur.execute(f"DELETE FROM {tbl} WHERE phone_number=%s", (phone,))
                conn.commit()
            except Exception:
                conn.rollback()
    cur.close(); conn.close()

def slug_for_phone(phone: str):
    conn = psycopg2.connect(DSN); cur = conn.cursor()
    cur.execute("SELECT creator_slug FROM contacts WHERE phone_number=%s", (phone,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row else None

passed = 0
failed = 0
def check(name, cond, detail=""):
    global passed, failed
    if cond: passed += 1; print(f"  [PASS] {name}" + (f" — {detail}" if detail else ""))
    else:    failed += 1; print(f"  [FAIL] {name}" + (f" — {detail}" if detail else ""))

cleanup()

print("═" * 70)
print("Section 1: Direct PostgresStorage instances with different slugs")
print("═" * 70)
s_marcus = PostgresStorage(dsn=DSN, creator_slug="marcus_cole")
s_zarna  = PostgresStorage(dsn=DSN, creator_slug="zarna")

check("marcus storage._creator_slug == 'marcus_cole'",
      s_marcus._creator_slug == "marcus_cole", s_marcus._creator_slug)
check("zarna storage._creator_slug  == 'zarna'",
      s_zarna._creator_slug == "zarna", s_zarna._creator_slug)
check("process-global _CREATOR_SLUG is unused per-instance",
      s_marcus._creator_slug != s_zarna._creator_slug)

s_marcus.save_contact(TEST_PHONES[0])
s_zarna.save_contact(TEST_PHONES[1])

check("marcus-stored contact tagged 'marcus_cole'",
      slug_for_phone(TEST_PHONES[0]) == "marcus_cole",
      f"got {slug_for_phone(TEST_PHONES[0])!r}")
check("zarna-stored contact tagged 'zarna'",
      slug_for_phone(TEST_PHONES[1]) == "zarna",
      f"got {slug_for_phone(TEST_PHONES[1])!r}")

print("\n" + "═" * 70)
print("Section 2: Full brain path — create_brain + handle_incoming_message")
print("═" * 70)
from app.brain.handler import create_brain

b_marcus = create_brain(slug="marcus_cole")
b_zarna  = create_brain(slug="zarna")

check("marcus brain storage tagged correctly",
      getattr(b_marcus.storage, "_creator_slug", None) == "marcus_cole")
check("zarna  brain storage tagged correctly",
      getattr(b_zarna.storage,  "_creator_slug", None) == "zarna")

# Fire an actual inbound message through each brain — this exercises the
# same save_contact call production SMS traffic triggers.
try:
    b_marcus.handle_incoming_message(TEST_PHONES[2], "hey")
except Exception as e:
    print(f"  (marcus handle_incoming_message raised: {e!r} — continuing check)")

try:
    b_zarna.handle_incoming_message(TEST_PHONES[3], "hey")
except Exception as e:
    print(f"  (zarna handle_incoming_message raised: {e!r} — continuing check)")

check("marcus-brain fan ended up tagged 'marcus_cole'",
      slug_for_phone(TEST_PHONES[2]) == "marcus_cole",
      f"got {slug_for_phone(TEST_PHONES[2])!r}")
check("zarna-brain  fan ended up tagged 'zarna'",
      slug_for_phone(TEST_PHONES[3]) == "zarna",
      f"got {slug_for_phone(TEST_PHONES[3])!r}")

print("\n" + "═" * 70)
print("Section 3: ON CONFLICT DO NOTHING — first-write-wins guarantee")
print("═" * 70)
# If a phone is already in contacts, re-saving through the OTHER brain
# must NOT change its slug (so one bot can't overwrite the other's tag).
b_zarna.storage.save_contact(TEST_PHONES[2])  # attempt to poach marcus's fan
check("marcus's fan stays tagged 'marcus_cole' even after zarna.save_contact",
      slug_for_phone(TEST_PHONES[2]) == "marcus_cole",
      f"got {slug_for_phone(TEST_PHONES[2])!r}")

print("\n" + "═" * 70)
print(f"  RESULTS: {passed} passed, {failed} failed")
print("═" * 70)
cleanup()
print("  (test rows cleaned up)")
sys.exit(0 if failed == 0 else 1)
