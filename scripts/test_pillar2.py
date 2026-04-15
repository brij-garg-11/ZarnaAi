"""
One-shot test for Pillar 2: link_clicked_1h tracking.

Run from the repo root:
    python scripts/test_pillar2.py
"""
import base64
import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

PHONE = "+16466406086"
RAILWAY_URL = "https://zarnaai-production.up.railway.app"

db_url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
if not db_url:
    sys.exit("DATABASE_URL not set")

print("Connecting to DB...")
conn = psycopg2.connect(db_url, connect_timeout=10)
print("Connected\n")

# ── Step 1: insert a fresh test assistant message ────────────────────────────
with conn:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO messages (phone_number, role, text, has_link, msg_source) "
            "VALUES (%s, 'assistant', '[TEST] Pillar 2 test blast', TRUE, 'blast') "
            "RETURNING id",
            (PHONE,),
        )
        msg_id = cur.fetchone()[0]
print(f"✓ Inserted test message  id={msg_id}")

# ── Step 2: grab any tracked link slug ──────────────────────────────────────
with conn.cursor() as cur:
    cur.execute("SELECT slug FROM tracked_links LIMIT 1")
    row = cur.fetchone()
    if not row:
        conn.close()
        sys.exit("✗ No rows in tracked_links — create one in the admin Conversions tab first")
    slug = row[0]
print(f"✓ Using slug:            {slug}")

# ── Step 3: simulate a fan click with personalized URL ──────────────────────
token = base64.urlsafe_b64encode(PHONE.encode()).decode().rstrip("=")
url = f"{RAILWAY_URL}/t/{slug}?f={token}"
print(f"✓ Clicking URL:          {url}")

ret = os.system(f'curl -s -o /dev/null -w "  curl: %{{http_code}} -> %{{redirect_url}}\\n" --max-time 15 "{url}"')

time.sleep(2)  # give Railway a moment to write to DB

# ── Step 4: verify link_clicked_1h was flipped ──────────────────────────────
with conn.cursor() as cur:
    cur.execute(
        "SELECT id, link_clicked_1h, msg_source FROM messages WHERE id = %s",
        (msg_id,),
    )
    row = cur.fetchone()

conn.close()
print()
if row[1]:
    print(f"✅  PASS — message {row[0]}: link_clicked_1h=True  (msg_source={row[2]})")
else:
    print(f"❌  FAIL — link_clicked_1h is still {row[1]} on message {row[0]}")
    print("    Check Railway logs for: track_redirect: set link_clicked_1h")
