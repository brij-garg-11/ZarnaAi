"""
Real-world API test: boot the operator Flask app via its test client, log in
as a fake performer creator, and hit GET /api/provisioning/status.

We verify:
  - 401 for anonymous requests
  - 200 with expected JSON for authenticated requests
  - status field flips as we change bot_configs.provisioning_status in the DB
  - error_message only surfaces when status='failed'

This runs inside operator/ subprocess to dodge the `operator` builtin
module shadow.
"""
from __future__ import annotations
import os, sys, subprocess, json
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPERATOR = os.path.join(ROOT, "operator")

TEST_SLUG = "api_realworld_test"
TEST_EMAIL = "api_realworld_test@example.com"

script = r"""
import sys, os, json, psycopg2
sys.path.insert(0, '__OPERATOR__')

from dotenv import load_dotenv; load_dotenv('__ROOT__/.env')

# Seed test user + bot_config
dsn = os.getenv('DATABASE_URL').replace('postgres://','postgresql://',1)
conn = psycopg2.connect(dsn); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("DELETE FROM bot_configs WHERE creator_slug=%s",("__SLUG__",))
    cur.execute("DELETE FROM operator_users WHERE email=%s",("__EMAIL__",))
    cur.execute(
        "INSERT INTO operator_users (email, password_hash, is_active, account_type, creator_slug) "
        "VALUES (%s,'pw',TRUE,'performer',%s) RETURNING id",
        ("__EMAIL__","__SLUG__"),
    )
    uid = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO bot_configs (creator_slug, operator_user_id, account_type, status, provisioning_status) "
        "VALUES (%s,%s,'performer','pending','in_progress')",
        ("__SLUG__", uid),
    )

# Build the app
from app import create_app
app = create_app()
app.config['TESTING'] = True
client = app.test_client()

results = []

# 1) anonymous request → 401
r = client.get('/api/provisioning/status')
results.append(('anon_401', r.status_code, None))

# 2) log in, hit again → 200 with status='in_progress'
with client.session_transaction() as sess:
    sess['operator_user_id'] = uid
r = client.get('/api/provisioning/status')
results.append(('logged_in_in_progress', r.status_code, r.get_json()))

# 3) flip DB to 'live' → should report live with no error_message
with conn.cursor() as cur:
    cur.execute("UPDATE bot_configs SET provisioning_status='live' WHERE creator_slug=%s",("__SLUG__",))
r = client.get('/api/provisioning/status')
results.append(('live', r.status_code, r.get_json()))

# 4) flip DB to 'failed' with error → surfaces error_message
with conn.cursor() as cur:
    cur.execute(
        "UPDATE bot_configs SET provisioning_status='failed', error_message='scrape timed out' WHERE creator_slug=%s",
        ("__SLUG__",),
    )
r = client.get('/api/provisioning/status')
results.append(('failed_with_error', r.status_code, r.get_json()))

# 5) flip back to 'live' → error_message should NOT surface
with conn.cursor() as cur:
    cur.execute("UPDATE bot_configs SET provisioning_status='live' WHERE creator_slug=%s",("__SLUG__",))
r = client.get('/api/provisioning/status')
results.append(('live_after_failed', r.status_code, r.get_json()))

# Cleanup
with conn.cursor() as cur:
    cur.execute("DELETE FROM bot_configs WHERE creator_slug=%s",("__SLUG__",))
    cur.execute("DELETE FROM operator_users WHERE email=%s",("__EMAIL__",))
conn.close()

print('--- RESULTS_START ---')
print(json.dumps(results, default=str))
print('--- RESULTS_END ---')
"""
script = (
    script.replace("__OPERATOR__", OPERATOR)
          .replace("__ROOT__", ROOT)
          .replace("__SLUG__", TEST_SLUG)
          .replace("__EMAIL__", TEST_EMAIL)
)

proc = subprocess.run(
    [sys.executable, "-c", script],
    cwd=OPERATOR, capture_output=True, text=True, timeout=90,
)
if proc.returncode != 0:
    print("SUBPROCESS STDERR:")
    print(proc.stderr[-3000:])
    print("\nSUBPROCESS STDOUT:")
    print(proc.stdout[-2000:])
    sys.exit(1)

s = proc.stdout
a = s.find("--- RESULTS_START ---")
b = s.find("--- RESULTS_END ---")
if a < 0 or b < 0:
    print("No results block in stdout:"); print(s[-2000:]); sys.exit(1)
data = json.loads(s[a:b].split("\n",1)[1])

failures = []
def ok(label, cond, detail=""):
    mark = "✓" if cond else "✗"
    print(f"  [{mark}] {label}" + (f"  — {detail}" if detail else ""))
    if not cond: failures.append(label)

print("=== Real-world API test: /api/provisioning/status ===")
step_map = {name: (status, payload) for name, status, payload in data}

# 1) anon
s1, p1 = step_map["anon_401"]
ok("anon returns 401", s1 == 401, f"status={s1}")

# 2) in_progress
s2, p2 = step_map["logged_in_in_progress"]
ok("authenticated returns 200", s2 == 200, f"status={s2}")
ok("status == 'in_progress'", p2 and p2.get("status") == "in_progress", f"payload={p2}")
ok("error_message is None when status != failed",
   p2 and p2.get("error_message") in (None, ""), f"err={p2.get('error_message') if p2 else None}")

# 3) live
s3, p3 = step_map["live"]
ok("live returns 200", s3 == 200)
ok("status == 'live'", p3 and p3.get("status") == "live")

# 4) failed with error
s4, p4 = step_map["failed_with_error"]
ok("failed returns 200", s4 == 200)
ok("status == 'failed'", p4 and p4.get("status") == "failed")
ok("error_message surfaces when failed",
   p4 and "timed out" in (p4.get("error_message") or ""), f"err={p4.get('error_message') if p4 else None}")

# 5) live again — error_message should be hidden even though DB still has text
s5, p5 = step_map["live_after_failed"]
ok("live after failed returns 200", s5 == 200)
ok("status reverted to 'live'", p5 and p5.get("status") == "live")
ok("error_message hidden when status is 'live' (no leak)",
   p5 and p5.get("error_message") in (None, ""),
   f"err={p5.get('error_message') if p5 else None}")

print("\n" + "=" * 50)
if failures:
    print(f"FAIL — {len(failures)} check(s)")
    for f in failures: print(f"  - {f}")
    sys.exit(1)
print("PASS — real-world /api/provisioning/status works end-to-end via Flask test client")
