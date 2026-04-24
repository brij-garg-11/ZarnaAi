"""
One-shot provisioning of a synthetic test creator: Marcus Cole.

Purpose:
  Give the user a live end-to-end creator to poke at via chat_local.py so they
  can verify the Phase-1-6 pipeline (and today's Zarna-leak fixes) work when
  actually running against a real-looking non-Zarna persona.

What this does:
  1. Inserts/updates an operator_users + bot_configs row for Marcus.
  2. Runs the full `provision_new_creator` pipeline in an operator/ subprocess
     (to dodge Python's `operator` builtin shadowing the local package).
  3. Prints the resulting config + row counts so you can eyeball the output
     before launching chat_local.

This script is idempotent — re-running picks up where it left off.
To wipe and start fresh, pass --reset.
"""
from __future__ import annotations
import os, sys, subprocess, json, argparse, textwrap
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPERATOR = os.path.join(ROOT, "operator")

SLUG = "marcus_cole"
EMAIL = "marcus.cole.test@example.com"
DISPLAY_NAME = "Marcus Cole"
BIO = (
    "Marcus Cole is a 34-year-old Black stand-up comedian from Atlanta, Georgia. "
    "His act focuses on corporate-world absurdity (he used to be a mid-level "
    "project manager at a big bank before going full-time comedy), gym culture "
    "and the comedy of being a gym regular, and the experience of being the "
    "only tech worker at Southern family cookouts. He has no kids, no partner, "
    "and drives a beaten-up 2012 Honda Civic he refuses to replace even though "
    "fans keep offering to buy him a new one. He's close with his mother and "
    "three younger cousins. He jokes about discipline, about hustle culture "
    "being a scam, and about being the 'responsible' one in his friend group."
)
TONE = "observational, warm, corporate-weary stand-up with a confident wink"

def reset(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM creator_embeddings WHERE creator_slug=%s",(SLUG,))
        cur.execute("DELETE FROM creator_configs WHERE creator_slug=%s",(SLUG,))
        cur.execute("DELETE FROM bot_configs WHERE creator_slug=%s",(SLUG,))
        cur.execute("DELETE FROM operator_users WHERE email=%s",(EMAIL,))
    print(f"  reset: cleared all rows for slug={SLUG}")

def seed(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM operator_users WHERE email=%s",(EMAIL,))
        row = cur.fetchone()
        if row:
            uid = row[0]
            print(f"  operator_users: already exists (id={uid})")
        else:
            cur.execute(
                """INSERT INTO operator_users
                   (email, password_hash, is_active, account_type, creator_slug)
                   VALUES (%s,'test_pw_hash',TRUE,'performer',%s)
                   RETURNING id""",
                (EMAIL, SLUG),
            )
            uid = cur.fetchone()[0]
            print(f"  operator_users: created id={uid}")

        cur.execute("SELECT id FROM bot_configs WHERE creator_slug=%s",(SLUG,))
        if cur.fetchone():
            print(f"  bot_configs: row already exists for {SLUG}")
        else:
            cur.execute(
                """INSERT INTO bot_configs
                   (creator_slug, operator_user_id, account_type, status)
                   VALUES (%s,%s,'performer','submitted')""",
                (SLUG, uid),
            )
            print(f"  bot_configs: created for {SLUG}")

        return uid

def run_provisioning_subprocess(user_id: int, form: dict):
    """
    Run provision_new_creator inside operator/ so imports resolve correctly.
    Returns stdout so the caller can parse any status info.
    """
    script = textwrap.dedent(f"""
        import sys, json
        sys.path.insert(0, {OPERATOR!r})
        from dotenv import load_dotenv; load_dotenv({os.path.join(ROOT, '.env')!r})

        from app.provisioning import provision_new_creator
        provision_new_creator({user_id}, {SLUG!r}, {form!r})

        print('--- DONE ---')
    """).strip()
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=OPERATOR, capture_output=True, text=True, timeout=300,
    )
    return proc

def print_result(conn):
    print("\n=== Final state in Postgres ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT provisioning_status, error_message FROM bot_configs WHERE creator_slug=%s",
            (SLUG,),
        )
        status, err = cur.fetchone() or (None, None)
        print(f"  provisioning_status: {status!r}")
        if err:
            print(f"  error_message: {err[:300]}...")

        cur.execute("SELECT phone_number FROM operator_users WHERE email=%s",(EMAIL,))
        row = cur.fetchone()
        print(f"  phone_number: {row[0] if row else None}")

        cur.execute(
            "SELECT config_json->>'display_name', "
            "LENGTH(config_json->>'style_rules_text'), "
            "LENGTH(config_json->>'voice_lock_rules_text'), "
            "LENGTH(config_json->>'tone_examples_text'), "
            "LENGTH(config_json->>'hard_fact_guardrails_text') "
            "FROM creator_configs WHERE creator_slug=%s",
            (SLUG,),
        )
        row = cur.fetchone()
        if row:
            name, style_n, voice_n, tone_n, guard_n = row
            print(f"  creator_configs.display_name: {name!r}")
            print(f"  style_rules_text:           {style_n} chars")
            print(f"  voice_lock_rules_text:      {voice_n} chars")
            print(f"  tone_examples_text:         {tone_n} chars")
            print(f"  hard_fact_guardrails_text:  {guard_n} chars")
        else:
            print("  creator_configs: NO ROW")

        cur.execute(
            "SELECT COUNT(*), COUNT(DISTINCT source) FROM creator_embeddings WHERE creator_slug=%s",
            (SLUG,),
        )
        n, src_n = cur.fetchone()
        print(f"  creator_embeddings: {n} chunks across {src_n} distinct sources")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="wipe existing rows before provisioning")
    args = ap.parse_args()

    import psycopg2
    dsn = os.getenv("DATABASE_URL","").replace("postgres://","postgresql://",1)
    assert dsn, "DATABASE_URL not set"
    conn = psycopg2.connect(dsn); conn.autocommit = True

    try:
        print(f"=== Provisioning {DISPLAY_NAME} (slug={SLUG}) ===")
        if args.reset:
            reset(conn)

        print("\n[1/2] Seed operator_users + bot_configs")
        user_id = seed(conn)

        print("\n[2/2] Run provision_new_creator(...)")
        form = {
            "display_name": DISPLAY_NAME,
            "account_type": "performer",
            "sms_keyword": "MARCUS",
            "tone": TONE,
            "bio": BIO,
            "extra_context": "",
            "seed_facts": [
                f"Name: {DISPLAY_NAME}, Atlanta Georgia stand-up comedian.",
                "Corporate background: ex-project-manager at a large Atlanta bank.",
                "Act topics: corporate absurdity, gym culture, Southern family cookouts, hustle culture critique.",
                "Family: close with his mother and three younger cousins. No partner, no children.",
                "Car: beaten-up 2012 Honda Civic, jokes about refusing to upgrade it.",
            ],
        }
        proc = run_provisioning_subprocess(user_id, form)
        print(proc.stdout or "")
        if proc.returncode != 0:
            print("\n=== SUBPROCESS FAILED ===")
            print(proc.stderr[-3000:])
            sys.exit(1)

        print_result(conn)

        # Final fetch of status
        with conn.cursor() as cur:
            cur.execute(
                "SELECT provisioning_status FROM bot_configs WHERE creator_slug=%s",
                (SLUG,),
            )
            status = (cur.fetchone() or [None])[0]
        if status == "live":
            print("\n" + "=" * 60)
            print("SUCCESS — Marcus Cole is LIVE in the pipeline.")
            print("\nLaunch the interactive chat to test him:")
            print(f"\n  python3 scripts/chat_local.py --slug {SLUG}")
            print("\nTry messages like:")
            print("  'yo been following you for a while, your corporate stuff is killing me'")
            print("  'how do you even go to the gym at 5am'")
            print("  'what do you think about hustle culture'")
            print("  'I'm having a rough week at work, advice?'   (tests sincere lane)")
            print("  'tell me about your wife'                     (tests guardrails — he has none)")
            print("  'what do you think of Shalabh?'               (tests Zarna-leak — he shouldn't recognize this name)")
        else:
            print(f"\nUnexpected final status: {status!r}")
            sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
