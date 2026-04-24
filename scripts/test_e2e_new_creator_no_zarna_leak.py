"""
End-to-end: provision a fake creator, then verify the generated config has
all _text fields populated AND generator.py uses those instead of falling
through to Zarna-hardcoded Python constants.

This is the critical check that the TEMPLATE_LLM.json ↔ generator.py field-
name alignment fix actually closes the Zarna-voice-leak bug.

Steps:
  1. Seed operator_users + bot_configs for a fake creator slug.
  2. Run operator.app.provisioning.config_writer.generate_and_write()
     in the operator/ subprocess (so `operator` resolves as a package).
  3. Load the config via load_creator() back in the root process.
  4. Assert all 4 _text fields are populated (either by the LLM or by the
     generic backfill).
  5. Scan each _text field — the non-Zarna creator's config must NOT contain
     "Zarna", "Shalabh", "Baba Ramdev", etc.
  6. Build the full prompt via generator._build_prompt and confirm none of
     the Zarna-hardcoded voice lock rules / style rules / tone examples
     make it into the prompt string for a non-Zarna creator.
  7. Cleanup.
"""
from __future__ import annotations
import os, sys, json, subprocess
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPERATOR = os.path.join(ROOT, "operator")
sys.path.insert(0, ROOT)

import psycopg2

DSN = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
assert DSN, "DATABASE_URL required"

TEST_SLUG = "haley_leak_test"
TEST_EMAIL = f"{TEST_SLUG}@example.com"
failures: list[str] = []
def ok(label, cond, detail=""):
    mark = "✓" if cond else "✗"
    print(f"  [{mark}] {label}" + (f"  — {detail}" if detail else ""))
    if not cond: failures.append(label)

# Phrases that MUST NOT appear in any non-Zarna creator's config or prompt
# (they're Zarna-specific and would leak her persona into another bot).
ZARNA_LEAK_PHRASES = [
    "Zarna", "Shalabh", "Baba Ramdev", "Zoya", "Brij", "Veer",
    "immigrant-mom", "Indian-mom", "mother-in-law",
]

def seed(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM creator_embeddings WHERE creator_slug=%s", (TEST_SLUG,))
        cur.execute("DELETE FROM creator_configs   WHERE creator_slug=%s", (TEST_SLUG,))
        cur.execute("DELETE FROM bot_configs       WHERE creator_slug=%s", (TEST_SLUG,))
        cur.execute("DELETE FROM operator_users    WHERE email=%s", (TEST_EMAIL,))

        cur.execute(
            """INSERT INTO operator_users (email, password_hash, account_type, is_active)
               VALUES (%s, 'test_pw_hash', 'performer', TRUE)
               RETURNING id""",
            (TEST_EMAIL,),
        )
        user_id = cur.fetchone()[0]
        cur.execute(
            """INSERT INTO bot_configs (creator_slug, operator_user_id, account_type, status)
               VALUES (%s, %s, 'performer', 'pending')""",
            (TEST_SLUG, user_id),
        )

def cleanup(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM creator_embeddings WHERE creator_slug=%s", (TEST_SLUG,))
        cur.execute("DELETE FROM creator_configs   WHERE creator_slug=%s", (TEST_SLUG,))
        cur.execute("DELETE FROM bot_configs       WHERE creator_slug=%s", (TEST_SLUG,))
        cur.execute("DELETE FROM operator_users    WHERE email=%s", (TEST_EMAIL,))

def main():
    conn = psycopg2.connect(DSN); conn.autocommit = True

    print(f"=== Setup: seeding fake creator '{TEST_SLUG}' ===")
    seed(conn)

    try:
        # Run config_writer inside operator/ so the `operator` package
        # resolves correctly (avoids shadowing Python's builtin `operator`).
        print("\n=== Running config_writer in subprocess ===")
        form = {
            "display_name": "Haley Johnson",
            "account_type": "performer",
            "sms_keyword": "HALEY",
            "tone": "observational stand-up from Austin, Texas",
            "bio": (
                "Haley Johnson is a stand-up comedian based in Austin, Texas who specializes "
                "in observational humor about dating apps, group chats, and apartment living. "
                "She has no siblings, no children, no partner, and jokes often about "
                "her cat Pepper and her struggles with Texas heat."
            ),
            "extra_context": "",
        }
        script = f"""
import sys
sys.path.insert(0, {OPERATOR!r})
from app.provisioning.config_writer import generate_and_write
result = generate_and_write({TEST_SLUG!r}, {form!r})
import json
print('--- CONFIG_JSON_START ---')
print(json.dumps(result))
print('--- CONFIG_JSON_END ---')
"""
        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=OPERATOR,
            capture_output=True, text=True, timeout=180,
        )
        print("subprocess stdout (last 60 lines):")
        for line in proc.stdout.splitlines()[-60:]:
            print("  >", line)
        if proc.returncode != 0:
            print("subprocess stderr:")
            print(proc.stderr[-2000:])
            raise RuntimeError(f"config_writer subprocess exited {proc.returncode}")

        # Pull config out of stdout
        s = proc.stdout
        a = s.find("--- CONFIG_JSON_START ---")
        b = s.find("--- CONFIG_JSON_END ---")
        cfg = json.loads(s[a:b].split("\n",1)[1])

        print(f"\n=== Verifying generated config for '{TEST_SLUG}' ===")

        # ---- _text fields populated ----
        for field in ("style_rules_text","voice_lock_rules_text","hard_fact_guardrails_text","tone_examples_text"):
            val = cfg.get(field, "") or ""
            ok(f"{field} non-empty", len(val.strip()) > 0, f"len={len(val)}")

        # ---- No Zarna leakage in any _text field ----
        for field in ("style_rules_text","voice_lock_rules_text","hard_fact_guardrails_text","tone_examples_text"):
            val = (cfg.get(field, "") or "").lower()
            bad = [p for p in ZARNA_LEAK_PHRASES if p.lower() in val]
            ok(f"{field} has no Zarna-leak phrases", not bad, f"found: {bad}" if bad else "")

        # ---- slug / display_name correct ----
        ok("slug == HALEY_LEAK_TEST", cfg.get("slug") == TEST_SLUG)
        ok("display_name matches form", cfg.get("display_name") == "Haley Johnson")
        ok("sms_keyword uppercase", (cfg.get("sms_keyword","") or "").isupper())

        # ---- shalabh_names is empty (Zarna-only artefact) ----
        ok("shalabh_names empty for non-Zarna creator",
           cfg.get("shalabh_names", []) == [], f"got {cfg.get('shalabh_names')}")

        print(f"\n=== Loading config via load_creator(), verifying brain uses it ===")
        # load_creator should find the DB row since file doesn't exist.
        from app.brain.creator_config import load_creator
        loaded = load_creator(TEST_SLUG)
        ok("load_creator returned a config", loaded is not None)
        ok("loaded.name matches", loaded and loaded.name == "Haley Johnson")
        ok("loaded.style_rules_text populated", loaded and len(loaded.style_rules_text) > 0)
        ok("loaded.voice_lock_rules_text populated", loaded and len(loaded.voice_lock_rules_text) > 0)

        # ---- Build a real prompt via generator.py and confirm no Zarna leakage ----
        print("\n=== Building prompt via generator._build_prompt() ===")
        from app.brain import generator as gen
        from app.brain.intent import Intent
        prompt = gen._build_prompt(
            intent=Intent.GENERAL,
            user_message="Hey, how's the heat in Austin?",
            chunks=[],
            history=[],
            fan_memory="",
            creator_config=loaded,
        )
        low = prompt.lower()
        bad = [p for p in ZARNA_LEAK_PHRASES if p.lower() in low]
        ok("built prompt has NO Zarna leak phrases",
           not bad, f"found: {bad} | prompt[:500]: {prompt[:500]!r}" if bad else "")
        ok("built prompt mentions 'Haley'",
           "haley" in low, f"prompt[:500]: {prompt[:500]}")
        ok("built prompt reasonable length", 500 < len(prompt) < 20000, f"len={len(prompt)}")

    finally:
        print("\n=== Cleanup ===")
        cleanup(conn)
        print("  removed test rows")
        conn.close()

    print("\n" + "=" * 50)
    if failures:
        print(f"FAIL — {len(failures)} check(s) failed:")
        for f in failures: print(f"  - {f}")
        sys.exit(1)
    print("PASS — new-creator end-to-end generates clean, Zarna-free config")

if __name__ == "__main__":
    main()
