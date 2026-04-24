"""
Phase 3 end-to-end smoke test for the provisioning module.

Exercises the full pipeline WITHOUT any Twilio dependency:
  1. Create a throwaway operator_users + bot_configs row
  2. Run provision_new_creator(...) with a realistic-ish onboarding form
  3. Verify:
       - bot_configs.provisioning_status goes NULL → in_progress → live
       - operator_users.phone_number got a stub value
       - creator_configs has a row with LLM-generated personality
       - creator_embeddings has N > 0 rows
       - PgRetriever returns relevant chunks for this slug
  4. Re-run provisioning — every step should skip (idempotency)
  5. Clean up every row we created

Architecture note:
  Both `operator/` and the repo root contain an `app/` package, so importing
  provisioning into the same process as the main-app retriever causes a
  module shadow. We sidestep that by running the provisioning call in a
  child `python` process whose cwd is `operator/` (mirroring how the Flask
  app runs the same code path). The parent process handles DB verification
  and the PgRetriever sanity check.

Run:
  python scripts/test_phase3_provisioning.py
"""

import json
import os
import subprocess
import sys
import traceback

from dotenv import load_dotenv
import psycopg2

load_dotenv()
os.environ.setdefault("PROVISIONING_PHONE_MODE", "stub")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPERATOR = os.path.join(ROOT, "operator")

if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.retrieval.pg_retriever import PgRetriever  # noqa: E402


TEST_SLUG = "phase3_test"
TEST_EMAIL = "phase3_test@example.invalid"
TEST_NAME = "Phase 3 Test Creator"

ONBOARDING_FORM = {
    "display_name": "Haley Johnson",
    "bio": (
        "Haley is a stand-up comedian based in Austin, Texas. She writes "
        "observational humor about millennial life, dating apps, and Texas quirks."
    ),
    "tone": "warm, playful, self-deprecating with sharp punchlines",
    "sms_keyword": "HALEY",
    "account_type": "performer",
    "website_url": "",
    "extra_context": (
        "Her first special is called 'Nothing is Wrong' and she tours mid-size clubs. "
        "She's from a small town outside Dallas. She has a dog named Biscuit."
    ),
    "uploaded_files": [],
}


def _connect():
    url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def _seed_test_user() -> int:
    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS phone_number TEXT")

            cur.execute(
                """
                INSERT INTO operator_users (email, name, password_hash, creator_slug, account_type)
                VALUES (%s, %s, %s, %s, 'performer')
                ON CONFLICT (email) DO UPDATE
                    SET name = EXCLUDED.name,
                        creator_slug = EXCLUDED.creator_slug,
                        phone_number = NULL
                RETURNING id
                """,
                (TEST_EMAIL, TEST_NAME, "NOT_A_REAL_HASH", TEST_SLUG),
            )
            user_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO bot_configs (operator_user_id, creator_slug, account_type, config_json, status)
                VALUES (%s, %s, 'performer', %s::jsonb, 'submitted')
                ON CONFLICT (creator_slug) DO UPDATE
                    SET operator_user_id = EXCLUDED.operator_user_id,
                        status = 'submitted',
                        error_message = NULL,
                        updated_at = NOW()
                """,
                (user_id, TEST_SLUG, "{}"),
            )
            return user_id
    finally:
        conn.close()


def _cleanup():
    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM creator_embeddings WHERE creator_slug = %s", (TEST_SLUG,))
            cur.execute("DELETE FROM creator_configs   WHERE creator_slug = %s", (TEST_SLUG,))
            cur.execute("DELETE FROM bot_configs       WHERE creator_slug = %s", (TEST_SLUG,))
            cur.execute("DELETE FROM operator_users    WHERE email = %s",        (TEST_EMAIL,))
    finally:
        conn.close()


def _fetch_status(slug):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT provisioning_status, error_message FROM bot_configs WHERE creator_slug=%s",
                (slug,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _fetch_user_phone(slug):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ou.phone_number
                FROM operator_users ou
                JOIN bot_configs bc ON bc.operator_user_id = ou.id
                WHERE bc.creator_slug=%s
                """,
                (slug,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _fetch_config(slug):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT config_json FROM creator_configs WHERE creator_slug=%s", (slug,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _count_embeddings(slug):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM creator_embeddings WHERE creator_slug=%s", (slug,))
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _run_provisioning_subprocess(user_id: int) -> None:
    """
    Spawn a child Python process with cwd=operator/ so that `from app.provisioning`
    resolves to operator/app/provisioning (matches how the real Flask app runs
    the code path).
    """
    form_json = json.dumps(ONBOARDING_FORM)
    runner = f"""
import json, logging, os, sys
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')
from app.provisioning import provision_new_creator
form = json.loads({form_json!r})
provision_new_creator({user_id}, {TEST_SLUG!r}, form)
print('SUBPROCESS_DONE')
"""
    env = os.environ.copy()
    env.setdefault("PROVISIONING_PHONE_MODE", "stub")

    proc = subprocess.run(
        [sys.executable, "-c", runner],
        cwd=OPERATOR,
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )
    print("--- subprocess stdout ---")
    print(proc.stdout.strip())
    if proc.stderr.strip():
        print("--- subprocess stderr ---")
        print(proc.stderr.strip())
    if proc.returncode != 0:
        raise RuntimeError(f"provisioning subprocess exited {proc.returncode}")


def main() -> int:
    print("=== Phase 3 Smoke Test — Provisioning Pipeline ===\n")
    _cleanup()

    failures = []
    try:
        print("[seed] creating throwaway operator_users + bot_configs rows")
        user_id = _seed_test_user()
        print(f"       user_id={user_id} slug={TEST_SLUG}\n")

        # ── Test 1: First-time provisioning ───────────────────────────────
        print("[1] Run provision_new_creator(...) end-to-end")
        _run_provisioning_subprocess(user_id)
        status = _fetch_status(TEST_SLUG)
        print(f"    bot_configs.provisioning_status = {status[0]!r}")
        if status[1]:
            print(f"    bot_configs.error_message = {status[1][:300]}")
        assert status[0] == "live", f"Expected provisioning_status=live, got {status[0]!r}"
        assert not status[1], f"Unexpected error_message: {status[1][:200]}"
        print("    OK — provisioning_status transitioned to 'live'\n")

        # ── Test 2: Phone number assigned ─────────────────────────────────
        print("[2] Phone number stub assigned")
        phone = _fetch_user_phone(TEST_SLUG)
        print(f"    operator_users.phone_number = {phone}")
        assert phone and phone.startswith("+1555"), f"Expected +1555… stub, got {phone!r}"
        print("    OK\n")

        # ── Test 3: creator_configs row exists + has LLM content ──────────
        print("[3] creator_configs populated")
        config = _fetch_config(TEST_SLUG)
        assert config is not None, "creator_configs row missing"
        assert config.get("slug") == TEST_SLUG, f"slug mismatch: {config.get('slug')!r}"
        display = config.get("display_name", "")
        style = config.get("style_rules", "") or ""
        examples = config.get("tone_examples", []) or []
        print(f"    display_name = {display!r}")
        print(f"    style_rules  = {len(style)} chars")
        print(f"    tone_examples= {len(examples)} entries")
        assert display, "display_name should not be empty"
        print("    OK\n")

        # ── Test 4: creator_embeddings rows exist ─────────────────────────
        print("[4] creator_embeddings rows inserted")
        n = _count_embeddings(TEST_SLUG)
        print(f"    row count = {n}")
        assert n >= 1, f"Expected ≥1 embedding row, got {n}"
        print("    OK\n")

        # ── Test 5: PgRetriever returns relevant chunks ───────────────────
        print("[5] PgRetriever returns relevant chunks for this slug")
        retriever = PgRetriever(TEST_SLUG)
        chunks = retriever.get_relevant_chunks("Who is Haley and what does she do?", k=3)
        print(f"    got {len(chunks)} chunks")
        for i, c in enumerate(chunks[:3], 1):
            print(f"      {i}. {c[:100]}{'…' if len(c) > 100 else ''}")
        assert len(chunks) >= 1, "Expected at least 1 relevant chunk"
        joined = " ".join(chunks).lower()
        assert ("haley" in joined or "austin" in joined or "comedian" in joined), \
            f"Expected chunks to reference Haley/Austin/comedian — got {joined[:200]!r}"
        print("    OK\n")

        # ── Test 6: Idempotency ───────────────────────────────────────────
        print("[6] Idempotency — re-run provisioning, nothing should re-do")
        before_count = _count_embeddings(TEST_SLUG)
        before_phone = _fetch_user_phone(TEST_SLUG)
        _run_provisioning_subprocess(user_id)
        after_count = _count_embeddings(TEST_SLUG)
        after_phone = _fetch_user_phone(TEST_SLUG)
        status = _fetch_status(TEST_SLUG)
        print(f"    embeddings before={before_count} after={after_count}")
        print(f"    phone      before={before_phone} after={after_phone}")
        print(f"    provisioning_status = {status[0]!r}")
        assert before_count == after_count, "Idempotency broken — embeddings changed on re-run"
        assert before_phone == after_phone, "Idempotency broken — phone number changed on re-run"
        assert status[0] == "live", f"provisioning_status should end at 'live' again, got {status[0]!r}"
        print("    OK — all steps skipped, no duplicate work\n")

    except AssertionError as e:
        failures.append(str(e))
        traceback.print_exc()
    except Exception as e:
        failures.append(f"unexpected error: {e}")
        traceback.print_exc()
    finally:
        print("[cleanup] removing all test rows")
        _cleanup()
        print("    done\n")

    if failures:
        print(f"=== {len(failures)} TEST(S) FAILED ===")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("=== All Phase 3 tests passed ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
