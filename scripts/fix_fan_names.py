"""
Audit and correct wrong fan names in the contacts table.

The live extractor sometimes stored the creator's or family's name (fans open
with "Hi Zarna", "tell Shalabh...") or a filler word ("Hey", "Stop") as the
fan's own name. This script:

  1. Scans every contact that has a fan_name set.
  2. Keeps names that already look valid (per app.brain.memory.is_valid_fan_name).
  3. For each bad name, tries to recover the fan's REAL first name from:
        a. the fan_memory paragraph (high-precision regex, free), then
        b. the fan's own inbound messages via Gemini (only when needed).
     A recovered name must itself pass validation.
  4. Writes the recovered name, or clears the field to '' (unknown) when no
     real name can be found — never leaves a wrong name in place.

SAFETY: dry-run by default. Nothing is written unless you pass --apply.

Usage:
    # Preview what would change (no writes):
    DATABASE_URL="postgresql://..." GEMINI_API_KEY="..." python3 scripts/fix_fan_names.py

    # Apply the changes:
    DATABASE_URL="postgresql://..." GEMINI_API_KEY="..." python3 scripts/fix_fan_names.py --apply

    # Only clear bad names, don't try to recover real ones via Gemini:
    python3 scripts/fix_fan_names.py --no-recover --apply
"""

import os
import re
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌  DATABASE_URL not set.")
    sys.exit(1)

DSN = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Make the app package importable so we share ONE definition of what a valid
# fan name is (blocklist of creator/family names lives in app.brain.memory).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.brain.memory import is_valid_fan_name  # noqa: E402

# High-precision "the fan stated their own name" patterns for the memory text
# and message history. Deliberately strict to avoid false positives like
# "I'm good" or "this is amazing" — anything caught still gets re-validated.
_RECOVER_PATTERNS = [
    re.compile(r"\bfan(?:'s)?\s+(?:name\s+is|is\s+named|is\s+called)\s+([A-Za-z][A-Za-z'\-]{1,19})\b", re.I),
    re.compile(r"\bmy\s+name(?:'s|\s+is)\s+([A-Za-z][A-Za-z'\-]{1,19})\b", re.I),
    re.compile(r"\bname(?:'s|\s+is)\s+([A-Za-z][A-Za-z'\-]{1,19})\b", re.I),
    re.compile(r"\bcall\s+me\s+([A-Za-z][A-Za-z'\-]{1,19})\b", re.I),
]


def _recover_from_text(text: str) -> str:
    for pat in _RECOVER_PATTERNS:
        m = pat.search(text or "")
        if m:
            cand = m.group(1).strip()
            if is_valid_fan_name(cand):
                return cand.title()
    return ""


def _recover_with_gemini(user_messages: list) -> str:
    """Ask Gemini for the fan's own first name from their inbound messages.
    Returns '' unless it finds an explicit, valid self-stated name."""
    if not user_messages:
        return ""
    try:
        from google import genai
        from app.config import GEMINI_API_KEY, INTENT_MODEL
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception:
        return ""

    convo = "\n".join(f"- {m}" for m in user_messages[:30])
    prompt = (
        "Below are text messages a fan sent to a comedian's SMS chatbot.\n"
        "What is THE FAN'S OWN first name, only if they explicitly stated it "
        "(e.g. \"I'm Priya\", \"my name is Raj\", \"this is Sam\")?\n"
        "Rules:\n"
        "- Reply with ONLY the first name, or an empty response if they never gave their own name.\n"
        "- Do NOT return the comedian's name (Zarna) or her family's names (Shalabh, Zoya, Brij, Veer).\n"
        "- Do NOT guess from greetings, filler words, or who they're talking about.\n\n"
        f"Messages:\n{convo}"
    )
    try:
        resp = client.models.generate_content(model=INTENT_MODEL, contents=prompt)
        raw = (resp.text or "").strip().strip('"').strip("'").split()
        cand = raw[0] if raw else ""
        if is_valid_fan_name(cand):
            return cand.title()
    except Exception:
        return ""
    return ""


def get_named_fans(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT phone_number, fan_name, fan_memory
            FROM   contacts
            WHERE  fan_name IS NOT NULL AND fan_name <> ''
            ORDER BY created_at DESC
            """
        )
        return [(r["phone_number"], r["fan_name"], r["fan_memory"] or "") for r in cur.fetchall()]


def get_user_messages(conn, phone_number: str) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT text FROM messages
            WHERE  phone_number = %s AND role = 'user'
            ORDER BY created_at ASC
            LIMIT 30
            """,
            (phone_number,),
        )
        return [r["text"] for r in cur.fetchall() if r["text"]]


def set_fan_name(conn, phone_number: str, name: str) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE contacts SET fan_name = %s WHERE phone_number = %s",
                (name[:80], phone_number),
            )


def process_fan(phone_number: str, current: str, memory: str, recover: bool) -> tuple:
    """Return (phone, current, action, new_value) where action is
    'keep' | 'fix' | 'clear'."""
    if is_valid_fan_name(current):
        return phone_number, current, "keep", current

    # Bad name — try to recover the fan's real name.
    recovered = _recover_from_text(memory)

    if not recovered and recover:
        conn = psycopg2.connect(DSN)
        try:
            msgs = get_user_messages(conn, phone_number)
        finally:
            conn.close()
        # Also scan the raw messages with the cheap regex before paying for Gemini.
        recovered = _recover_from_text("\n".join(msgs))
        if not recovered:
            recovered = _recover_with_gemini(msgs)

    if recovered:
        return phone_number, current, "fix", recovered
    return phone_number, current, "clear", ""


def main():
    parser = argparse.ArgumentParser(description="Correct wrong fan names in contacts.")
    parser.add_argument("--apply", action="store_true",
                        help="Write changes to the DB (default is a dry run).")
    parser.add_argument("--no-recover", action="store_true",
                        help="Don't call Gemini to recover real names; only clear bad ones.")
    parser.add_argument("--workers", type=int, default=8, help="Thread pool size.")
    args = parser.parse_args()

    apply = args.apply
    recover = not args.no_recover

    print("🔍  Connecting to database…")
    conn = psycopg2.connect(DSN)
    fans = get_named_fans(conn)
    conn.close()

    total = len(fans)
    if total == 0:
        print("✅  No fans have a name set. Nothing to do.")
        return

    mode = "APPLY (writing)" if apply else "DRY RUN (no writes)"
    print(f"📋  {total} fans have a name set.  Mode: {mode}")
    print(f"⏳  Auditing…\n{'─' * 64}")

    kept = fixed = cleared = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_fan, phone, current, memory, recover): phone
            for phone, current, memory in fans
        }
        for future in as_completed(futures):
            phone, current, action, new_value = future.result()
            short = f"...{phone[-4:]}"
            if action == "keep":
                kept += 1
                continue

            if apply:
                conn = psycopg2.connect(DSN)
                try:
                    set_fan_name(conn, phone, new_value)
                finally:
                    conn.close()

            tag = "" if apply else "[DRY] "
            if action == "fix":
                fixed += 1
                print(f"  🔧 {tag}{short}  {current!r}  →  {new_value!r}")
            else:
                cleared += 1
                print(f"  🧹 {tag}{short}  {current!r}  →  (unknown)")

    print(f"\n{'─' * 64}")
    print(f"✅  Done.  {kept} kept, {fixed} corrected, {cleared} cleared to unknown "
          f"(of {total} named fans).")
    if not apply:
        print("   DRY RUN — nothing was written. Re-run with --apply to make these changes.")


if __name__ == "__main__":
    main()
