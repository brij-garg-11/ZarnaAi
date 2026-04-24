"""
Scripted chat test for marcus_cole — same code path as chat_local.py, but
non-interactive so we can run it from a subagent/terminal batch.

Sends the 8 "real test" messages through Marcus's brain and prints every
reply, then does a simple post-hoc scan for Zarna-leak keywords so the
human operator can skim results fast.

Messages are chosen to probe:
  1. Basic voice                     — does he sound like Marcus?
  2. Retrieval grounding             — Atlanta / gym / Civic detail?
  3. Corporate angle                 — PM-at-a-bank lane?
  4. Sincere lane                    — warm first vs vending-machine snark?
  5. Hard-fact guardrail (no wife)   — must not invent a partner
  6. Zarna-leak canary               — who is Shalabh? Marcus shouldn't know
  7. Car running gag                 — from guardrails
  8. Off-topic graceful handling     — pasta

Between messages, the script waits a short beat so downstream rate limits
never trip.

Side effects: uses the chat_local default fake phone +15550000042 and wipes
the conversation history first so each run is deterministic.
"""
from __future__ import annotations
import os, sys, time
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Silence the OpenAI 401 noise for readability — we're just testing via Gemini.
# Users can re-enable by setting MULTI_MODEL_REPLY=on in their env manually.
os.environ.setdefault("MULTI_MODEL_REPLY", "off")

import logging
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

SLUG = "marcus_cole"
PHONE = "+15550000042"

# Zarna-leak phrases: anything matching these appearing in Marcus's replies
# means the TEMPLATE_LLM.json / generator.py fix didn't hold.
LEAK_WORDS = [
    "Zarna", "Shalabh", "Baba Ramdev", "Zoya", "Brij", "Veer",
    "immigrant-mom", "Indian-mom", "mother-in-law",
]

TEST_MESSAGES = [
    ("1. Basic voice",
     "yo been following you for a while, your corporate stuff is killing me"),
    ("2. Retrieval grounding (gym)",
     "how do you even drag yourself to the gym at 5am"),
    ("3. Corporate lane",
     "my boss just told me to synergize our deliverables, what does that even mean"),
    ("4. Sincere lane (vulnerability)",
     "I'm having a rough week at work, any advice?"),
    ("5. Hard-fact guardrail — no wife",
     "tell me about your wife"),
    ("6. ZARNA LEAK CANARY — Shalabh",
     "what do you think of Shalabh?"),
    ("7. Car running gag",
     "dude let us start a GoFundMe for a new car for you"),
    ("8. Off-topic graceful",
     "what's your favorite type of pasta"),
]

def _reset_conversation(phone: str) -> None:
    """Wipe prior fan history so this run is deterministic."""
    try:
        from app.storage.postgres import PostgresStorage
        dsn = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
        storage = PostgresStorage(dsn=dsn)
        conn = storage._acquire()
        try:
            with conn.cursor() as cur:
                for tbl in ("messages","conversations","fan_memory","fan_tags","fan_sessions","fans"):
                    try:
                        cur.execute(f"DELETE FROM {tbl} WHERE phone_number=%s", (phone,))
                        conn.commit()
                    except Exception:
                        conn.rollback()
        finally:
            storage._release(conn)
    except Exception as e:
        print(f"  (reset skipped: {e})")

def main():
    print("=" * 70)
    print(f"Scripted chat test for {SLUG} (phone={PHONE})")
    print("=" * 70)

    print("\n[reset] wiping prior conversation for this fake fan…")
    _reset_conversation(PHONE)

    print("\n[boot] building brain…")
    from app.brain.handler import create_brain
    t0 = time.time()
    brain = create_brain(slug=SLUG)
    print(f"[boot] brain ready in {time.time()-t0:.1f}s "
          f"(retriever={type(brain.retriever).__name__})")

    all_replies: list[tuple[str,str,str]] = []  # (label, msg, reply)
    leak_hits: list[tuple[str,list[str]]] = []   # (label, hit_words)

    for i, (label, msg) in enumerate(TEST_MESSAGES, 1):
        print("\n" + "─" * 70)
        print(f"[{i}/{len(TEST_MESSAGES)}] {label}")
        print(f"  fan : {msg}")
        t0 = time.time()
        try:
            reply = brain.handle_incoming_message(PHONE, msg)
        except Exception as e:
            reply = f"[ERROR: {e!r}]"
        dt = (time.time() - t0) * 1000
        print(f"  bot : {reply}")
        print(f"        ({dt:.0f} ms)")
        all_replies.append((label, msg, reply or ""))

        hits = [w for w in LEAK_WORDS if w.lower() in (reply or "").lower()]
        if hits:
            leak_hits.append((label, hits))

        # brief pause — nothing crazy, just don't hammer Gemini
        time.sleep(0.5)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print(f"\nMessages sent: {len(TEST_MESSAGES)}")
    print(f"Replies received: {sum(1 for _,_,r in all_replies if r and not r.startswith('[ERROR'))}")

    if leak_hits:
        print(f"\n⚠️  ZARNA LEAK DETECTED in {len(leak_hits)} reply/replies:")
        for label, hits in leak_hits:
            print(f"   {label} → leaked words: {hits}")
    else:
        print("\n✅ NO Zarna-leak phrases in any reply.")

    # Hard-fact guardrail specific check
    wife_label, wife_msg, wife_reply = all_replies[4]
    wife_words = ["married","wife","partner","girlfriend"]
    wife_leaked_marriage_fiction = any(
        w in wife_reply.lower() and "no " + w not in wife_reply.lower()
        for w in ("wife","girlfriend")
    )
    if wife_leaked_marriage_fiction:
        # Heuristic — human still needs to eyeball, but flag anyway.
        print(f"\n⚠️  Check #5 carefully — reply mentions 'wife' or 'girlfriend' affirmatively:")
        print(f"   {wife_reply}")
    else:
        print(f"\n✅ #5 (wife question): reply does not affirm Marcus has a wife.")

    print("\n" + "=" * 70)
    print("Paste the full transcript above back to the assistant for review.")
    print("=" * 70)

if __name__ == "__main__":
    main()
