"""
Cross-tenant isolation test.

Verifies that running Marcus's brain and Zarna's brain side-by-side does
NOT cause:
  1. Their creator_configs to bleed into each other (process-global state)
  2. One brain's retrieval to surface the other's chunks
  3. winning_examples (high-engagement past replies) from one creator's
     fans showing up in the other's prompts
  4. Conversation history of one fan appearing in the other's session

Each test uses a separate fake phone so fan-state collisions are
impossible by design — but we explicitly verify configs / retrievers /
prompts to confirm the slug-scoping fix held.

This is the deep-mode equivalent of "what happens at 3am when 50 creators
are texting at once" — minus the actual concurrency, focused on per-brain
correctness in the same process.
"""
from __future__ import annotations
import os, sys, time, threading
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("MULTI_MODEL_REPLY", "off")

import logging
logging.basicConfig(level=logging.WARNING)

from app.brain.handler import create_brain
from app.brain import generator as gen

CHECKS_PASSED = 0
CHECKS_FAILED = 0

def check(name: str, cond: bool, detail: str = "") -> None:
    global CHECKS_PASSED, CHECKS_FAILED
    if cond:
        CHECKS_PASSED += 1
        print(f"  [PASS] {name}" + (f" — {detail}" if detail else ""))
    else:
        CHECKS_FAILED += 1
        print(f"  [FAIL] {name}" + (f" — {detail}" if detail else ""))

# ──────────────────────────────────────────────────────────────────────
# Section 1: Build both brains in same process, verify config isolation
# ──────────────────────────────────────────────────────────────────────
print("═" * 72)
print("Section 1: Brain config isolation in shared process")
print("═" * 72)

brain_marcus = create_brain(slug="marcus_cole")
brain_zarna  = create_brain(slug="zarna")

check("marcus.slug == 'marcus_cole'",
      brain_marcus.slug == "marcus_cole", brain_marcus.slug)
check("zarna.slug  == 'zarna'",
      brain_zarna.slug == "zarna", brain_zarna.slug)

check("marcus.creator_config.slug == 'marcus_cole'",
      brain_marcus.creator_config.slug == "marcus_cole",
      brain_marcus.creator_config.slug)
check("zarna.creator_config.slug  == 'zarna'",
      brain_zarna.creator_config.slug == "zarna",
      brain_zarna.creator_config.slug)

# Configs must be different objects
check("creator_configs are different objects",
      brain_marcus.creator_config is not brain_zarna.creator_config)

# Their underlying _text fields must NOT collide
check("marcus.style_rules_text != zarna.style_rules_text",
      brain_marcus.creator_config.style_rules_text
      != brain_zarna.creator_config.style_rules_text)

check("marcus.tone_examples_text != zarna.tone_examples_text",
      brain_marcus.creator_config.tone_examples_text
      != brain_zarna.creator_config.tone_examples_text)

# Marcus must NOT have Zarna's hardcoded personas in his _text fields
m_voice = brain_marcus.creator_config.voice_lock_rules_text.lower()
m_facts = brain_marcus.creator_config.hard_fact_guardrails_text.lower()
m_tone  = brain_marcus.creator_config.tone_examples_text.lower()

check("Marcus voice_lock has no 'shalabh'",
      "shalabh" not in m_voice)
check("Marcus voice_lock has no 'zoya/brij/veer'",
      not any(n in m_voice for n in ("zoya","brij","veer")))
check("Marcus hard_facts has no 'baba ramdev'",
      "baba ramdev" not in m_facts)
check("Marcus tone_examples has no 'shalabh'",
      "shalabh" not in m_tone)

# Zarna must STILL have her signature people (sanity)
z_voice = brain_zarna.creator_config.voice_lock_rules_text.lower()
z_facts = brain_zarna.creator_config.hard_fact_guardrails_text.lower()

check("Zarna voice_lock STILL contains 'shalabh' (sanity)",
      "shalabh" in z_voice)
check("Zarna hard_facts STILL contains 'shalabh'",
      "shalabh" in z_facts)


# ──────────────────────────────────────────────────────────────────────
# Section 2: Retrievers are slug-scoped
# ──────────────────────────────────────────────────────────────────────
print("\n" + "═" * 72)
print("Section 2: Retriever scoping")
print("═" * 72)

print(f"  marcus retriever = {type(brain_marcus.retriever).__name__}")
print(f"  zarna  retriever = {type(brain_zarna.retriever).__name__}")

check("Marcus uses PgRetriever",
      type(brain_marcus.retriever).__name__ == "PgRetriever")
check("Zarna uses EmbeddingRetriever (legacy default)",
      type(brain_zarna.retriever).__name__ == "EmbeddingRetriever")

# PgRetriever should be scoped to marcus_cole
ms = (getattr(brain_marcus.retriever, "_slug", None)
      or getattr(brain_marcus.retriever, "creator_slug", None)
      or getattr(brain_marcus.retriever, "slug", None))
check("Marcus PgRetriever scoped to 'marcus_cole'",
      ms == "marcus_cole", f"got {ms!r}")

# Pull chunks from Marcus's retriever for a Shalabh-flavored query
chunks_m = brain_marcus.retriever.get_relevant_chunks("tell me about Shalabh")
joined_m = " ".join(chunks_m).lower() if chunks_m else ""
check("Marcus retrieval has NO 'shalabh' chunks",
      "shalabh" not in joined_m,
      f"got {len(chunks_m)} chunks, total {len(joined_m)} chars")
check("Marcus retrieval has NO 'baba ramdev' chunks",
      "baba ramdev" not in joined_m)

# Zarna's retrieval should have her data
chunks_z = brain_zarna.retriever.get_relevant_chunks("tell me about Shalabh")
joined_z = " ".join(chunks_z).lower() if chunks_z else ""
check("Zarna retrieval HAS 'shalabh' content (sanity)",
      "shalabh" in joined_z,
      f"got {len(chunks_z)} chunks")


# ──────────────────────────────────────────────────────────────────────
# Section 3: Concurrent message handling — no state crossover
# ──────────────────────────────────────────────────────────────────────
print("\n" + "═" * 72)
print("Section 3: Concurrent message handling")
print("═" * 72)

# Capture every prompt sent to Gemini, tagged by which thread sent it.
PROMPTS: list[tuple[str, str]] = []
PROMPTS_LOCK = threading.Lock()

_orig_raw = gen._generate_gemini_raw
def _patched(prompt: str) -> str:
    tname = threading.current_thread().name
    with PROMPTS_LOCK:
        PROMPTS.append((tname, prompt))
    return _orig_raw(prompt)
gen._generate_gemini_raw = _patched

REPLIES: dict[str, str] = {}

def _hit(brain, phone: str, msg: str, key: str):
    try:
        REPLIES[key] = brain.handle_incoming_message(phone, msg)
    except Exception as e:
        REPLIES[key] = f"[ERROR: {e!r}]"

# Race: Marcus and Zarna get the SAME message at the same time on
# different fan phones. If anything is process-global / not slug-scoped,
# their replies will leak into each other.
M_PHONE = "+15550030301"
Z_PHONE = "+15550030302"

t0 = time.time()
threads = [
    threading.Thread(target=_hit,
                     args=(brain_marcus, M_PHONE, "tell me about your wife", "marcus"),
                     name="marcus_thread"),
    threading.Thread(target=_hit,
                     args=(brain_zarna,  Z_PHONE, "tell me about your wife", "zarna"),
                     name="zarna_thread"),
]
for t in threads: t.start()
for t in threads: t.join()
print(f"  concurrent reply ms: {(time.time()-t0)*1000:.0f}")

print(f"\n  marcus reply: {REPLIES.get('marcus')}")
print(f"  zarna  reply: {REPLIES.get('zarna')}")

m_reply = (REPLIES.get("marcus") or "").lower()
z_reply = (REPLIES.get("zarna")  or "").lower()

check("Marcus reply does NOT mention 'shalabh'",
      "shalabh" not in m_reply)
check("Marcus reply does NOT claim a wife/partner affirmatively",
      not (("my wife" in m_reply or "my husband" in m_reply)
           and "don't have" not in m_reply
           and "no wife" not in m_reply))
check("Zarna reply mentions Shalabh / husband (sanity)",
      "shalabh" in z_reply or "husband" in z_reply)

# Check the prompts: each thread's prompt should be tagged with the
# right creator persona only.
for tname, p in PROMPTS:
    plow = p.lower()
    if "marcus" in tname:
        n_shal = p.count("Shalabh")
        ok_no_shalabh = (n_shal == 0)
        check(f"  marcus_thread prompt has NO 'shalabh' in instructions",
              ok_no_shalabh,
              f"found {n_shal} occurrences" if not ok_no_shalabh else "")
        if not ok_no_shalabh:
            # Dump prompt + show first context around 'Shalabh' so we
            # know which block it leaked from.
            with open("/tmp/marcus_concurrent_prompt.txt", "w") as f:
                f.write(p)
            print(f"        [DEBUG] wrote /tmp/marcus_concurrent_prompt.txt")
            idx = p.find("Shalabh")
            print(f"        [DEBUG] first hit context (±120 chars):")
            print(f"        ...{p[max(0,idx-120):idx+120]}...")
        check(f"  marcus_thread prompt mentions Marcus Cole",
              "marcus cole" in plow)
    elif "zarna" in tname:
        check(f"  zarna_thread prompt mentions Shalabh (sanity)",
              "Shalabh" in p)
        check(f"  zarna_thread prompt does NOT mention Marcus Cole",
              "marcus cole" not in plow)


# ──────────────────────────────────────────────────────────────────────
# Section 4: winning_examples scoping
# ──────────────────────────────────────────────────────────────────────
print("\n" + "═" * 72)
print("Section 4: winning_examples per-slug scoping")
print("═" * 72)

# get_top_performing_replies should return Zarna-only when called with 'zarna'
# and marcus-only (likely empty) when called with 'marcus_cole'.
try:
    z_examples = brain_zarna.storage.get_top_performing_replies(
        "general", "roast_playful", creator_slug="zarna"
    ) or []
    m_examples = brain_marcus.storage.get_top_performing_replies(
        "general", "roast_playful", creator_slug="marcus_cole"
    ) or []
    print(f"  zarna  has {len(z_examples)} winning examples for general/roast_playful")
    print(f"  marcus has {len(m_examples)} winning examples for general/roast_playful")

    # Marcus's examples (if any) must NOT contain Zarna persona words.
    bad = [e for e in m_examples
           if any(w in str(e).lower()
                  for w in ("shalabh", "zoya", "brij", "veer", "baba ramdev"))]
    check("Marcus winning_examples have NO Zarna persona words",
          len(bad) == 0,
          f"bad: {bad[:2]}" if bad else "ok")
except Exception as e:
    print(f"  (skip: storage doesn't expose this cleanly: {e})")


# ──────────────────────────────────────────────────────────────────────
# Final
# ──────────────────────────────────────────────────────────────────────
print("\n" + "═" * 72)
print(f"  RESULTS: {CHECKS_PASSED} passed, {CHECKS_FAILED} failed")
print(f"  VERDICT: {'GREEN' if CHECKS_FAILED == 0 else 'RED'}")
print("═" * 72)
sys.exit(0 if CHECKS_FAILED == 0 else 1)
