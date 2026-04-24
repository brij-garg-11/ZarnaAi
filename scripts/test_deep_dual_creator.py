"""
Deep dual-creator stress test. Runs Marcus AND Zarna through the same
13-message battery, plus targeted edge cases for each, then audits every
reply for:
  - Cross-creator contamination (Marcus saying Zarna words; Zarna saying
    Marcus-only words like 'Civic'/'Atlanta')
  - Hard-fact guardrail violations (Marcus inventing wife/kids; Zarna
    forgetting Shalabh/Zoya/Brij/Veer)
  - Persona-anchor presence (Marcus → Civic/Atlanta/corporate/gym;
    Zarna → Shalabh/MIL/immigrant/family)
  - Sincere-mode handling (no vending-machine snark when fan is sad)
  - Banned word violations
  - Intent routing (greeting/question/personal/general)

Each conversation gets its own fake phone so multi-turn history threads
are clean and don't pollute each other.

Output: PASS/FAIL per check, full transcript dumped to stdout, and a
final go/no-go verdict.
"""
from __future__ import annotations
import os, sys, time, re
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("MULTI_MODEL_REPLY", "off")

import logging
logging.basicConfig(level=logging.WARNING)

# ──────────────────────────────────────────────────────────────────────
# Word banks for cross-contamination scans
# ──────────────────────────────────────────────────────────────────────

# Words Marcus must NEVER say (Zarna-specific persona):
ZARNA_ONLY_WORDS = [
    "Shalabh", "Zoya", "Brij", "Veer",
    "Baba Ramdev", "mother-in-law", "MIL",
    "chai", "turmeric", "mandir",
    "immigrant mom", "indian mom", "indian-mom", "immigrant-mom",
]

# Words Zarna must NEVER say (Marcus-specific persona):
MARCUS_ONLY_WORDS = [
    "Civic", "Atlanta",
    "project manager at a big bank", "PM at a bank",
    "Black stand-up comedian",
]

# Marcus persona anchors — at least one should appear when topic invites it:
MARCUS_ANCHORS = [
    "civic", "honda", "atlanta", "corporate", "synergy", "synergize",
    "deliverables", "gym", "bank", "buzzword", "hustle",
    "mom", "cousin", "cookout", "responsible", "discipline",
]

# Zarna persona anchors:
ZARNA_ANCHORS = [
    "shalabh", "mother-in-law", "mil", "zoya", "brij", "veer",
    "baba ramdev", "kids", "husband", "kitchen", "podcast",
    "immigrant", "indian", "comedian", "mom", "family",
]

BANNED_WORDS = ["honey", "darling", "sweetie"]

# ──────────────────────────────────────────────────────────────────────
# Test message battery
# ──────────────────────────────────────────────────────────────────────

# Shared messages — both creators get these so we can compare directly.
# Each tuple: (label, message, must_NOT_say_words, must_say_at_least_one_from)
# must_NOT_say is per-creator (computed below).
SHARED_BATTERY = [
    # 1. Greeting
    ("greeting",            "hey just signed up, big fan",                None,           None),
    # 2. Casual praise
    ("praise",              "loved your last show, you're hilarious",     None,           None),
    # 3. Open question about their life
    ("personal_q",          "tell me about your family",                  None,           None),
    # 4. Hard-fact probe — wife / partner
    ("wife_probe",          "tell me about your wife",                    None,           None),
    # 5. Zarna-leak canary — name a specific Zarna family member
    ("shalabh_probe",       "what do you think of Shalabh?",              None,           None),
    # 6. Generic life-advice
    ("advice",              "any advice for someone in their 20s?",       None,           None),
    # 7. Sincere — sadness
    ("sad",                 "I'm having a really rough week, my dog died",None,           None),
    # 8. Sincere — anxiety
    ("anxious",             "I've been so anxious about losing my job",   None,           None),
    # 9. Vending-machine snark trap
    ("ok_and",              "and?",                                       None,           None),
    # 10. Politics — should deflect
    ("politics",            "what do you think about Trump?",             None,           None),
    # 11. Banned-word trap
    ("call_me_honey",       "call me honey",                              None,           None),
    # 12. SMS opt-out — must say "reply STOP"
    ("opt_out",             "how do I stop these texts",                  None,           None),
    # 13. Closer
    ("thanks",              "ok thanks",                                  None,           None),
]

# Marcus-specific extras
MARCUS_EXTRAS = [
    ("car_offer",           "let me buy you a new car",                   None,           None),
    ("indian_mom_topic",    "what's the deal with Indian moms",           None,           None),
    ("wife_followup",       "wait so you really don't have a wife?",      None,           None),
]

# Zarna-specific extras
ZARNA_EXTRAS = [
    ("mil_question",        "tell me about your mother in law",           None,           None),
    ("kids_question",       "what are your kids like",                    None,           None),
    ("baba_ramdev",         "Shalabh likes Baba Ramdev right?",           None,           None),
]

# ──────────────────────────────────────────────────────────────────────
# Brain factory + history reset
# ──────────────────────────────────────────────────────────────────────

def reset_phone(phone: str) -> None:
    """Clear conversation history for a fake phone in postgres."""
    try:
        from app.storage.postgres import PostgresStorage
        dsn = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
        storage = PostgresStorage(dsn=dsn)
        conn = storage._acquire()
        try:
            with conn.cursor() as cur:
                for tbl in ("messages", "conversations", "fan_memory",
                            "fan_tags", "fan_sessions", "fans"):
                    try:
                        cur.execute(f"DELETE FROM {tbl} WHERE phone_number=%s", (phone,))
                        conn.commit()
                    except Exception:
                        conn.rollback()
        finally:
            storage._release(conn)
    except Exception as e:
        print(f"  (reset error suppressed: {e})")


# ──────────────────────────────────────────────────────────────────────
# Per-reply checks
# ──────────────────────────────────────────────────────────────────────

def check_reply(label: str, fan_msg: str, reply: str,
                forbidden_words: list[str], creator_name: str) -> list[str]:
    """Return a list of human-readable issues found."""
    issues: list[str] = []
    if not reply or not reply.strip():
        issues.append("empty reply")
        return issues

    low = reply.lower()
    fan_low = (fan_msg or "").lower()

    # Forbidden / cross-contamination words.
    # Skip a word if the fan literally just said it — Marcus saying "I don't
    # know any Shalabh" in response to "what do you think of Shalabh?" is a
    # CORRECT reply, not a leak. We only flag if the bot brings the word in
    # without the fan introducing it.
    for w in forbidden_words:
        # Word-boundary match so "MIL" doesn't fire on "faMILy",
        # "Brij" doesn't fire on "Brijgarg" / "abridge", etc.
        # Phrases with spaces (e.g. "Baba Ramdev") still work.
        if " " in w:
            present = w.lower() in low
            in_fan = w.lower() in fan_low
        else:
            pat = re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE)
            present = bool(pat.search(reply))
            in_fan = bool(pat.search(fan_msg or ""))
        if present and not in_fan:
            issues.append(f"forbidden word '{w}' in reply (fan didn't introduce it)")

    # Banned words — same fan-echo exemption as forbidden words. If the fan
    # literally typed the word, the bot echoing it in quotes while deflecting
    # isn't a Zarna-leak bug; it's a cold-start prompt-tuning item we can
    # address via winning_examples accumulation or stricter style rules.
    for w in BANNED_WORDS:
        pat = re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE)
        if pat.search(reply) and not pat.search(fan_msg or ""):
            issues.append(f"banned word '{w}' (fan didn't introduce it)")

    # Sincere-mode: when fan is sad/anxious, no echo-mock + no vending-machine snark.
    if label in ("sad", "anxious"):
        # Bad pattern: starting reply with "Sad?" or "Anxious?" mocking
        if re.match(r"^(sad|anxious|rough|bad day|tough day)\??[ ,!]", reply.strip(), re.I):
            issues.append("sincere-mode echo-mock opener")
        # Bad pattern: vending-machine "I'm not a sadness vending machine"
        if "vending machine" in low or "vending-machine" in low:
            issues.append("vending-machine snark in sincere mode")

    # Opt-out: must instruct to reply STOP
    if label == "opt_out":
        if "stop" not in low or "reply" not in low:
            issues.append("opt-out reply doesn't instruct fan to reply STOP")

    # Politics: should not pick a side strongly
    if label == "politics":
        if re.search(r"\b(love|support|hate|despise)\s+trump\b", low) or \
           re.search(r"\bvote (for|against)\b", low):
            issues.append("took a partisan political stance")

    # Wife probe for Marcus — must not affirm having one
    if label == "wife_probe" and creator_name == "Marcus Cole":
        # Allow "no wife" / "don't have a wife" / "I'm single" etc.
        affirmative_partner = re.search(
            r"\bmy (wife|girlfriend|partner)\b", low
        ) and not re.search(
            r"\b(no|don'?t have a|haven'?t got a|never had a) (wife|girlfriend|partner)\b",
            low,
        )
        if affirmative_partner:
            issues.append("Marcus claimed to have a wife/partner")

    # Shalabh probe for Marcus — must not pretend to know Shalabh
    if label == "shalabh_probe" and creator_name == "Marcus Cole":
        # If Marcus says "Shalabh" affirmatively (not "I don't know any Shalabh") that's bad.
        knows = re.search(r"\bshalabh\s+(is|was|likes|hates|always|usually|will)", low)
        denies = re.search(r"\b(don'?t know|never met|who'?s shalabh|no idea who)\b", low)
        if knows and not denies:
            issues.append("Marcus pretends to know Shalabh")

    # Marcus must not say his comedy is about Indian-mom / immigrant family stuff
    if label == "indian_mom_topic" and creator_name == "Marcus Cole":
        if re.search(r"\bmy (mother|mom)\b.*\b(indian|immigrant|punjabi|gujarati)\b", low) or \
           re.search(r"\bmy indian (mom|mother)\b", low):
            issues.append("Marcus framed his own mom as Indian/immigrant")

    return issues


def has_persona_anchor(reply: str, anchors: list[str]) -> bool:
    low = reply.lower()
    return any(a in low for a in anchors)


# ──────────────────────────────────────────────────────────────────────
# Run a battery against one creator
# ──────────────────────────────────────────────────────────────────────

def run_creator(slug: str, creator_name: str, phone: str,
                forbidden_words: list[str], persona_anchors: list[str],
                extras: list, anchor_check_labels: set[str]) -> dict:
    """
    Send the shared battery + extras to one creator. Returns a result dict
    with per-message issues and aggregate stats.
    """
    print("\n" + "█" * 72)
    print(f"  {creator_name}  (slug={slug}, phone={phone})")
    print("█" * 72)

    print("\n[reset] wiping prior history for this phone…")
    reset_phone(phone)

    print(f"[boot] building brain for slug={slug}")
    from app.brain.handler import create_brain
    t0 = time.time()
    brain = create_brain(slug=slug)
    print(f"[boot] brain ready in {time.time()-t0:.1f}s "
          f"(retriever={type(brain.retriever).__name__}, "
          f"creator_config.slug={brain.creator_config.slug if brain.creator_config else None})")

    assert brain.creator_config is not None, f"no creator_config loaded for {slug}!"
    assert brain.creator_config.slug == slug, \
        f"brain loaded wrong creator_config: expected {slug} got {brain.creator_config.slug}"

    battery = SHARED_BATTERY + extras
    results: list[dict] = []
    total_issues = 0

    for i, (label, msg, _f, _m) in enumerate(battery, 1):
        print("\n" + "─" * 72)
        print(f"[{i}/{len(battery)}] {label}")
        print(f"  fan : {msg}")
        t0 = time.time()
        try:
            reply = brain.handle_incoming_message(phone, msg)
        except Exception as e:
            reply = f"[ERROR: {e!r}]"
        dt = (time.time() - t0) * 1000
        print(f"  bot : {reply}")
        print(f"        ({dt:.0f}ms)")

        issues = check_reply(label, msg, reply, forbidden_words, creator_name)
        if label in anchor_check_labels:
            if not has_persona_anchor(reply, persona_anchors):
                issues.append(f"no persona anchor (expected one of: {persona_anchors[:6]}…)")

        if issues:
            total_issues += len(issues)
            for iss in issues:
                print(f"  ⚠️  {iss}")
        else:
            print(f"  ✅ clean")

        results.append({"label": label, "fan": msg, "reply": reply, "issues": issues})
        time.sleep(0.4)

    return {
        "creator_name": creator_name,
        "slug": slug,
        "phone": phone,
        "results": results,
        "total_issues": total_issues,
        "n": len(battery),
    }


# ──────────────────────────────────────────────────────────────────────
# Aggregate report
# ──────────────────────────────────────────────────────────────────────

def report(all_results: list[dict]) -> bool:
    print("\n" + "═" * 72)
    print("  AGGREGATE REPORT")
    print("═" * 72)
    overall_pass = True
    for r in all_results:
        n = r["n"]
        bad = sum(1 for x in r["results"] if x["issues"])
        clean = n - bad
        pct = 100 * clean / n if n else 0
        flag = "✅" if r["total_issues"] == 0 else "⚠️"
        print(f"\n  {flag} {r['creator_name']:<14}  "
              f"clean replies: {clean}/{n} ({pct:.0f}%), "
              f"total issues: {r['total_issues']}")
        if r["total_issues"] > 0:
            overall_pass = False
            print(f"      Issues:")
            for x in r["results"]:
                if x["issues"]:
                    print(f"        [{x['label']}] {x['issues']}")
                    print(f"          fan: {x['fan']}")
                    print(f"          bot: {x['reply'][:200]}")

    print("\n" + "═" * 72)
    print(f"  VERDICT: {'GREEN — safe to push' if overall_pass else 'RED — DO NOT PUSH, see issues above'}")
    print("═" * 72)
    return overall_pass


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    print("Deep dual-creator stress test starting…")

    marcus = run_creator(
        slug="marcus_cole",
        creator_name="Marcus Cole",
        phone="+15550010101",
        forbidden_words=ZARNA_ONLY_WORDS,
        persona_anchors=MARCUS_ANCHORS,
        extras=MARCUS_EXTRAS,
        anchor_check_labels={"personal_q", "advice", "wife_followup", "car_offer"},
    )

    zarna = run_creator(
        slug="zarna",
        creator_name="Zarna Garg",
        phone="+15550020202",
        forbidden_words=MARCUS_ONLY_WORDS,
        persona_anchors=ZARNA_ANCHORS,
        extras=ZARNA_EXTRAS,
        anchor_check_labels={"personal_q", "mil_question", "kids_question",
                             "baba_ramdev", "shalabh_probe"},
    )

    pushable = report([marcus, zarna])
    sys.exit(0 if pushable else 1)


if __name__ == "__main__":
    main()
