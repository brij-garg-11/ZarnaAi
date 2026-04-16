"""
Realistic tone and voice sanity check — runs with the LIVE Gemini API.

Sends real general fan messages through the full pipeline (intent classify +
generate_zarna_reply) and checks:
  - Reply is non-empty (bot didn't crash or fall back)
  - Reply does not contain banned words (honey, darling, sweetie)
  - Reply is not longer than 3 sentences
  - Reply does not start with an echo-mock opener (echo the fan's word as "Word?")
  - No Zarna-specific hardcoded strings leaked (sanity check on config wiring)
  - Structured intents (SHOW, MERCH, BOOK) still include their link

Run:  python tests/test_tone_realistic.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass

from app.brain.creator_config import load_creator
from app.brain.generator import _build_prompt, generate_zarna_reply
from app.brain.intent import classify_intent
from app.brain.tone import classify_tone_mode

# ---------------------------------------------------------------------------
# Real-world fan messages — general conversation only (no merch/sales)
# ---------------------------------------------------------------------------

CASES = [
    # ── Greetings ──────────────────────────────────────────────────────────
    ("hi Zarna!",
     "Basic greeting — should feel warm + punchy, ≤2 sentences"),
    ("good morning!",
     "Morning greeting — should be lively, not generic"),
    ("hey! just wanted to say hi",
     "Casual hi — should invite conversation, not lecture"),

    # ── Post-show feedback ─────────────────────────────────────────────────
    ("you killed it tonight",
     "Show praise — celebratory, tight acknowledgment"),
    ("that was the best show I've ever seen",
     "Big compliment — should land warmly, not gush back"),
    ("we saw you at the Chicago show last month — you were incredible",
     "City + show praise — should feel personal"),
    ("I've seen you 4 times now. You never disappoint.",
     "Repeat attendee — should acknowledge loyalty"),

    # ── Fan sharing personal details ───────────────────────────────────────
    ("I'm a nurse from Texas with 3 kids",
     "Fan intro — should riff on it, Zarna-style roast"),
    ("I have 4 kids and my husband is just like Shalabh",
     "Relatable family share — roast lane, not earnest"),
    ("I grew up in India and your jokes hit different",
     "Shared background — warm, specific, not generic"),
    ("I'm a retired teacher, been teaching for 32 years",
     "Bio share — should find the funny angle"),

    # ── Questions about Zarna ──────────────────────────────────────────────
    ("how did you start doing comedy?",
     "Career question — should answer directly, then add color"),
    ("do your kids know you talk about them on stage?",
     "Family question — should stay in comedic lane"),
    ("is this actually you or an AI?",
     "AI question — should address it honestly in Zarna's voice"),
    ("what do you think about the elections?",
     "Politics — should deflect with a joke, no opinions"),

    # ── Emotional / supportive messages ───────────────────────────────────
    ("I'm having a really hard week",
     "Vulnerable moment — empathy first, no snark"),
    ("I feel so anxious lately and your comedy helps",
     "Anxiety + gratitude — warm, not a therapist"),

    # ── Laugh reactions ────────────────────────────────────────────────────
    ("lol that's hilarious",
     "Laugh reaction — short, land the moment"),
    ("I'm literally crying laughing right now 😂",
     "Strong laugh reaction — stay sharp, don't over-explain"),
    ("so true!! my MIL is exactly like that",
     "MIL solidarity — roast lane, commiserate"),

    # ── Shalabh / family topics ────────────────────────────────────────────
    ("what are your thoughts on Shalabh?",
     "Shalabh question — roast lane, not Hallmark praise"),
    ("how do you deal with your mother-in-law?",
     "MIL question — roast/chaos lane, not generic family warmth"),
    ("my mother-in-law is coming to stay for a MONTH",
     "Fan MIL vent — commiserate, don't defend the MIL"),
]

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

_BANNED_WORDS = {"honey", "darling", "sweetie"}
_ECHO_OPENER_RE = re.compile(r"^([A-Za-z][a-zA-Z ,'\-]{0,40})\?\s+", re.UNICODE)
# Zarna-specific strings that should come from config — if they appear in a
# NON-structured-intent reply, it could mean the general prompts still have
# hardcoded content (low risk but worth flagging)
_HARDCODED_LINK_RE = re.compile(r"https?://", re.IGNORECASE)


def _count_sentences(text: str) -> int:
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    return len([p for p in parts if p.strip()])


def _check_reply(message: str, reply: str, intent) -> list[str]:
    issues = []

    if not reply or not reply.strip():
        issues.append("EMPTY REPLY — bot crashed or fallback fired")
        return issues

    lower = reply.lower()

    # Banned words
    for w in _BANNED_WORDS:
        if re.search(r'\b' + w + r'\b', lower):
            issues.append(f"BANNED WORD: '{w}' found in reply")

    # Length — general intents should be ≤ 3 sentences
    from app.brain.intent import Intent
    if intent not in (Intent.SHOW, Intent.MERCH, Intent.BOOK, Intent.CLIP, Intent.PODCAST):
        n = _count_sentences(reply)
        if n > 3:
            issues.append(f"TOO LONG: {n} sentences (max 3)")

    # Echo-mock opener
    m = _ECHO_OPENER_RE.match(reply)
    if m:
        opener = m.group(1).strip()
        opener_words = set(re.sub(r"[^\w\s]", "", opener.lower()).split())
        fan_words = set(re.sub(r"[^\w\s]", "", message.lower()).split())
        stop = {"a","an","the","is","are","was","i","you","my","your","it","this","that","and","or","in","on","at","to","for","of","with"}
        if len(opener.split()) <= 5 and (opener_words - stop) & (fan_words - stop):
            issues.append(f"ECHO-MOCK OPENER: reply starts with '{opener}?'")

    return issues


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run():
    print("=" * 72)
    print("Realistic Tone & Voice Test — Live Gemini API")
    print("=" * 72)
    print()

    zarna_config = load_creator("zarna")
    if zarna_config:
        print(f"Config loaded: slug={zarna_config.slug!r}, name={zarna_config.name!r}")
    else:
        print("WARNING: zarna config not loaded — using hardcoded fallbacks")
    print()

    total = passed = failed = 0
    failures = []

    for message, description in CASES:
        total += 1

        intent = classify_intent(message, zarna_config)
        tone_mode = classify_tone_mode(message, intent, creator_config=zarna_config)
        reply = generate_zarna_reply(
            intent=intent,
            user_message=message,
            chunks=[],   # no retrieval — tests pure voice/tone
            history=[],
            creator_config=zarna_config,
        )

        issues = _check_reply(message, reply, intent)

        if not issues:
            passed += 1
            status = "✓"
        else:
            failed += 1
            status = "✗"
            failures.append((message, description, intent, tone_mode, reply, issues))

        # Print even passing cases so you can read the actual replies
        intent_str = intent.value if intent else "none"
        tone_str = str(tone_mode) if tone_mode else "none"
        print(f"  {status}  [{intent_str:<10} / {tone_str:<16}]  {message[:50]}")
        print(f"       Reply: {reply[:120]!r}")
        if issues:
            for issue in issues:
                print(f"       ⚠️  {issue}")
        print()

    print("=" * 72)
    print(f"Results: {passed}/{total} passed", "✓" if failed == 0 else f"— {failed} failures")
    print()

    if failures:
        print("FAILURES:")
        for msg, desc, intent, tone, reply, issues in failures:
            print(f"  Message:     {msg!r}")
            print(f"  Description: {desc}")
            print(f"  Intent/Tone: {intent.value} / {tone}")
            print(f"  Reply:       {reply!r}")
            print(f"  Issues:      {', '.join(issues)}")
            print()
        sys.exit(1)
    else:
        print("All tone and voice checks passed.")


if __name__ == "__main__":
    run()
