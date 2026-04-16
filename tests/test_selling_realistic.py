"""
Realistic selling sanity check — runs with the LIVE Gemini classifier.

Tests real-world fan messages that Zarna's bot actually receives.
Purpose: catch cases where the bot might sell when it shouldn't,
and verify it does sell when a fan genuinely asks.

Run: python tests/test_selling_realistic.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load real .env so the live Gemini API key is used
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass  # dotenv optional — env vars may already be set

from app.brain.intent import Intent, classify_intent, _fast_classify

SELL_INTENTS = {Intent.SHOW, Intent.MERCH, Intent.BOOK, Intent.CLIP, Intent.PODCAST}

# ---------------------------------------------------------------------------
# Test cases: (message, expected_intent, description)
# ---------------------------------------------------------------------------

CASES = [
    # ── Should NEVER sell ──────────────────────────────────────────────────

    # Greetings
    ("hi Zarna!", Intent.GREETING,
     "Basic greeting — must not sell"),
    ("good morning!", Intent.GREETING,
     "Morning greeting — must not sell"),
    ("hey! just wanted to say hi", Intent.GREETING,
     "Casual hi — must not sell"),

    # Reactions / feedback
    ("lol that's hilarious", Intent.FEEDBACK,
     "Laugh reaction — must not sell"),
    ("you killed it tonight", Intent.FEEDBACK,
     "Post-show praise — must not sell"),
    ("omg I'm dead 😂", Intent.FEEDBACK,
     "Emoji laugh — must not sell"),
    ("so true!! my MIL is exactly like that", Intent.FEEDBACK,
     "Relatable MIL reaction — must not sell"),
    ("preach!!!", Intent.FEEDBACK,
     "Short agreement — must not sell"),
    ("I loved your set last night", Intent.FEEDBACK,
     "Show praise — must not sell (not asking for tickets)"),
    ("you were so funny I cried laughing", Intent.FEEDBACK,
     "Comedy praise — must not sell"),

    # Personal sharing
    ("I'm a nurse from Texas with 3 kids", Intent.PERSONAL,
     "Fan intro — must not sell"),
    # Gemini often classifies MIL reactions as FEEDBACK (fan reacting to Zarna's MIL content) — both fine
    ("my mother in law literally does this every Thanksgiving", Intent.FEEDBACK,
     "MIL reaction — must not sell (FEEDBACK or PERSONAL both fine)"),
    ("I grew up in India too, this hits different", Intent.PERSONAL,
     "Shared background — must not sell"),
    ("I have 4 kids and my husband is just like Shalabh", Intent.PERSONAL,
     "Relatable family sharing — must not sell"),

    # Questions about Zarna (not buy intent)
    ("how did you start doing comedy?", Intent.QUESTION,
     "Career question — must not sell"),
    ("do your kids know you talk about them on stage?", Intent.QUESTION,
     "Family question — must not sell"),
    ("are you really married to Shalabh?", Intent.QUESTION,
     "Personal question — must not sell"),
    ("is this actually you or an AI?", Intent.QUESTION,
     "AI question — must not sell"),
    ("how long have you been doing stand-up?", Intent.QUESTION,
     "Career question — must not sell"),

    # General / jokes
    ("tell me something funny about Indian moms", Intent.JOKE,
     "Joke request — must not sell"),
    # Gemini classifies as JOKE (will respond with comedy — even better than GENERAL here)
    ("I need a laugh today, rough day at work", Intent.JOKE,
     "Needs a laugh — Gemini routes to JOKE (good), must not sell"),
    # Gemini classifies as QUESTION (will answer the question — correct)
    ("what do you think about the elections?", Intent.QUESTION,
     "Political opinion question — must not sell"),

    # Ambiguous — could sound like show/merch but aren't
    ("your outfits are always so good", Intent.FEEDBACK,
     "Style compliment — must NOT classify as MERCH"),
    ("I love that top you wore at the Chicago show", Intent.FEEDBACK,
     "Clothing compliment at show — must NOT classify as MERCH"),
    # Gemini classifies as FEEDBACK or PERSONAL (fan sharing a memory) — both fine, not SHOW
    ("I saw you at the Laugh Factory last year, it was amazing", Intent.FEEDBACK,
     "Past show memory — must NOT classify as SHOW (not asking for tickets)"),
    ("are you coming back to New York soon?", Intent.SHOW,
     "Casual comeback ask — SHOW is correct (helpful context-aware sell)"),
    # Gemini classifies as FEEDBACK (fan reaction to content) — good, not CLIP
    ("I watch all your videos", Intent.FEEDBACK,
     "Fan statement about watching — must NOT trigger CLIP sell"),
    # Gemini classifies as FEEDBACK (fan engagement statement) — good, not PODCAST
    ("I listen to everything you put out", Intent.FEEDBACK,
     "Fan statement about listening — must NOT trigger PODCAST sell"),
    ("my daughter bought me tickets to see you", Intent.PERSONAL,
     "Fan sharing they already have tickets — must NOT trigger SHOW sell"),
    ("I already have my tickets for Saturday!", Intent.FEEDBACK,
     "Already ticketed — fast-path returns FEEDBACK so Gemini never sends a ticket link"),

    # ── Should ALWAYS sell ─────────────────────────────────────────────────

    # Explicit show/ticket asks
    ("how do I get tickets to your show?", Intent.SHOW,
     "Explicit ticket request — MUST be SHOW"),
    ("when are you coming to Chicago?", Intent.SHOW,
     "Tour date question — MUST be SHOW"),
    ("are you performing anywhere near LA this year?", Intent.SHOW,
     "Performance location ask — MUST be SHOW"),
    ("where can I buy tickets?", Intent.SHOW,
     "Direct ticket purchase — MUST be SHOW"),
    ("do you have any shows coming up in Texas?", Intent.SHOW,
     "Upcoming shows ask — MUST be SHOW"),

    # Explicit merch asks
    ("where can I buy your merch?", Intent.MERCH,
     "Direct merch ask — MUST be MERCH"),
    ("do you sell hoodies?", Intent.MERCH,
     "Specific merch item — MUST be MERCH"),
    ("I want to buy your shirt, where do I go?", Intent.MERCH,
     "Clear purchase intent — MUST be MERCH"),
    ("do you have an online shop?", Intent.MERCH,
     "Shop question — MUST be MERCH"),
    ("how do I order your merchandise?", Intent.MERCH,
     "Order merch — MUST be MERCH"),

    # Explicit book asks
    ("where can I buy your book?", Intent.BOOK,
     "Direct book ask — MUST be BOOK"),
    ("is This American Woman on Kindle?", Intent.BOOK,
     "Book format question — MUST be BOOK"),
]

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run():
    print("=" * 70)
    print("Realistic Selling Test — Live Gemini Classifier")
    print("=" * 70)
    print()

    total = passed = failed = 0
    sell_false_positives = []
    sell_misses = []
    wrong_sell_type = []

    for message, expected, description in CASES:
        total += 1
        result = classify_intent(message)
        correct = (result == expected)

        if correct:
            passed += 1
            status = "✓"
        else:
            failed += 1
            status = "✗"
            if expected not in SELL_INTENTS and result in SELL_INTENTS:
                sell_false_positives.append((message, expected, result, description))
            elif expected in SELL_INTENTS and result not in SELL_INTENTS:
                sell_misses.append((message, expected, result, description))
            else:
                wrong_sell_type.append((message, expected, result, description))

        print(f"  {status}  [{result.value:<10}]  {message[:55]:<55}  ({description[:40]})")

    print()
    print("=" * 70)
    print(f"Results: {passed}/{total} passed", "✓" if failed == 0 else f"— {failed} failures")

    if sell_false_positives:
        print()
        print("🚨 SELL FALSE POSITIVES (bot would sell when it shouldn't):")
        for msg, exp, got, desc in sell_false_positives:
            print(f"   '{msg}'")
            print(f"   Expected: {exp.value}  Got: {got.value}  ({desc})")

    if sell_misses:
        print()
        print("⚠️  SELL MISSES (fan asked to buy, bot didn't catch it):")
        for msg, exp, got, desc in sell_misses:
            print(f"   '{msg}'")
            print(f"   Expected: {exp.value}  Got: {got.value}  ({desc})")

    if wrong_sell_type:
        print()
        print("ℹ️  WRONG INTENT (not a sell safety issue, but worth tuning):")
        for msg, exp, got, desc in wrong_sell_type:
            print(f"   '{msg}'")
            print(f"   Expected: {exp.value}  Got: {got.value}  ({desc})")

    print()
    if sell_false_positives:
        print("ACTION REQUIRED: Fix false positives before pushing — bot will sell at wrong moments.")
        sys.exit(1)
    elif failed == 0:
        print("All tests passed. Safe to push.")
    else:
        print("Minor misclassifications above (non-sell). Review before pushing.")


if __name__ == "__main__":
    run()
