import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.brain.intent import classify_intent, Intent
from app.brain.generator import generate_zarna_reply, _build_prompt, _format_history


def test_format_history_includes_anti_anchoring_guidance():
    # Empty history → no block at all.
    assert _format_history([]) == ""

    hist = [
        {"role": "assistant", "text": "I'm watching you!"},
        {"role": "user", "text": "Do you have any shows in Kentucky?"},
    ]
    out = _format_history(hist)
    # Still contains the actual turns...
    assert "I'm watching you!" in out
    assert "Do you have any shows in Kentucky?" in out
    # ...but now frames them as background context, not the thing to respond to.
    lowered = out.lower()
    assert "background context only" in lowered
    assert "current message" in lowered


def test_question_prompt_carries_history_guidance():
    # The bleed showed up in QUESTION/PERSONAL/GENERAL replies, so the guidance
    # must reach those prompts (they include the history block).
    prompt = _build_prompt(
        Intent.QUESTION,
        "what's your favorite food?",
        chunks=[],
        history=[{"role": "assistant", "text": "I'm watching you!"}],
    )
    assert "BACKGROUND CONTEXT ONLY" in prompt


def test_intent_classification():
    cases = [
        ("tell me a joke about Indian moms", Intent.JOKE),
        ("give me something funny", Intent.JOKE),
        ("can you recommend a clip", Intent.CLIP),
        ("where are her shows", Intent.SHOW),
        ("when is her next tour", Intent.SHOW),
        ("hi", Intent.GREETING),
        ("hey!", Intent.GREETING),
        ("hello", Intent.GREETING),
        ("good morning", Intent.GREETING),
        ("how are you", Intent.GREETING),
        ("I'm a retired teacher from Ohio", Intent.PERSONAL),
        ("I have 3 kids and live in New Jersey", Intent.PERSONAL),
        ("great show tonight!", Intent.FEEDBACK),
        ("you were amazing!", Intent.FEEDBACK),
        ("you killed it", Intent.FEEDBACK),
        ("lol", Intent.GENERAL),
        ("buy milk on the way home", Intent.GENERAL),
        ("where can I buy your book", Intent.BOOK),
    ]

    print("--- Intent Classification ---")
    all_passed = True
    for message, expected in cases:
        result = classify_intent(message)
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_passed = False
        print(f"{status} '{message}' → {result.value}  (expected: {expected.value})")

    if all_passed:
        print("\nAll intent tests passed.")
    else:
        print("\nSome intents were off — may need prompt tuning.")


def test_generation():
    sample_chunks = [
        "My husband once told me he needed a break from the kids. I said great, me too, so now we're both just sitting here waiting for someone else to show up.",
        "Every Indian mom has the same three sentences: eat something, call me back, and why aren't you a doctor yet.",
    ]

    print("\n--- Generation ---")

    for intent in [Intent.JOKE, Intent.GENERAL, Intent.SHOW, Intent.CLIP]:
        reply = generate_zarna_reply(
            intent=intent,
            user_message="give me something funny about family",
            chunks=sample_chunks,
        )
        print(f"\n[{intent.value.upper()}]\n{reply}")

    print("\nGeneration test complete.")


if __name__ == "__main__":
    test_intent_classification()
    test_generation()
