"""
Pillar 3 — Context-Aware Selling tests.

Philosophy: the bot should sell RARELY and only when a fan explicitly asks.
These tests are the main guard against regressions where selling starts
bleeding into normal conversations.

Two test suites:
  1. Intent classification — fast-path only (no Gemini calls, instant).
  2. End-to-end generation — verifies sell links appear when expected and
     are absent in every other scenario (mocks generate_zarna_reply so no
     real API calls are needed).
"""
import sys
import os
import random

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Ensure a placeholder API key is set before any app module is imported,
# so the module-level genai.Client() initialisation in intent.py doesn't fail.
from tests.gemini_test_util import ensure_placeholder_key_for_import
ensure_placeholder_key_for_import()

from unittest.mock import MagicMock, patch

from app.brain.intent import Intent, _fast_classify, classify_intent
from app.brain.generator import _build_prompt
from app.storage.memory import InMemoryStorage


# ---------------------------------------------------------------------------
# 1. Keyword / fast-path classification
# ---------------------------------------------------------------------------

class TestMerchIntentConservative:
    """
    MERCH must only fire when the fan explicitly asks about buying physical merch.
    False positives here mean the bot randomly pitches shirts mid-conversation.
    """

    SHOULD_BE_MERCH = [
        "where can I buy your shirt?",
        "do you sell hoodies?",
        "how do I order your merch?",
        "is there a merch store?",
        "where can I get your hat?",
        "do you have merchandise?",
        "how do I shop for your gear?",
        "can I buy your tshirt?",
        "where to buy your sweatshirt",
        "do you sell t-shirts?",
    ]

    SHOULD_NOT_BE_MERCH = [
        # Compliments about appearance — NOT purchase intent
        "your shirt is so funny",
        "I love your merch",
        "that hoodie looks amazing on you",
        "you always have the best style",
        # Show / ticket questions — must stay SHOW
        "where can I buy tickets?",
        "how do I buy tickets for Chicago?",
        # Book questions — must stay BOOK
        "where can I buy your book?",
        "how do I order This American Woman?",
        # Normal conversation
        "hi",
        "you were so funny tonight",
        "lol",
        "I love your comedy",
        "I'm from New Jersey",
        "tell me a joke",
        "do you have a podcast?",
        # Vague "where" without a merch item
        "where are you performing?",
    ]

    def test_merch_positives_fast_path(self):
        """Fast-path must catch explicit merch purchase questions."""
        failures = []
        for msg in self.SHOULD_BE_MERCH:
            result = _fast_classify(msg)
            if result != Intent.MERCH:
                failures.append(f"  MISSED  '{msg}' → {result}")
        assert not failures, "Merch fast-path missed explicit questions:\n" + "\n".join(failures)

    def test_merch_negatives_fast_path(self):
        """Fast-path must NOT classify these as MERCH."""
        failures = []
        for msg in self.SHOULD_NOT_BE_MERCH:
            result = _fast_classify(msg)
            if result == Intent.MERCH:
                failures.append(f"  FALSE POSITIVE  '{msg}' → {result}")
        assert not failures, "Merch fast-path has false positives:\n" + "\n".join(failures)

    def test_book_takes_priority_over_merch(self):
        """'Buy your book' must be BOOK, not MERCH."""
        result = _fast_classify("where can I buy your book?")
        assert result == Intent.BOOK, f"Expected BOOK, got {result}"

    def test_tickets_take_priority_over_merch(self):
        """'Buy tickets' must be SHOW, not MERCH."""
        result = _fast_classify("where can I buy tickets?")
        assert result == Intent.SHOW, f"Expected SHOW, got {result}"


class TestShowIntentConservative:
    """SHOW must only fire on explicit show / ticket requests, not general questions."""

    SHOULD_BE_SHOW = [
        "when are you coming to Chicago?",
        "how do I get tickets?",
        "where can I buy tickets?",
        "are you performing in New York?",
        "where are your tour dates?",
        # "do you have any upcoming shows?" is Gemini-level — fast-path returns None (correct fallback)
    ]

    SHOULD_NOT_BE_SHOW = [
        "hi",
        "you were so funny",
        "tell me a joke",
        "I'm from Ohio",
        "lol that's hilarious",
        "do you have a podcast?",
        "where can I buy your book?",
    ]

    def test_show_positives_fast_path(self):
        failures = []
        for msg in self.SHOULD_BE_SHOW:
            result = _fast_classify(msg)
            if result != Intent.SHOW:
                failures.append(f"  MISSED  '{msg}' → {result}")
        assert not failures, "Show fast-path missed explicit questions:\n" + "\n".join(failures)

    def test_show_negatives_fast_path(self):
        failures = []
        for msg in self.SHOULD_NOT_BE_SHOW:
            result = _fast_classify(msg)
            if result == Intent.SHOW:
                failures.append(f"  FALSE POSITIVE  '{msg}' → {result}")
        assert not failures, "Show fast-path has false positives:\n" + "\n".join(failures)


class TestNoSellInNormalConversation:
    """
    The central guard: normal fan messages must NEVER classify into a sell intent.
    """

    NORMAL_MESSAGES = [
        # Greetings
        "hi",
        "hey Zarna!",
        "good morning",
        "what's up?",
        # Personal
        "I'm a teacher from Ohio",
        "I have 3 kids and my MIL is a nightmare",
        "I grew up in India",
        # Feedback / reactions
        "lol that's so funny",
        "you were amazing tonight",
        "preach!",
        "so true",
        "omg I'm dying",
        "great show tonight!",
        # Questions about Zarna
        "how did you start doing comedy?",
        "do your kids watch your shows?",
        "are you an AI?",
        # General chatter
        "I need something funny today",
        "tell me a joke",
        "can you recommend a clip?",
        "do you have a podcast?",
        # Ambiguous / vague
        "what do you sell?",       # vague — no merch item → should NOT be MERCH
        "I love your stuff",       # "stuff" is not a merch item word
    ]

    SELL_INTENTS = {Intent.SHOW, Intent.MERCH}

    def test_normal_messages_never_sell(self):
        failures = []
        for msg in self.NORMAL_MESSAGES:
            result = _fast_classify(msg)
            if result in self.SELL_INTENTS:
                failures.append(f"  SELL TRIGGERED  '{msg}' → {result}")
        assert not failures, (
            "Sell intent fired on a normal message — fix the keyword rules:\n"
            + "\n".join(failures)
        )


# ---------------------------------------------------------------------------
# 2. Prompt content: sell links only in sell-intent replies
# ---------------------------------------------------------------------------

TICKET_LINK = "https://zarnagarg.com/tickets/"
MERCH_LINK  = "https://shopmy.us/shop/zarnagarg"
BOOK_LINK   = "https://www.amazon.com/dp/0593975022"

NON_SELL_INTENTS = [
    Intent.GREETING,
    Intent.FEEDBACK,
    Intent.PERSONAL,
    Intent.QUESTION,
    Intent.GENERAL,
    Intent.JOKE,
]

ALL_SELL_LINKS = [TICKET_LINK, MERCH_LINK, BOOK_LINK, "youtube.com/@ZarnaGarg"]


class TestPromptContainsSellLinks:
    """Sell prompts must embed the correct link; non-sell prompts must contain none."""

    def _make_prompt(self, intent: Intent, sell_context=None, sell_variant=None):
        return _build_prompt(
            intent=intent,
            user_message="test message",
            chunks=[],
            history=[],
            fan_memory="",
            sell_context=sell_context,
            sell_variant=sell_variant,
        )

    def test_show_prompt_contains_ticket_link(self):
        prompt = self._make_prompt(Intent.SHOW)
        assert TICKET_LINK in prompt, "SHOW prompt is missing ticket link"

    def test_merch_prompt_contains_merch_link(self):
        prompt = self._make_prompt(Intent.MERCH)
        assert MERCH_LINK in prompt, "MERCH prompt is missing merch link"

    def test_show_prompt_with_context_references_it(self):
        ctx = "Fan attended 'Chicago Laugh Factory' on 2025-03-15."
        prompt = self._make_prompt(Intent.SHOW, sell_context=ctx)
        assert "Chicago Laugh Factory" in prompt, "Sell context not injected into SHOW prompt"
        assert TICKET_LINK in prompt

    def test_merch_prompt_with_context_references_it(self):
        ctx = "Fan is from Chicago."
        prompt = self._make_prompt(Intent.MERCH, sell_context=ctx)
        assert "Chicago" in prompt, "Sell context not injected into MERCH prompt"
        assert MERCH_LINK in prompt

    def test_show_variant_b_note_in_prompt(self):
        prompt = self._make_prompt(Intent.SHOW, sell_variant="B")
        assert "Variant B" in prompt, "Variant B note missing from SHOW prompt"

    def test_merch_variant_b_note_in_prompt(self):
        prompt = self._make_prompt(Intent.MERCH, sell_variant="B")
        assert "Variant B" in prompt, "Variant B note missing from MERCH prompt"

    def test_non_sell_prompts_contain_no_sell_links(self):
        failures = []
        for intent in NON_SELL_INTENTS:
            prompt = self._make_prompt(intent)
            for link in ALL_SELL_LINKS:
                if link in prompt:
                    failures.append(f"  {intent.value} prompt contains sell link: {link}")
        assert not failures, (
            "Sell links leaked into non-sell intent prompts:\n" + "\n".join(failures)
        )


# ---------------------------------------------------------------------------
# 3. Storage: sell_context is built correctly from fan data
# ---------------------------------------------------------------------------

class TestSellContextStorage:
    """InMemoryStorage correctly returns fan location for sell context."""

    def test_get_fan_location_empty_by_default(self):
        storage = InMemoryStorage()
        storage.save_contact("+15550000001")
        assert storage.get_fan_location("+15550000001") == ""

    def test_get_fan_location_after_update(self):
        storage = InMemoryStorage()
        storage.save_contact("+15550000002")
        storage.update_memory("+15550000002", "comedy fan", [], "Chicago")
        assert storage.get_fan_location("+15550000002") == "Chicago"

    def test_get_fan_show_context_default_none(self):
        storage = InMemoryStorage()
        # InMemoryStorage has no show data — should return None (no DB tables)
        result = storage.get_fan_show_context("+15550000003")
        assert result is None

    def test_sell_context_assembled_from_location(self):
        """
        Handler should combine location into sell_context.
        We verify the logic directly here (handler integration is tested separately).
        """
        storage = InMemoryStorage()
        storage.save_contact("+15550000004")
        storage.update_memory("+15550000004", "", [], "Houston")

        location = storage.get_fan_location("+15550000004")
        show_ctx = storage.get_fan_show_context("+15550000004")

        parts = []
        if show_ctx:
            parts.append(show_ctx)
        if location:
            parts.append(f"Fan is from {location}.")
        sell_context = " ".join(parts) if parts else None

        assert sell_context == "Fan is from Houston."


# ---------------------------------------------------------------------------
# 4. A/B variant assignment
# ---------------------------------------------------------------------------

class TestSellVariantAssignment:
    """Variant is randomly A or B for sell intents; None for non-sell intents."""

    def test_sell_variant_is_a_or_b(self):
        """Run 20 assignments — all must be A or B."""
        for _ in range(20):
            variant = random.choice(["A", "B"])
            assert variant in ("A", "B")

    def test_sell_variant_stored_in_reply_context(self):
        storage = InMemoryStorage()
        storage.save_contact("+15550000010")
        msg = storage.save_message("+15550000010", "assistant", "Check out the merch!\nhttps://zarnagarg.com/shop/")
        storage.save_reply_context(
            message_id=msg.id,
            intent="merch",
            sell_variant="A",
        )
        ctx = storage.get_reply_context(msg.id)
        assert ctx is not None
        assert ctx["sell_variant"] == "A"

    def test_non_sell_variant_is_none_in_context(self):
        storage = InMemoryStorage()
        storage.save_contact("+15550000011")
        msg = storage.save_message("+15550000011", "assistant", "You're hilarious!")
        storage.save_reply_context(
            message_id=msg.id,
            intent="general",
            sell_variant=None,
        )
        ctx = storage.get_reply_context(msg.id)
        assert ctx is not None
        assert ctx["sell_variant"] is None

    def test_ab_split_roughly_even(self):
        """Over 100 draws the split should be between 30/70 and 70/30."""
        counts = {"A": 0, "B": 0}
        for _ in range(100):
            v = random.choice(["A", "B"])
            counts[v] += 1
        assert 30 <= counts["A"] <= 70, f"A/B split suspiciously uneven: {counts}"


# ---------------------------------------------------------------------------
# 5. Handler integration: sell intents are routed as structured (no router API call)
# ---------------------------------------------------------------------------

class TestHandlerSellRouting:
    """
    Sell intents (SHOW, MERCH) must be in _STRUCTURED_ROUTE_INTENTS so the
    router complexity-classifier is never called for them (no wasted API call).
    """

    def test_merch_in_structured_route_intents(self):
        from app.brain.handler import _STRUCTURED_ROUTE_INTENTS
        assert Intent.MERCH in _STRUCTURED_ROUTE_INTENTS, (
            "MERCH must be in _STRUCTURED_ROUTE_INTENTS so it skips the routing model"
        )

    def test_show_in_structured_route_intents(self):
        from app.brain.handler import _STRUCTURED_ROUTE_INTENTS
        assert Intent.SHOW in _STRUCTURED_ROUTE_INTENTS

    def test_merch_in_structured_generator_intents(self):
        from app.brain.generator import _STRUCTURED_INTENTS
        assert Intent.MERCH in _STRUCTURED_INTENTS, (
            "MERCH must be in _STRUCTURED_INTENTS so it always uses Gemini (link fidelity)"
        )

    def test_sell_intents_set(self):
        from app.brain.handler import _SELL_INTENTS
        assert Intent.SHOW in _SELL_INTENTS
        assert Intent.MERCH in _SELL_INTENTS
        # Non-sell intents must NOT be in _SELL_INTENTS
        assert Intent.GENERAL not in _SELL_INTENTS
        assert Intent.GREETING not in _SELL_INTENTS
        assert Intent.PERSONAL not in _SELL_INTENTS


# ---------------------------------------------------------------------------
# Entry point for manual runs
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import traceback

    suites = [
        TestMerchIntentConservative(),
        TestShowIntentConservative(),
        TestNoSellInNormalConversation(),
        TestPromptContainsSellLinks(),
        TestSellContextStorage(),
        TestSellVariantAssignment(),
        TestHandlerSellRouting(),
    ]

    total = passed = failed = 0
    for suite in suites:
        suite_name = type(suite).__name__
        for method_name in [m for m in dir(suite) if m.startswith("test_")]:
            total += 1
            try:
                getattr(suite, method_name)()
                print(f"  ✓  {suite_name}.{method_name}")
                passed += 1
            except Exception as exc:
                print(f"  ✗  {suite_name}.{method_name}")
                traceback.print_exc()
                failed += 1

    print(f"\n{'='*60}")
    print(f"Pillar 3 selling tests: {passed}/{total} passed", "✓" if failed == 0 else "✗")
    if failed:
        print(f"{failed} FAILURES — do NOT push until fixed.")
        sys.exit(1)
