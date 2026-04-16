"""
Tests for the multi-creator platform abstraction.

Four test layers:
  1. Config loading — valid file, missing file, partial file, bad JSON
  2. Golden output — Zarna config produces prompts with Zarna's links & name
  3. Bleed-through — test_creator config produces prompts with NO Zarna strings
  4. Tone classification — config-aware family-roast regex fires on the right terms
"""
import os
import sys

# Ensure the module-level GEMINI_API_KEY is set before any app imports.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import pytest

from app.brain.creator_config import CreatorConfig, CreatorLinks, load_creator
from app.brain.generator import _build_prompt
from app.brain.intent import Intent, classify_intent
from app.brain.tone import classify_tone_mode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _zarna_config() -> CreatorConfig:
    cfg = load_creator("zarna")
    assert cfg is not None, "creator_config/zarna.json must exist and parse correctly"
    return cfg


def _test_creator_config() -> CreatorConfig:
    cfg = load_creator("test_creator")
    assert cfg is not None, "creator_config/test_creator.json must exist and parse correctly"
    return cfg


def _prompt_for_intent(intent: Intent, cfg: CreatorConfig) -> str:
    return _build_prompt(
        intent=intent,
        user_message="test message",
        chunks=[],
        history=[],
        creator_config=cfg,
    )


# ---------------------------------------------------------------------------
# 1. Config loading
# ---------------------------------------------------------------------------

class TestCreatorConfigLoading:
    def test_zarna_config_loads(self):
        cfg = load_creator("zarna")
        assert cfg is not None
        assert cfg.slug == "zarna"
        assert cfg.name == "Zarna Garg"

    def test_zarna_links_populated(self):
        cfg = _zarna_config()
        assert cfg.links.tickets == "https://zarnagarg.com/tickets/"
        assert cfg.links.merch == "https://shopmy.us/shop/zarnagarg"
        assert cfg.links.book == "https://www.amazon.com/dp/0593975022"
        assert cfg.links.youtube == "https://www.youtube.com/@ZarnaGarg"
        assert cfg.links.book_title == "This American Woman"

    def test_zarna_name_variants_populated(self):
        cfg = _zarna_config()
        assert len(cfg.name_variants) > 0
        assert "zara" in cfg.name_variants
        assert "varna" in cfg.name_variants

    def test_zarna_shalabh_names_populated(self):
        cfg = _zarna_config()
        assert len(cfg.shalabh_names) > 0
        assert "shalabh" in cfg.shalabh_names

    def test_zarna_mil_answers_populated(self):
        cfg = _zarna_config()
        assert len(cfg.mil_answers) > 0
        assert any("mother in law" in m for m in cfg.mil_answers)

    def test_zarna_family_roast_names_populated(self):
        cfg = _zarna_config()
        assert len(cfg.family_roast_names) > 0
        assert "shalabh" in cfg.family_roast_names

    def test_missing_slug_returns_none(self):
        cfg = load_creator("nonexistent_creator_xyz_12345")
        assert cfg is None

    def test_test_creator_loads(self):
        cfg = _test_creator_config()
        assert cfg is not None
        assert cfg.slug == "test_creator"
        assert cfg.name == "Alex Rivera"

    def test_test_creator_links_differ_from_zarna(self):
        zarna = _zarna_config()
        test_c = _test_creator_config()
        assert test_c.links.tickets != zarna.links.tickets
        assert test_c.links.merch != zarna.links.merch
        assert test_c.links.book != zarna.links.book
        assert test_c.links.youtube != zarna.links.youtube


# ---------------------------------------------------------------------------
# 2. Golden output — Zarna prompts must contain Zarna's links and name
# ---------------------------------------------------------------------------

class TestGoldenOutputZarna:
    """Verify that Zarna's config produces prompts with the expected links/name.
    These tests act as a regression guard — if a link changes in zarna.json,
    these will catch it before it reaches production."""

    def test_show_prompt_contains_zarna_tickets(self):
        prompt = _prompt_for_intent(Intent.SHOW, _zarna_config())
        assert "zarnagarg.com/tickets" in prompt, f"Expected Zarna ticket link in SHOW prompt:\n{prompt}"

    def test_merch_prompt_contains_zarna_merch(self):
        prompt = _prompt_for_intent(Intent.MERCH, _zarna_config())
        assert "shopmy.us/shop/zarnagarg" in prompt, f"Expected Zarna merch link in MERCH prompt:\n{prompt}"

    def test_book_prompt_contains_zarna_book(self):
        prompt = _prompt_for_intent(Intent.BOOK, _zarna_config())
        assert "amazon.com/dp/0593975022" in prompt, f"Expected Zarna book link in BOOK prompt:\n{prompt}"
        assert "This American Woman" in prompt, f"Expected book title in BOOK prompt:\n{prompt}"

    def test_clip_prompt_contains_zarna_youtube(self):
        prompt = _prompt_for_intent(Intent.CLIP, _zarna_config())
        assert "youtube.com/@ZarnaGarg" in prompt, f"Expected Zarna YouTube link in CLIP prompt:\n{prompt}"

    def test_podcast_prompt_contains_zarna_youtube(self):
        prompt = _prompt_for_intent(Intent.PODCAST, _zarna_config())
        assert "youtube.com/@ZarnaGarg" in prompt, f"Expected Zarna YouTube link in PODCAST prompt:\n{prompt}"

    def test_show_prompt_contains_creator_name(self):
        prompt = _prompt_for_intent(Intent.SHOW, _zarna_config())
        assert "Zarna Garg" in prompt

    def test_merch_prompt_contains_creator_name(self):
        prompt = _prompt_for_intent(Intent.MERCH, _zarna_config())
        assert "Zarna Garg" in prompt

    def test_book_prompt_contains_creator_name(self):
        prompt = _prompt_for_intent(Intent.BOOK, _zarna_config())
        assert "Zarna Garg" in prompt

    def test_zarna_guardrails_contain_shalabh(self):
        """Zarna's prompts must still contain her family members from her config."""
        prompt = _prompt_for_intent(Intent.GENERAL, _zarna_config())
        assert "Shalabh" in prompt, "Zarna's guardrails must include Shalabh"

    def test_zarna_guardrails_contain_children(self):
        prompt = _prompt_for_intent(Intent.GENERAL, _zarna_config())
        assert "Zoya" in prompt or "Brij" in prompt or "Veer" in prompt, (
            "Zarna's guardrails must include at least one child's name"
        )

    def test_zarna_voice_lock_contains_baba_ramdev(self):
        prompt = _prompt_for_intent(Intent.QUESTION, _zarna_config())
        assert "Baba Ramdev" in prompt, "Zarna's voice-lock rules must include Baba Ramdev"


# ---------------------------------------------------------------------------
# 3. Bleed-through — test_creator prompts must NOT contain Zarna-specific strings
# ---------------------------------------------------------------------------

_ZARNA_STRINGS = [
    "zarnagarg.com",
    "shopmy.us/shop/zarnagarg",
    "amazon.com/dp/0593975022",
    "youtube.com/@ZarnaGarg",
    "This American Woman",
    "Zarna Garg",
]

# Zarna-specific personal biography terms that should NOT appear in any
# test_creator prompt now that guardrails/voice-lock/style/examples come from config.
_ZARNA_PERSONAL_STRINGS = [
    "Shalabh",
    "Zoya",
    "Baba Ramdev",
    "Indian-mom",
    "immigrant-family",
]

_SELL_AND_STRUCTURED_INTENTS = [
    Intent.SHOW,
    Intent.MERCH,
    Intent.BOOK,
    Intent.CLIP,
    Intent.PODCAST,
]

_ALL_INTENTS = [
    Intent.SHOW,
    Intent.MERCH,
    Intent.BOOK,
    Intent.CLIP,
    Intent.PODCAST,
    Intent.GREETING,
    Intent.FEEDBACK,
    Intent.QUESTION,
    Intent.PERSONAL,
    Intent.GENERAL,
    Intent.JOKE,
]


class TestBleedThroughTestCreator:
    """Verify that no Zarna-specific content leaks into a different creator's prompts."""

    @pytest.mark.parametrize("intent", _SELL_AND_STRUCTURED_INTENTS)
    def test_no_zarna_link_strings_in_structured_prompts(self, intent: Intent):
        cfg = _test_creator_config()
        prompt = _prompt_for_intent(intent, cfg)
        for zarna_str in _ZARNA_STRINGS:
            assert zarna_str not in prompt, (
                f"Zarna string '{zarna_str}' found in {intent.value} prompt for test_creator:\n{prompt[:500]}"
            )

    @pytest.mark.parametrize("intent", _ALL_INTENTS)
    def test_no_zarna_personal_strings_in_any_prompt(self, intent: Intent):
        """Shalabh, Zoya, Baba Ramdev etc. must not appear in any test_creator prompt."""
        cfg = _test_creator_config()
        prompt = _prompt_for_intent(intent, cfg)
        for zarna_str in _ZARNA_PERSONAL_STRINGS:
            assert zarna_str not in prompt, (
                f"Zarna personal string '{zarna_str}' found in {intent.value} prompt for test_creator.\n"
                f"This means guardrails/voice-lock/style/examples still use Zarna fallbacks.\n"
                f"Prompt excerpt: {prompt[:500]}"
            )

    def test_test_creator_show_prompt_has_its_own_link(self):
        cfg = _test_creator_config()
        prompt = _prompt_for_intent(Intent.SHOW, cfg)
        assert cfg.links.tickets in prompt

    def test_test_creator_merch_prompt_has_its_own_link(self):
        cfg = _test_creator_config()
        prompt = _prompt_for_intent(Intent.MERCH, cfg)
        assert cfg.links.merch in prompt

    def test_test_creator_book_prompt_has_its_own_link(self):
        cfg = _test_creator_config()
        prompt = _prompt_for_intent(Intent.BOOK, cfg)
        assert cfg.links.book in prompt
        assert cfg.links.book_title in prompt

    def test_test_creator_name_in_prompts(self):
        cfg = _test_creator_config()
        for intent in _SELL_AND_STRUCTURED_INTENTS:
            prompt = _prompt_for_intent(intent, cfg)
            assert cfg.name in prompt, f"Expected creator name '{cfg.name}' in {intent.value} prompt"

    def test_test_creator_guardrails_text_used(self):
        """The test_creator's own guardrails text must appear in its prompts."""
        cfg = _test_creator_config()
        assert cfg.hard_fact_guardrails_text, "test_creator must have hard_fact_guardrails_text set"
        prompt = _prompt_for_intent(Intent.GENERAL, cfg)
        # The first sentence of test_creator's guardrails should appear
        first_line = cfg.hard_fact_guardrails_text.strip().splitlines()[0]
        assert first_line in prompt, (
            f"test_creator guardrails text not found in GENERAL prompt.\n"
            f"Expected: {first_line!r}\nPrompt: {prompt[:500]}"
        )

    def test_test_creator_style_text_used(self):
        """The test_creator's own style rules must appear in its general prompts."""
        cfg = _test_creator_config()
        assert cfg.style_rules_text, "test_creator must have style_rules_text set"
        prompt = _prompt_for_intent(Intent.GENERAL, cfg)
        first_line = cfg.style_rules_text.strip().splitlines()[0]
        assert first_line in prompt, (
            f"test_creator style text not found in GENERAL prompt.\n"
            f"Expected: {first_line!r}\nPrompt: {prompt[:500]}"
        )


# ---------------------------------------------------------------------------
# 4. Tone classification — config-aware family-roast regex
# ---------------------------------------------------------------------------

class TestToneWithCreatorConfig:
    def test_zarna_config_roast_on_shalabh(self):
        cfg = _zarna_config()
        tone = classify_tone_mode("what does Shalabh think?", Intent.GENERAL, creator_config=cfg)
        assert tone == "roast_playful"

    def test_zarna_config_roast_on_mil(self):
        cfg = _zarna_config()
        tone = classify_tone_mode("my mother-in-law is coming over", Intent.GENERAL, creator_config=cfg)
        assert tone == "roast_playful"

    def test_test_creator_roast_on_its_family_term(self):
        cfg = _test_creator_config()
        # "marco" is in test_creator's family_roast_names
        tone = classify_tone_mode("what does Marco think about this?", Intent.GENERAL, creator_config=cfg)
        assert tone == "roast_playful"

    def test_test_creator_no_roast_on_shalabh(self):
        cfg = _test_creator_config()
        # "shalabh" is NOT in test_creator's family_roast_names
        tone = classify_tone_mode("what does Shalabh think?", Intent.GENERAL, creator_config=cfg)
        # Without "shalabh" triggering the regex, a question falls to direct_answer
        assert tone == "direct_answer"

    def test_no_config_fallback_roast_on_shalabh(self):
        # Without config, the hardcoded regex still fires on Shalabh
        tone = classify_tone_mode("what does Shalabh think?", Intent.GENERAL, creator_config=None)
        assert tone == "roast_playful"


# ---------------------------------------------------------------------------
# 5. Intent classify_intent accepts creator_config without breaking
# ---------------------------------------------------------------------------

class TestClassifyIntentWithConfig:
    def test_zarna_config_passed_does_not_break_fast_path(self):
        cfg = _zarna_config()
        # Fast-path messages should still be classified the same way
        assert classify_intent("ticket", cfg) == Intent.SHOW
        assert classify_intent("lol", cfg) == Intent.FEEDBACK
        assert classify_intent("hi", cfg) == Intent.GREETING

    def test_none_config_still_works(self):
        assert classify_intent("ticket", None) == Intent.SHOW
        assert classify_intent("lol", None) == Intent.FEEDBACK
