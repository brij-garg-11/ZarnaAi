"""
Tests for creator custom links (Item 3).

Covers the generator prompt-injection helper and the CreatorConfig parsing.
The operator API round-trip + sanitization is covered in
operator/tests/test_bot_data_my_bot.py.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

from app.brain.creator_config import CreatorConfig, CreatorLinks, _build_from_dict
from app.brain.generator import _build_prompt, _format_custom_links
from app.brain.intent import Intent


def _cfg(custom_links=()):
    return CreatorConfig(
        slug="cl",
        name="Test Creator",
        links=CreatorLinks(tickets="https://example.com/tickets/"),
        custom_links=tuple(custom_links),
    )


class TestFormatCustomLinks:
    def test_empty_when_no_config(self):
        assert _format_custom_links(None) == ""

    def test_empty_when_no_links(self):
        assert _format_custom_links(_cfg()) == ""

    def test_includes_label_url_and_when(self):
        block = _format_custom_links(_cfg([
            {"label": "Cooking Course", "url": "https://x/c", "when_to_send": "fan asks about cooking"},
        ]))
        assert "Cooking Course" in block
        assert "https://x/c" in block
        assert "fan asks about cooking" in block

    def test_link_without_when(self):
        block = _format_custom_links(_cfg([{"label": "Patreon", "url": "https://p/z"}]))
        assert "Patreon" in block and "https://p/z" in block

    def test_skips_malformed_rows(self):
        block = _format_custom_links(_cfg([
            {"label": "", "url": "https://x"},
            {"label": "X", "url": ""},
            "not-a-dict",
            {"label": "Good", "url": "https://good"},
        ]))
        assert "https://good" in block
        assert block.count("- ") == 1  # only the one valid row


class TestConfigParsing:
    def test_custom_links_parsed_from_dict(self):
        cfg = _build_from_dict("x", {
            "display_name": "X",
            "custom_links": [
                {"label": "A", "url": "https://a"},
                "junk",
                {"label": "B", "url": "https://b", "when_to_send": "when relevant"},
            ],
        })
        assert len(cfg.custom_links) == 2
        assert cfg.custom_links[0]["label"] == "A"


class TestGeneratorIntegration:
    def _prompt(self, intent, cfg):
        return _build_prompt(intent, "do you have a cooking course?", chunks=[], history=[],
                             creator_config=cfg)

    def test_general_prompt_includes_custom_links(self):
        cfg = _cfg([{"label": "Cooking Course", "url": "https://x/c",
                     "when_to_send": "fan asks about cooking"}])
        prompt = self._prompt(Intent.GENERAL, cfg)
        assert "Cooking Course" in prompt and "https://x/c" in prompt

    def test_question_prompt_includes_custom_links(self):
        cfg = _cfg([{"label": "Patreon", "url": "https://p/z"}])
        assert "https://p/z" in self._prompt(Intent.QUESTION, cfg)

    def test_personal_prompt_includes_custom_links(self):
        cfg = _cfg([{"label": "Patreon", "url": "https://p/z"}])
        assert "https://p/z" in self._prompt(Intent.PERSONAL, cfg)

    def test_no_custom_links_keeps_prompt_clean(self):
        prompt = self._prompt(Intent.GENERAL, _cfg())
        assert "Additional links" not in prompt

    def test_show_prompt_does_not_include_custom_links(self):
        # Structured SHOW replies stay focused on the ticket link only.
        cfg = _cfg([{"label": "Patreon", "url": "https://p/z"}])
        prompt = self._prompt(Intent.SHOW, cfg)
        assert "https://p/z" not in prompt
