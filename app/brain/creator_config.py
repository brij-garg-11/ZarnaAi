"""
Creator-specific configuration loader.

Each creator has a JSON config file in creator_config/<slug>.json.
The CreatorConfig dataclass holds all creator-specific values that were
previously hardcoded in generator.py, intent.py, and tone.py.

Safety: load_creator() returns None if the file is missing or malformed.
Every caller must fall back to its own hardcoded defaults so Zarna's
behaviour is completely unchanged if the config fails to load.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import FrozenSet, List, Optional, Tuple

_LOGGER = logging.getLogger(__name__)

# creator_config/ lives at the project root, two levels above this file
# (app/brain/creator_config.py → app/ → project root).
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CONFIG_DIR = os.path.join(_BASE_DIR, "creator_config")


@dataclass
class CreatorLinks:
    tickets: str = ""
    merch: str = ""
    book: str = ""
    youtube: str = ""
    book_title: str = ""
    book_phrases: Tuple[str, ...] = field(default_factory=tuple)


@dataclass
class CreatorConfig:
    slug: str
    name: str  # display name, e.g. "Zarna Garg"
    description: str = ""  # short bio used in the classification prompt
    voice_style: str = ""  # one-liner voice descriptor for prompts
    banned_words: Tuple[str, ...] = field(default_factory=tuple)

    # Intent classification helpers
    name_variants: FrozenSet[str] = field(default_factory=frozenset)
    shalabh_names: Tuple[str, ...] = field(default_factory=tuple)  # or equivalent "spouse name"
    mil_answers: Tuple[str, ...] = field(default_factory=tuple)    # quiz-answer phrases
    family_roast_names: Tuple[str, ...] = field(default_factory=tuple)  # tone.py regex terms

    links: CreatorLinks = field(default_factory=CreatorLinks)

    # Prompt text blocks — when non-empty, replace the Python constants in generator.py.
    # Empty string means "use the hardcoded Python fallback for this field."
    hard_fact_guardrails_text: str = ""
    voice_lock_rules_text: str = ""
    style_rules_text: str = ""
    tone_examples_text: str = ""


# ---------------------------------------------------------------------------
# Internal builder
# ---------------------------------------------------------------------------

def _build_from_dict(slug: str, data: dict) -> CreatorConfig:
    links_raw = data.get("links", {})
    links = CreatorLinks(
        tickets=links_raw.get("tickets", ""),
        merch=links_raw.get("merch", ""),
        book=links_raw.get("book", ""),
        youtube=links_raw.get("youtube", ""),
        book_title=links_raw.get("book_title", ""),
        book_phrases=tuple(links_raw.get("book_phrases", [])),
    )
    return CreatorConfig(
        slug=slug,
        name=data.get("display_name", data.get("name", slug)),
        description=data.get("description", ""),
        voice_style=data.get("voice_style", ""),
        banned_words=tuple(data.get("banned_words", [])),
        name_variants=frozenset(data.get("name_variants", [])),
        shalabh_names=tuple(data.get("shalabh_names", [])),
        mil_answers=tuple(data.get("mil_answers", [])),
        family_roast_names=tuple(data.get("family_roast_names", [])),
        links=links,
        hard_fact_guardrails_text=data.get("hard_fact_guardrails_text", ""),
        voice_lock_rules_text=data.get("voice_lock_rules_text", ""),
        style_rules_text=data.get("style_rules_text", ""),
        tone_examples_text=data.get("tone_examples_text", ""),
    )


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

def load_creator(slug: str) -> Optional[CreatorConfig]:
    """
    Load a CreatorConfig from creator_config/<slug>.json.

    Returns None if the file is not found or cannot be parsed — callers
    must fall back to their own hardcoded defaults in that case.
    Debug logs make it easy to trace which config was picked up at startup.
    """
    path = os.path.join(_CONFIG_DIR, f"{slug}.json")
    if not os.path.exists(path):
        _LOGGER.debug(
            "CreatorConfig[%s]: config file not found at %s — all callers will use hardcoded defaults",
            slug,
            path,
        )
        return None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:
        _LOGGER.warning(
            "CreatorConfig[%s]: failed to parse %s (%s) — using hardcoded defaults",
            slug,
            path,
            exc,
        )
        return None

    try:
        config = _build_from_dict(slug, data)
    except Exception as exc:
        _LOGGER.warning(
            "CreatorConfig[%s]: failed to build config from %s (%s) — using hardcoded defaults",
            slug,
            path,
            exc,
        )
        return None

    _LOGGER.debug(
        "CreatorConfig[%s]: loaded OK — name=%r tickets=%r merch=%r book=%r youtube=%r "
        "name_variants=%d shalabh_names=%d mil_answers=%d family_roast_names=%d "
        "guardrails=%s voice_lock=%s style=%s tone_examples=%s",
        slug,
        config.name,
        config.links.tickets,
        config.links.merch,
        config.links.book,
        config.links.youtube,
        len(config.name_variants),
        len(config.shalabh_names),
        len(config.mil_answers),
        len(config.family_roast_names),
        "yes" if config.hard_fact_guardrails_text else "fallback",
        "yes" if config.voice_lock_rules_text else "fallback",
        "yes" if config.style_rules_text else "fallback",
        "yes" if config.tone_examples_text else "fallback",
    )
    return config
