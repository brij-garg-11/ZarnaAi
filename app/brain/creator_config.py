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

def _load_from_db(slug: str) -> Optional[dict]:
    """
    Pull the config_json blob from the creator_configs Postgres table.

    This is the path that matters for dynamically provisioned creators:
    their config is written to Postgres by operator/app/provisioning/
    config_writer.py and never lands on disk. Zarna's legacy file still
    wins when present (see load_creator below) so her deploy is unchanged
    if DATABASE_URL or the table happens to be unavailable.

    Returns None on any failure (missing env var, connection error, no
    matching row, malformed JSON) — caller falls back to hardcoded defaults.
    """
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        return None
    try:
        import psycopg2
    except ImportError:
        _LOGGER.debug("CreatorConfig[%s]: psycopg2 unavailable — skipping DB lookup", slug)
        return None
    try:
        conn = psycopg2.connect(dsn.replace("postgres://", "postgresql://", 1))
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT config_json FROM creator_configs WHERE creator_slug = %s",
                    (slug,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception as exc:
        _LOGGER.warning(
            "CreatorConfig[%s]: DB lookup failed (%s) — will fall back to file or defaults",
            slug, exc,
        )
        return None
    if not row or not row[0]:
        return None
    data = row[0]
    # psycopg2 normally returns JSONB as a dict already, but some setups
    # (older psycopg versions, missing extensions) return a str. Handle both.
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception as exc:
            _LOGGER.warning(
                "CreatorConfig[%s]: DB row had non-JSON config_json (%s)", slug, exc,
            )
            return None
    if not isinstance(data, dict):
        _LOGGER.warning(
            "CreatorConfig[%s]: DB config_json is not an object (got %s)",
            slug, type(data).__name__,
        )
        return None
    return data


def load_creator(slug: str) -> Optional[CreatorConfig]:
    """
    Load a CreatorConfig for the given slug.

    Lookup order:
      1. creator_config/<slug>.json on disk (Zarna's authoritative source
         today — unchanged behaviour for her deployment).
      2. creator_configs Postgres table (dynamically provisioned creators
         never have a file, their config only exists here).

    Returns None only if BOTH sources miss or fail — callers must then
    fall back to their hardcoded defaults. Debug/warning logs make the
    picked source obvious at startup.
    """
    path = os.path.join(_CONFIG_DIR, f"{slug}.json")
    data: Optional[dict] = None
    source = ""

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            source = f"file:{path}"
        except Exception as exc:
            _LOGGER.warning(
                "CreatorConfig[%s]: failed to parse %s (%s) — trying DB fallback",
                slug, path, exc,
            )

    if data is None:
        db_data = _load_from_db(slug)
        if db_data is not None:
            data = db_data
            source = "db:creator_configs"

    if data is None:
        _LOGGER.debug(
            "CreatorConfig[%s]: no file at %s and no DB row — callers will use hardcoded defaults",
            slug, path,
        )
        return None

    try:
        config = _build_from_dict(slug, data)
    except Exception as exc:
        _LOGGER.warning(
            "CreatorConfig[%s]: failed to build config from %s (%s) — using hardcoded defaults",
            slug, source, exc,
        )
        return None

    _LOGGER.debug(
        "CreatorConfig[%s]: loaded OK from %s — name=%r tickets=%r merch=%r book=%r youtube=%r "
        "name_variants=%d shalabh_names=%d mil_answers=%d family_roast_names=%d "
        "guardrails=%s voice_lock=%s style=%s tone_examples=%s",
        slug,
        source,
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
