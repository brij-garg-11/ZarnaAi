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

    # Tour calendar (Item 1). bandsintown_artist enables the live Bandsintown API
    # lookup; upcoming_shows is the manual fallback list. Both optional — when
    # absent, SHOW replies use the generic links.tickets URL exactly as before.
    bandsintown_artist: str = ""
    upcoming_shows: Tuple[dict, ...] = field(default_factory=tuple)

    # Custom links (Item 3) — creator-defined links the AI may surface when
    # relevant. Each item: {"label": str, "url": str, "when_to_send": str}.
    custom_links: Tuple[dict, ...] = field(default_factory=tuple)

    # SMS profile + first message (Item 2). All optional and OFF by default, so a
    # creator with none of these set behaves exactly as before (no vCard, no
    # opt-in message). send_contact_card gates the vCard; first_message gates the
    # welcome text. profile_photo_url / sms_display_name feed the contact card.
    sms_display_name: str = ""
    profile_photo_url: str = ""
    send_contact_card: bool = False
    first_message: str = ""

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
        bandsintown_artist=data.get("bandsintown_artist", ""),
        upcoming_shows=tuple(s for s in data.get("upcoming_shows", []) if isinstance(s, dict)),
        custom_links=tuple(l for l in data.get("custom_links", []) if isinstance(l, dict)),
        sms_display_name=data.get("sms_display_name", ""),
        profile_photo_url=data.get("profile_photo_url", ""),
        send_contact_card=bool(data.get("send_contact_card", False)),
        first_message=data.get("first_message", ""),
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


# Fields the operator dashboard ("My Bot") owns and that must take effect on the
# live bot the moment the creator saves them — even for a creator like Zarna whose
# base config lives in a JSON file. These are written to bot_configs.config_json by
# operator save_bot_data(). We deliberately overlay ONLY this allowlist so the
# dashboard can't accidentally alter voice/guardrail config that lives in the file.
_BOT_OVERRIDE_FIELDS = (
    "send_contact_card",
    "profile_photo_url",
    "sms_display_name",
    "first_message",
)


def _load_bot_overrides(slug: str) -> dict:
    """Return the allowlisted SMS-profile overrides saved via the operator dashboard.

    Reads bot_configs.config_json (same Postgres the operator writes to) and returns
    only the _BOT_OVERRIDE_FIELDS that are present. Returns {} on any failure so the
    live config is never blocked by a DB hiccup.
    """
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}
    try:
        conn = psycopg2.connect(dsn.replace("postgres://", "postgresql://", 1))
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT config_json FROM bot_configs WHERE creator_slug = %s",
                    (slug,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception as exc:
        _LOGGER.debug("CreatorConfig[%s]: bot_configs overlay lookup failed (%s)", slug, exc)
        return {}
    if not row or not row[0]:
        return {}
    cfg = row[0]
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            return {}
    if not isinstance(cfg, dict):
        return {}
    return {k: cfg[k] for k in _BOT_OVERRIDE_FIELDS if k in cfg}


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

    # Overlay the dashboard-owned SMS-profile fields (vCard + first message) so the
    # operator's "My Bot" toggle takes effect on the live bot without a redeploy or a
    # file edit. Scoped to _BOT_OVERRIDE_FIELDS; everything else stays from the file.
    overrides = _load_bot_overrides(slug)
    if overrides:
        data = {**data, **overrides}
        _LOGGER.info(
            "CreatorConfig[%s]: applied dashboard overrides for %s",
            slug, sorted(overrides.keys()),
        )

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
