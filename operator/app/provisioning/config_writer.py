"""
Personality config generation for a new creator.

Takes their onboarding form (display_name, bio, tone, extra_context) and
asks Gemini to fill in TEMPLATE_LLM.json — producing a full CreatorConfig-
shaped JSON. Result is written to creator_configs (Postgres), NOT to disk.

Why Postgres and not a file?
  Railway redeploys wipe the container filesystem. A disk-written config
  would vanish on the next push. Storing in Postgres means every creator's
  personality survives every deploy.

Idempotent: if a row for this slug already exists in creator_configs, we
skip. To regenerate, delete the row first.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, Optional

from google import genai

from ..db import get_conn

_log = logging.getLogger(__name__)

# Repo root: app/provisioning/config_writer.py → go up 3 levels to reach the
# repo root where creator_config/ lives.
_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
_TEMPLATE_PATH = os.path.join(_ROOT, "creator_config", "TEMPLATE_LLM.json")

_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
_GEMINI_MODEL = os.getenv("CONFIG_WRITER_MODEL", "gemini-2.5-flash")


# ---------------------------------------------------------------------------
# Load / seed helpers
# ---------------------------------------------------------------------------

def _load_template() -> Dict[str, Any]:
    """Read TEMPLATE_LLM.json. Raises if missing — this is a hard dependency."""
    if not os.path.exists(_TEMPLATE_PATH):
        raise FileNotFoundError(
            f"TEMPLATE_LLM.json missing at {_TEMPLATE_PATH}. "
            "Create it before running config_writer."
        )
    with open(_TEMPLATE_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    # Drop the internal schema note before handing to the LLM — it's noise.
    data.pop("_schema_note", None)
    return data


def _existing_config(slug: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT config_json FROM creator_configs WHERE creator_slug=%s",
                (slug,),
            )
            row = cur.fetchone()
            return dict(row[0]) if row and row[0] else None
    finally:
        conn.close()


def _save_config(slug: str, config: Dict[str, Any]) -> None:
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO creator_configs (creator_slug, config_json)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (creator_slug)
                DO UPDATE SET
                    config_json = EXCLUDED.config_json,
                    updated_at  = NOW()
                """,
                (slug, json.dumps(config)),
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Prompt + extraction
# ---------------------------------------------------------------------------

def _build_prompt(slug: str, form: Dict[str, Any], template: Dict[str, Any]) -> str:
    display_name = (form.get("display_name") or "").strip()
    bio = (form.get("bio") or "").strip()
    tone = (form.get("tone") or "").strip()
    extra = (form.get("extra_context") or "").strip()
    keyword = (form.get("sms_keyword") or slug.upper()).strip()
    account_type = (form.get("account_type") or "performer").strip()

    template_json = json.dumps(template, indent=2, ensure_ascii=False)

    return f"""You are generating a personality configuration for a new AI texting bot.
A fan of the creator will text the bot's phone number and receive replies that sound like the creator.

The creator just filled out a short onboarding form. Your job is to turn that into a complete
configuration JSON that matches the template schema exactly.

=== Creator's onboarding form ===
slug:         {slug}
display_name: {display_name}
account_type: {account_type}
sms_keyword:  {keyword}
tone:         {tone}
bio:          {bio}
extra_context:{extra}

=== Output schema (copy every key, fill every value) ===
{template_json}

=== Rules ===
1. Return ONLY valid JSON — no markdown fences, no commentary, no prefix.
2. Match the template's shape EXACTLY. Every key in the template must appear in the output.
3. slug must equal "{slug}" verbatim.
4. display_name must equal "{display_name}" verbatim (or best-effort clean version).
5. sms_keyword must be uppercase letters only, ≤ 14 chars, default to the slug upper-cased if unclear.
6. style_rules_text: 150-300 words as a SINGLE PROSE STRING (with newlines). Concrete, specific to THIS creator.
   Cover: register (playful / sincere / roast), when to be warm vs when to joke, length preference, what to avoid,
   voice signatures they use. Start with "Voice: …" then paragraphs or short bulleted lines separated by \\n.
7. tone_examples_text: 5-8 example fan/reply pairs as one string. Format each exactly:
     Fan: "..."
     {display_name}: "..."
   Separate pairs with a blank line. Use bio + extra_context to infer vocabulary and cadence.
   Include at least one sincere/vulnerable example and one playful example.
8. voice_lock_rules_text: 100-250 words as a prose string. Rules that preserve {display_name}'s specific comedic/authorial
   POV — e.g. "don't describe {display_name} as generic life coach", "default tone on family topics is X not Y",
   tonal traps to avoid. If you have no signal, leave as "".
9. hard_fact_guardrails_text: 80-200 words as a prose string. Non-negotiable factual guardrails — things the bot
   MUST NOT invent about this creator (family, biography, personal claims). If no signal, leave as "".
10. name_variants: likely misspellings fans might text (e.g. "Haleyy", "Hayley"). 3-8 items.
11. banned_words: words this creator would never use — generic endearments like "honey" unless clearly on-brand.
12. Leave fields empty/[] if you have no signal. Do NOT invent family members, partners, or children.
13. links: all "" unless extra_context mentions specific URLs.
14. shalabh_names, mil_answers, family_roast_names: Zarna-only artefacts. For every other creator return [].

Return the JSON now."""


_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)
# Trailing commas before } or ] are the most common Gemini-produced invalid-JSON
# artefact. Strip them if a strict parse fails.
_TRAILING_COMMA_RE = re.compile(r",(\s*[}\]])")


def _extract_json(raw: str) -> Dict[str, Any]:
    """
    Strip accidental markdown fences, find the outermost {...}, parse it.
    If strict JSON parsing fails due to trailing commas (common Gemini
    artefact), retry after stripping them. Raises ValueError if no parseable
    JSON object is found even after cleanup.
    """
    text = _FENCE_RE.sub("", raw).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError(f"No JSON object found in LLM output: {text[:200]!r}")
    candidate = text[start : end + 1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        cleaned = _TRAILING_COMMA_RE.sub(r"\1", candidate)
        return json.loads(cleaned)


# Generic prompt blocks used when the LLM leaves a creator's _text fields
# empty. CRITICAL: without this safety net, generator.py falls through to
# Zarna-hardcoded Python constants (_STYLE_RULES, _VOICE_LOCK_RULES, etc.)
# which mention Shalabh, Baba Ramdev, immigrant-mom voice — leaking Zarna's
# persona into every non-Zarna bot. Keep these GENERIC and CREATOR-NEUTRAL.
_GENERIC_STYLE_RULES = (
    "Voice: warm, specific, conversational. Match the creator's tone from their bio. "
    "Never generic, never Wikipedia voice.\n\n"
    "Register:\n"
    "- Playful → comedy OK; stay in the creator's comedic lane.\n"
    "- Sincere appreciation, nostalgia, vulnerability → warm first; humor optional and light.\n"
    "- Never lead with sarcasm when the fan is sincere.\n\n"
    "Length: match the moment; max 3 sentences. No filler, no joke explanation, don't copy "
    "retrieval chunks verbatim.\n"
    "Banned: honey, darling, sweetie (unless on-brand); profanity unless on-brand; "
    "slurs of any kind.\n\n"
    "Emphasis: default no asterisks. At most one short *span* if the joke needs it. Never emphasis "
    "when they're sad or anxious. Never **bold**.\n\n"
    "Direct questions → answer first in plain language. Optional second sentence of color.\n"
    "Questions back to the fan: at most one every 3–4 fan messages; never two in a row."
)
_GENERIC_VOICE_LOCK_RULES = (
    "Voice lock:\n"
    "- Stay in the creator's established tone from their bio and example replies.\n"
    "- Do NOT invent biographical details — family members, partners, children, locations, employers.\n"
    "- If the fan asks about something you don't know, redirect gracefully or ask a light question.\n"
    "- Do NOT break character to be earnestly inspirational unless the creator's voice clearly supports it."
)
_GENERIC_HARD_FACT_GUARDRAILS = (
    "Non-negotiable factual guardrails:\n"
    "- Do NOT invent family members, pets, or personal biography.\n"
    "- Do NOT claim specific cities, venues, or dates unless they appear verbatim in the retrieval context.\n"
    "- If the fan asks for a fact you cannot verify, acknowledge uncertainty or change the subject playfully."
)
_GENERIC_TONE_EXAMPLES = (
    "Examples of matching tone:\n\n"
    "Fan: \"Hey! Been following you for a while, you crack me up.\"\n"
    "Reply: \"Thank you for sticking around — truly. It means a lot to know you're laughing.\"\n\n"
    "Fan: \"Rough day. Just wanted to say hi.\"\n"
    "Reply: \"I'm glad you checked in. Rough days are easier when you're not alone with them — "
    "want to tell me what happened, or should we just sit with it?\"\n\n"
    "Fan: \"When's your next show?\"\n"
    "Reply: \"Dates are on my site — come say hi after, I always remember the ones who text first.\""
)

_GENERIC_TEXT_DEFAULTS = {
    "style_rules_text": _GENERIC_STYLE_RULES,
    "voice_lock_rules_text": _GENERIC_VOICE_LOCK_RULES,
    "hard_fact_guardrails_text": _GENERIC_HARD_FACT_GUARDRAILS,
    "tone_examples_text": _GENERIC_TONE_EXAMPLES,
}


def _merge_safe_defaults(llm_output: Dict[str, Any], template: Dict[str, Any], slug: str) -> Dict[str, Any]:
    """
    Take the template as the baseline, overlay LLM output on top. Guarantees
    every field the brain expects is present even if the LLM missed some.
    Also ensures each prompt-block `_text` field is NEVER empty — if the LLM
    left it blank, backfill with creator-neutral generic text so the bot doesn't
    fall through to Zarna-voiced Python constants in generator.py.
    """
    merged = json.loads(json.dumps(template))  # deep copy
    for key, value in llm_output.items():
        if key.startswith("_"):
            continue
        merged[key] = value
    merged["slug"] = slug  # always pin the slug — LLM can't change it

    for key, default in _GENERIC_TEXT_DEFAULTS.items():
        val = merged.get(key, "") or ""
        # If the LLM returned short/empty prose for a _text field, use the
        # generic block instead of shipping a bot with no voice guidance.
        if len(val.strip()) < 40:
            merged[key] = default

    return merged


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def generate_and_write(slug: str, form: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a personality config for `slug` from the onboarding `form`
    and write it to creator_configs. Returns the written config.

    Idempotent: if a row already exists for this slug, returns it unchanged.
    """
    existing = _existing_config(slug)
    if existing:
        _log.info("config_writer[%s]: existing row found — skipping regeneration", slug)
        return existing

    template = _load_template()

    if not _GEMINI_API_KEY:
        _log.warning(
            "config_writer[%s]: GEMINI_API_KEY not set — writing minimal fallback config",
            slug,
        )
        fallback = _merge_safe_defaults({
            "display_name": form.get("display_name", slug),
            "description":  form.get("bio", ""),
            "sms_keyword":  (form.get("sms_keyword") or slug).upper()[:14],
        }, template, slug)
        _save_config(slug, fallback)
        return fallback

    client = genai.Client(api_key=_GEMINI_API_KEY)
    prompt = _build_prompt(slug, form, template)

    _log.info("config_writer[%s]: calling Gemini (model=%s)", slug, _GEMINI_MODEL)
    # response_mime_type forces Gemini to emit a valid JSON body (no fences,
    # no stray prose). The genai SDK routes this via GenerationConfig but we
    # pass it through `config=` for cross-version compatibility; if the SDK
    # rejects the kwarg we fall back to plain generation + lenient parsing.
    try:
        response = client.models.generate_content(
            model=_GEMINI_MODEL,
            contents=prompt,
            config={"response_mime_type": "application/json"},
        )
    except Exception as exc:
        _log.warning(
            "config_writer[%s]: JSON-mode call failed (%s) — falling back to plain generation",
            slug, exc,
        )
        response = client.models.generate_content(
            model=_GEMINI_MODEL,
            contents=prompt,
        )
    raw = getattr(response, "text", "") or ""
    if not raw:
        raise RuntimeError("config_writer: Gemini returned empty response")

    llm_output = _extract_json(raw)
    config = _merge_safe_defaults(llm_output, template, slug)
    _save_config(slug, config)
    _log.info(
        "config_writer[%s]: wrote creator_configs row "
        "(style=%d chars, tone_examples=%d chars, voice_lock=%d chars, guardrails=%d chars)",
        slug,
        len(config.get("style_rules_text", "") or ""),
        len(config.get("tone_examples_text", "") or ""),
        len(config.get("voice_lock_rules_text", "") or ""),
        len(config.get("hard_fact_guardrails_text", "") or ""),
    )
    return config
