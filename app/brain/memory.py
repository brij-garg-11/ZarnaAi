"""
Fan memory extractor.

After each exchange, this module runs a lightweight Gemini call that:
  1. Updates a short plain-text summary of what the fan has shared
  2. Assigns/updates a list of normalized tags for filtering in the admin

The call happens in a background thread — zero impact on reply latency.

Privacy guardrails (applied before any storage):
  - Messages that indicate the sender may be a minor are never stored.
  - Sensitive categories (health conditions, mental health, immigration
    status, precise home address, financial distress, sexual orientation,
    religious affiliation) are explicitly excluded from the stored profile.
  - The "minor detected" flag causes the caller to skip storage entirely
    and clear any existing profile for that number.
"""

import json
import logging
import re
from functools import lru_cache
from typing import Tuple

from google import genai

from app.config import GEMINI_API_KEY, INTENT_MODEL, CREATOR_SLUG

logger = logging.getLogger(__name__)
_client = genai.Client(api_key=GEMINI_API_KEY)

# ── Fan-name validation ──────────────────────────────────────────────────────
# The extractor occasionally mistakes the creator's or family's names (fans open
# with "Hi Zarna", "tell Shalabh...") or a filler word for the fan's own name.
# is_valid_fan_name() is the single source of truth used both here (live path)
# and by scripts/fix_fan_names.py (backfill/cleanup). A name that fails is stored
# as "" (unknown) rather than a wrong value.

# Generic tokens that are never a fan's first name.
_NAME_STOPWORDS = frozenset({
    "fan", "fans", "bot", "zarnabot", "zbot", "friend", "buddy", "guy", "guys",
    "hi", "hey", "hello", "hiya", "yo", "sup", "hola", "stop", "help", "start",
    "unsubscribe", "yes", "no", "yeah", "yep", "nope", "ok", "okay", "sure",
    "thanks", "thank", "ty", "please", "lol", "lmao", "lmfao", "haha", "hahaha",
    "omg", "love", "like", "you", "your", "me", "my", "here", "there", "who",
    "what", "why", "how", "when", "where", "mom", "mommy", "mother", "mama",
    "dad", "daddy", "father", "husband", "wife", "hubby", "auntie", "aunty",
    "sir", "maam", "madam", "unknown", "none", "null", "na", "nan", "test",
    "good", "great", "nice", "cool", "fine", "morning", "night", "day",
    # Common English function/filler words that leak in via phrases like
    # "call me on my cell", "text me back", "I'm at work" — never real names.
    "on", "off", "in", "at", "to", "up", "so", "of", "or", "if", "is", "it",
    "as", "an", "be", "by", "we", "he", "she", "him", "her", "us", "them",
    "the", "and", "but", "for", "yet", "not", "now", "back", "later", "again",
    "just", "really", "very", "too", "also", "still", "then", "than", "out",
    "about", "from", "with", "this", "that", "these", "those", "was", "were",
    "are", "am", "im", "ive", "id", "ill", "dont", "doing", "done", "got",
    "get", "going", "gonna", "wanna", "know", "think", "feel", "want", "need",
})


@lru_cache(maxsize=4)
def _creator_name_blocklist(slug: str) -> frozenset:
    """Lowercased tokens that must never be stored as a fan's name for this
    creator: the creator's own name/variants, their spouse, and family members.

    Built from creator_config so it stays correct per-tenant; falls back to
    Zarna's known names if the config can't be loaded so the live path is never
    left unguarded."""
    block: set = set()

    def _add(value: str) -> None:
        for tok in re.split(r"[\s\-]+", (value or "").lower()):
            tok = tok.strip(".,!?'\"")
            if tok.isalpha() and len(tok) >= 2:
                block.add(tok)

    try:
        from app.brain.creator_config import load_creator
        cfg = load_creator(slug)
    except Exception:  # config load must never break extraction
        cfg = None

    if cfg is not None:
        _add(cfg.name)
        for v in cfg.name_variants:
            _add(v)
        for v in cfg.shalabh_names:
            _add(v)
        for v in cfg.family_roast_names:
            _add(v)

    # Hard fallback: Zarna-universe names (also covers the case where config is
    # missing). Children aren't exposed on CreatorConfig, so list them here.
    block |= {
        "zarna", "zara", "garg", "shalabh", "shalab",
        "zoya", "brij", "veer", "ramdev",
    }
    return frozenset(block)


def is_valid_fan_name(name: str, slug: str = CREATOR_SLUG) -> bool:
    """True if `name` looks like a real fan first name we should store.

    Rejects empties, non-alphabetic tokens, absurd lengths, filler words, and
    the creator's/family's names (the common misfires)."""
    if not name:
        return False
    n = name.strip().strip(".,!?'\"").strip()
    if not n:
        return False
    # Allow internal hyphens/apostrophes (e.g. "Mary-Jane", "O'Brien") but the
    # rest must be alphabetic (unicode letters incl. accents are fine).
    core = n.replace("-", "").replace("'", "").replace(" ", "")
    if not core.isalpha():
        return False
    if not (2 <= len(core) <= 20):
        return False
    lowered = n.lower()
    first_tok = lowered.split()[0]
    if lowered in _NAME_STOPWORDS or first_tok in _NAME_STOPWORDS:
        return False
    block = _creator_name_blocklist(slug)
    if lowered in block or first_tok in block:
        return False
    return True

# ── Minor detection ───────────────────────────────────────────────────────
# Catch explicit age statements indicating under-18. Checked before any
# Gemini call — pure regex, fast, no API cost.
_MINOR_AGE_PATTERN = re.compile(
    # "I am 15" / "I'm 14" / "im 16" / "turning 17" / "just turned 15"
    r"\b(i\s*am|i'm|im|turning|just\s+turned)\s*(1[0-7]|[4-9])\b"
    r"|"
    # "I am 15 years old" / "I'm 14 yr old"
    r"\b(i\s*am|i'm|im)\s*(1[0-7]|[4-9])\s*(years?\s*old|yr[s]?\s*old)\b"
    r"|"
    # Grade / school signals (sender attending these = minor)
    r"\b(in\s+)?(middle\s+school|elementary\s+school|7th\s+grade|8th\s+grade|9th\s+grade|10th\s+grade|11th\s+grade)\b",
    re.IGNORECASE,
)

def _message_may_be_minor(message: str) -> bool:
    """Return True if the message contains signals the sender may be under 18."""
    return bool(_MINOR_AGE_PATTERN.search(message))

# Controlled vocabulary of tags Gemini is allowed to assign.
# Freeform additions land in a catch-all "other" bucket.
_ALLOWED_TAGS = {
    # Professions
    "doctor", "nurse", "dentist", "pharmacist", "therapist", "lawyer",
    "accountant", "engineer", "teacher", "professor", "scientist",
    "entrepreneur", "business-owner", "marketing", "finance",
    "government", "military", "artist", "writer", "actor",
    "chef", "real-estate", "tech", "student", "retired",
    # Family
    "married", "single", "divorced", "widowed",
    "parent", "mom", "grandparent", "empty-nester",
    "1-kid", "2-kids", "3-kids", "4-kids", "5-plus-kids",
    # Heritage / background
    "indian-american", "south-asian", "first-gen-immigrant", "immigrant",
    "desi", "indian", "pakistani", "bangladeshi",
    "white", "black", "hispanic", "asian", "middle-eastern", "mixed",
    # Fan loyalty
    "longtime-fan", "repeat-attendee", "first-show",
    # Other notable
    "dog-owner", "cat-owner", "pet-owner",
    "boston", "new-york", "chicago", "los-angeles", "texas",
    "international",
}

_EXTRACT_PROMPT = """You are a data extractor for a comedian's fan SMS chatbot.

Current fan profile (may be empty for new fans):
{memory}

New message from fan:
{message}

PRIVACY RULES — apply strictly before writing anything:
- If the message suggests the sender is under 18 (mentions school grade, age under 18, etc.),
  return exactly: {{"memory": "", "name": "", "location": "", "tags": [], "minor": true}}
- NEVER store: specific health diagnoses, mental health conditions, immigration legal status,
  precise street address, financial debt or hardship, sexual orientation, religious beliefs,
  or any information the person did not openly and voluntarily share.
- City/state/country as location is fine. Street address is not.
- "Immigrant" or "first-gen" heritage is fine. Specific visa/legal status is not.

Your job (if not a minor):
1. Write an updated fan profile — a single short paragraph of plain facts only.
   - Include: name (if given), profession, family situation, heritage, fun details shared.
   - Do NOT invent facts. Only record what the fan explicitly stated.
   - Max 300 characters. If nothing new was revealed, return the existing profile unchanged.
   - Write in third-person ("Fan is a doctor..."), no first-person.

2. Extract name — the fan's first name only, if they have stated it. Empty string if unknown.
   Only use a name the fan explicitly gave (e.g. "I'm Priya" or "My name is Raj").

3. Extract location — city, state, or country only. Empty string if not mentioned.

4. Return tags from this allowed list only (be conservative):
{allowed_tags}

Respond ONLY with valid JSON — no markdown, no explanation:
{{"memory": "...", "name": "...", "location": "...", "tags": ["tag1", "tag2"], "minor": false}}"""


def extract_memory(
    current_memory: str,
    user_message: str,
) -> Tuple[str, list, str, bool, str]:
    """
    Returns (memory, tags, location, minor_detected, name).

    If minor_detected is True, the caller must clear any existing profile for
    this phone number and skip storage — COPPA compliance.

    On any extraction error, returns the original memory unchanged.
    """
    # Fast regex check before hitting the API — no cost, no latency
    if _message_may_be_minor(user_message):
        logger.info("Minor signal detected in message — skipping memory storage")
        return "", [], "", True, ""

    allowed_str = ", ".join(sorted(_ALLOWED_TAGS))
    prompt = _EXTRACT_PROMPT.format(
        memory=current_memory or "(none yet)",
        message=user_message,
        allowed_tags=allowed_str,
    )

    try:
        response = _client.models.generate_content(
            model=INTENT_MODEL,
            contents=prompt,
        )
        raw = response.text.strip()

        # Strip markdown code fences if model wraps in ```json ... ```
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        data = json.loads(raw)

        # Check if Gemini itself flagged a minor
        if data.get("minor", False):
            logger.info("Gemini flagged minor in message — skipping memory storage")
            return "", [], "", True, ""

        memory   = str(data.get("memory", current_memory or ""))[:400]
        location = str(data.get("location", ""))[:100]
        name     = str(data.get("name", ""))[:80].strip()
        tags     = [t for t in data.get("tags", []) if t in _ALLOWED_TAGS]

        # Guard against storing the creator's/family's name or a filler word as
        # the fan's name (fans open with "Hi Zarna", "tell Shalabh...", etc.).
        if name and not is_valid_fan_name(name):
            logger.info("Rejected implausible fan name %r — storing as unknown", name)
            name = ""

        return memory, tags, location, False, name

    except Exception as exc:
        logger.warning("Memory extraction failed (non-fatal): %s", exc)
        return current_memory or "", [], "", False, ""
