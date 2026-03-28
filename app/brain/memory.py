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
from typing import Tuple

from google import genai

from app.config import GEMINI_API_KEY, INTENT_MODEL

logger = logging.getLogger(__name__)
_client = genai.Client(api_key=GEMINI_API_KEY)

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
  return exactly: {{"memory": "", "location": "", "tags": [], "minor": true}}
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

2. Extract location — city, state, or country only. Empty string if not mentioned.

3. Return tags from this allowed list only (be conservative):
{allowed_tags}

Respond ONLY with valid JSON — no markdown, no explanation:
{{"memory": "...", "location": "...", "tags": ["tag1", "tag2"], "minor": false}}"""


def extract_memory(
    current_memory: str,
    user_message: str,
) -> Tuple[str, list, str, bool]:
    """
    Returns (memory, tags, location, minor_detected).

    If minor_detected is True, the caller must clear any existing profile for
    this phone number and skip storage — COPPA compliance.

    On any extraction error, returns the original memory unchanged.
    """
    # Fast regex check before hitting the API — no cost, no latency
    if _message_may_be_minor(user_message):
        logger.info("Minor signal detected in message — skipping memory storage")
        return "", [], "", True

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
            return "", [], "", True

        memory   = str(data.get("memory", current_memory or ""))[:400]
        location = str(data.get("location", ""))[:100]
        tags     = [t for t in data.get("tags", []) if t in _ALLOWED_TAGS]
        return memory, tags, location, False

    except Exception as exc:
        logger.warning("Memory extraction failed (non-fatal): %s", exc)
        return current_memory or "", [], "", False
