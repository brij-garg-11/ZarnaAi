"""
Fan memory extractor.

After each exchange, this module runs a lightweight Gemini call that:
  1. Updates a short plain-text summary of what the fan has shared
  2. Assigns/updates a list of normalized tags for filtering in the admin

The call happens in a background thread — zero impact on reply latency.
"""

import json
import logging
from typing import Tuple

from google import genai

from app.config import GEMINI_API_KEY, INTENT_MODEL

logger = logging.getLogger(__name__)
_client = genai.Client(api_key=GEMINI_API_KEY)

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

Your job:
1. Write an updated fan profile — a single short paragraph of plain facts only.
   - Include: name (if given), profession, family situation, heritage, fun details they shared.
   - Do NOT store: health conditions, financial details, political views, or anything sensitive.
   - Do NOT invent facts. Only record what the fan explicitly stated.
   - Max 300 characters. If nothing new was revealed, return the existing profile unchanged.
   - Write in third-person ("Fan is a doctor..."), no first-person.

2. Extract location — city, state, or country if the fan mentioned where they are from or live.
   Return an empty string if no location was mentioned. Examples: "Boston, MA", "Chicago", "India", "New York".

3. Return a JSON list of tags from this exact allowed list (pick all that apply, be conservative):
{allowed_tags}

Respond ONLY with valid JSON in this exact format, nothing else:
{{"memory": "...", "location": "...", "tags": ["tag1", "tag2"]}}"""


def extract_memory(
    current_memory: str,
    user_message: str,
) -> Tuple[str, list, str]:
    """
    Returns (updated_memory_string, updated_tags_list, location_string).
    On any error, returns the original memory unchanged with empty tags and location.
    """
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
        memory = str(data.get("memory", current_memory or ""))[:400]
        location = str(data.get("location", ""))[:100]
        tags = [t for t in data.get("tags", []) if t in _ALLOWED_TAGS]
        return memory, tags, location

    except Exception as exc:
        logger.warning("Memory extraction failed (non-fatal): %s", exc)
        return current_memory or "", [], ""
