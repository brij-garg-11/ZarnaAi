from enum import Enum

from google import genai

from app.config import GEMINI_API_KEY, GENERATION_MODEL


class Intent(str, Enum):
    JOKE = "joke"
    CLIP = "clip"
    SHOW = "show"
    GENERAL = "general"


_client = genai.Client(api_key=GEMINI_API_KEY)


def classify_intent(message: str) -> Intent:
    prompt = f"""Classify this user message into exactly one intent.

Intents:
- joke: user wants a joke, something funny, or comedy content
- clip: user wants a video or clip recommendation
- show: user wants show dates, tour info, ticket links, or where to see Zarna live
- general: general conversation, questions, or anything else

Message: "{message}"

Reply with only one word: joke, clip, show, or general"""

    response = _client.models.generate_content(
        model=GENERATION_MODEL,
        contents=prompt,
    )

    raw = response.text.strip().lower()

    try:
        return Intent(raw)
    except ValueError:
        return Intent.GENERAL
