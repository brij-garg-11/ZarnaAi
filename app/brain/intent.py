from enum import Enum

from google import genai

from app.config import GEMINI_API_KEY, GENERATION_MODEL


class Intent(str, Enum):
    JOKE    = "joke"
    CLIP    = "clip"
    SHOW    = "show"
    BOOK    = "book"
    PODCAST = "podcast"
    GENERAL = "general"


_client = genai.Client(api_key=GEMINI_API_KEY)

# Keywords that unambiguously signal an intent — no API call needed for these.
_SHOW_KEYWORDS = {
    "ticket", "tickets", "show", "shows", "tour", "touring",
    "performing", "performance", "live", "see you", "come see",
    "where are you", "when are you", "dates", "venue",
}
_JOKE_KEYWORDS = {
    "joke", "jokes", "funny", "laugh", "comedy", "make me laugh",
    "tell me something funny", "humor", "humour", "lol", "haha",
    "roast", "one liner", "one-liner",
}
_CLIP_KEYWORDS = {
    "video", "videos", "clip", "clips", "youtube", "watch",
    "special", "stand up", "standup", "stand-up",
}
_PODCAST_KEYWORDS = {
    "podcast",
}


def _fast_classify(message: str) -> Intent | None:
    """
    Cheap keyword scan — returns an Intent immediately if the message is
    unambiguous, or None to fall through to the Gemini classifier.
    """
    lower = message.lower()
    words = set(lower.split())
    if words & _SHOW_KEYWORDS or any(k in lower for k in _SHOW_KEYWORDS if " " in k):
        return Intent.SHOW
    if words & _JOKE_KEYWORDS or any(k in lower for k in _JOKE_KEYWORDS if " " in k):
        return Intent.JOKE
    if words & _CLIP_KEYWORDS or any(k in lower for k in _CLIP_KEYWORDS if " " in k):
        return Intent.CLIP
    if words & _PODCAST_KEYWORDS:
        return Intent.PODCAST
    return None


def classify_intent(message: str) -> Intent:
    # Try free keyword classification first — saves ~1-2s on clear cases
    fast = _fast_classify(message)
    if fast is not None:
        return fast

    # Fall back to Gemini for ambiguous messages
    prompt = f"""Classify this user message into exactly one intent.

Intents:
- joke: user wants a joke, something funny, or comedy content
- clip: user wants a video or clip recommendation
- show: user wants show dates, tour info, ticket links, or where to see Zarna live
- book: user is asking about Zarna's book "This American Woman", where to buy it, or how to get it
- podcast: user is EXPLICITLY asking about the podcast by name, asking if there's a podcast episode on a specific topic, or asking where to listen. Questions about Zarna's family members (husband, kids, Shalabh, Veer, Brij, Zoya) are NOT podcast intent — they are general.
- general: general conversation, questions about Zarna or her family, or anything else

Message: "{message}"

Reply with only one word: joke, clip, show, book, podcast, or general"""

    response = _client.models.generate_content(
        model=GENERATION_MODEL,
        contents=prompt,
    )

    raw = response.text.strip().lower()

    try:
        return Intent(raw)
    except ValueError:
        return Intent.GENERAL
