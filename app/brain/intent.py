from enum import Enum
import re

from google import genai

from app.config import GEMINI_API_KEY, INTENT_MODEL


class Intent(str, Enum):
    JOKE     = "joke"
    CLIP     = "clip"
    SHOW     = "show"
    BOOK     = "book"
    PODCAST  = "podcast"
    GREETING = "greeting"
    PERSONAL = "personal"
    FEEDBACK = "feedback"
    QUESTION = "question"
    GENERAL  = "general"


_client = genai.Client(api_key=GEMINI_API_KEY)

# ---------------------------------------------------------------------------
# Keyword / heuristic tables
# ---------------------------------------------------------------------------

_SHOW_KEYWORDS = {
    "ticket", "tickets", "tour", "touring",
    "performing", "performance", "come see",
    "where are you", "when are you", "tour dates", "venue",
}
_JOKE_KEYWORDS = {
    "joke", "jokes", "laugh", "laughter", "comedy", "comic",
    "make me laugh", "tell me something funny", "tell me a joke",
    "humor", "humour",
    "roast", "one liner", "one-liner", "hilarious", "witty",
    "crack me up", "make me smile",
}
_CLIP_KEYWORDS = {
    "video", "videos", "clip", "clips", "youtube", "watch",
    "special", "stand up", "standup", "stand-up", "reel", "reels",
}
_PODCAST_KEYWORDS = {
    "podcast", "episode", "listen", "audio show",
}
_BOOK_PHRASES = (
    "this american woman",
    "your book",
    "the book",
    "read your book",
    "buy your book",
    "buy the book",
    "order your book",
    "order the book",
    "zarna's book",
    "zarnas book",
    "zarna book",
    "get your book",
    "where to buy",
    "amazon.com/dp",
)
_BOOK_EXTRA_WORDS = frozenset({"kindle", "hardcover", "paperback"})

# Greeting: very short, low-ambiguity openers
_GREETING_EXACT = frozenset({
    "hi", "hey", "hello", "hola", "yo", "howdy", "sup",
    "hii", "hiii", "heyyy", "heyy", "hiiii",
})
_GREETING_PHRASES = (
    "what's up", "whats up", "wassup", "whaddup", "good morning",
    "good afternoon", "good evening", "good night",
    "how are you", "how's it going", "how you doing",
)
_GREETING_MAX_WORDS = 6

# Feedback: post-show compliments, reactions to Zarna's performance
_FEEDBACK_PHRASES = (
    "great show", "amazing show", "awesome show", "best show",
    "loved the show", "loved your show", "loved it tonight",
    "you were amazing", "you were great", "you were incredible",
    "you killed it", "you crushed it", "you were hilarious",
    "so funny tonight", "had a blast", "best night ever",
    "such a great time", "what a show", "incredible performance",
    "funniest show", "thank you for the show",
)

# Personal: fan sharing biographical info about themselves
_PERSONAL_PHRASES = re.compile(
    r"\b("
    r"i'm a |i am a |i'm from |i am from |i live in |i work |"
    r"my name is |my husband |my wife |my kids |my daughter |my son |"
    r"i have \d+ kids|i'm \d+ years|i am \d+ years|"
    r"i just moved|i grew up|born in |raised in "
    r")",
    re.IGNORECASE,
)


def _fast_book_intent(lower: str, words: set) -> bool:
    if any(p in lower for p in _BOOK_PHRASES):
        return True
    if words & _BOOK_EXTRA_WORDS:
        return True
    if "book" in words and ("zarna" in lower or "american woman" in lower):
        return True
    return False


def _fast_classify(message: str) -> Intent | None:
    """
    Cheap keyword scan — returns an Intent immediately if the message is
    unambiguous, or None to fall through to the Gemini classifier.
    """
    lower = message.lower().strip()
    words = set(lower.split())

    # Greeting — very short openers, check before anything else
    if len(words) <= _GREETING_MAX_WORDS:
        stripped = lower.rstrip("!.? ")
        if stripped in _GREETING_EXACT:
            return Intent.GREETING
        if any(lower.startswith(p) for p in _GREETING_PHRASES):
            return Intent.GREETING

    # Feedback — post-show praise (check before JOKE to avoid "funny" overlap)
    if any(p in lower for p in _FEEDBACK_PHRASES):
        return Intent.FEEDBACK

    # Structured intents
    if words & _SHOW_KEYWORDS or any(k in lower for k in _SHOW_KEYWORDS if " " in k):
        return Intent.SHOW
    if words & _JOKE_KEYWORDS or any(k in lower for k in _JOKE_KEYWORDS if " " in k):
        return Intent.JOKE
    if words & _CLIP_KEYWORDS or any(k in lower for k in _CLIP_KEYWORDS if " " in k):
        return Intent.CLIP
    if words & _PODCAST_KEYWORDS or any(k in lower for k in _PODCAST_KEYWORDS if " " in k):
        return Intent.PODCAST
    if _fast_book_intent(lower, words):
        return Intent.BOOK

    # Personal — fan sharing bio info (conservative: phrase-based)
    if _PERSONAL_PHRASES.search(lower) and "?" not in lower:
        return Intent.PERSONAL

    return None


def classify_intent(message: str) -> Intent:
    fast = _fast_classify(message)
    if fast is not None:
        return fast

    prompt = f"""Classify this user message into exactly one intent.

Intents:
- greeting: casual opener like hi, hello, hey, how are you — with no real question or topic yet
- joke: user wants a joke, something funny, or comedy content
- clip: user wants a video or clip recommendation
- show: user is EXPLICITLY asking for ticket links, tour dates, or where to see Zarna perform. Personal stories, fun facts about themselves, or general conversation are NEVER show intent.
- book: user is asking about Zarna's book "This American Woman", where to buy it, or how to get it
- podcast: user is EXPLICITLY asking about the podcast by name, asking if there's a podcast episode on a specific topic, or asking where to listen. Questions about Zarna's family members (husband, kids, Shalabh, Veer, Brij, Zoya) are NOT podcast intent — they are general.
- personal: fan sharing facts about themselves — their name, job, city, family, hobbies, life story. NOT asking a question.
- feedback: fan giving a review, compliment, or reaction to Zarna's show or content (e.g. "great show!", "you were so funny tonight")
- question: fan asking Zarna a question about her life, family, opinions, or advice — a direct question expecting a real answer
- general: anything else — banter, one-word reactions, random topics

Message: "{message}"

Reply with only one word: greeting, joke, clip, show, book, podcast, personal, feedback, question, or general"""

    try:
        response = _client.models.generate_content(
            model=INTENT_MODEL,
            contents=prompt,
        )
        raw = response.text.strip().lower()
    except Exception:
        return Intent.GENERAL

    try:
        return Intent(raw)
    except ValueError:
        return Intent.GENERAL
