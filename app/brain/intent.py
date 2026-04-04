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
    "joke", "jokes", "laughter", "comedy", "comic",
    "make me laugh", "tell me something funny", "tell me a joke",
    "humor", "humour",
    "roast", "one liner", "one-liner", "witty",
    "make me smile",
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

# ── Greeting ────────────────────────────────────────────────────────────────
_GREETING_EXACT = frozenset({
    "hi", "hey", "hello", "hola", "yo", "howdy", "sup",
    "hii", "hiii", "heyyy", "heyy", "hiiii",
    "namaste", "namasté",
    "good night", "goodnight", "night night",
})
_GREETING_PHRASES = (
    "what's up", "whats up", "wassup", "whaddup",
    "good morning", "good afternoon", "good evening",
    "how are you", "how's it going", "how you doing",
    "are you there", "is anyone there", "you there",
    "what's going on", "whats going on", "what's happening",
    "hi zarna", "hey zarna", "hello zarna",
    "hi there", "hey there", "hello there",
)
_GREETING_MAX_WORDS = 7

# ── Feedback: reactions, laughs, compliments, quiz answers ──────────────────
# Laugh / reaction words caught by exact word match (very short messages)
_LAUGH_EXACT = frozenset({
    "lol", "lmao", "lmfao", "rofl", "haha", "hahaha", "hahahaha",
    "hahahahaha", "ha", "hah", "hehe", "heehee", "😂", "😆", "🤣",
    "dead", "💀", "omg", "omfg", "lololol", "lolol",
})
# MIL quiz answers — fans answering "who is Zarna's enemy #1?"
_MIL_ANSWERS = (
    "mother in law", "mother-in-law", "mil ", " mil",
    "her mother in law", "your mother in law", "mom in law",
    "the mother in law", "mother in laws", "mothr in law",
)
_FEEDBACK_PHRASES = (
    # post-show praise
    "great show", "amazing show", "awesome show", "best show",
    "loved the show", "loved your show", "loved it tonight",
    "you were amazing", "you were great", "you were incredible",
    "you killed it", "you crushed it", "you were hilarious",
    "so funny tonight", "had a blast", "best night ever",
    "such a great time", "what a show", "incredible performance",
    "funniest show", "thank you for the show",
    # general positive reactions
    "so funny", "that's so funny", "that is so funny",
    "hilarious", "you're hilarious", "you are hilarious",
    "you crack me up", "cracking me up", "cracking up",
    "i'm dying", "i am dying", "dying laughing",
    "tears down my face", "laughing so hard", "in stitches",
    "you had me", "had me laughing", "can't stop laughing",
    "love this", "love it", "love you zarna", "love zarna",
    "you're amazing", "you are amazing", "you're the best",
    "preach", "so true", "100%", "exactly",
    "this is gold", "gold", "fire 🔥", "this is fire",
    "well said", "couldn't agree more", "agree",
    "thank you zarna", "thanks zarna", "thank you for this",
    "good night was fun", "had so much fun",
    "you were awesome tonight", "awesome tonight",
    "we have seen you", "seen you many times",
)

# ── Personal: fan sharing biographical info about themselves ─────────────────
_PERSONAL_PHRASES = re.compile(
    r"\b("
    r"i'?m a |i am a |i'?m from |i am from |i live in |i work(ed)? (as|at|for|in)|"
    r"my name is |my husband |my wife |my kids |my daughter |my son |my family |"
    r"i have \d+ kids|i'?m \d+ years|i am \d+ years|i just turned \d+|"
    r"i just moved|i grew up|born in |raised in |"
    r"i'?m (a |an |the )?(mom|dad|mother|father|teacher|nurse|doctor|lawyer|engineer|"
    r"therapist|chef|artist|writer|student|retired)|"
    r"three facts|3 facts|\d+ facts about (me|myself)|facts about me|"
    r"fun fact.{0,5}(i |about me)|"
    r"i love (to |my |our )?(cook|hike|travel|read|danc|sing|paint|garden|yoga)|"
    r"introvert|extrovert|i'?m (jewish|hindu|muslim|catholic|christian|sikh|desi|indian|"
    r"south asian|desi|gori|white|black|latina|asian)"
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
    # Strip punctuation from word tokens for keyword matching
    words = set(re.sub(r"[^\w\s]", "", lower).split())

    # Emoji-only laugh reactions (stripped before word split, so check raw lower)
    if lower.strip() in _LAUGH_EXACT:
        return Intent.FEEDBACK

    # Greeting — short openers, check before anything else
    if len(words) <= _GREETING_MAX_WORDS:
        stripped = lower.rstrip("!.? ")
        if stripped in _GREETING_EXACT:
            return Intent.GREETING
        if any(lower.startswith(p) for p in _GREETING_PHRASES):
            return Intent.GREETING

    # Personal first for longer messages (before feedback, to avoid substring collisions)
    if len(words) > 4 and _PERSONAL_PHRASES.search(lower) and "?" not in lower:
        return Intent.PERSONAL

    # Feedback — laugh/reaction words for very short messages (≤4 words)
    if len(words) <= 4 and words & _LAUGH_EXACT:
        return Intent.FEEDBACK
    # MIL quiz answers ("mother in law" etc.) → engagement reaction = feedback
    if any(p in lower for p in _MIL_ANSWERS):
        return Intent.FEEDBACK
    # Post-show praise and general reactions
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

    # Personal — short messages (≤4 words) with bio phrases
    if _PERSONAL_PHRASES.search(lower) and "?" not in lower:
        return Intent.PERSONAL

    return None


def classify_intent(message: str) -> Intent:
    fast = _fast_classify(message)
    if fast is not None:
        return fast

    prompt = f"""Classify this fan message to Zarna Garg (Indian-American comedian) into exactly one intent.

INTENTS with examples:

greeting — opening the conversation, checking in, saying goodbye
  examples: "hi", "hey Zarna!", "are you there?", "good morning", "goodnight", "what's going on?"

feedback — reacting positively to Zarna's content, laughing, giving a review, answering her bits
  examples: "lol", "hahaha", "😂", "so funny!", "that's hilarious", "you were amazing tonight",
            "great show", "loved it", "preach!", "so true", "you crack me up",
            "mother in law" (answering Zarna's "who's my enemy #1?" game),
            "your husband 😂", "her MIL!", "omg I'm dying"

personal — fan sharing facts about themselves (name, job, city, family, hobbies, life story)
  examples: "I'm a teacher from Ohio", "my name is Susan, I'm a mom of 3",
            "3 facts about me: I love hiking, I'm 45, I have 2 dogs",
            "I grew up in India", "I'm Jewish and this hits different",
            "I hate my mother-in-law too!", "I'm happily divorced"

question — fan asking Zarna a direct question expecting a real answer
  examples: "what does Shalabh think of your comedy?", "do your kids watch your shows?",
            "is this actually you or AI?", "how do you deal with the MIL?",
            "what's your advice for dealing with difficult in-laws?",
            "when did you start doing comedy?"

show — explicitly asking for ticket links, show dates, or where to see Zarna perform
  examples: "how do I get tickets?", "when are you coming to LA?", "where can I buy tickets?",
            "are you performing in Chicago?"

book — asking about Zarna's book "This American Woman"
  examples: "where can I buy your book?", "is your book on Kindle?", "loved This American Woman"

joke — wants a joke or comedy content from Zarna
  examples: "tell me a joke", "make me laugh", "I need something funny"

clip — wants a video or clip
  examples: "do you have videos?", "where can I watch your specials?", "send me a clip"

podcast — asking about Zarna's podcast
  examples: "do you have a podcast?", "where can I listen to your podcast?"

general — ONLY use this if the message truly fits none of the above:
  short mid-conversation replies ("yes", "ok", "I know", "maybe"), random non-Zarna topics,
  spam, or messages that make no sense out of context.
  DO NOT use general for laughs, reactions, MIL answers, or fan self-introductions.

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
