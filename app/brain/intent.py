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
    MERCH    = "merch"
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
# Fan statements about already having tickets — must NOT trigger a SHOW sell reply.
# "I already have my tickets!" should stay PERSONAL/FEEDBACK, not get a ticket link.
_SHOW_POSSESSION_PHRASES = (
    "already have",
    "i have my ticket",
    "i got my ticket",
    "i got ticket",
    "already bought ticket",
    "already got my ticket",
    "got my ticket",
    "have my ticket",
)
_JOKE_KEYWORDS = {
    "joke", "jokes", "laughter", "comedy", "comic",
    "make me laugh", "tell me something funny", "tell me a joke",
    "humor", "humour",
    "roast", "one liner", "one-liner", "witty",
    "make me smile",
}
_CLIP_KEYWORDS = {
    # Unambiguous request words only.
    # "watch", "video", "videos", "stand-up/standup/stand up" are NOT here — they fire
    # on fan statements ("I watch all your videos", "how long have you done stand-up?")
    # which are NOT clip requests. Gemini handles the ambiguous cases.
    "clip", "clips", "youtube", "special", "reel", "reels",
}
_PODCAST_KEYWORDS = {
    # "listen" removed — "I listen to everything you put out" is a fan statement, not
    # a podcast request. Gemini handles "I listen to your podcast" correctly.
    "podcast", "episode",
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

# ── Merch — physical merchandise (shirts, hoodies, hats, etc.) ───────────────
# Two-signal check (item word + purchase word) catches most cases.
# "your shirt is hilarious" → item word but no purchase word → not MERCH.
# "where can I buy tickets" → purchase words but no merch item → not MERCH (→ SHOW).
# Explicit "do you have merch?" phrases are caught by _MERCH_QUERY_PHRASES.
_MERCH_ITEM_WORDS = frozenset({
    "merch", "merchandise", "shirt", "shirts", "tshirt", "tshirts",
    "hoodie", "hoodies", "hat", "hats", "mug", "mugs",
    "sweatshirt", "sweatshirts", "gear", "apparel", "clothing",
    "tee", "tees",
})
_MERCH_PURCHASE_WORDS = frozenset({
    "buy", "buying", "purchase", "order", "ordering", "get",
    "shop", "shopping", "find", "sell", "selling", "sold",
    "available", "store", "where",
})
# Explicit "do you have merch?" patterns that lack a purchase word
_MERCH_QUERY_PHRASES = (
    "do you have merch",
    "do you have merchandise",
    "do you have a merch",
    "do you have any merch",
    "is there a merch",
    "is there merch",
    "do you sell merch",
    "do you sell merchandise",
    "do you have a shop",
    "do you have an online store",
)

# ── Greeting ────────────────────────────────────────────────────────────────
_GREETING_EXACT = frozenset({
    "hi", "hey", "hello", "hola", "yo", "howdy", "sup",
    "hii", "hiii", "heyyy", "heyy", "hiiii",
    "namaste", "namasté",
    "good night", "goodnight", "night night",
    # Zarna name misspellings — fan trying to address her
    "zara", "zaria", "zarnas", "varna", "zarana", "zarha",
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

# ── Location: fans replying with where they're from ──────────────────────────
# Checked for very short messages (≤4 words) — almost certainly answering
# "where are you from?" during a live show → classify as PERSONAL
_LOCATION_EXACT = frozenset({
    # US States
    "alabama","alaska","arizona","arkansas","california","colorado",
    "connecticut","delaware","florida","georgia","hawaii","idaho",
    "illinois","indiana","iowa","kansas","kentucky","louisiana","maine",
    "maryland","massachusetts","michigan","minnesota","mississippi",
    "missouri","montana","nebraska","nevada","new hampshire","new jersey",
    "new mexico","new york","north carolina","north dakota","ohio",
    "oklahoma","oregon","pennsylvania","rhode island","south carolina",
    "south dakota","tennessee","texas","utah","vermont","virginia",
    "washington","west virginia","wisconsin","wyoming",
    # Major US cities
    "new york city","los angeles","chicago","houston","phoenix",
    "philadelphia","san antonio","san diego","dallas","san jose",
    "austin","jacksonville","san francisco","seattle","denver",
    "nashville","boston","las vegas","portland","memphis","louisville",
    "baltimore","milwaukee","atlanta","new orleans","tampa","orlando",
    "miami","raleigh","minneapolis","cleveland","pittsburgh","cincinnati",
    "kansas city","sacramento","salt lake city","richmond","spokane",
    "des moines","hartford","bridgeport","new haven","jersey city",
    "newark","buffalo","rochester","grand rapids","madison","providence",
    "fort lauderdale","baton rouge","little rock","albuquerque","tucson",
    "fresno","oklahoma city","el paso","corpus christi","lubbong",
    "arlington","plano","garland","lincoln","omaha","wichita",
    "colorado springs","greensboro","durham","charlotte","columbia",
    "charleston","savannah","tallahassee","birmingham","montgomery",
    "mobile","knoxville","chattanooga","lexington","indianapolis",
    "fort wayne","columbus","akron","toledo","dayton","detroit",
    "flint","lansing","ann arbor","st louis","springfield","st paul",
    "sioux falls","fargo","bismarck","billings","boise","eugene",
    "salem","tacoma","bellevue","olympia","anchorage","juneau","honolulu",
    "south bend","palo alto","boulder","pasadena","irvine","scottsdale",
    "tempe","chandler","mesa","glendale","peoria","fort worth",
    "lubbock","garland","irving","laredo","amarillo","mcallen",
    "fresno","bakersfield","stockton","modesto","riverside","ontario",
    "santa ana","anaheim","chula vista","oceanside","escondido",
    "oxnard","elk grove","corona","salinas","sunnyvale","hayward",
    "pomona","torrance","moreno valley","garden grove","palmdale",
    "santa clarita","paterson","yonkers","worcester","cape coral",
    "fort collins","aurora","lakewood","thornton","westminster",
    # Common abbreviations / nicknames
    "nyc","la","sf","dc","atl","chi","phx","philly","nola","kc",
    "brooklyn","queens","bronx","manhattan","long island",
    "the bronx","staten island","jersey","nj","ct","ny","ca","tx","fl",
    # Canadian cities
    "toronto","vancouver","calgary","montreal","ottawa","edmonton",
    "winnipeg","halifax","victoria","saskatoon","regina",
    # International (common in Zarna's desi fanbase)
    "mumbai","delhi","new delhi","bangalore","chennai","hyderabad",
    "pune","ahmedabad","kolkata","lucknow","jaipur","surat","chandigarh",
    "london","toronto","sydney","dubai","singapore","auckland",
})
# Matches "City, ST" or "City, State" patterns (e.g. "Hartford, CT", "Pasadena, CA")
_LOCATION_CITY_STATE_RE = re.compile(
    r"^[a-z][a-z\s\-]{1,25},\s*[a-z]{2,}$", re.IGNORECASE
)

# ── Short affirmations: fans confirming/reacting in 1-3 words ────────────────
# Maps to FEEDBACK (positive acknowledgment) — better than GENERAL's generic reply
_AFFIRMATION_EXACT = frozenset({
    "yes","yep","yup","yeah","yea","yass","yasss","yaaaas",
    "correct","right","true","absolutely","definitely","exactly",
    "of course","for sure","totally","certainly","indeed","yes indeed",
    "yes ma am","yes maam","yes ma'am",
    "congrats","congratulations","congrats!","yay","woohoo","woo hoo",
    "awesome","great","nice","cool","sweet","dope","lit","fire",
    "thanks","thank you","ty","thx","thank u",
    "no","nope","nah","not yet","almost","not really","kind of","kinda",
    "maybe","perhaps","idk","idc","sure","ok","okay","okk","okkk","k",
    "yup yup","yes yes","no no","oh yes","oh yeah","oh no",
    "shut up","stop it","no way","no way!","get out","get outta here",
})

# ── AI / bot questions ────────────────────────────────────────────────────────
_AI_QUESTION_PHRASES = (
    "are you ai", "are you an ai", "is this ai", "is this an ai",
    "are you a bot", "is this a bot", "are you real",
    "am i talking to ai", "am i talking to a bot",
    "is this really zarna", "is this actually zarna",
    "are you actually zarna", "this is ai", "this is a bot",
    "what ai", "which ai", "what model", "what llm",
    "powered by", "chatgpt", "chat gpt", "openai", "claude", "gemini",
    "nice job ai", "good job ai", "wow ai", "hey ai",
)

# ── Shalabh / name references → PERSONAL ─────────────────────────────────────
_SHALABH_NAMES = (
    "shalabh", "shalab", "shalabhs",
)
# Common mis-spellings of Zarna's name as standalone messages → GREETING
_ZARNA_VARIANTS = frozenset({
    "zara", "zaria", "zarnas", "varna", "zarana", "zarha",
    "zarna", "zarna!", "zarna?",
})

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
    r"south asian|desi|gori|white|black|latina|asian)|"
    r"shalabh|shalab"
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
    clean = re.sub(r"[^\w\s]", "", lower)
    words = set(clean.split())

    # Emoji-only laugh reactions (stripped before word split, so check raw lower)
    if lower.strip() in _LAUGH_EXACT:
        return Intent.FEEDBACK

    # Greeting — short openers (incl. Zarna name misspellings)
    if len(words) <= _GREETING_MAX_WORDS:
        stripped = lower.rstrip("!.? ")
        if stripped in _GREETING_EXACT:
            return Intent.GREETING
        if any(lower.startswith(p) for p in _GREETING_PHRASES):
            return Intent.GREETING

    # Location: very short messages that are entirely a known location → PERSONAL
    if len(words) <= 4:
        if clean.strip() in _LOCATION_EXACT:
            return Intent.PERSONAL
        if _LOCATION_CITY_STATE_RE.match(lower.strip()):
            return Intent.PERSONAL

    # Personal first for longer messages (before feedback, to avoid substring collisions)
    if len(words) > 4 and _PERSONAL_PHRASES.search(lower) and "?" not in lower:
        return Intent.PERSONAL

    # Feedback — laugh/reaction words for very short messages (≤4 words)
    if len(words) <= 4 and words & _LAUGH_EXACT:
        return Intent.FEEDBACK
    # Short affirmations / one-word reactions (≤3 words)
    if len(words) <= 3 and clean.strip() in _AFFIRMATION_EXACT:
        return Intent.FEEDBACK
    # MIL quiz answers ("mother in law" etc.) → engagement reaction = feedback
    if any(p in lower for p in _MIL_ANSWERS):
        return Intent.FEEDBACK
    # Post-show praise and general reactions
    if any(p in lower for p in _FEEDBACK_PHRASES):
        return Intent.FEEDBACK

    # AI / bot questions → QUESTION
    if any(p in lower for p in _AI_QUESTION_PHRASES):
        return Intent.QUESTION

    # Structured intents
    # SHOW: guard against fan *statements* about already having tickets.
    # "I already have my tickets for Saturday!" must return FEEDBACK (fan sharing),
    # not SHOW (which would send a redundant ticket link). Force FEEDBACK so the
    # Gemini fallback is never consulted for this pattern.
    _show_hit = words & _SHOW_KEYWORDS or any(k in lower for k in _SHOW_KEYWORDS if " " in k)
    if _show_hit:
        if any(p in lower for p in _SHOW_POSSESSION_PHRASES):
            return Intent.FEEDBACK  # fan statement about having tickets → acknowledge, don't sell
        return Intent.SHOW
    if words & _JOKE_KEYWORDS or any(k in lower for k in _JOKE_KEYWORDS if " " in k):
        return Intent.JOKE
    if words & _CLIP_KEYWORDS or any(k in lower for k in _CLIP_KEYWORDS if " " in k):
        return Intent.CLIP
    if words & _PODCAST_KEYWORDS or any(k in lower for k in _PODCAST_KEYWORDS if " " in k):
        return Intent.PODCAST
    # Merch: checked before BOOK so "where to buy your shirt" doesn't match the
    # "where to buy" book phrase. Book is safe because "book" is not in
    # _MERCH_ITEM_WORDS, so book questions won't trigger this branch.
    # Two signals required: item word + purchase/query word — keeps false-positive
    # rate near zero. "your shirt is amazing" has item word but no purchase word → skipped.
    if any(p in lower for p in _MERCH_QUERY_PHRASES):
        return Intent.MERCH
    if words & _MERCH_ITEM_WORDS and words & _MERCH_PURCHASE_WORDS:
        return Intent.MERCH

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

greeting — opening the conversation, checking in, saying goodbye, or calling out Zarna's name
  examples: "hi", "hey Zarna!", "are you there?", "good morning", "goodnight", "what's going on?",
            "Zara" (fan mishearing/misspelling Zarna), "Zaria", "Varna"

feedback — reacting, laughing, agreeing, answering Zarna's interactive bits, short affirmations
  examples: "lol", "hahaha", "😂", "so funny!", "that's hilarious", "you were amazing tonight",
            "great show", "loved it", "preach!", "so true", "you crack me up",
            "mother in law" (answering Zarna's "who's my enemy #1?" game),
            "your husband 😂", "her MIL!", "omg I'm dying",
            "yes", "yeah", "correct", "absolutely", "congrats", "yay",
            "awesome", "nice", "thanks", "no", "nope", "not yet", "almost",
            "shut up" (as in "shut up that's so funny!"), "stop it", "no way"

personal — fan sharing facts about themselves OR their location (city, state, country)
  examples: "I'm a teacher from Ohio", "my name is Susan, I'm a mom of 3",
            "3 facts about me: I love hiking, I'm 45, I have 2 dogs",
            "I grew up in India", "I'm Jewish and this hits different",
            "Houston" (fan answering "where are you from?"),
            "NYC", "New Jersey", "Connecticut", "Pasadena, CA",
            "Hartford, CT", "South Bend", "Boulder, CO",
            "Shalabh is hilarious", "Shalabh seems so patient",
            "I'm happily divorced", "I hate my MIL too", "married with 2 kids"

question — fan asking Zarna a direct question expecting a real answer
  examples: "what does Shalabh think of your comedy?", "do your kids watch your shows?",
            "is this actually you or AI?", "are you an AI?", "what AI model do you use?",
            "how do you deal with the MIL?", "when did you start doing comedy?",
            "are you coming to Toronto?", "how is Zarna doing?"

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

merch — asking about buying Zarna's physical merchandise (shirts, hoodies, hats, etc.)
  ONLY use when the fan explicitly wants to buy or find merch items — not for compliments about her style.
  examples: "do you have merch?", "where can I buy your shirt?", "is there a merch store?",
            "how do I order a hoodie?", "do you sell merchandise?"
  DO NOT use for: "your shirt is so funny" (→ feedback), "where can I buy tickets?" (→ show),
                  "where can I buy your book?" (→ book)

general — ONLY use this if the message truly fits none of the above:
  random non-Zarna topics, spam, or deeply context-dependent messages that make no sense alone.
  DO NOT use general for: laughs, reactions, affirmations (yes/no/thanks/congrats),
  location names, MIL answers, fan self-introductions, or Zarna/Shalabh name mentions.

Message: "{message}"

Reply with only one word: greeting, joke, clip, show, book, podcast, merch, personal, feedback, question, or general"""

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
