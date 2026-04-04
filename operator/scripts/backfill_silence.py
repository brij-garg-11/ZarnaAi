#!/usr/bin/env python3
"""
Nightly cron: mark bot replies that never received a fan response,
close stale conversation sessions, and fill came_back_within_7d.

Run on Railway as a cron job (or locally):
    python scripts/backfill_silence.py

All SQL is self-contained — no imports from the main app/ package.
Safe to re-run: all updates are idempotent.

Performance: uses composite indexes on messages(phone_number, role, created_at)
and a single-pass window for msgs_after_this (not per-row correlated counts).
"""

import logging
import os
import sys
import time

try:
    from dotenv import load_dotenv
    _here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv()
    load_dotenv(os.path.join(_here, "..", ".env"))  # operator/.env
    load_dotenv(os.path.join(_here, "..", "..", ".env"))  # repo root
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [backfill_silence] %(message)s",
    stream=sys.stdout,
    force=True,
)
_logger = logging.getLogger(__name__)


def _banner(msg: str) -> None:
    """Loud lines for Railway / cron logs (stdout + logger)."""
    line = f"=== BACKFILL_CRON {msg} ==="
    print(line, flush=True)
    _logger.info(msg)

SILENCE_HOURS: int = int(os.getenv("SILENCE_HOURS", "24"))
SESSION_GAP_HOURS: int = int(os.getenv("SESSION_GAP_HOURS", "24"))


def _ensure_perf_indexes(conn) -> None:
    """Create indexes if missing (cron may run before main app migration). Idempotent."""
    stmts = [
        """
        CREATE INDEX IF NOT EXISTS idx_messages_phone_role_created
            ON messages (phone_number, role, created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sessions_phone_started
            ON conversation_sessions (phone_number, started_at)
        """,
    ]
    with conn:
        with conn.cursor() as cur:
            for sql in stmts:
                cur.execute(sql)
    _logger.info("ensure_perf_indexes: OK")


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url:
        _logger.error("DATABASE_URL not set — nothing to do.")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def backfill_silence(conn) -> int:
    """Mark bot messages with no subsequent fan reply as went_silent_after=TRUE."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages AS bot_msg
                SET did_user_reply    = FALSE,
                    went_silent_after = TRUE
                WHERE bot_msg.role           = 'assistant'
                  AND bot_msg.did_user_reply IS NULL
                  AND bot_msg.created_at     < NOW() - make_interval(hours => %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM messages AS fan_msg
                      WHERE fan_msg.phone_number = bot_msg.phone_number
                        AND fan_msg.role         = 'user'
                        AND fan_msg.created_at   > bot_msg.created_at
                  )
                """,
                (SILENCE_HOURS,),
            )
            return cur.rowcount


def backfill_msgs_after_this(conn) -> int:
    """Fill msgs_after_this for bot replies that were replied-to but missing the count.

    One pass: window over all rows per phone (chronological), not N correlated COUNTs.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH ordered AS (
                    SELECT id,
                           COALESCE(
                               SUM(CASE WHEN role = 'user' THEN 1 ELSE 0 END) OVER (
                                   PARTITION BY phone_number
                                   ORDER BY created_at ASC, id ASC
                                   ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                               ),
                               0
                           )::int AS users_after
                    FROM messages
                )
                UPDATE messages AS m
                SET msgs_after_this = o.users_after
                FROM ordered AS o
                WHERE m.id = o.id
                  AND m.role = 'assistant'
                  AND m.did_user_reply = TRUE
                  AND m.msgs_after_this IS NULL
                """
            )
            return cur.rowcount


def close_stale_sessions(conn) -> int:
    """Close open sessions where last_active_at is older than SESSION_GAP_HOURS."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE conversation_sessions
                SET    ended_at = last_active_at,
                       ended_by = 'user_silence'
                WHERE  ended_at IS NULL
                  AND  last_active_at < NOW() - make_interval(hours => %s)
                """,
                (SESSION_GAP_HOURS,),
            )
            return cur.rowcount


def backfill_intent(conn) -> int:
    """Fill intent for assistant messages that have NULL intent.

    Finds each assistant row's preceding user message, runs keyword
    classification (no API calls), and sets the intent. Remaining NULLs
    are set to 'general'.
    """
    import re as _re

    # Inline keyword tables (mirror app/brain/intent.py — no app imports in cron)
    _SHOW_KW = {
        "ticket", "tickets", "tour", "touring",
        "performing", "performance", "come see",
        "where are you", "when are you", "tour dates", "venue",
    }
    _JOKE_KW = {
        "joke", "jokes", "laughter", "comedy", "comic",
        "make me laugh", "tell me something funny", "tell me a joke",
        "humor", "humour", "roast", "one liner", "one-liner", "witty",
        "make me smile",
    }
    _CLIP_KW = {
        "video", "videos", "clip", "clips", "youtube", "watch",
        "special", "stand up", "standup", "stand-up", "reel", "reels",
    }
    _PODCAST_KW = {"podcast", "episode", "listen", "audio show"}
    _BOOK_PHRASES = (
        "this american woman", "your book", "the book", "read your book",
        "buy your book", "buy the book", "order your book", "order the book",
        "zarna's book", "zarnas book", "zarna book", "get your book",
        "where to buy", "amazon.com/dp",
    )
    _BOOK_EXTRA = {"kindle", "hardcover", "paperback"}
    _GREETING_EXACT = {
        "hi", "hey", "hello", "hola", "yo", "howdy", "sup",
        "hii", "hiii", "heyyy", "heyy", "hiiii",
        "namaste", "namasté", "good night", "goodnight", "night night",
        # Zarna name misspellings
        "zara", "zaria", "zarnas", "varna", "zarana", "zarha",
    }
    _GREETING_PHRASES = (
        "what's up", "whats up", "wassup", "whaddup",
        "good morning", "good afternoon", "good evening",
        "how are you", "how's it going", "how you doing",
        "are you there", "is anyone there", "you there",
        "what's going on", "whats going on", "what's happening",
        "hi zarna", "hey zarna", "hello zarna",
        "hi there", "hey there", "hello there",
    )
    _LAUGH_EXACT = {
        "lol", "lmao", "lmfao", "rofl", "haha", "hahaha", "hahahaha",
        "hahahahaha", "ha", "hah", "hehe", "heehee", "😂", "😆", "🤣",
        "dead", "💀", "omg", "omfg", "lololol", "lolol",
    }
    _MIL_ANSWERS = (
        "mother in law", "mother-in-law", "mil ", " mil",
        "her mother in law", "your mother in law", "mom in law",
        "the mother in law", "mother in laws",
    )
    _FEEDBACK_PHRASES = (
        "great show", "amazing show", "awesome show", "best show",
        "loved the show", "loved your show", "loved it tonight",
        "you were amazing", "you were great", "you were incredible",
        "you killed it", "you crushed it", "you were hilarious",
        "so funny tonight", "had a blast", "best night ever",
        "such a great time", "what a show", "incredible performance",
        "funniest show", "thank you for the show",
        "so funny", "that's so funny", "that is so funny",
        "hilarious", "you're hilarious", "you are hilarious",
        "you crack me up", "cracking me up", "cracking up",
        "i'm dying", "i am dying", "dying laughing",
        "tears down my face", "laughing so hard", "in stitches",
        "love this", "love it", "love you zarna", "love zarna",
        "you're amazing", "you are amazing", "you're the best",
        "preach", "so true", "exactly",
        "this is gold", "well said", "couldn't agree more",
        "thank you zarna", "thanks zarna", "good night was fun",
        "you were awesome tonight", "we have seen you",
    )
    _AFFIRMATION_EXACT = {
        "yes","yep","yup","yeah","yea","yass","yasss","yaaaas",
        "correct","right","true","absolutely","definitely","exactly",
        "of course","for sure","totally","certainly","indeed","yes indeed",
        "yes ma am","yes maam","yes ma'am",
        "congrats","congratulations","yay","woohoo","woo hoo",
        "awesome","great","nice","cool","sweet","dope","lit","fire",
        "thanks","thank you","ty","thx","thank u",
        "no","nope","nah","not yet","almost","not really","kind of","kinda",
        "maybe","perhaps","idk","idc","sure","ok","okay","okk","okkk","k",
        "yup yup","yes yes","no no","oh yes","oh yeah","oh no",
        "shut up","stop it","no way","get out","get outta here",
    }
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
    _LOCATION_EXACT = {
        "alabama","alaska","arizona","arkansas","california","colorado",
        "connecticut","delaware","florida","georgia","hawaii","idaho",
        "illinois","indiana","iowa","kansas","kentucky","louisiana","maine",
        "maryland","massachusetts","michigan","minnesota","mississippi",
        "missouri","montana","nebraska","nevada","new hampshire","new jersey",
        "new mexico","new york","north carolina","north dakota","ohio",
        "oklahoma","oregon","pennsylvania","rhode island","south carolina",
        "south dakota","tennessee","texas","utah","vermont","virginia",
        "washington","west virginia","wisconsin","wyoming",
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
        "fresno","oklahoma city","el paso","corpus christi",
        "arlington","plano","garland","lincoln","omaha","wichita",
        "colorado springs","greensboro","durham","charlotte","columbia",
        "charleston","savannah","tallahassee","birmingham","montgomery",
        "mobile","knoxville","chattanooga","lexington","indianapolis",
        "fort wayne","columbus","akron","toledo","dayton","detroit",
        "flint","lansing","ann arbor","st louis","springfield","st paul",
        "sioux falls","fargo","bismarck","billings","boise","eugene",
        "salem","tacoma","bellevue","olympia","anchorage","juneau","honolulu",
        "south bend","palo alto","boulder","pasadena","irvine","scottsdale",
        "tempe","chandler","mesa","glendale","fort worth","lubbock",
        "nyc","la","sf","dc","atl","chi","phx","philly","nola","kc",
        "brooklyn","queens","bronx","manhattan","long island",
        "jersey","nj","ct","ny","ca","tx","fl",
        "toronto","vancouver","calgary","montreal","ottawa","edmonton",
        "winnipeg","halifax",
        "mumbai","delhi","new delhi","bangalore","chennai","hyderabad",
        "pune","ahmedabad","kolkata","lucknow","jaipur","surat","chandigarh",
        "london","sydney","dubai","singapore",
    }
    _LOCATION_CITY_STATE_RE = _re.compile(
        r"^[a-z][a-z\s\-]{1,25},\s*[a-z]{2,}$", _re.IGNORECASE
    )
    _PERSONAL_RE = _re.compile(
        r"\b(i'?m a |i am a |i'?m from |i am from |i live in |i work(ed)? (as|at|for|in)|"
        r"my name is |my husband |my wife |my kids |my daughter |my son |my family |"
        r"i have \d+ kids|i'?m \d+ years|i am \d+ years|i just turned \d+|"
        r"i just moved|i grew up|born in |raised in |"
        r"i'?m (a |an |the )?(mom|dad|mother|father|teacher|nurse|doctor|lawyer|engineer|"
        r"therapist|chef|artist|writer|student|retired)|"
        r"three facts|3 facts|\d+ facts about (me|myself)|facts about me|"
        r"fun fact.{0,5}(i |about me)|"
        r"introvert|extrovert|i'?m (jewish|hindu|muslim|catholic|christian|sikh|desi|indian|"
        r"south asian|gori|white|black|latina|asian)|"
        r"shalabh|shalab)",
        _re.IGNORECASE,
    )

    def _classify(text: str) -> str:
        lower = (text or "").lower().strip()
        if not lower:
            return "general"
        # Strip punctuation for word-set matching
        clean = _re.sub(r"[^\w\s]", "", lower)
        words = set(clean.split())

        # Emoji-only laugh reactions (stripped before word split)
        if lower.strip() in _LAUGH_EXACT:
            return "feedback"

        # Greeting (incl. Zarna name misspellings)
        if len(words) <= 7:
            stripped = lower.rstrip("!.? ")
            if stripped in _GREETING_EXACT:
                return "greeting"
            if any(lower.startswith(p) for p in _GREETING_PHRASES):
                return "greeting"

        # Location: very short messages that are entirely a known location → personal
        if len(words) <= 4:
            if clean.strip() in _LOCATION_EXACT:
                return "personal"
            if _LOCATION_CITY_STATE_RE.match(lower.strip()):
                return "personal"

        # Personal first for longer messages (before feedback)
        if len(words) > 4 and _PERSONAL_RE.search(lower) and "?" not in lower:
            return "personal"

        # Feedback: laugh reactions for short messages, MIL answers, praise, affirmations
        if len(words) <= 4 and words & _LAUGH_EXACT:
            return "feedback"
        if len(words) <= 3 and clean.strip() in _AFFIRMATION_EXACT:
            return "feedback"
        if any(p in lower for p in _MIL_ANSWERS):
            return "feedback"
        if any(p in lower for p in _FEEDBACK_PHRASES):
            return "feedback"

        # AI / bot questions
        if any(p in lower for p in _AI_QUESTION_PHRASES):
            return "question"

        # Structured
        if words & _SHOW_KW or any(k in lower for k in _SHOW_KW if " " in k):
            return "show"
        if words & _JOKE_KW or any(k in lower for k in _JOKE_KW if " " in k):
            return "joke"
        if words & _CLIP_KW or any(k in lower for k in _CLIP_KW if " " in k):
            return "clip"
        if words & _PODCAST_KW or any(k in lower for k in _PODCAST_KW if " " in k):
            return "podcast"
        # Book
        if any(p in lower for p in _BOOK_PHRASES) or words & _BOOK_EXTRA:
            return "book"
        if "book" in words and ("zarna" in lower or "american woman" in lower):
            return "book"

        # Personal (short messages)
        if _PERSONAL_RE.search(lower) and "?" not in lower:
            return "personal"

        # Question
        if "?" in lower:
            return "question"

        return "general"

    # Pull assistant messages with NULL intent OR legacy 'general' label
    # + the preceding user message text for each.
    # For 'general' rows we only overwrite when the classifier finds a more
    # specific bucket — if it resolves back to 'general' we leave it alone.
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id,
                       a.intent AS current_intent,
                       (SELECT u.text FROM messages u
                        WHERE u.phone_number = a.phone_number
                          AND u.role = 'user'
                          AND u.created_at <= a.created_at
                        ORDER BY u.created_at DESC
                        LIMIT 1
                       ) AS user_text
                FROM messages a
                WHERE a.role = 'assistant'
                  AND a.source IS DISTINCT FROM 'csv_import'
                  AND a.source IS DISTINCT FROM 'blast'
                  AND (a.intent IS NULL OR a.intent = 'general')
                """
            )
            rows = cur.fetchall()

    if not rows:
        return 0

    updates: dict[str, list[int]] = {}
    for msg_id, current_intent, user_text in rows:
        intent = _classify(user_text)
        # For previously-labeled 'general' rows, only upgrade to a specific bucket
        if current_intent == "general" and intent == "general":
            continue
        updates.setdefault(intent, []).append(msg_id)

    total = 0
    with conn:
        with conn.cursor() as cur:
            for intent_val, ids in updates.items():
                batch_size = 500
                for i in range(0, len(ids), batch_size):
                    batch = ids[i:i + batch_size]
                    cur.execute(
                        "UPDATE messages SET intent = %s WHERE id = ANY(%s)",
                        (intent_val, batch),
                    )
                    total += cur.rowcount
    return total


def backfill_came_back_within_7d(conn) -> int:
    """For closed sessions, fill came_back_within_7d based on subsequent sessions."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE conversation_sessions AS s
                SET    came_back_within_7d = EXISTS (
                    SELECT 1 FROM conversation_sessions AS s2
                    WHERE  s2.phone_number = s.phone_number
                      AND  s2.id          != s.id
                      AND  s2.started_at  >  s.ended_at
                      AND  s2.started_at  <= s.ended_at + INTERVAL '7 days'
                )
                WHERE  s.ended_at IS NOT NULL
                  AND  s.came_back_within_7d IS NULL
                """
            )
            return cur.rowcount


def main():
    t0 = time.perf_counter()
    _banner("START (scripts/backfill_silence.py)")
    _logger.info(
        "config silence_hours=%s session_gap_hours=%s cwd=%s",
        SILENCE_HOURS,
        SESSION_GAP_HOURS,
        os.getcwd(),
    )
    conn = _get_conn()
    try:
        _logger.info("database connected")

        t = time.perf_counter()
        _banner("phase: ensure_perf_indexes")
        _ensure_perf_indexes(conn)
        _logger.info("ensure_perf_indexes done in %.2fs", time.perf_counter() - t)

        t = time.perf_counter()
        _banner("phase: backfill_silence")
        silence_count = backfill_silence(conn)
        _logger.info(
            "backfill_silence: marked %d bot replies as went_silent_after=TRUE (%.2fs)",
            silence_count,
            time.perf_counter() - t,
        )

        t = time.perf_counter()
        _banner("phase: backfill_msgs_after_this (window query)")
        msgs_count = backfill_msgs_after_this(conn)
        _logger.info(
            "backfill_msgs_after_this: filled msgs_after_this for %d rows (%.2fs)",
            msgs_count,
            time.perf_counter() - t,
        )

        t = time.perf_counter()
        _banner("phase: backfill_intent (keyword classifier)")
        intent_count = backfill_intent(conn)
        _logger.info(
            "backfill_intent: classified %d rows (%.2fs)",
            intent_count,
            time.perf_counter() - t,
        )

        t = time.perf_counter()
        _banner("phase: close_stale_sessions")
        closed = close_stale_sessions(conn)
        _logger.info("close_stale_sessions: closed %d sessions (%.2fs)", closed, time.perf_counter() - t)

        t = time.perf_counter()
        _banner("phase: backfill_came_back_within_7d")
        came_back = backfill_came_back_within_7d(conn)
        _logger.info(
            "backfill_came_back_within_7d: updated %d sessions (%.2fs)",
            came_back,
            time.perf_counter() - t,
        )

        t = time.perf_counter()
        _banner("phase: ANALYZE")
        with conn:
            with conn.cursor() as cur:
                cur.execute("ANALYZE messages")
                cur.execute("ANALYZE conversation_sessions")
        _logger.info("ANALYZE done (%.2fs)", time.perf_counter() - t)

        _banner(
            f"DONE total_wall_s={time.perf_counter() - t0:.2f}s "
            f"(silence={silence_count} msgs_after={msgs_count} intent={intent_count} "
            f"closed={closed} came_back={came_back})"
        )
    except Exception:
        _logger.exception("BACKFILL_CRON FATAL — uncaught exception")
        print("=== BACKFILL_CRON FATAL (see log above) ===", flush=True)
        raise
    finally:
        conn.close()
        _logger.info("connection closed")


if __name__ == "__main__":
    main()
