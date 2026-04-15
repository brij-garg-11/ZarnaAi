"""
Bot reply link tracker.

Rewrites URLs in outbound bot messages to tracked /t/<slug> redirects,
transparently to the fan.  Two canonical "bot buckets" accumulate all
bot-driven clicks into a single row each, so the Conversions tab always
shows one aggregate number per category.

  bot-website  →  any zarnagarg.com URL (homepage, /tickets, anything)
  bot-podcast  →  any known podcast platform URL

Configure via env vars:
  TRACK_WEBSITE_DOMAIN   (default: zarnagarg.com)
  TRACK_PODCAST_DOMAINS  (default: spotify.com,podcasts.apple.com,…)
  MAIN_APP_BASE_URL      (e.g. https://zarnaai-production.up.railway.app)
"""

import base64
import logging
import os
import re
from urllib.parse import urlparse

_logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────

_WEBSITE_DOMAIN = os.getenv("TRACK_WEBSITE_DOMAIN", "zarnagarg.com").lower().lstrip("www.")

_PODCAST_DOMAINS: set[str] = {
    d.strip().lower()
    for d in os.getenv(
        "TRACK_PODCAST_DOMAINS",
        "spotify.com,podcasts.apple.com,anchor.fm,buzzsprout.com,"
        "podbean.com,soundcloud.com,pca.st,overcast.fm",
    ).split(",")
    if d.strip()
}

# Fixed slugs — same row reused every time the bot sends these link types
_SLUG_WEBSITE = "bot-website"
_SLUG_PODCAST = "bot-podcast"

# Lazy cache so we hit the DB once per process lifetime, not per reply
_short_url_cache: dict[str, str] = {}

# Matches any http(s):// URL in text (greedy up to next whitespace)
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


# ── DB helpers ──────────────────────────────────────────────────────────────

def _get_db():
    try:
        import psycopg2
        url = os.getenv("DATABASE_URL", "")
        if not url:
            return None
        dsn = url.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(dsn)
    except Exception:
        return None


def _ensure_bot_links(base_url: str) -> dict[str, str]:
    """
    Idempotently create the two canonical bot tracked-link rows, then
    cache their short URLs for the life of this process.
    Returns {_SLUG_WEBSITE: short_url, _SLUG_PODCAST: short_url}.
    """
    global _short_url_cache
    if _short_url_cache:
        return _short_url_cache

    conn = _get_db()
    if not conn:
        return {}

    rows = [
        (_SLUG_WEBSITE, "Bot → Website / Tickets", "ticket",  f"https://{_WEBSITE_DOMAIN}"),
        (_SLUG_PODCAST, "Bot → Podcast",           "podcast", "https://open.spotify.com"),
    ]
    result: dict[str, str] = {}
    try:
        with conn:
            with conn.cursor() as cur:
                for slug, label, ctype, dest in rows:
                    cur.execute(
                        """
                        INSERT INTO tracked_links (slug, label, campaign_type, destination)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (slug) DO NOTHING
                        """,
                        (slug, label, ctype, dest),
                    )
                    # Fix any row that was previously stored without https://
                    cur.execute(
                        """
                        UPDATE tracked_links
                        SET destination = %s
                        WHERE slug = %s AND destination NOT LIKE 'https://%%'
                        """,
                        (dest, slug),
                    )
                    result[slug] = f"{base_url}/t/{slug}"
        _short_url_cache = result
        _logger.info("link_tracker: bot canonical links ensured — %s", list(result.keys()))
    except Exception as e:
        _logger.warning("link_tracker _ensure_bot_links error: %s", e)
    finally:
        conn.close()
    return result


# ── Phone token helpers ─────────────────────────────────────────────────────

def encode_phone_token(phone: str) -> str:
    """Encode a phone number to a URL-safe base64 token for embedding in tracked links."""
    return base64.urlsafe_b64encode(phone.encode()).decode()


def decode_phone_token(token: str) -> str | None:
    """Decode a base64 phone token back to a phone number string. Returns None on error."""
    try:
        # Add padding if needed
        padding = 4 - len(token) % 4
        if padding != 4:
            token += "=" * padding
        return base64.urlsafe_b64decode(token).decode()
    except Exception:
        return None


# ── URL classification ───────────────────────────────────────────────────────

def _classify_url(url: str) -> str | None:
    """Return 'website', 'podcast', or None (not a tracked domain)."""
    try:
        domain = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return None
    if _WEBSITE_DOMAIN and (domain == _WEBSITE_DOMAIN or domain.endswith("." + _WEBSITE_DOMAIN)):
        return "website"
    if any(domain == pd or domain.endswith("." + pd) for pd in _PODCAST_DOMAINS):
        return "podcast"
    return None


# ── Public entry point ───────────────────────────────────────────────────────

def rewrite_bot_reply(reply: str, phone_number: str | None = None) -> str:
    """
    Scan an outbound bot reply for tracked domains and replace matching
    URLs with the canonical short /t/<slug> link.  Non-matching URLs are
    left untouched.  Returns the (possibly rewritten) reply string.

    When phone_number is supplied, a ?f=<token> query parameter is appended
    to each rewritten URL so clicks can be attributed back to this fan and
    link_clicked_1h can be set on their message row.
    """
    if not reply:
        return reply

    # Quick bail-out before any DB/regex work
    lower_reply = reply.lower()
    has_website = _WEBSITE_DOMAIN and _WEBSITE_DOMAIN in lower_reply
    has_podcast = any(pd in lower_reply for pd in _PODCAST_DOMAINS)
    if not has_website and not has_podcast:
        return reply

    # Build our own base URL
    base_url = os.getenv("MAIN_APP_BASE_URL", "").rstrip("/")
    if not base_url:
        railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
        base_url = f"https://{railway_domain}" if railway_domain else ""
    if not base_url:
        _logger.warning("link_tracker: MAIN_APP_BASE_URL not set — skipping URL rewrite")
        return reply

    slug_map = _ensure_bot_links(base_url)
    if not slug_map:
        return reply

    fan_suffix = f"?f={encode_phone_token(phone_number)}" if phone_number else ""
    website_short = slug_map.get(_SLUG_WEBSITE, "")
    podcast_short = slug_map.get(_SLUG_PODCAST, "")

    def _replace(m: re.Match) -> str:
        url = m.group(0)
        bucket = _classify_url(url)
        if bucket == "website" and website_short:
            short = f"{website_short}{fan_suffix}"
            _logger.info("link_tracker: website %r → %s", url[:80], short)
            return short
        if bucket == "podcast" and podcast_short:
            short = f"{podcast_short}{fan_suffix}"
            _logger.info("link_tracker: podcast %r → %s", url[:80], short)
            return short
        return url

    return _URL_RE.sub(_replace, reply)
