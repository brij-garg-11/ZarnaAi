"""Giveaway keyword detection + entry recording.

Called once per inbound message from ZarnaBrain.handle_incoming_message (the
single seam both the Twilio and SlickText webhooks funnel through). If the
message matches the keyword of an active giveaway campaign AND the fan isn't
already entered, we record the entry and return a confirmation string — the
existing reply pipeline then sends it as the fan's reply instead of an AI
answer. On anything else (no active campaign, no keyword match, already
entered, or any error) we return None so the normal AI reply proceeds.

Design notes:
  * Matching is "contains" (case-insensitive substring), per campaign config.
  * The confirmation replaces the AI reply ONLY on a brand-new entry. An
    already-entered fan who later says something containing the keyword keeps
    chatting with the AI normally — so ongoing conversations aren't hijacked.
  * Active-campaign lookups are cached briefly per process to avoid a DB round
    trip on every inbound during the ~360 days/year no giveaway is running.
  * Every DB path is defensive: a giveaway failure must never block a reply.
"""

from __future__ import annotations

import logging
import os
import threading
import time

_logger = logging.getLogger(__name__)

_DEFAULT_CONFIRMATION = (
    "🎉 You're in! You've been entered into this week's giveaway. "
    "Winners get a call live every Friday — good luck! 🍍"
)

# Short TTL cache of active campaigns, keyed by creator_slug. Bounds DB load to
# ~once per _CACHE_TTL per worker regardless of message volume. A newly created
# campaign starts catching entries within _CACHE_TTL seconds.
_CACHE_TTL = 20
_cache: dict[str, tuple[float, list[dict]]] = {}
_cache_lock = threading.Lock()


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


def keyword_matches(keyword: str, message: str) -> bool:
    """Pure predicate: does `message` contain `keyword` (case-insensitive)?

    Kept dependency-free so it's trivially unit-testable.
    """
    if not keyword or not message:
        return False
    return keyword.strip().lower() in message.lower()


def _active_campaigns(slug: str) -> list[dict]:
    """Active campaigns for this slug, cached for _CACHE_TTL seconds."""
    now = time.monotonic()
    with _cache_lock:
        cached = _cache.get(slug)
        if cached and (now - cached[0]) < _CACHE_TTL:
            return cached[1]

    conn = _get_db()
    if not conn:
        return []
    rows: list[dict] = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, keyword, confirmation_message
                    FROM giveaway_campaigns
                    WHERE creator_slug = %s
                      AND (starts_at IS NULL OR starts_at <= NOW())
                      AND (ends_at   IS NULL OR ends_at   >= NOW())
                    """,
                    (slug,),
                )
                for cid, keyword, confirmation in cur.fetchall():
                    rows.append(
                        {"id": cid, "keyword": keyword, "confirmation": confirmation}
                    )
    except Exception:
        _logger.warning("[ZARNA] giveaway: active-campaign lookup failed", exc_info=True)
        return []
    finally:
        conn.close()

    with _cache_lock:
        _cache[slug] = (now, rows)
    return rows


def invalidate_cache(slug: str | None = None) -> None:
    """Drop cached active campaigns (called after create/delete in the admin)."""
    with _cache_lock:
        if slug is None:
            _cache.clear()
        else:
            _cache.pop(slug, None)


def _insert_entry(
    campaign_id: int, phone_number: str, message_id, source: str, slug: str
) -> bool:
    """Insert an entry; return True only if a NEW row was created (dedup-aware)."""
    conn = _get_db()
    if not conn:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO giveaway_entries
                        (campaign_id, phone_number, message_id, source, creator_slug)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id, phone_number) DO NOTHING
                    RETURNING id
                    """,
                    (campaign_id, phone_number, message_id, source, slug),
                )
                return cur.fetchone() is not None
    except Exception:
        _logger.warning("[ZARNA] giveaway: entry insert failed", exc_info=True)
        return False
    finally:
        conn.close()


def try_giveaway_entry(
    phone_number: str,
    message_text: str,
    message_id=None,
    slug: str = "zarna",
    source: str | None = None,
) -> str | None:
    """Record a giveaway entry if the message matches an active campaign.

    Returns the confirmation string to send back (on a brand-new entry), or
    None if nothing should change about the normal reply flow.
    """
    if not message_text or not phone_number:
        return None

    campaigns = _active_campaigns(slug)
    if not campaigns:
        return None

    confirmation: str | None = None
    for c in campaigns:
        if not keyword_matches(c["keyword"], message_text):
            continue
        created = _insert_entry(c["id"], phone_number, message_id, source or "", slug)
        if created:
            _logger.info(
                "[ZARNA] giveaway entry recorded campaign=%s phone=...%s",
                c["id"], phone_number[-4:] if phone_number else "?",
            )
            if confirmation is None:
                confirmation = (c["confirmation"] or "").strip() or _DEFAULT_CONFIRMATION
    return confirmation
