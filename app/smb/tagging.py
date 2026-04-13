"""
SMB subscriber auto-tagging.

Infers segment preferences from observable signals — no user input required.
Tags are stored in smb_preferences under dedicated question_keys and are used
by blast.py's existing get_subscribers_by_segment() to target sends.

  question_key='geo'           → LOCAL | OUT_OF_TOWN   (set once at signup)
  question_key='engagement'    → HIGH                  (set when ≥3 inbound messages)
  question_key='intent_tickets'→ YES                   (set when ticket/show intent detected)
  question_key='intent_deals'  → YES                   (set when deal/discount intent detected)

All tagging is additive and idempotent (upsert). Tags are never downgraded once set.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Geo tagging
# ---------------------------------------------------------------------------

# NYC boroughs + immediate commuter belt treated as LOCAL
_LOCAL_AREA_CODES: frozenset[str] = frozenset({
    # NYC five boroughs
    "212", "718", "929", "646", "347", "917",
    # New Jersey (Hudson, Essex, Bergen, Union, Passaic counties)
    "201", "973", "908", "732", "848", "551",
    # Westchester / Long Island / Hudson Valley / Connecticut suburbs
    "914", "516", "631", "845", "203",
})


def infer_geo(phone_number: str) -> str:
    """
    Return 'LOCAL' or 'OUT_OF_TOWN' based on the subscriber's area code.
    Treats NYC + immediate NJ/suburban-NY area codes as LOCAL.
    International numbers (non-+1) are always OUT_OF_TOWN.
    """
    digits = re.sub(r"\D", "", phone_number or "")
    if digits.startswith("1"):
        digits = digits[1:]
    # Non-US number (too short or +44/+52/etc.) → out of town
    if len(digits) < 10:
        return "OUT_OF_TOWN"
    area_code = digits[:3]
    return "LOCAL" if area_code in _LOCAL_AREA_CODES else "OUT_OF_TOWN"


def tag_geo(conn, subscriber_id: int, phone_number: str) -> None:
    """
    Save geo preference for a new subscriber. Called synchronously at signup
    inside the same DB transaction as subscriber creation. Safe to call multiple
    times — upserts, so a re-signup won't flip the tag.
    """
    geo = infer_geo(phone_number)
    smb_storage.save_preference(conn, subscriber_id, "geo", geo)
    logger.info(
        "SMB tagging: geo=%s for subscriber_id=%d (phone=...%s)",
        geo, subscriber_id, phone_number[-4:] if phone_number else "?",
    )


# ---------------------------------------------------------------------------
# Engagement + intent tagging
# ---------------------------------------------------------------------------

_HIGH_ENGAGEMENT_THRESHOLD = 3   # inbound messages to qualify as HIGH engagement

_TICKET_INTENT_RE = re.compile(
    r"\b(ticket|tickets|buy|purchase|grab|book|reserve|seat|seats|tonight|show|shows|"
    r"available|opening|headliner|lineup)\b",
    re.IGNORECASE,
)

_DEAL_INTENT_RE = re.compile(
    r"\b(deal|deals|discount|discounts|promo|off|cheap|price|prices|free|code|coupon|"
    r"special|offer|sale)\b",
    re.IGNORECASE,
)


def tag_engagement_async(
    phone_number: str,
    subscriber_id: int,
    tenant_slug: str,
    message_text: str,
    inbound_count: int,
) -> None:
    """
    Background task: update engagement and intent tags from a single message.

    Gets its own DB connection. Never raises — errors are logged and swallowed
    so a tagging failure never interrupts the reply flow.

    Arguments:
        phone_number:   subscriber's phone (for logging only)
        subscriber_id:  DB id of the subscriber row
        tenant_slug:    used for logging
        message_text:   the inbound message to scan for intent signals
        inbound_count:  total inbound messages so far (from conversation history)
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn:
            if inbound_count >= _HIGH_ENGAGEMENT_THRESHOLD:
                smb_storage.save_preference(conn, subscriber_id, "engagement", "HIGH")
                logger.debug(
                    "SMB tagging: engagement=HIGH sub_id=%d tenant=%s",
                    subscriber_id, tenant_slug,
                )

            if _TICKET_INTENT_RE.search(message_text):
                smb_storage.save_preference(conn, subscriber_id, "intent_tickets", "YES")
                logger.debug(
                    "SMB tagging: intent_tickets=YES sub_id=%d tenant=%s",
                    subscriber_id, tenant_slug,
                )

            if _DEAL_INTENT_RE.search(message_text):
                smb_storage.save_preference(conn, subscriber_id, "intent_deals", "YES")
                logger.debug(
                    "SMB tagging: intent_deals=YES sub_id=%d tenant=%s",
                    subscriber_id, tenant_slug,
                )
    except Exception:
        logger.warning(
            "SMB tagging: engagement tag failed for sub_id=%d tenant=%s",
            subscriber_id, tenant_slug,
            exc_info=True,
        )
    finally:
        conn.close()
