"""
Executes a blast from a blast_drafts record.
Runs in a background thread (for send-now) or via the scheduler (for scheduled).
Never exposes phone numbers outside this module.
"""

import logging
import os
import sys
import threading
import time

logger = logging.getLogger(__name__)


def execute_blast(draft_id: int):
    """Load draft, fetch audience phones, send via the chosen channel."""
    from .queries import (
        get_blast_draft,
        get_audience_phones,
        mark_blast_sent,
        mark_blast_cancelled,
    )

    draft = get_blast_draft(draft_id)
    if not draft:
        logger.error("Blast draft %s not found", draft_id)
        return

    phones = get_audience_phones(
        audience_type=draft["audience_type"],
        audience_filter=draft["audience_filter"] or "",
        sample_pct=int(draft["audience_sample_pct"] or 100),
    )

    if not phones:
        mark_blast_sent(draft_id, 0, 0, 0)
        return

    body = draft["body"]
    channel = draft["channel"]
    sent = 0
    failed = 0

    for phone in phones:
        try:
            ok = _send_one(phone, body, channel)
            if ok:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            logger.warning("Blast send error for ...%s: %s", phone[-4:], e)
            failed += 1
        time.sleep(0.05)

    mark_blast_sent(draft_id, sent, failed, len(phones))
    logger.info("Blast %s complete: %s sent, %s failed of %s", draft_id, sent, failed, len(phones))


def execute_blast_async(draft_id: int):
    """Fire-and-forget: run execute_blast in a background thread."""
    t = threading.Thread(target=execute_blast, args=(draft_id,), daemon=True)
    t.start()


def _send_one(phone: str, body: str, channel: str) -> bool:
    """Route to Twilio or SlickText based on channel setting."""
    if channel == "slicktext":
        return _send_slicktext(phone, body)
    return _send_twilio(phone, body)


def _send_twilio(phone: str, body: str) -> bool:
    try:
        from twilio.rest import Client
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_number = os.getenv("TWILIO_PHONE_NUMBER", "")
        if not all([account_sid, auth_token, from_number]):
            logger.error("Twilio credentials not configured")
            return False
        client = Client(account_sid, auth_token)
        msg = client.messages.create(body=body, from_=from_number, to=phone)
        return msg.sid is not None
    except Exception as e:
        logger.warning("Twilio send error: %s", e)
        return False


def _send_slicktext(phone: str, body: str) -> bool:
    try:
        import requests
        api_key = os.getenv("SLICKTEXT_API_KEY", "")
        pub_key = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
        if not api_key or not pub_key:
            logger.error("SlickText credentials not configured")
            return False
        digits = "".join(c for c in phone if c.isdigit())
        if digits.startswith("1") and len(digits) == 11:
            digits = digits[1:]
        resp = requests.post(
            "https://api.slicktext.com/v1/messages",
            json={"number": digits, "message": body},
            auth=(pub_key, api_key),
            timeout=15,
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        logger.warning("SlickText send error: %s", e)
        return False
