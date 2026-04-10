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
        mark_blast_started,
        mark_blast_progress,
        mark_blast_sent,
        mark_blast_cancelled,
    )

    logger.info("=== BLAST WORKER starting for draft %s ===", draft_id)
    draft = get_blast_draft(draft_id)
    if not draft:
        logger.error("Blast draft %s not found in DB", draft_id)
        return

    media_url          = (draft.get("media_url") or "").strip()
    tracked_link_slug  = (draft.get("tracked_link_slug") or "").strip()
    tracked_short_url  = ""

    # Build the full public short URL from the stored slug (main app serves /t/<slug>)
    # We derive the base URL from DATABASE_URL domain or fall back to the env var.
    if tracked_link_slug:
        main_base = os.getenv("MAIN_APP_BASE_URL", "").rstrip("/")
        if not main_base:
            # Derive from Railway's public domain env var when MAIN_APP_BASE_URL not set
            railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
            main_base = f"https://{railway_domain}" if railway_domain else ""
        tracked_short_url = f"{main_base}/t/{tracked_link_slug}" if main_base else ""
        logger.info("  tracked link: slug=%r  short_url=%r", tracked_link_slug, tracked_short_url)

    logger.info("  draft: body=%r  channel=%r  audience_type=%r  audience_filter=%r  media_url=%r  link_slug=%r",
                (draft["body"] or "")[:60], draft["channel"],
                draft["audience_type"], draft["audience_filter"],
                media_url[:60] if media_url else "",
                tracked_link_slug or "")

    phones = get_audience_phones(
        audience_type=draft["audience_type"],
        audience_filter=draft["audience_filter"] or "",
        sample_pct=int(draft["audience_sample_pct"] or 100),
    )
    logger.info("  audience phones count: %d", len(phones))

    if not phones:
        logger.warning("  No phones found — marking sent with 0")
        mark_blast_sent(draft_id, 0, 0, 0)
        return

    body = draft["body"]
    channel = draft["channel"]

    if not body:
        logger.error("  body is empty in DB — aborting blast")
        mark_blast_sent(draft_id, 0, len(phones), len(phones))
        return

    # Append the tracked short URL to every outgoing message (invisible to operator)
    send_body = body
    if tracked_short_url:
        send_body = f"{body}\n{tracked_short_url}"
        logger.info("  appended tracked URL to body — final length=%d", len(send_body))

    total = len(phones)
    mark_blast_started(draft_id, total)

    sent = 0
    failed = 0

    for i, phone in enumerate(phones):
        try:
            ok = _send_one(phone, send_body, channel, media_url=media_url)
            logger.info("  send to ...%s via %s: %s", phone[-4:], channel, "OK" if ok else "FAIL")
            if ok:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            logger.warning("  send error for ...%s: %s", phone[-4:], e)
            failed += 1
        time.sleep(0.05)
        # Write progress every 50 sends so the UI can poll live counts
        if (i + 1) % 50 == 0:
            mark_blast_progress(draft_id, sent, failed)

    mark_blast_sent(draft_id, sent, failed, total)
    logger.info("=== BLAST %s DONE: %s sent, %s failed of %s ===", draft_id, sent, failed, len(phones))

    # If this was a quiz blast, create a quiz_sessions row now so inbound replies get context.
    if draft.get("is_quiz") and (draft.get("quiz_correct_answer") or "").strip():
        show_id = None
        if draft.get("audience_type") == "show" and (draft.get("audience_filter") or "").strip():
            try:
                show_id = int(draft["audience_filter"])
            except (ValueError, TypeError):
                pass
        _create_quiz_session(
            show_id=show_id,
            blast_draft_id=draft_id,
            question_text=draft["body"],
            correct_answer=draft["quiz_correct_answer"],
        )

    # Always create a blast_context_sessions row so inbound replies get AI context.
    # The context includes the blast body so the AI knows what was sent, plus any
    # optional operator note for additional background.
    blast_body = (draft.get("body") or "").strip()
    extra_note = (draft.get("blast_context_note") or "").strip()
    if blast_body:
        combined = f"The blast message that was sent: \"{blast_body}\""
        if extra_note:
            combined += f"\n\nAdditional context from the operator: {extra_note}"
        _create_blast_context_session(draft_id, combined)

    # Update tracked_links.sent_to with the number of recipients this blast reached
    if tracked_link_slug and len(phones) > 0:
        try:
            from .db import get_conn
            conn = get_conn()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tracked_links SET sent_to = sent_to + %s WHERE slug = %s",
                        (len(phones), tracked_link_slug),
                    )
            conn.close()
            logger.info("  updated sent_to +%d for slug=%r", len(phones), tracked_link_slug)
        except Exception as e:
            logger.warning("  could not update sent_to: %s", e)


def execute_blast_async(draft_id: int):
    """Fire-and-forget: run execute_blast in a background thread."""
    t = threading.Thread(target=execute_blast, args=(draft_id,), daemon=True)
    t.start()


def _create_quiz_session(
    show_id: int | None,
    blast_draft_id: int,
    question_text: str,
    correct_answer: str,
) -> None:
    """Insert a quiz_sessions row so the main app's inbound handler can quiz fans."""
    try:
        from .db import get_conn
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO quiz_sessions
                      (show_id, blast_draft_id, question_text, correct_answer, expires_at)
                    VALUES (%s, %s, %s, %s, NOW() + INTERVAL '24 hours')
                    """,
                    (show_id, blast_draft_id, question_text, correct_answer),
                )
        conn.close()
        logger.info(
            "_create_quiz_session: created for blast_draft_id=%s show_id=%s",
            blast_draft_id, show_id,
        )
    except Exception as e:
        logger.exception("_create_quiz_session failed: %s", e)


def _create_blast_context_session(blast_draft_id: int, context_note: str) -> None:
    """Insert a blast_context_sessions row so inbound replies get soft AI context."""
    try:
        from .db import get_conn
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO blast_context_sessions
                      (blast_draft_id, context_note, expires_at)
                    VALUES (%s, %s, NOW() + INTERVAL '24 hours')
                    """,
                    (blast_draft_id, context_note),
                )
        conn.close()
        logger.info(
            "_create_blast_context_session: created for blast_draft_id=%s",
            blast_draft_id,
        )
    except Exception as e:
        logger.exception("_create_blast_context_session failed: %s", e)


def _send_one(phone: str, body: str, channel: str, *, media_url: str = "") -> bool:
    """Route to Twilio or SlickText based on channel setting."""
    if channel == "slicktext":
        return _send_slicktext(phone, body, media_url=media_url)
    return _send_twilio(phone, body, media_url=media_url)


def _send_twilio(phone: str, body: str, *, media_url: str = "") -> bool:
    try:
        from twilio.rest import Client
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_number = os.getenv("TWILIO_PHONE_NUMBER", "")
        if not all([account_sid, auth_token, from_number]):
            logger.error("Twilio credentials not configured")
            return False
        client = Client(account_sid, auth_token)
        kwargs = dict(body=body, from_=from_number, to=phone)
        if media_url:
            kwargs["media_url"] = [media_url]
            logger.info("  [Twilio] sending MMS with media_url=%r", media_url[:60])
        msg = client.messages.create(**kwargs)
        return msg.sid is not None
    except Exception as e:
        logger.warning("Twilio send error: %s", e)
        return False


def _send_slicktext(phone: str, body: str, *, media_url: str = "") -> bool:
    """
    Send via SlickText v1 API — mirrors the main app's SlickTextAdapter._send_v1 exactly.
    Requires: SLICKTEXT_PUBLIC_KEY, SLICKTEXT_PRIVATE_KEY, SLICKTEXT_TEXTWORD_ID
    Supports MMS by adding mediaUrl when provided.
    """
    try:
        import requests
        pub_key     = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
        priv_key    = os.getenv("SLICKTEXT_PRIVATE_KEY", "")
        textword_id = os.getenv("SLICKTEXT_TEXTWORD_ID", "")

        logger.info("  [ST-v1] pub_key set=%s  priv_key set=%s  textword_id=%r",
                    bool(pub_key), bool(priv_key), textword_id)

        if not pub_key or not priv_key:
            logger.error("  [ST-v1] MISSING credentials: SLICKTEXT_PUBLIC_KEY / SLICKTEXT_PRIVATE_KEY not set")
            return False
        if not textword_id:
            logger.error("  [ST-v1] MISSING SLICKTEXT_TEXTWORD_ID — required for v1 outbound sends")
            return False

        payload = {
            "action":   "SEND",
            "textword": textword_id,
            "number":   phone,
            "body":     body,
        }
        if media_url:
            payload["mediaUrl"] = media_url
            logger.info("  [ST-v1] sending MMS with mediaUrl=%r", media_url[:60])

        logger.info("  [ST-v1] POST /v1/messages/  payload=%r", payload)

        resp = requests.post(
            "https://api.slicktext.com/v1/messages/",
            data=payload,
            auth=(pub_key, priv_key),
            timeout=10,
        )
        logger.info("  [ST-v1] response: status=%s  body=%r", resp.status_code, resp.text[:300])

        if resp.status_code == 200:
            logger.info("  [ST-v1] SENT OK to ...%s", phone[-4:])
            return True
        logger.error("  [ST-v1] FAILED: %s — %s", resp.status_code, resp.text[:300])
        return False
    except Exception as e:
        logger.exception("  [ST-v1] EXCEPTION: %s", e)
        return False
