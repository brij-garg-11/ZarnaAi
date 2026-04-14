"""
SMB subscriber onboarding flow.

Simplified model:
  1. First text from a new number that looks like an opt-in (keyword, YES, sure, etc.)
     → subscriber created (active immediately) + welcome message + preference question
     + STOP opt-out line sent back.
  2. First text that does NOT look like an opt-in → return None so the brain sends
     the invite nudge ("Want updates? Reply YES").
  3. All subsequent messages → return None so the conversational brain handles them.
  4. Preference saving → passive, background only.  If their next message looks like
     an answer to the preference question (not a question or bot request) we classify
     and save it silently.  The bot still replies normally via the brain.

Entry point: get_onboarding_reply(phone_number, message_text, tenant)
  Returns a reply string only for brand-new opt-in subscribers (the welcome message).
  Returns None for everyone else so the brain takes over.
"""

import logging
import os
import re
import threading
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb import ai as smb_ai
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage
from app.smb import tagging

logger = logging.getLogger(__name__)

_OPEN_QUESTION_KEY = "interest"

_QUESTION_WORDS = re.compile(
    r"^\s*(who|what|when|where|how|is|are|do|does|can|will|would|should|could|any|got)\b",
    re.IGNORECASE,
)

_OPT_IN_PATTERN = re.compile(
    r"^\s*(yes|yeah|yep|yup|sure|ok|okay|in|join|sign me up|subscribe|i'?m in|count me in)\s*[!.]*\s*$",
    re.IGNORECASE,
)


def _ai_thinks_opt_in(text: str) -> bool:
    """
    Ask AI whether a short ambiguous reply signals intent to subscribe.
    Returns True only on a confident YES — defaults to False on any error.
    """
    try:
        from app.smb import ai as smb_ai
        prompt = (
            "A person received an SMS invite to join a text list and replied with the following message.\n"
            f'Their reply: "{text}"\n\n'
            "Does this reply signal they want to subscribe / opt in?\n"
            "Reply with exactly YES or NO."
        )
        answer = smb_ai.generate(prompt)
        result = (answer or "").strip().upper().startswith("YES")
        logger.info("SMB opt-in AI check: '%s' → %s", text[:40], result)
        return result
    except Exception:
        logger.warning("SMB opt-in AI check failed for '%s' — defaulting to False", text[:40])
        return False


def _looks_like_opt_in(text: str, keyword: Optional[str] = None) -> bool:
    """
    Return True if the message looks like the person wants to subscribe.

    Fast path: keyword match or known opt-in word.
    Fallback: AI judgment for anything ambiguous (e.g. "sounds good", "let's do it", "add me").
    """
    stripped = text.strip()

    # Exact keyword match
    if keyword and stripped.upper() == keyword.strip().upper():
        return True

    # Known opt-in words — instant, no AI needed
    if _OPT_IN_PATTERN.match(stripped):
        return True

    # Skip AI for obvious non-opt-ins to avoid latency/cost:
    # questions, very long messages, or empty strings
    if not stripped or len(stripped) > 80 or stripped.endswith("?"):
        return False

    # AI fallback for short ambiguous replies
    return _ai_thinks_opt_in(stripped)


def _looks_like_question_or_request(text: str) -> bool:
    """Return True when the message looks like a question rather than a preference answer."""
    stripped = text.strip()
    if stripped.endswith("?"):
        return True
    if _QUESTION_WORDS.match(stripped):
        return True
    return False


def get_onboarding_reply(
    phone_number: str, message_text: str, tenant: BusinessTenant
) -> Optional[str]:
    """
    Main entry point.

    - Unknown number + opt-in message (keyword, YES, sure, etc.)
      → create subscriber, send vCard, return welcome + question + STOP line.
    - Unknown number + non-opt-in message
      → return None (brain sends the invite nudge).
    - Existing at step 0 → try to save preference passively in background, return None.
    - Everyone else   → return None (brain handles everything).
    """
    conn = get_db_connection()
    if not conn:
        logger.error("SMB onboarding: no DB connection available")
        return None

    try:
        with conn:
            subscriber = smb_storage.get_subscriber(conn, phone_number, tenant.slug)

            if subscriber is None:
                # Unknown number — only subscribe if they explicitly opted in
                if not _looks_like_opt_in(message_text, tenant.keyword):
                    return None  # brain will send the invite nudge
                new_sub = smb_storage.create_subscriber(conn, phone_number, tenant.slug)
                logger.info(
                    "SMB new subscriber: tenant=%s phone=...%s",
                    tenant.slug, phone_number[-4:] if phone_number else "?",
                )
                if new_sub:
                    tagging.tag_geo(conn, new_sub["id"], phone_number)

                # Check for an active timed offer (e.g. free ticket within 24h)
                invite = smb_storage.get_active_invite(conn, phone_number, tenant.slug)
                ticket_number = None
                if invite:
                    ticket_number = smb_storage.claim_invite(conn, invite["id"], tenant.slug)
                    logger.info(
                        "SMB outreach offer claimed: tenant=%s phone=...%s offer=%s ticket=#%s",
                        tenant.slug, phone_number[-4:] if phone_number else "?",
                        invite["offer"], ticket_number,
                    )

                threading.Thread(
                    target=_send_vcard_mms,
                    args=(phone_number, tenant),
                    daemon=True,
                ).start()
                return _welcome_and_question(
                    tenant,
                    claimed_offer=invite["offer"] if invite else None,
                    ticket_number=ticket_number,
                )

            # Existing subscriber who hasn't answered the preference question yet.
            # Let the brain reply normally, but try to save a preference in the background
            # if their message looks like an actual answer (not a question to the bot).
            if subscriber["onboarding_step"] == 0 and not _looks_like_question_or_request(message_text):
                threading.Thread(
                    target=_save_preference_async,
                    args=(phone_number, message_text, tenant),
                    daemon=True,
                ).start()

    except Exception:
        logger.exception(
            "SMB onboarding error: tenant=%s phone=...%s",
            tenant.slug, phone_number[-4:] if phone_number else "?",
        )
    finally:
        conn.close()

    return None  # brain handles all existing subscribers


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _welcome_and_question(
    tenant: BusinessTenant,
    claimed_offer: str | None = None,
    ticket_number: int | None = None,
) -> str:
    """Return the welcome message with the preference question and STOP opt-out line.

    If claimed_offer is set (e.g. 'free_ticket'), appends the reward line including
    the unique ticket_number so they can show it at the door.
    """
    stop_line = "Reply STOP any time to opt out."
    welcome = tenant.welcome_message or f"Welcome to {tenant.display_name}!"

    parts = [welcome]
    if claimed_offer == "free_ticket":
        if ticket_number is not None:
            parts.append(
                f"🎟️ Your free ticket number is #{ticket_number} — "
                f"just show this text at the box office when you arrive. Enjoy the show!"
            )
        else:
            parts.append(
                "As a thank-you for signing up, you've got a FREE ticket to any upcoming show "
                "— just show this text at the box office!"
            )
    if tenant.signup_question:
        parts.append(tenant.signup_question)
    parts.append(stop_line)
    return "\n\n".join(parts)


def _save_preference_async(phone_number: str, answer: str, tenant: BusinessTenant) -> None:
    """
    Background task: classify the subscriber's message as a preference and save it.
    Called fire-and-forget — never raises, never blocks the reply.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn:
            subscriber = smb_storage.get_subscriber(conn, phone_number, tenant.slug)
            if not subscriber or subscriber["onboarding_step"] != 0:
                return  # already classified, or gone

            classified = _classify_answer(answer, tenant)
            for q_key, seg_answer in classified.items():
                smb_storage.save_preference(conn, subscriber["id"], q_key, seg_answer)
            smb_storage.save_preference(conn, subscriber["id"], _OPEN_QUESTION_KEY, answer.strip()[:200])
            smb_storage.advance_onboarding(conn, subscriber["id"], 1, "active")
            logger.info(
                "SMB preference saved (passive): tenant=%s phone=...%s classified=%s",
                tenant.slug, phone_number[-4:] if phone_number else "?", classified,
            )
    except Exception:
        logger.warning(
            "SMB preference save failed: tenant=%s phone=...%s",
            tenant.slug, phone_number[-4:] if phone_number else "?",
            exc_info=True,
        )
    finally:
        conn.close()


def _classify_answer(answer: str, tenant: BusinessTenant) -> dict:
    """
    Use AI to classify the free-text answer into segment(s).
    Falls back to saving the raw answer if AI fails or segments aren't configured.
    """
    if not tenant.segments:
        return {"0": answer.strip()}

    seg_lines = "\n".join(
        f"- {s['name']}: {s.get('description', s['name'])}"
        for s in tenant.segments
        if s["question_key"] == "0"
    )
    seg_names = [s["name"] for s in tenant.segments if s["question_key"] == "0"]

    if not seg_names:
        return {"0": answer.strip()}

    prompt = (
        f"A subscriber to {tenant.display_name} (a {tenant.business_type}) "
        f"was asked: \"{tenant.signup_question}\"\n"
        f"They replied: \"{answer}\"\n\n"
        f"Classify them into ONE of these segments:\n{seg_lines}\n\n"
        f"If genuinely undecided or unclear, use BOTH if available, otherwise pick the closest.\n"
        f"Reply with ONLY the segment name. Options: {', '.join(seg_names)}"
    )

    classified = smb_ai.generate(prompt).strip().upper()

    if classified in {n.upper() for n in seg_names}:
        logger.info(
            "SMB onboarding: classified answer '%s' → %s (tenant=%s)",
            answer[:40], classified, tenant.slug,
        )
        return {"0": classified}

    logger.warning(
        "SMB onboarding: AI returned '%s', saving raw answer (tenant=%s)", classified, tenant.slug
    )
    return {"0": answer.strip()}


def _send_vcard_mms(phone_number: str, tenant: BusinessTenant) -> None:
    """Send the tenant vCard as a follow-up MMS so the subscriber can save the contact."""
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if not domain:
        logger.warning("SMB vcard: RAILWAY_PUBLIC_DOMAIN not set — skipping vCard MMS")
        return
    if not domain.startswith("http"):
        domain = f"https://{domain}"

    vcard_url = f"{domain}/smb/vcard/{tenant.slug}.vcf"

    try:
        from twilio.rest import Client
        from app.config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN

        if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN or not tenant.sms_number:
            logger.warning("SMB vcard: Twilio credentials or sms_number not set — skipping")
            return

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        msg = client.messages.create(
            to=phone_number,
            from_=tenant.sms_number,
            body=f"Tap to save {tenant.display_name} to your contacts.",
            media_url=[vcard_url],
        )
        logger.info("SMB vcard sent: to=...%s SID=%s", phone_number[-4:], msg.sid)
    except Exception:
        logger.warning(
            "SMB vcard: failed to send to ...%s", phone_number[-4:] if phone_number else "?",
            exc_info=True,
        )
