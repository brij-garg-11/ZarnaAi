"""
SMB subscriber onboarding flow.

New flow (single open-ended question):
  1. Customer texts the signup keyword (e.g. "COMEDY")
  2. Bot sends welcome message + one natural question at the end
  3. Customer replies in free text
  4. AI classifies the answer into the right segment(s) and saves preferences
  5. Bot sends a warm completion message

If no welcome_message / signup_question is configured, falls back to the
legacy multi-step forced-choice flow (signup_questions list).

Entry point: get_onboarding_reply(phone_number, message_text, tenant)
Returns a reply string if the message is part of the onboarding flow,
or None if it should be handled by the main SMB brain instead.
"""

import logging
import os
import threading
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb import ai as smb_ai
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)

# question_key used to store the classified answer from the open-ended question
_OPEN_QUESTION_KEY = "interest"


def is_signup_keyword(text: str, tenant: BusinessTenant) -> bool:
    """Return True if the message exactly matches the tenant's signup keyword."""
    if not tenant.keyword:
        return False
    return text.strip().upper() == tenant.keyword.strip().upper()


def get_onboarding_reply(
    phone_number: str, message_text: str, tenant: BusinessTenant
) -> Optional[str]:
    """
    Main entry point for the onboarding flow.

    Returns a reply string when:
    - The message is the signup keyword (starts/restarts onboarding)
    - The subscriber is mid-onboarding and this is their answer

    Returns None when this message is not part of onboarding.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("SMB onboarding: no DB connection available")
        return None

    try:
        with conn:
            subscriber = smb_storage.get_subscriber(conn, phone_number, tenant.slug)

            if is_signup_keyword(message_text, tenant):
                if subscriber and subscriber["onboarding_step"] > 0:
                    # Fully onboarded — already answered the question
                    return (
                        f"You're already subscribed to {tenant.display_name}! "
                        "We'll keep the good stuff coming your way."
                    )
                if subscriber is None:
                    subscriber = smb_storage.create_subscriber(conn, phone_number, tenant.slug)
                    logger.info(
                        "SMB new subscriber: tenant=%s phone=...%s",
                        tenant.slug, phone_number[-4:] if phone_number else "?",
                    )
                    # Send vCard as a follow-up MMS so they can save the contact
                    threading.Thread(
                        target=_send_vcard_mms,
                        args=(phone_number, tenant),
                        daemon=True,
                    ).start()
                return _welcome_and_question(tenant)

            # step == 0 means signed up but hasn't answered the preference question yet
            if subscriber and subscriber["onboarding_step"] == 0:
                return _handle_answer(conn, subscriber, message_text, tenant)

    except Exception:
        logger.exception(
            "SMB onboarding error: tenant=%s phone=...%s",
            tenant.slug, phone_number[-4:] if phone_number else "?",
        )
    finally:
        conn.close()

    return None


# ---------------------------------------------------------------------------
# Internal helpers — new open-ended flow
# ---------------------------------------------------------------------------

def _welcome_and_question(tenant: BusinessTenant) -> str:
    """
    Return the welcome message with the open-ended question appended.
    Falls back to legacy first question if no new config is set.
    """
    if tenant.signup_question:
        welcome = tenant.welcome_message or f"Welcome to {tenant.display_name}!"
        return f"{welcome}\n\n{tenant.signup_question}"

    # Legacy fallback: ask the first forced-choice question directly
    if tenant.signup_questions:
        return tenant.signup_questions[0]

    return f"Welcome to {tenant.display_name}! Reply STOP any time to unsubscribe."


def _handle_answer(
    conn, subscriber: dict, answer: str, tenant: BusinessTenant
) -> str:
    """
    Process the subscriber's answer.

    New flow: AI classifies the free-text answer into segment(s), saves all
    matching segment preferences, marks subscriber active.

    Legacy flow: saves raw answer for the current step, advances to next question.
    """
    if tenant.signup_question:
        return _handle_open_answer(conn, subscriber, answer, tenant)
    return _handle_legacy_answer(conn, subscriber, answer, tenant)


def _handle_open_answer(
    conn, subscriber: dict, answer: str, tenant: BusinessTenant
) -> str:
    """Classify the free-text answer and mark the subscriber active."""
    classified = _classify_answer(answer, tenant)

    # Save all matched segment preferences
    for q_key, seg_answer in classified.items():
        smb_storage.save_preference(conn, subscriber["id"], q_key, seg_answer)

    # Also save the raw interest text for future reference
    smb_storage.save_preference(conn, subscriber["id"], _OPEN_QUESTION_KEY, answer.strip()[:200])

    smb_storage.advance_onboarding(conn, subscriber["id"], 1, "active")
    logger.info(
        "SMB onboarding complete (open): tenant=%s phone=...%s classified=%s",
        tenant.slug,
        subscriber.get("phone_number", "?")[-4:],
        classified,
    )
    return _completion_message(tenant, classified)


def _handle_legacy_answer(
    conn, subscriber: dict, answer: str, tenant: BusinessTenant
) -> str:
    """Legacy: save raw answer for current step, ask next question."""
    step = subscriber["onboarding_step"]
    smb_storage.save_preference(conn, subscriber["id"], str(step), answer.strip())

    next_step = step + 1
    if next_step >= len(tenant.signup_questions):
        smb_storage.advance_onboarding(conn, subscriber["id"], next_step, "active")
        logger.info(
            "SMB onboarding complete (legacy): tenant=%s phone=...%s",
            tenant.slug,
            subscriber.get("phone_number", "?")[-4:],
        )
        return _completion_message(tenant, {})

    smb_storage.advance_onboarding(conn, subscriber["id"], next_step, "onboarding")
    return tenant.signup_questions[next_step]


def _classify_answer(answer: str, tenant: BusinessTenant) -> dict:
    """
    Use Gemini to classify the subscriber's free-text answer into segment(s).

    Returns a dict of {question_key: answer} to save as preferences.
    Falls back to saving the raw answer under question_key "0" if AI fails.
    """
    if not tenant.segments:
        return {"0": answer.strip()}

    # Build segment descriptions for the prompt
    seg_lines = "\n".join(
        f"- {s['name']}: {s.get('description', s['name'])}"
        for s in tenant.segments
        if s["question_key"] == "0"  # classify against the primary interest segments
    )
    seg_names = [s["name"] for s in tenant.segments if s["question_key"] == "0"]

    if not seg_names:
        return {"0": answer.strip()}

    prompt = (
        f"A new subscriber to {tenant.display_name} (a {tenant.business_type}) "
        f"was asked: \"{tenant.signup_question}\"\n"
        f"They replied: \"{answer}\"\n\n"
        f"Classify them into ONE of these segments:\n{seg_lines}\n\n"
        f"If they seem to like more than one, reply with the most specific one that fits. "
        f"If genuinely undecided or unclear, use BOTH if available, otherwise pick the closest.\n"
        f"Reply with ONLY the segment name, nothing else. Options: {', '.join(seg_names)}"
    )

    classified = smb_ai.generate(prompt).strip().upper()

    if classified in {n.upper() for n in seg_names}:
        logger.info(
            "SMB onboarding: classified answer '%s' → %s (tenant=%s)",
            answer[:40], classified, tenant.slug,
        )
        return {"0": classified}

    if classified:
        logger.warning(
            "SMB onboarding: AI returned unknown segment '%s', saving raw answer",
            classified,
        )
    else:
        logger.warning("SMB onboarding: all AI providers failed, saving raw answer")

    # Fallback: save raw answer
    return {"0": answer.strip()}


def _send_vcard_mms(phone_number: str, tenant: BusinessTenant) -> None:
    """
    Send the tenant vCard as a follow-up MMS so the subscriber can save
    the business as a named contact with one tap.
    Requires RAILWAY_PUBLIC_DOMAIN to build the absolute URL.
    """
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


def _completion_message(tenant: BusinessTenant, classified: dict) -> str:
    """Warm, personalised completion message based on what they said."""
    seg_answer = classified.get("0", "").upper()

    if seg_answer == "STANDUP":
        flavour = "We'll make sure you never miss a great standup show."
    elif seg_answer == "IMPROV":
        flavour = "We'll keep you posted on all our improv nights."
    elif seg_answer in ("BOTH", ""):
        flavour = "We'll keep you in the loop on everything happening at the club."
    else:
        flavour = "We'll keep you in the loop on what matters most to you."

    return (
        f"You're in! {flavour} "
        f"Expect exclusive updates and deals from {tenant.display_name}. "
        "Reply STOP any time to unsubscribe."
    )
