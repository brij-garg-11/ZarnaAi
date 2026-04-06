"""
SMB subscriber onboarding flow.

Handles the full signup conversation for a new subscriber:
  1. Customer texts the signup keyword (e.g. "COMEDY")
  2. Bot asks preference questions one at a time
  3. Answers are saved; subscriber is marked active when all answered

Entry point: get_onboarding_reply(phone_number, message_text, tenant)
Returns a reply string if the message is part of the onboarding flow,
or None if it should be handled by the main SMB brain instead.
"""

import logging
from typing import Optional

from app.admin_auth import get_db_connection
from app.smb.tenants import BusinessTenant
from app.smb import storage as smb_storage

logger = logging.getLogger(__name__)


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
    - The message is a signup keyword (starts or restarts onboarding)
    - The subscriber is mid-onboarding and this is their next answer

    Returns None when this message is not part of onboarding (hand off
    to the main SMB brain for regular conversation).
    """
    conn = get_db_connection()
    if not conn:
        logger.error("SMB onboarding: no DB connection available")
        return None

    try:
        with conn:
            subscriber = smb_storage.get_subscriber(conn, phone_number, tenant.slug)

            if is_signup_keyword(message_text, tenant):
                if subscriber and subscriber["status"] == "active":
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
                return _ask_question(tenant, 0)

            if subscriber and subscriber["status"] == "onboarding":
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
# Internal helpers
# ---------------------------------------------------------------------------

def _ask_question(tenant: BusinessTenant, step: int) -> str:
    """Return the question for the given step, or the completion message if done."""
    questions = tenant.signup_questions
    if step < len(questions):
        return questions[step]
    return _completion_message(tenant)


def _completion_message(tenant: BusinessTenant) -> str:
    return (
        f"You're all set! Welcome to {tenant.display_name}. "
        "You'll hear from us with tips and exclusive offers. "
        "Reply STOP any time to unsubscribe."
    )


def _handle_answer(
    conn, subscriber: dict, answer: str, tenant: BusinessTenant
) -> str:
    """Save the subscriber's answer, advance their step, return the next question."""
    step = subscriber["onboarding_step"]
    smb_storage.save_preference(conn, subscriber["id"], str(step), answer.strip())

    next_step = step + 1
    if next_step >= len(tenant.signup_questions):
        smb_storage.advance_onboarding(conn, subscriber["id"], next_step, "active")
        logger.info(
            "SMB onboarding complete: tenant=%s phone=...%s",
            tenant.slug, subscriber["phone_number"][-4:] if subscriber.get("phone_number") else "?",
        )
        return _completion_message(tenant)

    smb_storage.advance_onboarding(conn, subscriber["id"], next_step, "onboarding")
    return _ask_question(tenant, next_step)
