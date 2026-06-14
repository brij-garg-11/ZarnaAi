"""
SlickText → Twilio number migration responder.

During the cutover from the legacy SlickText number to the new Twilio number,
fans who still text the OLD SlickText number should be pulled onto the new
Twilio thread. When migration mode is enabled, the SlickText number stops
running the full AI conversation and instead:

  1. Sends a ONE-TIME opener FROM the new Twilio number so a thread starts
     there (with the standard compliance footer on first contact).
  2. Replies on SlickText pointing the fan to the new number (every time, as a
     gentle reminder).

STOP/opt-out handling and live-show joins are handled upstream in the webhook
and are unaffected. Everything here is gated by ``SLICKTEXT_MIGRATION_MODE`` and
is OFF by default, so normal SlickText behaviour is unchanged until toggled on.
"""
from __future__ import annotations

import logging
import os
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS twilio_migration_log (
    phone_number TEXT PRIMARY KEY,
    migrated_at  TIMESTAMPTZ DEFAULT NOW()
)
"""


def migration_enabled() -> bool:
    """True when the SlickText→Twilio redirect is active (env-gated, off by default)."""
    return os.getenv("SLICKTEXT_MIGRATION_MODE", "false").strip().lower() in (
        "1", "true", "yes", "on",
    )


def format_us_number(e164: str) -> str:
    """``+18556081717`` → ``(855) 608-1717`` for human-friendly display."""
    digits = "".join(ch for ch in (e164 or "") if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return (e164 or "").strip()


def twilio_opener_text() -> str:
    """The opener sent FROM the new Twilio number, with the compliance footer."""
    base = os.getenv(
        "SLICKTEXT_MIGRATION_OPENER",
        "Hey, it's Zarna! I've got a new number and this is it — save it and "
        "let's keep texting right here from now on. Talk soon!",
    )
    try:
        from app.messaging.contact_card import first_message_with_footer
        return first_message_with_footer(base)
    except Exception:  # pragma: no cover - footer is best-effort
        return base


def slicktext_bridge_text(new_number: str) -> str:
    """The reply sent on the OLD SlickText number pointing to the new number."""
    template = os.getenv(
        "SLICKTEXT_MIGRATION_REPLY",
        "It's Zarna! I've moved to a new number — text me here from now on: "
        "{new}. I just said hi from it. See you there!",
    )
    return template.format(new=format_us_number(new_number))


def claim_migration_send(phone_number: str, get_conn: Callable) -> bool:
    """Atomically record that the Twilio opener is being sent to this fan.

    Returns True only the FIRST time for a given number (so the caller sends the
    opener exactly once); False on subsequent texts. Lazily creates the tracking
    table so the feature is self-contained. Never raises — on any DB error we
    return False (skip the opener) rather than risk a crash in the webhook path.
    """
    if not phone_number:
        return False
    conn = None
    try:
        conn = get_conn()
        if not conn:
            return False
        with conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL)
                cur.execute(
                    "INSERT INTO twilio_migration_log (phone_number) VALUES (%s) "
                    "ON CONFLICT (phone_number) DO NOTHING",
                    (phone_number,),
                )
                return cur.rowcount == 1
    except Exception:
        logger.warning(
            "slicktext_migration: claim failed for ...%s",
            phone_number[-4:] if phone_number else "?", exc_info=True,
        )
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def handle_migration(
    phone_number: str,
    twilio_adapter,
    slicktext_adapter,
    get_conn: Callable,
    new_number: Optional[str] = None,
) -> None:
    """Pull a fan who texted the old SlickText number onto the new Twilio number.

    Sends a one-time opener FROM the Twilio number (starting the new thread) and
    a bridge reply on SlickText. Never raises — SMS failures are logged and
    swallowed so a delivery hiccup can't take down the webhook.
    """
    if not phone_number:
        return
    if new_number is None:
        new_number = os.getenv("TWILIO_PHONE_NUMBER", "")

    # 1. One-time opener from the NEW Twilio number (no from_number → routes via
    #    the verified A2P messaging service).
    first_time = claim_migration_send(phone_number, get_conn)
    if first_time and twilio_adapter is not None:
        try:
            twilio_adapter.send_reply(phone_number, twilio_opener_text())
        except Exception:
            logger.warning(
                "slicktext_migration: twilio opener failed for ...%s",
                phone_number[-4:], exc_info=True,
            )

    # 2. Bridge reply on the OLD SlickText number pointing to the new number.
    if slicktext_adapter is not None:
        try:
            slicktext_adapter.send_reply(phone_number, slicktext_bridge_text(new_number))
        except Exception:
            logger.warning(
                "slicktext_migration: slicktext bridge failed for ...%s",
                phone_number[-4:], exc_info=True,
            )
