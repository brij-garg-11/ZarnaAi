"""
SlickText → Twilio number migration responder.

During the cutover from the legacy SlickText number to the new Twilio number,
fans who still text the OLD SlickText number should be pulled onto the new
Twilio thread. When migration mode is enabled, the SlickText number stops
running the full AI conversation and instead:

  1. Sends a redirect opener FROM the new Twilio number so a thread starts (or
     resumes) there, with the standard compliance footer. This fires on every
     inbound, throttled by a per-fan cooldown (``SLICKTEXT_MIGRATION_OPENER_
     COOLDOWN_HOURS``, default 24h) so a fan who texts the old line repeatedly
     isn't spammed. This is the path we rely on, because it does NOT depend on
     SlickText being able to send.
  2. Best-effort reply ON SlickText pointing the fan to the new number. This
     requires an active SlickText plan with outbound credits; if SlickText is
     downgraded/cancelled the send simply fails and is swallowed — the fan is
     still reached via the Twilio opener in step 1.

STOP/opt-out handling and live-show joins are handled upstream in the webhook
and are unaffected. Everything here is gated by ``SLICKTEXT_MIGRATION_MODE`` and
is OFF by default, so normal SlickText behaviour is unchanged until toggled on.

NOTE: All of this only runs if SlickText still delivers inbound webhooks to us.
If the SlickText number is fully cancelled (number released) or inbound delivery
stops, nothing here fires — the redirect then has to happen at the carrier /
SlickText level, or by porting the old number to Twilio.
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


def opener_cooldown_seconds() -> int:
    """How long to wait before re-sending the Twilio opener to the same fan.

    Configurable via ``SLICKTEXT_MIGRATION_OPENER_COOLDOWN_HOURS`` (default 24h).
    A value of ``0`` means send on every single inbound (no throttling).
    """
    raw = os.getenv("SLICKTEXT_MIGRATION_OPENER_COOLDOWN_HOURS", "24").strip()
    try:
        hours = float(raw)
    except (TypeError, ValueError):
        hours = 24.0
    return max(0, int(hours * 3600))


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


def claim_migration_send(
    phone_number: str,
    get_conn: Callable,
    cooldown_seconds: Optional[int] = None,
) -> bool:
    """Atomically decide whether to send the Twilio opener to this fan now.

    Returns True when the opener should be sent: either the fan has never been
    redirected, or their last redirect was longer ago than ``cooldown_seconds``.
    Returns False while inside the cooldown window so a fan who texts the old
    number repeatedly isn't spammed from the new number. When ``cooldown_seconds``
    is 0 it returns True on every inbound.

    Lazily creates the tracking table so the feature is self-contained. Never
    raises — on any DB error we return False (skip the opener) rather than risk a
    crash in the webhook path.
    """
    if not phone_number:
        return False
    if cooldown_seconds is None:
        cooldown_seconds = opener_cooldown_seconds()
    conn = None
    try:
        conn = get_conn()
        if not conn:
            return False
        with conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL)
                # Insert on first contact; otherwise refresh the timestamp only
                # if we're past the cooldown. rowcount == 1 means "send now".
                cur.execute(
                    "INSERT INTO twilio_migration_log (phone_number, migrated_at) "
                    "VALUES (%s, NOW()) "
                    "ON CONFLICT (phone_number) DO UPDATE SET migrated_at = NOW() "
                    "WHERE twilio_migration_log.migrated_at < NOW() - make_interval(secs => %s)",
                    (phone_number, cooldown_seconds),
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

    Sends a redirect opener FROM the Twilio number (cooldown-throttled per fan)
    plus a best-effort bridge reply on SlickText. Never raises — SMS failures are
    logged and swallowed so a delivery hiccup can't take down the webhook. The
    Twilio opener is the reliable path; the SlickText bridge is best-effort and
    will quietly fail if SlickText has no outbound credits.
    """
    if not phone_number:
        return
    if new_number is None:
        new_number = os.getenv("TWILIO_PHONE_NUMBER", "")

    # One redirect nudge per fan per cooldown window. Gating BOTH sends behind the
    # cooldown keeps us inside the SlickText plan's monthly outbound-credit cap
    # (e.g. 500/mo on the $29 plan) — a fan who texts the old number repeatedly
    # gets redirected once per window, not once per text. Within the cooldown we
    # stay silent: the fan was already told where to go.
    if not claim_migration_send(phone_number, get_conn):
        return

    # 1. Opener from the NEW Twilio number (no from_number → routes via the
    #    verified A2P messaging service). This is the reliable path: it does NOT
    #    depend on SlickText being able to send, and it doesn't use SlickText
    #    credits, so it keeps working even if the SlickText plan runs dry.
    if twilio_adapter is not None:
        try:
            twilio_adapter.send_reply(phone_number, twilio_opener_text())
        except Exception:
            logger.warning(
                "slicktext_migration: twilio opener failed for ...%s",
                phone_number[-4:], exc_info=True,
            )

    # 2. Best-effort bridge reply on the OLD SlickText number pointing to the new
    #    number. Consumes one SlickText outbound credit — if the plan is out of
    #    credits (or downgraded/cancelled) this just fails and is swallowed; the
    #    fan was already reached via the Twilio opener above.
    if slicktext_adapter is not None:
        try:
            slicktext_adapter.send_reply(phone_number, slicktext_bridge_text(new_number))
        except Exception:
            logger.warning(
                "slicktext_migration: slicktext bridge failed for ...%s",
                phone_number[-4:], exc_info=True,
            )
