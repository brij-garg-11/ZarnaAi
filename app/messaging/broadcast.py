"""
Bulk / campaign outbound messaging for Live Shows (admin-triggered).

How providers handle “many recipients”
--------------------------------------

**Twilio** — There is no single REST call that texts an arbitrary list of numbers.
Official pattern: create one `messages.create` per recipient (often via a **Messaging
Service** so `MessagingServiceSid` is the sender pool instead of a single `From`).
Twilio queues sends and documents rate limits / scaling here:
https://www.twilio.com/docs/sms/services
https://www.twilio.com/docs/messaging/guides/scaling-queueing-latency

This module uses **per-recipient** `messages.create` with optional
`TWILIO_MESSAGING_SERVICE_SID`, and a small delay between calls to stay polite.

**SlickText v2** — Has a **Campaigns** API that sends one body to SlickText **lists or
segments** in one shot (status `"send"`). That requires contacts to exist in SlickText
and be on the target list — not the same as our arbitrary Postgres signup list unless
you sync numbers into SlickText first (Lists API: add contacts).
API reference: https://api.slicktext.com/docs/v2/campaigns

**SlickText v1** — Same as day-to-day replies: **POST /v1/messages/** once per number.

**What we implement**
- **Loop mode:** `SlickTextAdapter.send_reply` / Twilio `messages.create` once per signup.
- **SlickText campaign mode (v2):** see `app/messaging/slicktext_campaigns.py` — temp list,
  sync contacts, `POST /campaigns` with `status: send`. Wired from Live Shows admin.

Environment
-----------
- `LIVE_SHOW_BROADCAST_PROVIDER` — `slicktext` | `twilio` | `auto`
  (`auto` = SlickText if v1 or v2 outbound keys exist, else Twilio).
- `TWILIO_MESSAGING_SERVICE_SID` — optional; if set, bulk Twilio uses it instead of
  `TWILIO_PHONE_NUMBER` as `From`.
- `LIVE_SHOW_BROADCAST_DELAY_MS` — milliseconds between sends in loop mode (default 350).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Callable, List, Literal, Optional

logger = logging.getLogger(__name__)

ProviderName = Literal["slicktext", "twilio"]


@dataclass
class BroadcastResult:
    attempted: int
    succeeded: int
    failed: int
    errors: List[str]


def resolve_broadcast_provider() -> ProviderName:
    raw = (os.getenv("LIVE_SHOW_BROADCAST_PROVIDER") or "auto").strip().lower()
    if raw == "slicktext":
        return "slicktext"
    if raw == "twilio":
        return "twilio"
    # auto
    from app.config import (
        SLICKTEXT_API_KEY,
        SLICKTEXT_BRAND_ID,
        SLICKTEXT_PRIVATE_KEY,
        SLICKTEXT_PUBLIC_KEY,
    )

    slick_ok = (bool(SLICKTEXT_PUBLIC_KEY) and bool(SLICKTEXT_PRIVATE_KEY)) or (
        bool(SLICKTEXT_API_KEY) and bool(SLICKTEXT_BRAND_ID)
    )
    return "slicktext" if slick_ok else "twilio"


def _delay_between_sends():
    try:
        ms = int(os.getenv("LIVE_SHOW_BROADCAST_DELAY_MS", "350"))
    except ValueError:
        ms = 350
    time.sleep(max(0, ms) / 1000.0)


def normalize_e164(phone: str) -> str:
    """Strip whatsapp: prefix for SMS / SlickText."""
    p = (phone or "").strip()
    if p.lower().startswith("whatsapp:"):
        return p[9:].strip()
    return p


def _twilio_send_one(to_raw: str, body: str, deliver_whatsapp: bool) -> bool:
    from twilio.rest import Client
    from app.config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER

    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        logger.error("Twilio not configured for broadcast")
        return False

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    messaging_service_sid = (os.getenv("TWILIO_MESSAGING_SERVICE_SID") or "").strip()
    try:
        if deliver_whatsapp:
            wa_from = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886").strip()
            if not wa_from.startswith("whatsapp:"):
                wa_from = f"whatsapp:{wa_from}"
            to = to_raw if str(to_raw).startswith("whatsapp:") else f"whatsapp:{normalize_e164(to_raw)}"
            client.messages.create(to=to, from_=wa_from, body=body)
        else:
            to = normalize_e164(to_raw)
            kwargs = {"to": to, "body": body}
            if messaging_service_sid:
                kwargs["messaging_service_sid"] = messaging_service_sid
            else:
                if not TWILIO_PHONE_NUMBER:
                    logger.error("TWILIO_PHONE_NUMBER or TWILIO_MESSAGING_SERVICE_SID required")
                    return False
                fn = TWILIO_PHONE_NUMBER.replace("whatsapp:", "")
                kwargs["from_"] = fn
            client.messages.create(**kwargs)
        return True
    except Exception as e:
        logger.warning("Twilio broadcast to %s failed: %s", to_raw[-4:], e)
        return False


def run_loop_broadcast(
    *,
    phones: List[str],
    body: str,
    provider: ProviderName,
    deliver_whatsapp: bool,
    slicktext_send: Callable[[str, str], bool],
    progress: Optional[Callable[[int, int, int], None]] = None,
) -> BroadcastResult:
    """
    Send the same body to every phone. SlickText path uses the adapter (SMS).
    Twilio path uses REST directly to support Messaging Service.
    """
    errors: List[str] = []
    if deliver_whatsapp and provider == "slicktext":
        return BroadcastResult(
            attempted=len(phones),
            succeeded=0,
            failed=len(phones),
            errors=["SlickText integration is SMS-only; use Twilio for WhatsApp or set deliver_as to SMS."],
        )

    succeeded = 0
    failed = 0
    n = len(phones)
    for i, phone in enumerate(phones):
        ok = False
        if provider == "slicktext":
            to = normalize_e164(phone)
            ok = slicktext_send(to, body)
        else:
            ok = _twilio_send_one(phone, body, deliver_whatsapp=deliver_whatsapp)

        if ok:
            succeeded += 1
        else:
            failed += 1
        if progress:
            progress(i + 1, succeeded, failed)
        if i < n - 1:
            _delay_between_sends()

    return BroadcastResult(attempted=n, succeeded=succeeded, failed=failed, errors=errors[:20])
