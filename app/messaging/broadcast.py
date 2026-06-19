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
  (`auto` = Twilio when Twilio is configured, else SlickText).
- `TWILIO_MESSAGING_SERVICE_SID` — optional; if set, bulk Twilio uses it instead of
  `TWILIO_PHONE_NUMBER` as `From`.
- `TWILIO_BROADCAST_MPS` — Twilio messages-per-second cap for blasts (default 25).
  Twilio sends concurrently up to this rate. Lower it if you hit account limits.
- `LIVE_SHOW_BROADCAST_DELAY_MS` — milliseconds between sends for the SlickText
  loop (default 350). Ignored by the Twilio path, which is rate-limited by MPS.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
    # auto — default to Twilio now that the account supports high-throughput
    # (up to 25 MPS) sending, which is faster and cheaper for live-show blasts
    # than the per-number SlickText loop. Fall back to SlickText only when Twilio
    # isn't configured at all (so a Twilio-less deployment still works).
    from app.config import (
        TWILIO_ACCOUNT_SID,
        TWILIO_AUTH_TOKEN,
        TWILIO_MESSAGING_SERVICE_SID,
        TWILIO_PHONE_NUMBER,
    )

    twilio_ok = bool(TWILIO_ACCOUNT_SID) and bool(TWILIO_AUTH_TOKEN) and (
        bool(TWILIO_MESSAGING_SERVICE_SID) or bool(TWILIO_PHONE_NUMBER)
    )
    if twilio_ok:
        return "twilio"

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


# Default Twilio throughput for blasts. Accounts with a registered A2P/toll-free
# sender support 25 messages/sec; cap conservatively so a misconfigured env can't
# hammer the API into 429s.
_DEFAULT_BROADCAST_MPS = 25
_MAX_BROADCAST_MPS = 100


def _broadcast_mps() -> int:
    try:
        v = int(os.getenv("TWILIO_BROADCAST_MPS", str(_DEFAULT_BROADCAST_MPS)))
    except ValueError:
        v = _DEFAULT_BROADCAST_MPS
    return max(1, min(v, _MAX_BROADCAST_MPS))


class _RateLimiter:
    """Thread-safe pacer that caps acquisitions to ``rate_per_sec`` across threads.

    Each worker calls ``acquire()`` immediately before a send; the limiter spaces
    grants out by ``1/rate`` seconds so the combined throughput of all worker
    threads never exceeds the target messages-per-second.
    """

    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            scheduled = max(now, self._next_at)
            self._next_at = scheduled + self._interval
        wait = scheduled - now
        if wait > 0:
            time.sleep(wait)


def normalize_e164(phone: str) -> str:
    """Strip whatsapp: prefix for SMS / SlickText."""
    p = (phone or "").strip()
    if p.lower().startswith("whatsapp:"):
        return p[9:].strip()
    return p


def _twilio_client_or_none():
    """Build a Twilio REST client, or None when creds are missing."""
    from twilio.rest import Client
    from app.config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN

    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        logger.error("Twilio not configured for broadcast")
        return None
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def _twilio_send_with_client(client, to_raw: str, body: str, deliver_whatsapp: bool) -> bool:
    """Send a single message with an already-built client. Thread-safe."""
    from app.config import TWILIO_PHONE_NUMBER

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
        logger.warning("Twilio broadcast to %s failed: %s", str(to_raw)[-4:], e)
        return False


def _twilio_send_one(to_raw: str, body: str, deliver_whatsapp: bool) -> bool:
    client = _twilio_client_or_none()
    if client is None:
        return False
    return _twilio_send_with_client(client, to_raw, body, deliver_whatsapp)


def run_loop_broadcast(
    *,
    phones: List[str],
    body: str,
    provider: ProviderName,
    deliver_whatsapp: bool,
    slicktext_send: Callable[[str, str], bool],
    progress: Optional[Callable[[int, int, int], None]] = None,
    on_success: Optional[Callable[[str], None]] = None,
) -> BroadcastResult:
    """
    Send the same body to every phone. SlickText path uses the adapter (SMS).
    Twilio path uses REST directly to support Messaging Service.

    on_success, if provided, is called with each phone number that was sent
    successfully (used to persist the broadcast to conversation history). It is
    best-effort — exceptions are swallowed so persistence never blocks sending.
    """
    errors: List[str] = []
    if deliver_whatsapp and provider == "slicktext":
        return BroadcastResult(
            attempted=len(phones),
            succeeded=0,
            failed=len(phones),
            errors=["SlickText integration is SMS-only; use Twilio for WhatsApp or set deliver_as to SMS."],
        )

    # Twilio supports concurrent, high-throughput sending — run it in a thread
    # pool paced to the account's messages-per-second limit. SlickText keeps the
    # polite sequential loop (its per-number API has tighter rate limits).
    if provider == "twilio":
        return _run_twilio_broadcast(
            phones=phones,
            body=body,
            deliver_whatsapp=deliver_whatsapp,
            progress=progress,
            on_success=on_success,
        )

    succeeded = 0
    failed = 0
    n = len(phones)
    for i, phone in enumerate(phones):
        to = normalize_e164(phone)
        ok = slicktext_send(to, body)

        if ok:
            succeeded += 1
            if on_success:
                try:
                    on_success(phone)
                except Exception:
                    logger.warning(
                        "broadcast on_success hook failed for %s", phone[-4:], exc_info=True
                    )
        else:
            failed += 1
        if progress:
            progress(i + 1, succeeded, failed)
        if i < n - 1:
            _delay_between_sends()

    return BroadcastResult(attempted=n, succeeded=succeeded, failed=failed, errors=errors[:20])


def _run_twilio_broadcast(
    *,
    phones: List[str],
    body: str,
    deliver_whatsapp: bool,
    progress: Optional[Callable[[int, int, int], None]] = None,
    on_success: Optional[Callable[[str], None]] = None,
) -> BroadcastResult:
    """Concurrent Twilio blast, paced to TWILIO_BROADCAST_MPS messages/sec."""
    n = len(phones)
    if n == 0:
        return BroadcastResult(attempted=0, succeeded=0, failed=0, errors=[])

    # Fail fast (and report an accurate all-failed result) if creds are missing.
    if _twilio_client_or_none() is None:
        return BroadcastResult(
            attempted=n, succeeded=0, failed=n,
            errors=["Twilio not configured for broadcast"],
        )

    # Each worker thread gets its OWN Twilio client so no HTTP session is shared
    # across threads — removes any thread-safety doubt at the cost of a handful
    # of cheap client objects.
    _tls = threading.local()

    def _thread_client():
        c = getattr(_tls, "client", None)
        if c is None:
            c = _twilio_client_or_none()
            _tls.client = c
        return c

    mps = _broadcast_mps()
    limiter = _RateLimiter(mps)
    lock = threading.Lock()
    state = {"succeeded": 0, "failed": 0, "done": 0, "last_progress": 0.0}

    def worker(phone: str) -> None:
        limiter.acquire()
        client = _thread_client()
        ok = bool(client) and _twilio_send_with_client(client, phone, body, deliver_whatsapp)
        if ok and on_success:
            try:
                on_success(phone)
            except Exception:
                logger.warning(
                    "broadcast on_success hook failed for %s", str(phone)[-4:], exc_info=True
                )
        emit = None
        with lock:
            if ok:
                state["succeeded"] += 1
            else:
                state["failed"] += 1
            state["done"] += 1
            now = time.monotonic()
            # Throttle progress DB writes to ~2/sec; always emit the final one.
            if state["done"] == n or (now - state["last_progress"]) >= 0.5:
                state["last_progress"] = now
                emit = (state["done"], state["succeeded"], state["failed"])
        if emit and progress:
            try:
                progress(*emit)
            except Exception:
                logger.warning("broadcast progress hook failed", exc_info=True)

    workers = max(1, min(mps, n))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, phones))

    return BroadcastResult(
        attempted=n,
        succeeded=state["succeeded"],
        failed=state["failed"],
        errors=[],
    )
