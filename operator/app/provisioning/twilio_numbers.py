"""
Pure Twilio phone-number operations.

This module deliberately has ZERO dependencies on the operator package
(no `..db`, no Flask, no `from . import ...`) and never constructs a Twilio
client itself — the client is always injected by the caller. That keeps the
purchase/search/attach/release logic trivially unit-testable with a fake
client, and keeps all the env-reading + DB-writing concerns in phone.py.

Twilio SDK surface used (all on the injected `client`):
  - client.available_phone_numbers(country).local.list(...)
  - client.incoming_phone_numbers.create(...)
  - client.incoming_phone_numbers(sid).delete()
  - client.messaging.v1.services(msg_svc_sid).phone_numbers.create(...)
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Tuple

_log = logging.getLogger(__name__)


class NoNumbersAvailableError(RuntimeError):
    """Raised when Twilio's number pool has nothing matching the search."""


def search_available_numbers(
    client: Any,
    *,
    country: str = "US",
    area_code: Optional[str] = None,
    limit: int = 5,
) -> List[Any]:
    """
    Return a list of available local SMS-capable numbers. Empty list if none.

    `area_code` is best-effort: if provided we ask Twilio for that area code,
    but the caller decides whether an empty result should trigger a fallback
    search without the area code.
    """
    kwargs: dict = {"sms_enabled": True, "limit": limit}
    if area_code:
        kwargs["area_code"] = area_code
    numbers = client.available_phone_numbers(country).local.list(**kwargs)
    return list(numbers or [])


def provision_number(
    client: Any,
    *,
    webhook_url: str,
    messaging_service_sid: Optional[str] = None,
    country: str = "US",
    area_code: Optional[str] = None,
    search_limit: int = 5,
) -> Tuple[str, str]:
    """
    Search → purchase → attach a number, returning (e164_number, number_sid).

    Steps:
      1. Search for an available local number (preferring `area_code` if given,
         then falling back to any local number if the area-code search is empty).
      2. Purchase it with its inbound SMS webhook wired to `webhook_url`.
      3. If `messaging_service_sid` is set, add the number to that A2P
         messaging service so it inherits the approved campaign registration.

    Raises NoNumbersAvailableError if the pool is empty.
    """
    candidates = search_available_numbers(
        client, country=country, area_code=area_code, limit=search_limit
    )
    if not candidates and area_code:
        # Area code had nothing — fall back to any local number in-country.
        _log.info(
            "twilio_numbers: no numbers in area code %s — retrying without area code",
            area_code,
        )
        candidates = search_available_numbers(
            client, country=country, area_code=None, limit=search_limit
        )
    if not candidates:
        raise NoNumbersAvailableError(
            f"No Twilio local numbers available (country={country}, area_code={area_code})"
        )

    number_to_buy = candidates[0].phone_number

    purchased = client.incoming_phone_numbers.create(
        phone_number=number_to_buy,
        sms_url=webhook_url,
        sms_method="POST",
    )

    if messaging_service_sid:
        client.messaging.v1.services(messaging_service_sid).phone_numbers.create(
            phone_number_sid=purchased.sid,
        )
        _log.info(
            "twilio_numbers: %s attached to messaging service %s",
            purchased.phone_number, messaging_service_sid,
        )
    else:
        _log.warning(
            "twilio_numbers: no messaging_service_sid provided — %s bought but NOT "
            "attached to an A2P campaign (deliverability will be poor)",
            purchased.phone_number,
        )

    return purchased.phone_number, purchased.sid


def release_number(client: Any, number_sid: str) -> None:
    """
    Release a previously purchased number back to Twilio (stops billing).
    Idempotent from the caller's perspective: a missing/already-released
    number should be treated as success by the caller.
    """
    client.incoming_phone_numbers(number_sid).delete()
    _log.info("twilio_numbers: released number sid=%s", number_sid)
