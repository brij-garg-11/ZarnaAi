"""Area-code (NANP NPA) derivation from phone numbers.

A contact's area code is the 3-digit North American Numbering Plan prefix of
their phone number. It is stored on ``contacts.area_codes`` as a TEXT[] so a
contact can belong to more than one (auto-derived from their number, plus any
manual additions made by the operator).
"""
from __future__ import annotations


def area_code_from_phone(phone: str | None) -> str | None:
    """Return the 3-digit NANP area code for a US/Canada number, else None.

    Handles common stored formats: ``+1XXXXXXXXXX``, ``1XXXXXXXXXX``,
    ``XXXXXXXXXX`` and anything with separators. Non-NANP / WhatsApp / short
    numbers return None.
    """
    if not phone:
        return None
    p = phone.strip()
    if p.lower().startswith("whatsapp:"):
        p = p.split(":", 1)[1]
    digits = "".join(c for c in p if c.isdigit())
    if len(digits) == 11 and digits[0] == "1":
        return digits[1:4]
    if len(digits) == 10:
        return digits[:3]
    return None
