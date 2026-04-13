"""
Send the opt-in invite to a list of phone numbers from a CSV.

Usage:
    python scripts/send_outreach_invites.py path/to/numbers.csv [options]

The CSV must have a column containing phone numbers. The script auto-detects
columns named: phone, phone_number, mobile, cell, number, contact, or the
first column if none of those match.

Numbers are normalised to E.164 (+1XXXXXXXXXX). US numbers without a country
code are assumed to be +1.

The invite message (no STOP) is sent from the tenant's SMS number. When
recipients reply YES/yeah/sure/ok/join/COMEDY the existing bot flow subscribes
them and sends the welcome + STOP + vCard.

Options:
    --tenant SLUG       Tenant slug (default: west_side_comedy)
    --message TEXT      Custom invite message to send (default: auto-generated)
    --polish            Use AI to lightly clean up the message before sending
    --dry-run           Print numbers and message without sending anything
    --delay SECONDS     Pause between sends in seconds (default: 1.0)
    --column NAME       Force a specific column name for phone numbers
"""

import argparse
import csv
import os
import re
import sys
import time

# Make sure the project root is on the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv()

from app.smb.tenants import get_registry
from app.smb.brain import _signup_nudge
from app.smb import ai as smb_ai
from app.messaging.twilio_adapter import TwilioAdapter
from app.config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN

# ---------------------------------------------------------------------------
# Phone number normalisation
# ---------------------------------------------------------------------------

_DIGITS = re.compile(r"\D")

def _normalise_phone(raw: str) -> str | None:
    """Return E.164 string (+1XXXXXXXXXX) or None if invalid."""
    digits = _DIGITS.sub("", raw.strip())
    if len(digits) == 10:
        digits = "1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return None


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

_PHONE_COLUMN_HINTS = {"phone", "phone_number", "mobile", "cell", "number", "contact", "telephone"}

def _load_numbers(csv_path: str, column: str | None = None) -> list[str]:
    """Return a list of valid E.164 phone numbers from the CSV."""
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        if column:
            col = column
        else:
            col = None
            for h in headers:
                if h.strip().lower() in _PHONE_COLUMN_HINTS:
                    col = h
                    break
            if col is None and headers:
                col = headers[0]

        if col is None:
            print(f"ERROR: CSV has no headers. Columns found: {headers}")
            sys.exit(1)

        print(f"Reading phone numbers from column: '{col}'")

        numbers = []
        skipped = []
        for row in reader:
            raw = row.get(col, "").strip()
            if not raw:
                continue
            normalised = _normalise_phone(raw)
            if normalised:
                numbers.append(normalised)
            else:
                skipped.append(raw)

        if skipped:
            print(f"  Skipped {len(skipped)} invalid numbers: {skipped[:5]}{'…' if len(skipped)>5 else ''}")

        return numbers


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _polish_message(raw: str, tenant) -> str:
    """Use AI to lightly clean up the message — fix typos, tighten phrasing, preserve all facts."""
    prompt = (
        f"You are editing an outbound SMS invite for {tenant.display_name}. "
        f"The tone is: {tenant.tone}.\n\n"
        f"Lightly clean up this message: fix any typos, tighten the phrasing, "
        f"keep it under 160 characters if possible, and preserve the meaning exactly. "
        f"Do NOT add a STOP opt-out line — this is a pre-opt-in invite. "
        f"Reply with ONLY the cleaned message, no quotes or explanation.\n\n"
        f"Message: {raw}"
    )
    polished = smb_ai.generate(prompt)
    return polished.strip() if polished else raw


def main():
    parser = argparse.ArgumentParser(description="Send opt-in invite to a CSV of phone numbers")
    parser.add_argument("csv_path", help="Path to the CSV file")
    parser.add_argument("--tenant", default="west_side_comedy", help="Tenant slug (default: west_side_comedy)")
    parser.add_argument("--message", default=None, help="Custom invite message (default: auto-generated)")
    parser.add_argument("--polish", action="store_true", help="Use AI to lightly clean up the message")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between sends (default: 1.0)")
    parser.add_argument("--column", default=None, help="Force a specific column name for phone numbers")
    args = parser.parse_args()

    # Load tenant
    registry = get_registry()
    tenant = registry.get_by_slug(args.tenant)
    if tenant is None:
        print(f"ERROR: Tenant '{args.tenant}' not found. Available: {[t.slug for t in registry.all_tenants()]}")
        sys.exit(1)

    if not tenant.sms_number:
        print(f"ERROR: Tenant '{args.tenant}' has no SMS number configured.")
        sys.exit(1)

    # Build invite message
    if args.message:
        invite = args.message.strip()
    else:
        invite = _signup_nudge(tenant)

    if args.polish:
        print("Polishing message with AI...")
        original = invite
        invite = _polish_message(invite, tenant)
        if invite != original:
            print(f"  Original: {original}")
            print(f"  Polished: {invite}")
        else:
            print("  (no changes made)")

    print(f"\nTenant:  {tenant.display_name} ({tenant.slug})")
    print(f"From:    {tenant.sms_number}")
    print(f"Message: {invite}\n")

    # Load numbers
    numbers = _load_numbers(args.csv_path, args.column)
    print(f"Found {len(numbers)} valid phone numbers\n")

    if not numbers:
        print("Nothing to send.")
        sys.exit(0)

    if args.dry_run:
        print("--- DRY RUN — no messages will be sent ---")
        for n in numbers:
            print(f"  Would send to: {n}")
        print(f"\nTotal: {len(numbers)} messages")
        sys.exit(0)

    # Confirm before sending
    confirm = input(f"Send '{invite[:60]}...' to {len(numbers)} numbers from {tenant.sms_number}? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    # Send
    adapter = TwilioAdapter(
        account_sid=TWILIO_ACCOUNT_SID,
        auth_token=TWILIO_AUTH_TOKEN,
        from_number=tenant.sms_number,
    )

    sent = 0
    failed = 0
    for i, number in enumerate(numbers, 1):
        ok = adapter.send_reply(to_number=number, body=invite, from_number=tenant.sms_number)
        status = "✓" if ok else "✗"
        print(f"  [{i}/{len(numbers)}] {status} {number}")
        if ok:
            sent += 1
        else:
            failed += 1
        if i < len(numbers):
            time.sleep(args.delay)

    print(f"\nDone. Sent: {sent}  Failed: {failed}")


if __name__ == "__main__":
    main()
