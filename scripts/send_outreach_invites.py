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
    --tenant SLUG           Tenant slug (default: west_side_comedy)
    --message TEXT          Custom invite message to send (default: auto-generated)
    --polish                Use AI to lightly clean up the message before sending
    --free-ticket           Attach a free-ticket offer — recipients who reply within
                            24 hours get a free ticket line in their welcome message
                            with a unique ticket number starting at #100
    --batch-name NAME       Label for this blast batch (shown in admin dashboard)
    --batch-size N          Pause every N sends (default: 50) to avoid rate limits
    --batch-pause SECONDS   Pause duration between batches (default: 10)
    --dry-run               Print numbers and message without sending anything
    --delay SECONDS         Pause between individual sends (default: 1.0)
    --column NAME           Force a specific column name for phone numbers
"""

import argparse
import csv
import logging
import os
import re
import sys
import time

# Make sure the project root is on the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("outreach_blast")

from app.smb.tenants import get_registry
from app.smb.brain import _signup_nudge
from app.smb import ai as smb_ai
from app.smb import storage as smb_storage
from app.admin_auth import get_db_connection
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
            logger.error("CSV has no headers. Columns found: %s", headers)
            sys.exit(1)

        logger.info("Reading phone numbers from column: '%s'", col)

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
                logger.debug("Skipped invalid number: %s", raw)

        if skipped:
            logger.warning(
                "Skipped %d invalid/unrecognised numbers: %s%s",
                len(skipped),
                skipped[:5],
                "…" if len(skipped) > 5 else "",
            )

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
    parser.add_argument("--free-ticket", action="store_true", dest="free_ticket",
                        help="Recipients who reply within 24h get a unique free ticket number (#100, #101, …)")
    parser.add_argument("--batch-name", default=None, dest="batch_name",
                        help="Label for this blast (shown in admin dashboard, e.g. 'April 2026 CSV')")
    parser.add_argument("--batch-size", type=int, default=50, dest="batch_size",
                        help="Pause every N sends to respect rate limits (default: 50)")
    parser.add_argument("--batch-pause", type=float, default=10.0, dest="batch_pause",
                        help="Seconds to pause between batches (default: 10)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between individual sends (default: 1.0)")
    parser.add_argument("--column", default=None, help="Force a specific column name for phone numbers")
    args = parser.parse_args()

    # Load tenant
    registry = get_registry()
    tenant = registry.get_by_slug(args.tenant)
    if tenant is None:
        logger.error("Tenant '%s' not found. Available: %s", args.tenant, [t.slug for t in registry.all_tenants()])
        sys.exit(1)

    if not tenant.sms_number:
        logger.error("Tenant '%s' has no SMS number configured.", args.tenant)
        sys.exit(1)

    # Build invite message — priority: --message flag > tenant config > AI-generated nudge
    if args.message:
        invite = args.message.strip()
    elif tenant.outreach_invite_message:
        invite = tenant.outreach_invite_message.strip()
        logger.info("Using outreach_invite_message from tenant config.")
    else:
        invite = _signup_nudge(tenant)

    if args.polish:
        logger.info("Polishing message with AI...")
        original = invite
        invite = _polish_message(invite, tenant)
        if invite != original:
            logger.info("  Original : %s", original)
            logger.info("  Polished : %s", invite)
        else:
            logger.info("  (no changes made by AI)")

    offer = "free_ticket" if args.free_ticket else None
    batch_name = args.batch_name

    print(f"\n{'='*60}")
    print(f"  Tenant  : {tenant.display_name} ({tenant.slug})")
    print(f"  From    : {tenant.sms_number}")
    print(f"  Batch   : {batch_name or '(unlabelled)'}")
    print(f"  Offer   : {'Free ticket for replies within 24h (numbers start at #100)' if offer else 'None'}")
    print(f"  Message :\n\n{invite}\n")
    print(f"{'='*60}\n")

    # Load numbers
    numbers = _load_numbers(args.csv_path, args.column)
    logger.info("Loaded %d valid phone numbers from CSV", len(numbers))

    if not numbers:
        logger.warning("Nothing to send — exiting.")
        sys.exit(0)

    if args.dry_run:
        print("--- DRY RUN — no messages will be sent ---")
        for n in numbers:
            print(f"  Would send to: {n}")
        print(f"\nTotal: {len(numbers)} messages")
        if offer:
            print("Free ticket offer would be recorded for all successful sends.")
        sys.exit(0)

    # Confirm before sending
    confirm = input(
        f"Send to {len(numbers)} numbers from {tenant.sms_number}? [y/N] "
    ).strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    # Send
    adapter = TwilioAdapter(
        account_sid=TWILIO_ACCOUNT_SID,
        auth_token=TWILIO_AUTH_TOKEN,
        from_number=tenant.sms_number,
    )

    db_conn = get_db_connection() if offer else None

    sent = 0
    failed = 0
    batch_num = 1
    consecutive_failures = 0
    CONSECUTIVE_FAIL_LIMIT = 5   # pause and warn after this many in a row
    FAIL_RATE_LIMIT       = 0.20 # stop if failure rate exceeds 20% (after 20+ sends)

    for i, number in enumerate(numbers, 1):
        logger.debug("[%d/%d] Sending to ...%s", i, len(numbers), number[-4:])
        ok = adapter.send_reply(to_number=number, body=invite, from_number=tenant.sms_number)
        status = "✓" if ok else "✗"
        print(f"  [{i}/{len(numbers)}] {status} ...{number[-4:]}")

        if ok:
            sent += 1
            consecutive_failures = 0
            if db_conn and offer:
                try:
                    with db_conn:
                        smb_storage.record_outreach_invite(
                            db_conn, tenant.slug, number, offer,
                            batch_name=batch_name,
                        )
                    logger.debug("  ↳ Invite recorded in DB for ...%s", number[-4:])
                except Exception as e:
                    logger.warning("  ↳ DB record failed for ...%s: %s", number[-4:], e)
        else:
            failed += 1
            consecutive_failures += 1
            logger.warning("  ↳ Send FAILED for ...%s", number[-4:])

        # ── Health checks after every send ──

        # 1. Too many consecutive failures — something is wrong (Twilio down, rate limit, etc.)
        if consecutive_failures >= CONSECUTIVE_FAIL_LIMIT:
            print(f"\n  🚨  {consecutive_failures} CONSECUTIVE FAILURES — pausing 30s before continuing.")
            print(f"      Check your Twilio console or network. Press Ctrl+C now to abort.")
            logger.error(
                "CONSECUTIVE FAILURE LIMIT hit (%d in a row) at [%d/%d]. Pausing 30s.",
                consecutive_failures, i, len(numbers),
            )
            time.sleep(30)
            consecutive_failures = 0  # reset after pause so we don't loop-pause forever

        # 2. High failure rate after a meaningful sample — likely a systemic issue
        if i >= 20:
            fail_rate = failed / i
            if fail_rate > FAIL_RATE_LIMIT:
                print(f"\n  🛑  STOPPING EARLY — failure rate is {fail_rate:.0%} ({failed}/{i} failed).")
                print(f"      {sent} messages sent successfully and recorded before stopping.")
                logger.error(
                    "FAIL RATE %d%% exceeded limit (%d%%) at [%d/%d]. Stopping. sent=%d failed=%d",
                    int(fail_rate * 100), int(FAIL_RATE_LIMIT * 100), i, len(numbers), sent, failed,
                )
                numbers = numbers[:i]  # truncate so post-run summary shows actual work done
                break

        # Batch pause every batch_size sends
        if i < len(numbers):
            if i % args.batch_size == 0:
                batch_num += 1
                fail_rate_pct = round((failed / i) * 100) if i else 0
                logger.info(
                    "Batch %d complete (%d sent, %d failed, %d%% fail rate). Pausing %.0fs…",
                    batch_num - 1, sent, failed, fail_rate_pct, args.batch_pause,
                )
                print(f"\n  ── Batch {batch_num - 1} done │ sent: {sent} │ failed: {failed} │ fail rate: {fail_rate_pct}% │ pausing {args.batch_pause:.0f}s ──\n")
                time.sleep(args.batch_pause)
            else:
                time.sleep(args.delay)

    print(f"\n{'='*60}")
    print(f"  Done.  Sent: {sent}  Failed: {failed}  Total: {len(numbers)}")
    if offer:
        print(f"  Free ticket offer recorded for {sent} numbers.")
        print(f"  They have 24 hours to reply YES and claim their ticket.")
    if batch_name:
        print(f"  Batch '{batch_name}' is now visible in the WSCC admin dashboard.")
    print(f"{'='*60}\n")

    logger.info("Blast complete — sent=%d failed=%d total=%d batch=%s", sent, failed, len(numbers), batch_name)

    # ── Post-run DB verification ──
    if offer and db_conn is None:
        # re-open if we closed early
        db_conn = get_db_connection()

    if db_conn:
        try:
            logger.info("Running post-blast DB verification…")
            with db_conn.cursor() as cur:
                # How many invite rows exist for this batch in DB
                if batch_name:
                    cur.execute(
                        "SELECT COUNT(*) FROM smb_outreach_invites WHERE tenant_slug = %s AND batch_name = %s",
                        (tenant.slug, batch_name),
                    )
                else:
                    cur.execute(
                        "SELECT COUNT(*) FROM smb_outreach_invites WHERE tenant_slug = %s AND sent_at > NOW() - INTERVAL '1 hour'",
                        (tenant.slug,),
                    )
                db_recorded = cur.fetchone()[0]

            mismatch = db_recorded != sent
            print(f"\n{'='*60}")
            print(f"  POST-BLAST VERIFICATION")
            print(f"  Messages sent by Twilio : {sent}")
            print(f"  Invites recorded in DB  : {db_recorded}")
            if mismatch:
                print(f"\n  ⚠️  MISMATCH — {sent - db_recorded} invite(s) failed to record.")
                print(f"     Check logs above for '↳ DB record failed' warnings.")
                logger.warning("Post-blast mismatch: sent=%d db_recorded=%d", sent, db_recorded)
            else:
                print(f"\n  ✅  All good — Twilio count matches DB count.")
                logger.info("Post-blast verification passed: sent=%d db_recorded=%d", sent, db_recorded)
            print(f"{'='*60}\n")
        except Exception as e:
            logger.warning("Post-blast verification failed: %s", e)
        finally:
            db_conn.close()


if __name__ == "__main__":
    main()
