#!/usr/bin/env python3
"""
Nightly Twilio billing sync — writes exact SMS costs per creator per day to sms_cost_log.

Pulls from Twilio's Usage Records API (same source as your Twilio invoice).
Maps each phone number to a creator_slug via the contacts table.

Run manually:
  python scripts/sync_twilio_costs.py               # last 30 days
  python scripts/sync_twilio_costs.py --days 7      # last 7 days
  python scripts/sync_twilio_costs.py --dry-run     # preview without writing
"""
import argparse
import logging
import os
import sys
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _phone_to_creator_map(conn) -> dict[str, str]:
    """Build {phone_number: creator_slug} from contacts table."""
    import psycopg2.extras
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT phone_number, creator_slug FROM contacts WHERE creator_slug IS NOT NULL")
        return {r["phone_number"]: r["creator_slug"] for r in cur.fetchall()}


def _fetch_twilio_daily(account_sid: str, auth_token: str, start: date, end: date) -> list[dict]:
    """
    Fetch daily SMS usage records from Twilio for the given date range.
    Returns list of dicts with: date, to (phone), from (phone), direction,
    num_segments, price, price_unit.
    """
    import requests
    from requests.auth import HTTPBasicAuth

    records = []
    # Twilio's daily usage records don't break down by individual number —
    # we use the Messages resource instead to get per-number cost.
    base_url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    page_url = base_url
    params = {
        "DateSent>": (start - timedelta(days=1)).isoformat(),
        "DateSent<": (end + timedelta(days=1)).isoformat(),
        "PageSize": 1000,
    }
    auth = HTTPBasicAuth(account_sid, auth_token)

    from email.utils import parsedate_to_datetime as _parse_rfc2822

    while page_url:
        resp = requests.get(page_url, params=params if page_url == base_url else None, auth=auth, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for msg in data.get("messages", []):
            raw_date = msg.get("date_sent", "") or ""
            try:
                # Twilio returns RFC 2822 e.g. "Sat, 18 Apr 2026 00:00:00 +0000"
                parsed_date = _parse_rfc2822(raw_date).strftime("%Y-%m-%d")
            except Exception:
                # Fallback: already ISO format or empty
                parsed_date = raw_date[:10]
            records.append({
                "date": parsed_date,
                "from": msg.get("from", ""),
                "to": msg.get("to", ""),
                "direction": msg.get("direction", ""),  # inbound / outbound-api / outbound-reply
                "price": abs(float(msg.get("price") or 0)),
                "price_unit": msg.get("price_unit", "USD"),
            })
        next_page = data.get("next_page_uri")
        page_url = f"https://api.twilio.com{next_page}" if next_page else None
        params = None  # only used on first request

    logger.info("Fetched %d Twilio message records", len(records))
    return records


def _aggregate(records: list[dict], phone_map: dict[str, str]) -> dict:
    """
    Group costs by (date, creator_slug, phone_number).
    phone_number = the Twilio number (the 'from' for outbound, 'to' for inbound).
    """
    # agg[(log_date, creator_slug, twilio_phone)] = {in_count, out_count, in_cost, out_cost}
    agg = defaultdict(lambda: {"inbound_count": 0, "outbound_count": 0,
                                "inbound_cost_usd": 0.0, "outbound_cost_usd": 0.0})
    for r in records:
        direction = r["direction"]
        is_inbound = direction == "inbound"
        twilio_phone = r["to"] if is_inbound else r["from"]
        creator_slug = phone_map.get(twilio_phone)
        if not creator_slug:
            continue  # phone not in our contacts — skip (could be a test number etc.)
        key = (r["date"], creator_slug, twilio_phone)
        if is_inbound:
            agg[key]["inbound_count"] += 1
            agg[key]["inbound_cost_usd"] += r["price"]
        else:
            agg[key]["outbound_count"] += 1
            agg[key]["outbound_cost_usd"] += r["price"]
    return agg


def _upsert(conn, agg: dict, dry_run: bool) -> int:
    written = 0
    with conn:
        with conn.cursor() as cur:
            for (log_date, creator_slug, phone_number), costs in agg.items():
                if dry_run:
                    logger.info("  [DRY] %s | %s | %s | in=%d/$%.4f out=%d/$%.4f",
                                log_date, creator_slug, phone_number,
                                costs["inbound_count"], costs["inbound_cost_usd"],
                                costs["outbound_count"], costs["outbound_cost_usd"])
                    written += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO sms_cost_log
                        (log_date, creator_slug, phone_number,
                         inbound_count, outbound_count,
                         inbound_cost_usd, outbound_cost_usd, synced_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (log_date, phone_number) DO UPDATE
                        SET inbound_count     = EXCLUDED.inbound_count,
                            outbound_count    = EXCLUDED.outbound_count,
                            inbound_cost_usd  = EXCLUDED.inbound_cost_usd,
                            outbound_cost_usd = EXCLUDED.outbound_cost_usd,
                            synced_at         = NOW()
                    """,
                    (log_date, creator_slug, phone_number,
                     costs["inbound_count"], costs["outbound_count"],
                     round(costs["inbound_cost_usd"], 4), round(costs["outbound_cost_usd"], 4)),
                )
                written += 1
    return written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="How many days back to sync (default 30)")
    parser.add_argument("--dry-run", action="store_true", help="Print rows without writing")
    args = parser.parse_args()

    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
    auth_token  = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    db_url      = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)

    if not account_sid or not auth_token:
        logger.error("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN are required")
        sys.exit(1)
    if not db_url:
        logger.error("DATABASE_URL is required")
        sys.exit(1)

    import psycopg2
    conn = psycopg2.connect(db_url)

    end_date   = date.today()
    start_date = end_date - timedelta(days=args.days)
    logger.info("Syncing Twilio costs %s → %s (dry_run=%s)", start_date, end_date, args.dry_run)

    phone_map = _phone_to_creator_map(conn)
    logger.info("Loaded %d phone → creator mappings", len(phone_map))

    records = _fetch_twilio_daily(account_sid, auth_token, start_date, end_date)
    agg     = _aggregate(records, phone_map)
    written = _upsert(conn, agg, dry_run=args.dry_run)

    conn.close()
    logger.info("Done — %d rows %s", written, "previewed" if args.dry_run else "upserted")


if __name__ == "__main__":
    main()
