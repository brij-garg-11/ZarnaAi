#!/usr/bin/env python3
"""
Daily sync of SMB client stats → Notion master "SMB Clients" database.
On the 1st of each month, also saves a permanent snapshot to "SMB Billing History".

For each tenant this script writes:
  - Subscriber counts (active, total, completion rate)
  - Blast stats (count, delivery rate, last blast date)
  - Cost breakdown (Twilio actual via API + estimate, AI estimate, hosting share)
  - Health score (🟢 / 🟡 / 🔴)

On the 1st of each month (or via --snapshot):
  - Saves last month's final numbers to SMB_NOTION_BILLING_DB_ID
  - One row per client per month — permanent, never overwritten
  - Skips if snapshot for that month already exists (idempotent)

Env (required):
  NOTION_TOKEN                  Internal integration secret
  SMB_NOTION_DATABASE_ID        UUID of the "SMB Clients" Notion database
  DATABASE_URL                  Production Postgres

Env (optional):
  SMB_NOTION_BILLING_DB_ID      UUID of the "SMB Billing History" database (enables monthly snapshots)
  RAILWAY_MONTHLY_COST          Railway bill in USD (default: 5.0)
  TWILIO_ACCOUNT_SID            For pulling actual Twilio costs
  TWILIO_AUTH_TOKEN             For pulling actual Twilio costs
  NOTION_API_VERSION            default: 2022-06-28

Run:
  python scripts/sync_smb_clients_to_notion.py               # daily sync
  python scripts/sync_smb_clients_to_notion.py --check-schema
  python scripts/sync_smb_clients_to_notion.py --snapshot    # force a snapshot now (for testing)

SMB Billing History database properties:
  Month          → Title   (e.g. "West Side Comedy Club — Mar 2026")
  Client         → Text
  Month label    → Text    (e.g. "Mar 2026")
  MRR            → Number  $ USD
  Twilio cost    → Number  $ USD
  AI cost        → Number  $ USD
  Hosting share  → Number  $ USD
  Total cost     → Number  $ USD
  Gross margin   → Number  $ USD
  Subscribers    → Number
  New subs       → Number
  Blasts sent    → Number
  Delivered      → Number
  Delivery rate  → Number  %
  Payment status → Select  (Paid, Pending, Overdue, Waived)
  Notes          → Text
  Snapshot date  → Date
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests

try:
    from dotenv import load_dotenv
    _here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv()
    load_dotenv(os.path.join(_here, "..", ".env"))
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [smb_notion] %(message)s",
    stream=sys.stdout,
    force=True,
)
_log = logging.getLogger(__name__)

NOTION_API = "https://api.notion.com/v1"

# Twilio SMS prices (USD) — used for estimation when API isn't available
_TWILIO_OUTBOUND_PER_SMS = 0.0079
_TWILIO_INBOUND_PER_SMS  = 0.0079
# AI cost estimate per blast (1 enhance call + fraction of onboarding calls)
_AI_COST_PER_BLAST       = 0.0015
_AI_COST_PER_NEW_SUB     = 0.0005


# ---------------------------------------------------------------------------
# Notion helpers (shared pattern with sync_live_shows_to_notion.py)
# ---------------------------------------------------------------------------

def _headers() -> Dict[str, str]:
    token = (os.getenv("NOTION_TOKEN") or "").strip()
    ver = (os.getenv("NOTION_API_VERSION") or "2022-06-28").strip()
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": ver,
        "Content-Type": "application/json",
    }


def _normalize_uuid(raw: str) -> str:
    import re
    s = raw.strip().replace("-", "")
    if len(s) != 32 or not re.fullmatch(r"[0-9a-fA-F]{32}", s):
        return raw.strip()
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


def _title(content: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": (content or "")[:1900]}}]}


def _rich_text(content: str) -> Dict[str, Any]:
    content = (content or "")[:1900]
    if not content:
        return {"rich_text": []}
    return {"rich_text": [{"type": "text", "text": {"content": content}}]}


def _number(n: Optional[float]) -> Dict[str, Any]:
    if n is None:
        return {"number": None}
    return {"number": round(n, 4)}


def _select(name: str) -> Dict[str, Any]:
    return {"select": {"name": name}}


def _date(dt: Optional[datetime]) -> Dict[str, Any]:
    if dt is None:
        return {"date": None}
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return {"date": {"start": dt.astimezone(timezone.utc).strftime("%Y-%m-%d")}}


def _phone(val: Optional[str]) -> Dict[str, Any]:
    return {"phone_number": (val or "").strip() or None}


def notion_query_by_slug(database_id: str, slug: str) -> Optional[str]:
    """Return existing Notion page ID for this tenant slug, or None."""
    rid = _normalize_uuid(database_id)
    body = {
        "filter": {"property": "Slug", "rich_text": {"equals": slug}},
        "page_size": 1,
    }
    r = requests.post(f"{NOTION_API}/databases/{rid}/query",
                      headers=_headers(), json=body, timeout=60)
    if r.status_code != 200:
        return None
    results = r.json().get("results") or []
    return results[0]["id"] if results else None


def notion_create_page(database_id: str, properties: Dict[str, Any]) -> str:
    rid = _normalize_uuid(database_id)
    r = requests.post(f"{NOTION_API}/pages",
                      headers=_headers(),
                      json={"parent": {"database_id": rid}, "properties": properties},
                      timeout=60)
    if r.status_code != 200:
        _log.error("Notion create page failed: %s %s", r.status_code, r.text[:2000])
        raise RuntimeError(r.text)
    return r.json()["id"]


def notion_update_page(page_id: str, properties: Dict[str, Any]) -> None:
    pid = _normalize_uuid(page_id)
    r = requests.patch(f"{NOTION_API}/pages/{pid}",
                       headers=_headers(),
                       json={"properties": properties},
                       timeout=60)
    if r.status_code != 200:
        _log.error("Notion update page failed: %s %s", r.status_code, r.text[:2000])
        raise RuntimeError(r.text)


def notion_retrieve_database(database_id: str) -> Dict[str, Any]:
    rid = _normalize_uuid(database_id)
    r = requests.get(f"{NOTION_API}/databases/{rid}", headers=_headers(), timeout=60)
    if r.status_code != 200:
        _log.error("Notion retrieve database failed: %s %s", r.status_code, r.text[:2000])
        sys.exit(1)
    return r.json()


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def _get_db_conn():
    import psycopg2
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        _log.error("DATABASE_URL is not set")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def fetch_tenant_stats(conn, slug: str) -> Dict[str, Any]:
    """Pull subscriber + blast stats for one tenant from Postgres."""
    import psycopg2.extras

    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_start = (month_start - timedelta(days=1)).replace(day=1)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # --- Subscriber counts ---
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'active' AND onboarding_step > 0) AS pref_answered,
                COUNT(*) FILTER (WHERE status = 'active')                          AS active,
                COUNT(*)                                                            AS total,
                MIN(created_at)                                                     AS first_signup
            FROM smb_subscribers WHERE tenant_slug = %s
        """, (slug,))
        subs = dict(cur.fetchone() or {})

        # --- New subscribers this month (for AI cost estimate) ---
        cur.execute("""
            SELECT COUNT(*) AS new_this_month
            FROM smb_subscribers
            WHERE tenant_slug = %s AND created_at >= %s
        """, (slug, month_start))
        subs["new_this_month"] = (cur.fetchone() or {}).get("new_this_month") or 0

        # --- Subscriber growth vs last month ---
        cur.execute("""
            SELECT COUNT(*) AS new_last_month
            FROM smb_subscribers
            WHERE tenant_slug = %s AND created_at >= %s AND created_at < %s
        """, (slug, last_month_start, month_start))
        subs["new_last_month"] = (cur.fetchone() or {}).get("new_last_month") or 0

        # --- Blast stats this month ---
        cur.execute("""
            SELECT
                COUNT(*)        AS blast_count,
                SUM(attempted)  AS total_attempted,
                SUM(succeeded)  AS total_succeeded,
                MAX(sent_at)    AS last_blast_at
            FROM smb_blasts
            WHERE tenant_slug = %s AND sent_at >= %s
        """, (slug, month_start))
        blasts = dict(cur.fetchone() or {})

        # --- All-time blast count (for health scoring) ---
        cur.execute("""
            SELECT MAX(sent_at) AS last_blast_ever FROM smb_blasts WHERE tenant_slug = %s
        """, (slug,))
        row = cur.fetchone()
        blasts["last_blast_ever"] = (row or {}).get("last_blast_ever")

    return {"subscribers": subs, "blasts": blasts}


# ---------------------------------------------------------------------------
# Twilio cost fetcher
# ---------------------------------------------------------------------------

def fetch_twilio_cost(sms_number: Optional[str]) -> Optional[float]:
    """
    Try to pull actual Twilio costs for this month via the Usage API.
    Returns USD float or None if credentials not configured / call fails.
    """
    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not sid or not token:
        return None
    try:
        from twilio.rest import Client
        client = Client(sid, token)
        # Pull this month's outbound SMS spend
        records = client.usage.records.this_month.list(category="sms-outbound")
        total = sum(float(r.price or 0) for r in records)
        _log.info("Twilio API: this-month outbound SMS cost = $%.4f", total)
        # If we have the number, also pull inbound
        inbound = client.usage.records.this_month.list(category="sms-inbound")
        total += sum(float(r.price or 0) for r in inbound)
        return round(abs(total), 4)
    except Exception as exc:
        _log.warning("Twilio Usage API failed: %s — will use estimate only", exc)
        return None


def estimate_twilio_cost(blasts_succeeded: int, new_subscribers: int) -> float:
    """
    Estimate Twilio cost from blast send counts and new subscriber onboarding messages.
    Each blast = 1 outbound SMS per subscriber.
    Each new subscriber gets ~3 outbound messages (welcome + vcard + preference question).
    """
    blast_cost = blasts_succeeded * _TWILIO_OUTBOUND_PER_SMS
    onboarding_cost = new_subscribers * 3 * _TWILIO_OUTBOUND_PER_SMS
    inbound_cost = (blasts_succeeded + new_subscribers) * _TWILIO_INBOUND_PER_SMS
    return round(blast_cost + onboarding_cost + inbound_cost, 4)


# ---------------------------------------------------------------------------
# Health score
# ---------------------------------------------------------------------------

def compute_health(stats: Dict[str, Any]) -> str:
    blasts = stats["blasts"]
    subs = stats["subscribers"]

    last_blast = blasts.get("last_blast_ever")
    if last_blast and hasattr(last_blast, "tzinfo") and last_blast.tzinfo is None:
        last_blast = last_blast.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    days_since_blast = (now - last_blast).days if last_blast else 9999

    active = int(subs.get("active") or 0)
    new_this_month = int(subs.get("new_this_month") or 0)

    if days_since_blast <= 14 and (new_this_month > 0 or active > 0):
        return "🟢 Active"
    if days_since_blast <= 30:
        return "🟡 Stale"
    return "🔴 Inactive"


# ---------------------------------------------------------------------------
# Build Notion properties
# ---------------------------------------------------------------------------

def build_properties(tenant, stats: Dict[str, Any], num_tenants: int) -> Dict[str, Any]:
    subs = stats["subscribers"]
    blasts = stats["blasts"]

    active        = int(subs.get("active") or 0)
    total         = int(subs.get("total") or 0)
    pref_answered = int(subs.get("pref_answered") or 0)
    new_this_month = int(subs.get("new_this_month") or 0)
    blast_count   = int(blasts.get("blast_count") or 0)
    attempted     = int(blasts.get("total_attempted") or 0)
    succeeded     = int(blasts.get("total_succeeded") or 0)
    last_blast    = blasts.get("last_blast_at") or blasts.get("last_blast_ever")

    delivery_rate = round((succeeded / attempted) * 100, 1) if attempted else None

    # --- Costs ---
    railway_cost = float(os.getenv("RAILWAY_MONTHLY_COST") or "5.0")
    hosting_share = round(railway_cost / max(num_tenants, 1), 2)

    twilio_actual  = fetch_twilio_cost(tenant.sms_number)
    twilio_estimate = estimate_twilio_cost(succeeded, new_this_month)
    twilio_cost    = twilio_actual if twilio_actual is not None else twilio_estimate

    ai_cost = round(blast_count * _AI_COST_PER_BLAST + new_this_month * _AI_COST_PER_NEW_SUB, 4)
    total_cost = round(twilio_cost + ai_cost + hosting_share, 2)

    # --- Margin (MRR unknown at code time — will be 0 unless set in Notion manually) ---
    # We push cost; margin is a Notion formula: MRR - Cost / mo
    # (or we leave it as rich_text summary)

    completion_pct = round((pref_answered / total) * 100) if total else 0
    health = compute_health(stats)
    first_signup = subs.get("first_signup")

    props: Dict[str, Any] = {
        "Client":         _title(tenant.display_name),
        "Slug":           _rich_text(tenant.slug),
        "Subscribers":    _number(active),
        "Total sign-ups": _number(total),
        "Completion %":   _number(completion_pct),
        "Blasts / mo":    _number(blast_count),
        "Delivery rate":  _number(delivery_rate),
        "Cost / mo":      _number(total_cost),
        "Twilio cost":    _number(twilio_cost),
        "AI cost est":    _number(ai_cost),
        "Hosting share":  _number(hosting_share),
        "Health":         _select(health),
        "SMS number":     _phone(tenant.sms_number),
        "Synced at":      _date(datetime.now(timezone.utc)),
    }

    if last_blast:
        if hasattr(last_blast, "tzinfo") and last_blast.tzinfo is None:
            last_blast = last_blast.replace(tzinfo=timezone.utc)
        props["Last blast"] = _date(last_blast)

    if first_signup:
        if hasattr(first_signup, "tzinfo") and first_signup.tzinfo is None:
            first_signup = first_signup.replace(tzinfo=timezone.utc)
        props["Signed up"] = _date(first_signup)

    if tenant.owner_phone:
        props["Owner phone"] = _phone(tenant.owner_phone)

    return props


# ---------------------------------------------------------------------------
# Monthly snapshot helpers
# ---------------------------------------------------------------------------

def fetch_tenant_stats_for_month(conn, slug: str, month_start: datetime, month_end: datetime) -> Dict[str, Any]:
    """Pull stats scoped to a specific calendar month for snapshot."""
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Subscribers at end of month
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'active' AND onboarding_step > 0) AS pref_answered,
                COUNT(*) FILTER (WHERE status = 'active')                          AS active,
                COUNT(*)                                                            AS total
            FROM smb_subscribers
            WHERE tenant_slug = %s AND created_at < %s
        """, (slug, month_end))
        subs = dict(cur.fetchone() or {})

        # New subscribers during the month
        cur.execute("""
            SELECT COUNT(*) AS new_this_month
            FROM smb_subscribers
            WHERE tenant_slug = %s AND created_at >= %s AND created_at < %s
        """, (slug, month_start, month_end))
        subs["new_this_month"] = (cur.fetchone() or {}).get("new_this_month") or 0

        # Blast stats for the month
        cur.execute("""
            SELECT
                COUNT(*)        AS blast_count,
                SUM(attempted)  AS total_attempted,
                SUM(succeeded)  AS total_succeeded,
                MAX(sent_at)    AS last_blast_at
            FROM smb_blasts
            WHERE tenant_slug = %s AND sent_at >= %s AND sent_at < %s
        """, (slug, month_start, month_end))
        blasts = dict(cur.fetchone() or {})

    return {"subscribers": subs, "blasts": blasts}


def notion_query_snapshot_exists(billing_db_id: str, slug: str, month_label: str) -> bool:
    """Return True if a snapshot row already exists for this client + month."""
    rid = _normalize_uuid(billing_db_id)
    body = {
        "filter": {
            "and": [
                {"property": "Client", "rich_text": {"equals": slug}},
                {"property": "Month label", "rich_text": {"equals": month_label}},
            ]
        },
        "page_size": 1,
    }
    r = requests.post(f"{NOTION_API}/databases/{rid}/query",
                      headers=_headers(), json=body, timeout=60)
    if r.status_code != 200:
        return False
    return len(r.json().get("results") or []) > 0


def build_snapshot_properties(tenant, stats: Dict[str, Any], month_label: str, num_tenants: int) -> Dict[str, Any]:
    subs = stats["subscribers"]
    blasts = stats["blasts"]

    active         = int(subs.get("active") or 0)
    new_this_month = int(subs.get("new_this_month") or 0)
    blast_count    = int(blasts.get("blast_count") or 0)
    attempted      = int(blasts.get("total_attempted") or 0)
    succeeded      = int(blasts.get("total_succeeded") or 0)
    delivery_rate  = round((succeeded / attempted) * 100, 1) if attempted else None

    railway_cost   = float(os.getenv("RAILWAY_MONTHLY_COST") or "5.0")
    hosting_share  = round(railway_cost / max(num_tenants, 1), 2)
    twilio_actual  = fetch_twilio_cost(tenant.sms_number)
    twilio_est     = estimate_twilio_cost(succeeded, new_this_month)
    twilio_cost    = twilio_actual if twilio_actual is not None else twilio_est
    ai_cost        = round(blast_count * _AI_COST_PER_BLAST + new_this_month * _AI_COST_PER_NEW_SUB, 4)
    total_cost     = round(twilio_cost + ai_cost + hosting_share, 2)

    title = f"{tenant.display_name} — {month_label}"

    return {
        "Month":          _title(title),
        "Client":         _rich_text(tenant.slug),
        "Month label":    _rich_text(month_label),
        "Twilio cost":    _number(twilio_cost),
        "AI cost":        _number(ai_cost),
        "Hosting share":  _number(hosting_share),
        "Total cost":     _number(total_cost),
        "Subscribers":    _number(active),
        "New subs":       _number(new_this_month),
        "Blasts sent":    _number(blast_count),
        "Delivered":      _number(succeeded),
        "Delivery rate":  _number(delivery_rate),
        "Snapshot date":  _date(datetime.now(timezone.utc)),
    }


def run_monthly_snapshots(conn, tenants: list, billing_db_id: str, force_month: Optional[datetime] = None) -> None:
    """
    Save last month's stats as a permanent snapshot row per tenant.
    Skips if snapshot already exists (idempotent — safe to re-run).
    """
    now = datetime.now(timezone.utc)
    # Default: snapshot last month
    if force_month:
        month_start = force_month
    else:
        month_start = (now.replace(day=1) - timedelta(days=1)).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
    month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    month_label = month_start.strftime("%b %Y")  # e.g. "Mar 2026"

    _log.info("Monthly snapshot: saving %s for %d tenant(s)", month_label, len(tenants))

    ok = skipped = failed = 0
    for tenant in tenants:
        try:
            if notion_query_snapshot_exists(billing_db_id, tenant.slug, month_label):
                _log.info("Snapshot already exists for %s %s — skipping", tenant.slug, month_label)
                skipped += 1
                continue

            stats = fetch_tenant_stats_for_month(conn, tenant.slug, month_start, month_end)
            props = build_snapshot_properties(tenant, stats, month_label, len(tenants))
            notion_create_page(billing_db_id, props)
            _log.info("Snapshot saved: %s %s", tenant.slug, month_label)
            ok += 1
        except Exception:
            _log.exception("Snapshot failed for tenant=%s month=%s", tenant.slug, month_label)
            failed += 1

    _log.info("Monthly snapshot done: saved=%d skipped=%d failed=%d", ok, skipped, failed)


# ---------------------------------------------------------------------------
# Schema check
# ---------------------------------------------------------------------------

_REQUIRED_PROPS = [
    ("Client",         "title"),
    ("Slug",           "rich_text"),
    ("Status",         "select"),
    ("MRR",            "number"),
    ("Subscribers",    "number"),
    ("Total sign-ups", "number"),
    ("Completion %",   "number"),
    ("Blasts / mo",    "number"),
    ("Delivery rate",  "number"),
    ("Cost / mo",      "number"),
    ("Twilio cost",    "number"),
    ("AI cost est",    "number"),
    ("Hosting share",  "number"),
    ("Health",         "select"),
    ("Last blast",     "date"),
    ("Trial end",      "date"),
    ("Signed up",      "date"),
    ("Synced at",      "date"),
    ("Owner phone",    "phone_number"),
    ("SMS number",     "phone_number"),
]


def run_check_schema() -> None:
    database_id = (os.getenv("SMB_NOTION_DATABASE_ID") or "").strip()
    if not database_id:
        _log.error("SMB_NOTION_DATABASE_ID is not set")
        sys.exit(1)
    schema = notion_retrieve_database(database_id)
    props = schema.get("properties") or {}

    print("\n=== Your Notion SMB Clients database ===\n")
    for name in sorted(props.keys(), key=str.lower):
        print(f"  {name!r}  →  {props[name].get('type')!r}")

    print("\n=== Property checklist ===\n")
    for name, expected_type in _REQUIRED_PROPS:
        actual = props.get(name)
        atype = actual.get("type") if actual else None
        if atype is None:
            status = f"MISSING — add as {expected_type}"
        elif atype != expected_type:
            status = f"WRONG TYPE (is {atype!r}, need {expected_type!r})"
        else:
            status = "OK"
        print(f"  {name!r:25} → {status}")

    print("\nManual fields (do not sync, leave for you to fill): MRR, Status, Trial end, Notes, Tasks\n")


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------

def main_sync(force_snapshot: bool = False) -> None:
    token = (os.getenv("NOTION_TOKEN") or "").strip()
    database_id = (os.getenv("SMB_NOTION_DATABASE_ID") or "").strip()
    billing_db_id = (os.getenv("SMB_NOTION_BILLING_DB_ID") or "").strip()
    if not token or not database_id:
        _log.error("NOTION_TOKEN and SMB_NOTION_DATABASE_ID are required")
        sys.exit(1)

    # Load all SMB tenants
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from app.smb.tenants import get_registry
    registry = get_registry()
    tenants = registry.all_tenants()

    if not tenants:
        _log.warning("No SMB tenants found — nothing to sync")
        return

    _log.info("Syncing %d SMB tenant(s) to Notion", len(tenants))

    conn = _get_db_conn()
    ok = failed = 0
    try:
        # ── Daily sync: update live stats ──
        for tenant in tenants:
            try:
                stats = fetch_tenant_stats(conn, tenant.slug)
                props = build_properties(tenant, stats, len(tenants))

                page_id = notion_query_by_slug(database_id, tenant.slug)
                if page_id:
                    notion_update_page(page_id, props)
                    _log.info("Updated Notion page for tenant=%s page=%s", tenant.slug, page_id[:8])
                else:
                    page_id = notion_create_page(database_id, props)
                    _log.info("Created Notion page for tenant=%s page=%s", tenant.slug, page_id[:8])
                ok += 1
            except Exception:
                _log.exception("Failed to sync tenant=%s", tenant.slug)
                failed += 1

        # ── Monthly snapshot: 1st of the month or forced ──
        now = datetime.now(timezone.utc)
        is_first_of_month = now.day == 1
        if billing_db_id and (is_first_of_month or force_snapshot):
            run_monthly_snapshots(conn, tenants, billing_db_id)
        elif billing_db_id and not is_first_of_month and not force_snapshot:
            _log.info("Not the 1st of the month — skipping snapshot (use --snapshot to force)")
        elif not billing_db_id:
            _log.info("SMB_NOTION_BILLING_DB_ID not set — skipping monthly snapshots")

    finally:
        conn.close()

    _log.info("=== SMB NOTION SYNC DONE synced=%d/%d ===", ok, len(tenants))
    if failed:
        sys.exit(1)


def main() -> None:
    p = argparse.ArgumentParser(description="Sync SMB client stats to Notion.")
    p.add_argument("--check-schema", action="store_true",
                   help="List Notion DB properties and show what to add.")
    p.add_argument("--snapshot", action="store_true",
                   help="Force a monthly snapshot right now (useful for testing or backfill).")
    args = p.parse_args()
    if args.check_schema:
        run_check_schema()
    else:
        main_sync(force_snapshot=args.snapshot)


if __name__ == "__main__":
    main()
