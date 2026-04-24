"""
Notion CRM integration — auto-creates and syncs customer records.

Databases (inside the Zar CRM page):
  🎤 Performers  — NOTION_PERFORMERS_DB_ID
  🏢 Businesses  — NOTION_BUSINESSES_DB_ID

Cost model (monthly estimates):
  Phone rental : $1.15 / number
  SMS          : $0.0079 / message (Twilio standard)
  AI           : $0.004  / message (blended GPT-4o-mini + Anthropic)
"""

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

NOTION_API     = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

PERFORMERS_DB_ID = os.getenv("NOTION_PERFORMERS_DB_ID", "3480d9d6-3491-81d6-8df6-d337ee0944ae")
BUSINESSES_DB_ID = os.getenv("NOTION_BUSINESSES_DB_ID", "3480d9d6-3491-81a8-989a-eea353dc5a56")

# Cost constants
PHONE_RENTAL_MONTHLY = 1.15
SMS_COST_PER_MSG     = 0.0079
AI_COST_PER_MSG      = 0.004


def _headers() -> dict:
    token = os.getenv("NOTION_TOKEN", "").strip()
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _rich_text(value: str) -> list:
    return [{"type": "text", "text": {"content": str(value)[:2000]}}]


def _create_page(database_id: str, properties: dict, children: list) -> Optional[str]:
    """Create a Notion page in a database. Returns the page ID or None on failure."""
    try:
        resp = requests.post(
            f"{NOTION_API}/pages",
            headers=_headers(),
            json={"parent": {"database_id": database_id}, "properties": properties, "children": children},
            timeout=15,
        )
        resp.raise_for_status()
        page_id = resp.json().get("id")
        logger.info("notion_crm: created page %s in db %s", page_id, database_id)
        return page_id
    except Exception:
        logger.exception("notion_crm: failed to create page in db %s", database_id)
        return None


def _update_page(page_id: str, properties: dict) -> bool:
    try:
        resp = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=_headers(),
            json={"properties": properties},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception:
        logger.exception("notion_crm: failed to update page %s", page_id)
        return False


def _create_database(parent_page_id: str, title: str, properties: dict) -> Optional[str]:
    """Create a Notion database as a child of a page. Returns the database ID or None."""
    try:
        resp = requests.post(
            f"{NOTION_API}/databases",
            headers=_headers(),
            json={
                "parent": {"type": "page_id", "page_id": parent_page_id},
                "title": [{"type": "text", "text": {"content": title}}],
                "properties": properties,
            },
            timeout=15,
        )
        resp.raise_for_status()
        db_id = resp.json().get("id")
        logger.info("notion_crm: created database '%s' (%s) in page %s", title, db_id, parent_page_id)
        return db_id
    except Exception:
        logger.exception("notion_crm: failed to create database '%s' in page %s", title, parent_page_id)
        return None


def _query_database(database_id: str, filter_payload: Optional[dict] = None) -> list:
    """Query a Notion database. Returns list of page objects."""
    try:
        body = {}
        if filter_payload:
            body["filter"] = filter_payload
        resp = requests.post(
            f"{NOTION_API}/databases/{database_id}/query",
            headers=_headers(),
            json=body,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception:
        logger.exception("notion_crm: failed to query database %s", database_id)
        return []


def _find_page_by_slug(database_id: str, slug: str) -> Optional[str]:
    """Return the Notion page ID for a given slug, or None."""
    try:
        resp = requests.post(
            f"{NOTION_API}/databases/{database_id}/query",
            headers=_headers(),
            json={"filter": {"property": "Slug", "rich_text": {"equals": slug}}},
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return results[0]["id"] if results else None
    except Exception:
        logger.exception("notion_crm: failed to query db %s for slug %s", database_id, slug)
        return None


def _detail_page_children(config: dict, account_type: str) -> list:
    """Build the rich content blocks for the customer detail page."""
    blocks = []

    def heading(text):
        return {"object": "block", "type": "heading_2",
                "heading_2": {"rich_text": _rich_text(text)}}

    def para(text):
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": _rich_text(text) if text else []}}

    def bullet(text):
        return {"object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": _rich_text(text)}}

    def divider():
        return {"object": "block", "type": "divider", "divider": {}}

    blocks.append(heading("📝 Bio"))
    blocks.append(para(config.get("bio") or "Not provided."))
    blocks.append(divider())

    if account_type == "performer":
        blocks.append(heading("🎭 Personality"))
        blocks.append(bullet(f"Tone: {config.get('tone', 'casual')}"))
        if config.get("podcast_url"):
            blocks.append(bullet(f"Podcast: {config['podcast_url']}"))
        if config.get("media_urls"):
            blocks.append(heading("🎥 Media / Content URLs"))
            for url in config["media_urls"][:10]:
                blocks.append(bullet(url))
        blocks.append(divider())

    if config.get("extra_context"):
        blocks.append(heading("🤖 AI Context Notes"))
        blocks.append(para(config["extra_context"]))
        blocks.append(divider())

    blocks.append(heading("💰 Cost Tracking"))
    blocks.append(para("Monthly fees and cost estimates are synced daily from the database."))
    blocks.append(bullet("Phone Rental: $1.15/month (Twilio number)"))
    blocks.append(bullet("AI Cost: ~$0.004 per message processed"))
    blocks.append(bullet("SMS Cost: ~$0.0079 per message sent"))
    blocks.append(divider())

    blocks.append(heading("📋 Setup Checklist"))
    checklist_items = [
        ("Account created", True),
        ("Bot config saved", True),
        ("Twilio number assigned", False),
        ("AI personality generated", False),
        ("Website content ingested", False),
        ("First fan subscribed", False),
    ]
    for label, done in checklist_items:
        blocks.append({
            "object": "block", "type": "to_do",
            "to_do": {"rich_text": _rich_text(label), "checked": done}
        })

    return blocks


_MONTHLY_DB_PROPS = {
    "Month":           {"title": {}},
    "Messages":        {"number": {"format": "number"}},
    "AI Replies":      {"number": {"format": "number"}},
    "AI Cost ($)":     {"number": {"format": "dollar"}},
    "SMS Cost ($)":    {"number": {"format": "dollar"}},
    "Phone ($)":       {"number": {"format": "dollar"}},
    "Total Cost ($)":  {"number": {"format": "dollar"}},
    "Net Margin ($)":  {"number": {"format": "dollar"}},
    "Blasts":          {"number": {"format": "number"}},
    "Fans Reached":    {"number": {"format": "number"}},
    "Cost Exact":      {"checkbox": {}},
}


def _get_or_create_monthly_db(page_id: str, db_conn) -> Optional[str]:
    """
    Return the Notion database ID for the monthly cost history embedded in a client page.
    Creates the database on first call and caches the ID in bot_configs.
    """
    # Ensure columns exist (idempotent — safe to run on every call)
    try:
        with db_conn.cursor() as cur:
            cur.execute("ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS notion_page_id TEXT DEFAULT NULL")
            cur.execute("ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS notion_monthly_db_id TEXT DEFAULT NULL")
        db_conn.commit()
    except Exception:
        db_conn.rollback()

    # Check cache
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT notion_monthly_db_id FROM bot_configs WHERE notion_page_id = %s",
                (page_id,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except Exception:
        db_conn.rollback()

    # Scan the page's child blocks for an existing Monthly Cost History database
    existing_db_id: Optional[str] = None
    try:
        resp = requests.get(
            f"{NOTION_API}/blocks/{page_id}/children?page_size=50",
            headers=_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        for block in resp.json().get("results", []):
            if block.get("type") == "child_database":
                title = block.get("child_database", {}).get("title", "")
                if "Monthly Cost History" in title:
                    existing_db_id = block["id"]
                    break
    except Exception:
        logger.exception("notion_crm: failed to scan page children for %s", page_id)

    db_id = existing_db_id or _create_database(page_id, "📅 Monthly Cost History", _MONTHLY_DB_PROPS)

    # Persist to bot_configs
    if db_id:
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    "UPDATE bot_configs SET notion_monthly_db_id = %s WHERE notion_page_id = %s",
                    (db_id, page_id),
                )
            db_conn.commit()
        except Exception:
            db_conn.rollback()
            logger.exception("notion_crm: failed to cache notion_monthly_db_id")

    return db_id


def sync_monthly_cost_row(
    page_id: str,
    month_label: str,       # e.g. "April 2026"
    month_key: str,         # e.g. "2026-04"  (for filtering)
    messages: int,
    ai_replies: int,
    ai_cost: float,
    sms_cost: float,
    total_cost: float,
    net_margin: float,
    blasts: int,
    fans_reached: int,
    cost_exact: bool,
    db_conn,
) -> bool:
    """
    Upsert one row in the client's embedded Monthly Cost History database.
    Matches on the month_label title; creates if absent, patches if found.
    """
    monthly_db_id = _get_or_create_monthly_db(page_id, db_conn)
    if not monthly_db_id:
        return False

    props = {
        "Month":          {"title": _rich_text(month_label)},
        "Messages":       {"number": messages},
        "AI Replies":     {"number": ai_replies},
        "AI Cost ($)":    {"number": round(ai_cost, 4)},
        "SMS Cost ($)":   {"number": round(sms_cost, 4)},
        "Phone ($)":      {"number": PHONE_RENTAL_MONTHLY},
        "Total Cost ($)": {"number": round(total_cost, 2)},
        "Net Margin ($)": {"number": round(net_margin, 2)},
        "Blasts":         {"number": blasts},
        "Fans Reached":   {"number": fans_reached},
        "Cost Exact":     {"checkbox": cost_exact},
    }

    # Find existing row for this month
    existing = _query_database(
        monthly_db_id,
        filter_payload={"property": "Month", "title": {"equals": month_label}},
    )

    if existing:
        return _update_page(existing[0]["id"], props)

    try:
        resp = requests.post(
            f"{NOTION_API}/pages",
            headers=_headers(),
            json={"parent": {"database_id": monthly_db_id}, "properties": props},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception:
        logger.exception("notion_crm: failed to create monthly row for %s / %s", page_id, month_label)
        return False


def create_customer_in_notion(
    user_id: int,
    email: str,
    account_type: str,
    slug: str,
    config: dict,
    db_conn=None,
) -> None:
    """
    Create a row + detail page in the appropriate Notion database.
    Stores the notion_page_id back into bot_configs.
    Safe to call in a background thread.
    """
    display_name = config.get("display_name") or slug
    website_url  = config.get("website_url") or ""
    podcast_url  = config.get("podcast_url") or ""
    tone         = config.get("tone") or "casual"
    joined_str   = datetime.now(timezone.utc).date().isoformat()

    database_id = PERFORMERS_DB_ID if account_type == "performer" else BUSINESSES_DB_ID

    properties: dict = {
        "Name":    {"title": _rich_text(display_name)},
        "Slug":    {"rich_text": _rich_text(slug)},
        "Email":   {"email": email},
        "Status":  {"select": {"name": "submitted"}},
        "Joined":  {"date": {"start": joined_str}},
        "Phone Rental ($/mo)": {"number": PHONE_RENTAL_MONTHLY},
    }
    if website_url:
        properties["Website"] = {"url": website_url}
    if account_type == "performer":
        if tone:
            properties["Tone"] = {"select": {"name": tone}}
        if podcast_url:
            properties["Podcast"] = {"url": podcast_url}

    children = _detail_page_children(config, account_type)
    page_id = _create_page(database_id, properties, children)

    if page_id and db_conn is None:
        # Store notion_page_id in bot_configs
        try:
            from .db import get_conn
            conn = get_conn()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS notion_page_id TEXT DEFAULT NULL"
                    )
                    cur.execute(
                        "UPDATE bot_configs SET notion_page_id=%s WHERE creator_slug=%s",
                        (page_id, slug),
                    )
            conn.close()
        except Exception:
            logger.exception("notion_crm: failed to store notion_page_id for %s", slug)


def create_customer_async(user_id: int, email: str, account_type: str, slug: str, config: dict) -> None:
    """Fire-and-forget: create the Notion record in a background thread."""
    t = threading.Thread(
        target=create_customer_in_notion,
        args=(user_id, email, account_type, slug, config),
        daemon=True,
    )
    t.start()


def update_customer_plan(
    slug: str,
    account_type: str,
    *,
    plan_tier: str,
    plan_label: str,
    billing_cycle: str,
    monthly_fee: float,
    stripe_customer_id: Optional[str] = None,
    status_label: str = "active",
) -> bool:
    """
    Sync a customer's plan/billing info to their Notion page.
    Called from Stripe webhook handlers so Notion reflects subscription state.

    Safe to call in a background thread (never raises).
    """
    try:
        database_id = PERFORMERS_DB_ID if account_type == "performer" else BUSINESSES_DB_ID
        page_id = _find_page_by_slug(database_id, slug)
        if not page_id:
            logger.warning("notion_crm: update_customer_plan — no page for slug=%s", slug)
            return False

        properties: dict = {
            "Status":           {"select": {"name": status_label}},
            "Plan":             {"select": {"name": plan_label}},
            "Billing Cycle":    {"select": {"name": billing_cycle}},
            "Monthly Fee ($)":  {"number": float(monthly_fee)},
        }
        if stripe_customer_id:
            properties["Stripe Customer"] = {"rich_text": _rich_text(stripe_customer_id)}

        ok = _update_page(page_id, properties)
        logger.info(
            "notion_crm: update_customer_plan slug=%s tier=%s cycle=%s fee=$%s ok=%s",
            slug, plan_tier, billing_cycle, monthly_fee, ok,
        )
        return ok
    except Exception:
        logger.exception("notion_crm: update_customer_plan failed for %s", slug)
        return False


def update_customer_plan_async(
    slug: str,
    account_type: str,
    **kwargs,
) -> None:
    """Fire-and-forget version of update_customer_plan."""
    t = threading.Thread(
        target=update_customer_plan,
        args=(slug, account_type),
        kwargs=kwargs,
        daemon=True,
    )
    t.start()


def sync_customer_costs(slug: str, account_type: str, conn) -> bool:
    """
    Update cost and metrics columns for a customer's Notion page,
    and upsert the current month into their embedded Monthly Cost History database.
    Called by the daily sync script.
    """
    import psycopg2.extras

    database_id = PERFORMERS_DB_ID if account_type == "performer" else BUSINESSES_DB_ID
    page_id = _find_page_by_slug(database_id, slug)
    if not page_id:
        logger.warning("notion_crm: no Notion page found for slug=%s", slug)
        return False

    try:
        # Keep a single cursor open for all queries in this function.
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Subscriber count
            if account_type == "performer":
                cur.execute("SELECT COUNT(*) AS cnt FROM contacts WHERE creator_slug=%s", (slug,))
            else:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM smb_subscribers WHERE tenant_slug=%s AND status='active'",
                    (slug,),
                )
            subscribers = cur.fetchone()["cnt"]

            # Messages this month
            if account_type == "performer":
                cur.execute(
                    """SELECT COUNT(*) AS cnt FROM messages m
                       JOIN contacts c ON c.phone_number = m.phone_number
                       WHERE c.creator_slug=%s
                         AND m.created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
            else:
                cur.execute(
                    """SELECT COUNT(*) AS cnt FROM smb_messages
                       WHERE tenant_slug=%s
                         AND created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
            msgs_month = cur.fetchone()["cnt"]

            # Total messages all time
            if account_type == "performer":
                cur.execute(
                    """SELECT COUNT(*) AS cnt FROM messages m
                       JOIN contacts c ON c.phone_number = m.phone_number
                       WHERE c.creator_slug=%s""",
                    (slug,),
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM smb_messages WHERE tenant_slug=%s",
                    (slug,),
                )
            total_msgs = cur.fetchone()["cnt"]

            # Shows run (businesses only)
            shows_run = 0
            last_show = None
            if account_type == "business":
                cur.execute(
                    "SELECT COUNT(*) AS cnt, MAX(show_date) AS last FROM smb_shows WHERE tenant_slug=%s",
                    (slug,),
                )
                row = cur.fetchone()
                shows_run = row["cnt"]
                last_show = row["last"]

            # AI cost — hybrid: exact sum for tracked replies + flat-rate estimate for untracked
            if account_type == "performer":
                cur.execute(
                    """SELECT
                          COUNT(*) FILTER (WHERE m.ai_cost_usd IS NOT NULL) AS tracked_cnt,
                          COUNT(*) AS total_cnt,
                          COALESCE(SUM(m.ai_cost_usd), 0) AS exact_ai_cost
                       FROM messages m
                       JOIN contacts c ON c.phone_number = m.phone_number
                       WHERE c.creator_slug = %s AND m.role = 'assistant'
                         AND m.created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
                ai_row = cur.fetchone()
                untracked = ai_row["total_cnt"] - ai_row["tracked_cnt"]
                est_ai_cost = round(float(ai_row["exact_ai_cost"]) + untracked * AI_COST_PER_MSG, 4)
            else:
                est_ai_cost = round(msgs_month * AI_COST_PER_MSG, 2)

            # SMS cost — exact from nightly Twilio sync, else flat-rate estimate
            cur.execute(
                """SELECT COALESCE(SUM(inbound_cost_usd + outbound_cost_usd), -1) AS sms_cost
                   FROM sms_cost_log
                   WHERE creator_slug = %s
                     AND log_date >= DATE_TRUNC('month', CURRENT_DATE)""",
                (slug,),
            )
            sms_cost_row = cur.fetchone()["sms_cost"]
            est_sms_cost = (
                round(float(sms_cost_row), 4) if sms_cost_row >= 0
                else round(msgs_month * SMS_COST_PER_MSG, 2)
            )

        total_cost = round(PHONE_RENTAL_MONTHLY + est_ai_cost + est_sms_cost, 2)

        # Get monthly fee from existing Notion page to compute margin
        monthly_fee = 0.0
        try:
            resp = requests.get(f"{NOTION_API}/pages/{page_id}", headers=_headers(), timeout=10)
            props = resp.json().get("properties", {})
            fee_prop = props.get("Monthly Fee ($)", {}).get("number")
            if fee_prop is not None:
                monthly_fee = float(fee_prop)
        except Exception:
            pass

        net_margin = round(monthly_fee - total_cost, 2)

        update_props = {
            "Subscribers":           {"number": subscribers},
            "Total Messages":        {"number": total_msgs},
            "Messages This Month":   {"number": msgs_month},
            "Est AI Cost ($/mo)":    {"number": est_ai_cost},
            "Est SMS Cost ($/mo)":   {"number": est_sms_cost},
            "Total Cost ($/mo)":     {"number": total_cost},
            "Net Margin ($/mo)":     {"number": net_margin},
        }
        if account_type == "business":
            update_props["Shows Run"] = {"number": shows_run}
            if last_show:
                update_props["Last Show"] = {"date": {"start": last_show.isoformat()}}

        ok = _update_page(page_id, update_props)

        # Upsert current month into the embedded Monthly Cost History database
        from datetime import date
        today = date.today()
        month_key   = today.strftime("%Y-%m")
        month_label = today.strftime("%B %Y")   # e.g. "April 2026"

        # ai_replies = total assistant messages this month
        ai_replies_month = ai_row["total_cnt"] if account_type == "performer" else msgs_month
        ai_fully_exact   = (untracked == 0) if account_type == "performer" else False
        sms_exact        = sms_cost_row >= 0

        # blasts / fans this month
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur2:
                cur2.execute(
                    """SELECT COUNT(*) AS blasts, COALESCE(SUM(sent_count), 0) AS fans
                       FROM blast_drafts
                       WHERE status='sent' AND sent_at >= DATE_TRUNC('month', NOW())"""
                )
                brow = cur2.fetchone()
                blasts_month     = int(brow["blasts"] or 0)
                fans_month       = int(brow["fans"] or 0)
        except Exception:
            blasts_month = fans_month = 0

        sync_monthly_cost_row(
            page_id      = page_id,
            month_label  = month_label,
            month_key    = month_key,
            messages     = msgs_month,
            ai_replies   = ai_replies_month,
            ai_cost      = est_ai_cost,
            sms_cost     = est_sms_cost,
            total_cost   = total_cost,
            net_margin   = net_margin,
            blasts       = blasts_month,
            fans_reached = fans_month,
            cost_exact   = ai_fully_exact and sms_exact,
            db_conn      = conn,
        )

        return ok

    except Exception:
        logger.exception("notion_crm: sync_customer_costs failed for %s", slug)
        return False
