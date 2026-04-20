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


def sync_customer_costs(slug: str, account_type: str, conn) -> bool:
    """
    Update cost and metrics columns for a customer's Notion page.
    Called by the daily sync script.
    """
    import psycopg2.extras

    database_id = PERFORMERS_DB_ID if account_type == "performer" else BUSINESSES_DB_ID
    page_id = _find_page_by_slug(database_id, slug)
    if not page_id:
        logger.warning("notion_crm: no Notion page found for slug=%s", slug)
        return False

    try:
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

        est_ai_cost  = round(msgs_month * AI_COST_PER_MSG, 2)
        est_sms_cost = round(msgs_month * SMS_COST_PER_MSG, 2)
        total_cost   = round(PHONE_RENTAL_MONTHLY + est_ai_cost + est_sms_cost, 2)

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

        return _update_page(page_id, update_props)

    except Exception:
        logger.exception("notion_crm: sync_customer_costs failed for %s", slug)
        return False
