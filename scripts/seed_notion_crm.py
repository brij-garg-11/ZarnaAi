"""
One-off script: seed the two founding CRM rows in Notion.
  - Zarna Garg       → Performers DB
  - West Side Comedy → Businesses DB

Run once from project root:
  python scripts/seed_notion_crm.py
"""

import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

NOTION_API     = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

PERFORMERS_DB_ID = os.getenv("NOTION_PERFORMERS_DB_ID", "3480d9d6-3491-81d6-8df6-d337ee0944ae")
BUSINESSES_DB_ID = os.getenv("NOTION_BUSINESSES_DB_ID", "3480d9d6-3491-81a8-989a-eea353dc5a56")

PHONE_RENTAL   = 1.15
AI_PER_MSG     = 0.004
SMS_PER_MSG    = 0.0079


def _headers():
    token = os.getenv("NOTION_TOKEN", "").strip()
    if not token:
        print("ERROR: NOTION_TOKEN not set")
        sys.exit(1)
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def rt(text):
    return [{"type": "text", "text": {"content": str(text)[:2000]}}]


def heading(text):
    return {"object": "block", "type": "heading_2", "heading_2": {"rich_text": rt(text)}}


def para(text):
    return {"object": "block", "type": "paragraph", "paragraph": {"rich_text": rt(text) if text else []}}


def bullet(text):
    return {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": rt(text)}}


def todo(text, checked=False):
    return {"object": "block", "type": "to_do", "to_do": {"rich_text": rt(text), "checked": checked}}


def divider():
    return {"object": "block", "type": "divider", "divider": {}}


def create_page(database_id, properties, children):
    resp = requests.post(
        f"{NOTION_API}/pages",
        headers=_headers(),
        json={"parent": {"database_id": database_id}, "properties": properties, "children": children},
        timeout=15,
    )
    if not resp.ok:
        print(f"ERROR {resp.status_code}: {resp.text[:500]}")
        return None
    page_id = resp.json().get("id")
    print(f"  Created page: {page_id}")
    return page_id


# ── Zarna Garg ────────────────────────────────────────────────────────────────

def create_zarna():
    print("\n→ Creating Zarna Garg (Performers)...")

    # Cost estimates based on post-launch data:
    # ~4,800 messages/month (157/day × 30), bot launched Mar 27 2026
    msgs_month_est = 4800
    est_ai  = round(msgs_month_est * AI_PER_MSG, 2)
    est_sms = round(msgs_month_est * SMS_PER_MSG, 2)
    total   = round(PHONE_RENTAL + est_ai + est_sms, 2)

    properties = {
        "Name":                  {"title": rt("Zarna Garg")},
        "Slug":                  {"rich_text": rt("zarna")},
        "Email":                 {"email": "brij@zarnagarg.com"},
        "Status":                {"select": {"name": "live"}},
        "Joined":                {"date": {"start": "2026-03-27"}},
        "Website":               {"url": "https://zarnagarg.com"},
        "Podcast":               {"url": "https://feeds.megaphone.fm/ASTI4272864122"},
        "Tone":                  {"select": {"name": "sharp"}},
        "Phone Rental ($/mo)":   {"number": PHONE_RENTAL},
        "Monthly Fee ($)":       {"number": 0},
        "Subscribers":           {"number": 5175},
        "Total Messages":        {"number": 7858},
        "Messages This Month":   {"number": msgs_month_est},
        "Est AI Cost ($/mo)":    {"number": est_ai},
        "Est SMS Cost ($/mo)":   {"number": est_sms},
        "Total Cost ($/mo)":     {"number": total},
        "Net Margin ($/mo)":     {"number": round(0 - total, 2)},
    }

    children = [
        heading("📝 Bio"),
        para("Indian-American stand-up comedian. Married with three kids. Sharp, high-energy, culture- and family-aware voice. Known for immigrant-family comedy that crosses generations. Author of 'This American Woman.' Tours nationally — 4–6 shows per month. First Zar client and the proof-of-concept for the platform."),
        divider(),

        heading("📊 Live Stats (as of Apr 9, 2026)"),
        bullet("Total subscribers in DB: 5,175"),
        bullet("Post-bot subscribers (Mar 27+): 1,042"),
        bullet("Unique fans who've texted the bot: 1,132"),
        bullet("Bot replies sent (post-launch): 3,929"),
        bullet("Fan reply rate: 41.6% (industry avg: 5–10%)"),
        bullet("Blasts sent: 13 | Total delivered: 14,598 | Opt-out rate: 0.9%"),
        bullet("Total conversation sessions: 397 | Avg messages/session: 2.9"),
        bullet("Deepest session ever: 74 messages from one fan"),
        divider(),

        heading("🎤 Content & Channels"),
        bullet("Tickets: https://zarnagarg.com/tickets/"),
        bullet("Merch: https://shopmy.us/shop/zarnagarg"),
        bullet("Book: 'This American Woman' — https://www.amazon.com/dp/0593975022"),
        bullet("YouTube: https://www.youtube.com/@ZarnaGarg"),
        bullet("Podcast: Zarna Garg Family Podcast — https://feeds.megaphone.fm/ASTI4272864122"),
        bullet("SMS Keyword: ZARNA | Phone: +1 (877) 553-2629"),
        divider(),

        heading("🎭 Personality & Voice"),
        bullet("Voice: sharp, high-energy, opinionated, family- and culture-aware — conversational stand-up energy"),
        bullet("Tone: Never generic or male-coded. Immigrant family, Indian-mom angles. Parenting and marriage material."),
        bullet("Guardrails: Never claim she's personally texting. Never invent family details. Never Wikipedia-style facts."),
        bullet("Voice lock: Shalabh (husband) is a real person. Never make up what he says or does. MIL is comedic target — never defend her."),
        divider(),

        heading("💰 Cost Tracking"),
        bullet(f"Monthly cost estimate: ~${total}/mo (at ~4,800 msg/mo run rate)"),
        bullet(f"AI cost: ${est_ai}/mo | SMS cost: ${est_sms}/mo | Phone: ${PHONE_RENTAL}/mo"),
        bullet("Blended cost per message: ~$0.0119"),
        divider(),

        heading("📋 Setup Checklist"),
        todo("Account created", True),
        todo("Bot config live (creator_config/zarna.json)", True),
        todo("Twilio number assigned (+18775532629)", True),
        todo("SMS keyword active (ZARNA)", True),
        todo("SlickText integration live", True),
        todo("Live show keyword system active", True),
        todo("Blast system active", True),
        todo("Fan memory + profiling active", True),
        todo("Operator dashboard access", True),
        todo("pgvector migration (embeddings → DB)", False),
        todo("Self-serve provisioning (waiting on Twilio campaign SID)", False),
    ]

    return create_page(PERFORMERS_DB_ID, properties, children)


# ── West Side Comedy Club ─────────────────────────────────────────────────────

def create_wscc():
    print("\n→ Creating West Side Comedy Club (Businesses)...")

    properties = {
        "Name":                 {"title": rt("West Side Comedy Club")},
        "Slug":                 {"rich_text": rt("west_side_comedy")},
        "Email":                {"email": "bookingswestsidecomedy@gmail.com"},
        "Status":               {"select": {"name": "live"}},
        "Joined":               {"date": {"start": "2026-04-01"}},
        "Website":              {"url": "https://www.westsidecomedyclub.com"},
        "Phone Rental ($/mo)":  {"number": PHONE_RENTAL},
        "Monthly Fee ($)":      {"number": 0},
        "Subscribers":          {"number": 0},
        "Total Messages":       {"number": 0},
        "Messages This Month":  {"number": 0},
        "Est AI Cost ($/mo)":   {"number": 0},
        "Est SMS Cost ($/mo)":  {"number": 0},
        "Total Cost ($/mo)":    {"number": PHONE_RENTAL},
        "Net Margin ($/mo)":    {"number": round(0 - PHONE_RENTAL, 2)},
        "Shows Run":            {"number": 0},
    }

    children = [
        heading("📝 About"),
        para("Comedy club on the Upper West Side of Manhattan. First SMB pilot client — free for 3 months to prove the business model. Full-service venue with a Mexican food menu and bar. Books national headliners and up-and-coming talent."),
        divider(),

        heading("📍 Location & Hours"),
        bullet("Address: 201 West 75th Street, New York, NY (Upper West Side)"),
        bullet("Subway: 1/2/3 to 72nd St or B/C to 72nd St"),
        bullet("Hours: Tue–Fri 4pm–midnight | Sat 1pm–midnight | Sun 2pm–11pm | Mon closed"),
        bullet("Google Maps: https://maps.google.com/?q=201+West+75th+Street+New+York+NY"),
        divider(),

        heading("🎤 Bot Setup"),
        bullet("Tone: fun, casual, hype — like a friend telling you about a great show in NYC"),
        bullet("SMS Keyword: TBD (pending owner confirmation)"),
        bullet("Owner phone: TBD (pending owner confirmation)"),
        bullet("Welcome message: 'Thanks for joining West Side Comedy Club! Really glad you're here. You can text me anytime — show times, tickets, directions, what's on this weekend, whatever you need.'"),
        bullet("Segments: LOCAL, OUT_OF_TOWN, ENGAGED, STANDUP, IMPROV, TICKET_BUYERS, DEAL_SEEKERS"),
        divider(),

        heading("🌟 Notable Performers"),
        bullet("Past headliners: Bill Burr, Ronnie Chieng, Jessica Kirson"),
        bullet("Mix of top national talent and exciting up-and-comers"),
        divider(),

        heading("🔗 Tracked Links"),
        bullet("Tickets: https://www.westsidecomedyclub.com"),
        bullet("Calendar: https://www.westsidecomedyclub.com/calendar"),
        bullet("Menu: https://www.westsidecomedyclub.com/menu"),
        bullet("Directions: https://maps.google.com/?q=201+West+75th+Street+New+York+NY"),
        divider(),

        heading("💰 Billing"),
        bullet("Pilot arrangement: FREE for 3 months"),
        bullet("After pilot: SMB Standard plan (~$99/mo) or negotiated"),
        bullet("Phone rental: $1.15/mo (only current cost)"),
        divider(),

        heading("❓ Open Questions (from wscc_owner_questions.md)"),
        bullet("SMS keyword — what word should fans text to join? (e.g. COMEDY, WSCC, LAUGH)"),
        bullet("Owner phone number — for blast confirmations and blast triggers"),
        bullet("Parking — any nearby garage or lot to recommend?"),
        bullet("Accessible entrance — wheelchair access details?"),
        bullet("Pre-show dinner — how should guests book? Email ahead or just show up?"),
        bullet("Ticket price range — once comfortable sharing publicly"),
        divider(),

        heading("📋 Setup Checklist"),
        todo("Account created", True),
        todo("Bot config live (creator_config/west_side_comedy.json)", True),
        todo("Knowledge base populated (address, hours, policies, FAQ)", True),
        todo("Tracked links configured (tickets, calendar, menu, map)", True),
        todo("Fan segments defined (LOCAL, ENGAGED, STANDUP, IMPROV, etc.)", True),
        todo("Owner phone number confirmed", False),
        todo("SMS keyword confirmed and activated", False),
        todo("Twilio number assigned", False),
        todo("First blast sent", False),
        todo("Subscriber base growing", False),
    ]

    return create_page(BUSINESSES_DB_ID, properties, children)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    zarna_page_id = create_zarna()
    wscc_page_id  = create_wscc()

    print("\n✓ Done.")
    if zarna_page_id:
        print(f"  Zarna Garg page:            https://notion.so/{zarna_page_id.replace('-', '')}")
    if wscc_page_id:
        print(f"  West Side Comedy Club page: https://notion.so/{wscc_page_id.replace('-', '')}")
