"""
JSON API routes consumed by the Zar marketing site / Lovable React dashboard.

All routes require an active session (login via /api/auth/login first).
All routes return JSON — no HTML rendering.
"""

from flask import Blueprint, jsonify, request
from ..routes.auth import login_required, current_user
from ..queries import get_overview_stats, list_shows, list_blast_drafts, get_all_tags
from ..db import get_conn
import logging

logger = logging.getLogger(__name__)

api_bp = Blueprint("api", __name__)


# ── Dashboard ─────────────────────────────────────────────────────────────────

@api_bp.route("/api/dashboard/stats")
@login_required
def dashboard_stats():
    """Main dashboard stats — mirrors the HTML dashboard data."""
    try:
        stats = get_overview_stats()
    except Exception:
        logger.exception("api: failed to fetch overview stats")
        stats = {}

    def pct_delta(current, previous):
        if not previous:
            return {"pct": None, "dir": "neutral"}
        diff = current - previous
        pct = round(abs(diff) / previous * 100)
        return {"pct": pct, "dir": "up" if diff > 0 else "down"}

    return jsonify(
        total_subscribers=stats.get("total_subscribers", 0),
        total_messages=stats.get("total_messages", 0),
        messages_today=stats.get("messages_today", 0),
        messages_week=stats.get("messages_week", 0),
        new_subs_week=stats.get("new_subs_week", 0),
        profiled_fans=stats.get("profiled_fans", 0),
        week_delta=pct_delta(stats.get("messages_week", 0), stats.get("messages_prev_week", 0)),
        sub_delta=pct_delta(stats.get("new_subs_week", 0), stats.get("new_subs_prev_week", 0)),
        messages_by_day=[
            {"date": d, "count": c}
            for d, c in stats.get("messages_by_day", [])
        ],
        messages_by_hour=[
            {"hour": h, "count": c}
            for h, c in enumerate(stats.get("messages_by_hour", []))
        ],
        tag_breakdown=[
            {"tag": t, "count": c}
            for t, c in stats.get("tag_breakdown", [])
        ],
        top_area_codes=[
            {"area_code": a, "count": c}
            for a, c in stats.get("top_area_codes", [])
        ],
    )


# ── Shows ─────────────────────────────────────────────────────────────────────

@api_bp.route("/api/shows")
@login_required
def shows_list():
    """List all live shows grouped by status."""
    try:
        shows = list_shows()
    except Exception:
        logger.exception("api: failed to list shows")
        shows = []

    def fmt(s):
        return {
            "id": s["id"],
            "name": s["name"],
            "status": s["status"],
            "keyword": s.get("keyword"),
            "deliver_as": s.get("deliver_as"),
            "event_category": s.get("event_category"),
            "signup_count": s.get("signup_count", 0),
            "window_start": s["window_start"].isoformat() if s.get("window_start") else None,
            "window_end": s["window_end"].isoformat() if s.get("window_end") else None,
            "event_timezone": s.get("event_timezone", "America/New_York"),
            "created_at": s["created_at"].isoformat() if s.get("created_at") else None,
        }

    live   = [fmt(s) for s in shows if s["status"] == "live"]
    draft  = [fmt(s) for s in shows if s["status"] == "draft"]
    ended  = [fmt(s) for s in shows if s["status"] == "ended"]

    return jsonify(live=live, draft=draft, ended=ended, total=len(shows))


# ── Blasts ────────────────────────────────────────────────────────────────────

@api_bp.route("/api/blasts")
@login_required
def blasts_list():
    """List recent blast drafts with their send stats."""
    try:
        drafts = list_blast_drafts()
        tags = get_all_tags()
    except Exception:
        logger.exception("api: failed to list blasts")
        drafts, tags = [], []

    def fmt(d):
        return {
            "id": d["id"],
            "name": d.get("name") or "Untitled",
            "status": d["status"],
            "body": d.get("body", ""),
            "channel": d.get("channel", "sms"),
            "audience_type": d.get("audience_type"),
            "audience_filter": d.get("audience_filter"),
            "sent_count": d.get("sent_count", 0),
            "failed_count": d.get("failed_count", 0),
            "total_recipients": d.get("total_recipients", 0),
            "scheduled_at": d["scheduled_at"].isoformat() if d.get("scheduled_at") else None,
            "sent_at": d["sent_at"].isoformat() if d.get("sent_at") else None,
            "created_at": d["created_at"].isoformat() if d.get("created_at") else None,
        }

    return jsonify(
        drafts=[fmt(d) for d in drafts],
        tags=tags,
        total=len(drafts),
    )


# ── Audience ──────────────────────────────────────────────────────────────────

@api_bp.route("/api/audience")
@login_required
def audience():
    """Audience tags, area codes, and fan tier breakdown."""
    try:
        stats = get_overview_stats()
    except Exception:
        logger.exception("api: failed to fetch audience stats")
        stats = {}

    # Fan tier counts from contacts table
    tier_counts = {}
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT fan_tier, COUNT(*) as count
                FROM contacts
                WHERE fan_tier IS NOT NULL
                GROUP BY fan_tier
                ORDER BY count DESC
            """)
            tier_counts = {row["fan_tier"]: row["count"] for row in cur.fetchall()}
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch tier counts")

    tier_order = ["superfan", "engaged", "casual", "dormant"]
    tier_icons = {"superfan": "⭐", "engaged": "✅", "casual": "💬", "dormant": "😴"}

    return jsonify(
        tag_breakdown=[
            {"tag": t, "count": c}
            for t, c in stats.get("tag_breakdown", [])
        ],
        top_area_codes=[
            {"area_code": a, "count": c}
            for a, c in stats.get("top_area_codes", [])
        ],
        fan_tiers=[
            {
                "tier": tier,
                "count": tier_counts.get(tier, 0),
                "icon": tier_icons.get(tier, ""),
            }
            for tier in tier_order
        ],
        total_profiled=stats.get("profiled_fans", 0),
    )


# ── Inbox ─────────────────────────────────────────────────────────────────────

@api_bp.route("/api/inbox")
@login_required
def inbox():
    """
    Paginated list of recent conversations (25 per page), newest first.
    Each row = one fan thread: last message, timestamp, message count, fan tier.
    Phone numbers are masked to last-4 only.
    Query params: ?page=1
    """
    page = max(1, int(request.args.get("page", 1)))
    per_page = 25
    offset = (page - 1) * per_page

    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Total conversation count for pagination
            cur.execute("""
                SELECT COUNT(DISTINCT phone_number) AS total
                FROM messages
                WHERE role = 'user'
            """)
            total = cur.fetchone()["total"]

            # One row per fan: most recent message, message counts, fan info
            cur.execute("""
                SELECT
                    m.phone_number,
                    RIGHT(m.phone_number, 4) AS phone_last4,
                    MAX(m.created_at) AS last_message_at,
                    COUNT(*) FILTER (WHERE m.role = 'user') AS fan_messages,
                    COUNT(*) FILTER (WHERE m.role = 'assistant') AS bot_messages,
                    (
                        SELECT body FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                        ORDER BY m2.created_at DESC LIMIT 1
                    ) AS last_body,
                    (
                        SELECT role FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                        ORDER BY m2.created_at DESC LIMIT 1
                    ) AS last_role,
                    c.fan_tier,
                    c.fan_tags
                FROM messages m
                LEFT JOIN contacts c ON c.phone_number = m.phone_number
                GROUP BY m.phone_number, c.fan_tier, c.fan_tags
                ORDER BY last_message_at DESC
                LIMIT %s OFFSET %s
            """, (per_page, offset))
            rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch inbox")
        return jsonify(conversations=[], total=0, page=page, pages=0), 500

    conversations = []
    for r in rows:
        tags = r["fan_tags"] or []
        conversations.append({
            "phone_last4": r["phone_last4"],
            "last_message_at": r["last_message_at"].isoformat() if r["last_message_at"] else None,
            "last_body": (r["last_body"] or "")[:120],
            "last_role": r["last_role"],
            "fan_messages": r["fan_messages"],
            "bot_messages": r["bot_messages"],
            "fan_tier": r["fan_tier"],
            "fan_tags": tags[:5],
        })

    return jsonify(
        conversations=conversations,
        total=total,
        page=page,
        pages=-(-total // per_page),
    )


@api_bp.route("/api/inbox/<phone_last4>/thread")
@login_required
def inbox_thread(phone_last4):
    """
    Full message thread for a fan identified by their last-4 phone digits.
    If multiple fans share the same last-4, returns the most recently active one.
    """
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Find the full phone number (most recent match)
            cur.execute("""
                SELECT phone_number FROM messages
                WHERE RIGHT(phone_number, 4) = %s
                GROUP BY phone_number
                ORDER BY MAX(created_at) DESC
                LIMIT 1
            """, (phone_last4,))
            row = cur.fetchone()
            if not row:
                return jsonify(messages=[], fan={}), 404

            phone = row["phone_number"]

            cur.execute("""
                SELECT role, body, created_at, intent
                FROM messages
                WHERE phone_number = %s
                ORDER BY created_at ASC
            """, (phone,))
            messages = [
                {
                    "role": r["role"],
                    "body": r["body"],
                    "created_at": r["created_at"].isoformat(),
                    "intent": r.get("intent"),
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT fan_tier, fan_tags, fan_memory, created_at
                FROM contacts WHERE phone_number = %s
            """, (phone,))
            fan_row = cur.fetchone()
            fan = {}
            if fan_row:
                fan = {
                    "fan_tier": fan_row["fan_tier"],
                    "fan_tags": fan_row["fan_tags"] or [],
                    "fan_memory": fan_row["fan_memory"],
                    "joined_at": fan_row["created_at"].isoformat() if fan_row["created_at"] else None,
                }
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch thread for %s", phone_last4)
        return jsonify(messages=[], fan={}), 500

    return jsonify(messages=messages, fan=fan, phone_last4=phone_last4)


# ── Bot Data ──────────────────────────────────────────────────────────────────

@api_bp.route("/api/bot-data")
@login_required
def bot_data():
    """
    Returns the current bot configuration for the logged-in user.
    Stub: reads from creator_config/<slug>.json.
    Future: will read from DB per user's creator_slug.
    """
    import json, os
    from pathlib import Path

    # Stub: hardcoded to zarna until multi-tenant user→slug mapping is built
    slug = "zarna"
    config_path = Path(__file__).parents[4] / "creator_config" / f"{slug}.json"

    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except Exception:
        logger.exception("api: failed to load creator config for slug=%s", slug)
        return jsonify(error="Config not found"), 404

    # Return only the fields the UI needs — never expose internal prompt blocks
    links = cfg.get("links", {})
    return jsonify(
        name=cfg.get("name", ""),
        description=cfg.get("description", ""),
        voice_style=cfg.get("voice_style", ""),
        links={
            "tickets": links.get("tickets", ""),
            "merch": links.get("merch", ""),
            "book": links.get("book", ""),
            "youtube": links.get("youtube", ""),
        },
        banned_words=cfg.get("banned_words", []),
        name_variants=cfg.get("name_variants", []),
        # Edit count stub — will be real when plan tracking is built
        edits_used=0,
        edits_limit=20,
    )


# ── User ──────────────────────────────────────────────────────────────────────

@api_bp.route("/api/user")
@login_required
def user_info():
    """Returns the current logged-in user's info."""
    user = current_user()
    return jsonify(
        email=user["email"],
        name=user["name"],
        is_owner=user["is_owner"],
    )
