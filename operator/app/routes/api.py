"""
JSON API routes consumed by the Zar marketing site / Lovable React dashboard.

All routes require an active session (login via /api/auth/login first).
All routes return JSON — no HTML rendering.
"""

from pathlib import Path

from flask import Blueprint, jsonify, request, session
from ..routes.auth import login_required, current_user, resolve_slug, get_authorized_slugs

_BUSINESS_CONFIGS_DIR = Path(__file__).parent.parent / "business_configs"
from ..queries import get_overview_stats, list_shows, list_blast_drafts, get_all_tags
from ..db import get_conn
import logging

logger = logging.getLogger(__name__)

api_bp = Blueprint("api", __name__)


def _get_viewing_as() -> tuple[str | None, str | None]:
    """
    Return (slug, account_type) for the project a super-admin is currently
    viewing, preferring the per-request ``X-Viewing-As`` header over the
    server-side session.

    Why header-first?  The Flask session is a single shared cookie for the
    browser, so selecting a project in Tab B overwrites what Tab A was
    viewing.  The frontend stores the selection in ``sessionStorage`` (which
    is per-tab) and sends it as this header on every API call, making each
    tab completely independent.

    Falls back to ``session["viewing_as"]`` for backward compatibility with
    any code path that still sets the session (e.g. non-header callers or
    fresh tabs before they have made their first select-project call).
    """
    user = current_user()
    if not user or not user.get("is_super_admin"):
        return (None, None)

    header_slug = (request.headers.get("X-Viewing-As") or "").strip()
    if header_slug:
        # Look up account_type for the header slug so callers don't need to.
        account_type = session.get("viewing_as_account_type") if session.get("viewing_as") == header_slug else None
        if not account_type:
            try:
                _c = get_conn()
                with _c.cursor() as _cur:
                    _cur.execute(
                        "SELECT account_type FROM operator_users "
                        "WHERE creator_slug=%s AND is_active=TRUE LIMIT 1",
                        (header_slug,),
                    )
                    _row = _cur.fetchone()
                    account_type = (_row[0] if _row else None) or "performer"
                _c.close()
            except Exception:
                logger.exception("_get_viewing_as: DB lookup failed for slug=%s", header_slug)
                account_type = "performer"
        return (header_slug, account_type)

    # Fallback: legacy server-side session (shared across tabs, but kept for
    # backward compatibility with fresh tabs that haven't sent the header yet).
    slug = session.get("viewing_as")
    account_type = session.get("viewing_as_account_type") or "performer"
    return (slug or None, account_type if slug else None)


def _slug_or_abort():
    """
    Resolve the authorized creator_slug for the current request.
    Returns the slug string on success.
    Calls flask.abort() with 401/403 on authorization failure so callers
    never need to branch on the error code themselves.
    An empty slug means the account is not yet provisioned — treat as 403
    so unprovisioned users never see another tenant's data.
    """
    from flask import abort
    slug, err = resolve_slug()
    if err == 401:
        abort(401)
    if err == 403:
        abort(403)
    if not slug:
        abort(403)
    return slug


def _require_performer_account():
    """
    Performer-only endpoints (dashboard/stats, audience, inbox, shows, blasts,
    fan-of-the-week) are scoped to performer-style data shapes (fan tags, area
    codes, blast drafts, etc.) and the corresponding *performer* tables/queries.

    Business (SMB) accounts have parallel /api/business/* endpoints that read
    from smb_subscribers / smb_messages / smb_blasts. Returning 404 here keeps
    the response shape JSON and avoids leaking which paths exist while making
    the misuse explicit (so the frontend never silently shows zeroed-out data
    for the wrong account type). Super-admins are always allowed through so
    they can inspect any tenant.
    """
    from flask import abort
    user = current_user() or {}
    if user.get("is_super_admin"):
        return
    if (user.get("account_type") or "performer") == "business":
        abort(404)


# ── Dashboard ─────────────────────────────────────────────────────────────────

@api_bp.route("/api/dashboard/stats")
@login_required
def dashboard_stats():
    """Main dashboard stats — mirrors the HTML dashboard data."""
    _require_performer_account()
    slug = _slug_or_abort()
    try:
        stats = get_overview_stats(creator_slug=slug)
    except Exception:
        logger.exception("api: failed to fetch overview stats")
        stats = {}

    def pct_delta(current, previous):
        if not previous:
            return {"pct": None, "dir": "neutral"}
        diff = current - previous
        pct = round(abs(diff) / previous * 100)
        return {"pct": pct, "dir": "up" if diff > 0 else "down"}

    import os as _os
    sms_number = _os.getenv("TWILIO_PHONE_NUMBER", "")

    return jsonify(
        total_subscribers=stats.get("total_subscribers", 0),
        total_messages=stats.get("total_messages", 0),
        messages_today=stats.get("messages_today", 0),
        messages_week=stats.get("messages_week", 0),
        new_subs_week=stats.get("new_subs_week", 0),
        profiled_fans=stats.get("profiled_fans", 0),
        week_delta=pct_delta(stats.get("messages_week", 0), stats.get("messages_prev_week", 0)),
        sub_delta=pct_delta(stats.get("new_subs_week", 0), stats.get("new_subs_prev_week", 0)),
        sms_number=sms_number,
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
    """List all live shows grouped by status (tenant-scoped)."""
    _require_performer_account()
    user = current_user() or {}
    # Super-admins see every show; everyone else is scoped to their tenant.
    slug = None if user.get("is_super_admin") else (user.get("creator_slug") or None)
    if not user.get("is_super_admin") and not slug:
        # No tenant yet (e.g. post-removal user) → no shows to show.
        return jsonify(live=[], draft=[], ended=[])
    try:
        shows = list_shows(creator_slug=slug)
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
    """List recent blast drafts with their send stats (tenant-scoped)."""
    _require_performer_account()
    user = current_user() or {}
    slug = None if user.get("is_super_admin") else (user.get("creator_slug") or None)
    if not user.get("is_super_admin") and not slug:
        return jsonify(drafts=[], tags=[], total=0)
    try:
        drafts = list_blast_drafts(creator_slug=slug)
        tags = get_all_tags(creator_slug=slug)
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
    _require_performer_account()
    _slug = _slug_or_abort()
    try:
        stats = get_overview_stats(creator_slug=_slug)
    except Exception:
        logger.exception("api: failed to fetch audience stats")
        stats = {}

    # Fan tier counts from contacts table (slug already resolved above)
    tier_counts = {}
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT fan_tier, COUNT(*) as count
                FROM contacts
                WHERE creator_slug = %s AND fan_tier IS NOT NULL
                GROUP BY fan_tier
                ORDER BY count DESC
            """, (_slug,))
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
        messages_by_day=[
            {"date": d, "count": c}
            for d, c in stats.get("messages_by_day", [])
        ],
        messages_by_hour=[
            {"hour": h, "count": c}
            for h, c in enumerate(stats.get("messages_by_hour", []))
        ],
    )


@api_bp.route("/api/billing/cost-breakdown")
@login_required
def api_cost_breakdown():
    """
    GET /api/billing/cost-breakdown?slug=zarna&month=2026-04
    Returns exact AI cost (from messages.ai_cost_usd), SMS cost (from sms_cost_log),
    and phone rental for a given creator and calendar month.
    Falls back to flat estimates for any source not yet populated.

    Authorization: a non-super-admin may only request cost data for a slug
    they are explicitly authorized for (their own creator_slug, or a tenant
    they belong to via team_members). Anything else 403s — prevents an IDOR
    leak of another tenant's billing data.
    """
    import psycopg2.extras
    user = current_user() or {}
    slug  = request.args.get("slug", "").strip().lower()
    month = request.args.get("month", "").strip()  # e.g. "2026-04"
    if not slug:
        return jsonify(error="slug is required"), 400
    if not user.get("is_super_admin"):
        allowed = get_authorized_slugs(user.get("id"), user.get("creator_slug"))
        if slug not in {s.lower() for s in allowed}:
            return jsonify(error="forbidden"), 403
    if not month:
        from datetime import date
        month = date.today().strftime("%Y-%m")

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # AI cost — exact from messages table
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(m.ai_cost_usd), -1)       AS total_ai_usd,
                    COUNT(*)                                AS message_count,
                    COALESCE(SUM(m.prompt_tokens), 0)      AS prompt_tokens,
                    COALESCE(SUM(m.completion_tokens), 0)  AS completion_tokens,
                    COUNT(*) FILTER (WHERE m.provider = 'gemini')    AS gemini_msgs,
                    COUNT(*) FILTER (WHERE m.provider = 'openai')    AS openai_msgs,
                    COUNT(*) FILTER (WHERE m.provider = 'anthropic') AS anthropic_msgs,
                    COALESCE(SUM(m.ai_cost_usd) FILTER (WHERE m.provider = 'gemini'),    0) AS gemini_cost,
                    COALESCE(SUM(m.ai_cost_usd) FILTER (WHERE m.provider = 'openai'),    0) AS openai_cost,
                    COALESCE(SUM(m.ai_cost_usd) FILTER (WHERE m.provider = 'anthropic'), 0) AS anthropic_cost
                FROM messages m
                JOIN contacts c ON c.phone_number = m.phone_number
                WHERE c.creator_slug = %s
                  AND m.role = 'assistant'
                  AND TO_CHAR(m.created_at AT TIME ZONE 'UTC', 'YYYY-MM') = %s
                """,
                (slug, month),
            )
            ai_row = cur.fetchone()

            # SMS cost — from nightly Twilio sync
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(inbound_cost_usd + outbound_cost_usd), -1) AS total_sms_usd,
                    COALESCE(SUM(inbound_count), 0)   AS inbound_count,
                    COALESCE(SUM(outbound_count), 0)  AS outbound_count,
                    COALESCE(SUM(inbound_cost_usd), 0)  AS inbound_cost_usd,
                    COALESCE(SUM(outbound_cost_usd), 0) AS outbound_cost_usd
                FROM sms_cost_log
                WHERE creator_slug = %s
                  AND TO_CHAR(log_date, 'YYYY-MM') = %s
                """,
                (slug, month),
            )
            sms_row = cur.fetchone()

        conn.close()

        PHONE_RENTAL = 1.15
        AI_FALLBACK_PER_MSG  = 0.004
        SMS_FALLBACK_PER_MSG = 0.0079

        msg_count   = ai_row["message_count"] or 0
        total_ai    = float(ai_row["total_ai_usd"]) if ai_row["total_ai_usd"] >= 0 else round(msg_count * AI_FALLBACK_PER_MSG, 2)
        ai_exact    = ai_row["total_ai_usd"] >= 0

        total_sms   = float(sms_row["total_sms_usd"]) if sms_row["total_sms_usd"] >= 0 else round(msg_count * SMS_FALLBACK_PER_MSG, 2)
        sms_exact   = sms_row["total_sms_usd"] >= 0

        total_cost  = round(PHONE_RENTAL + total_ai + total_sms, 2)

        return jsonify(
            slug=slug,
            month=month,
            ai={
                "total_usd":         round(total_ai, 4),
                "exact":             ai_exact,
                "message_count":     msg_count,
                "prompt_tokens":     int(ai_row["prompt_tokens"] or 0),
                "completion_tokens": int(ai_row["completion_tokens"] or 0),
                "by_provider": {
                    "gemini":    round(float(ai_row["gemini_cost"] or 0), 4),
                    "openai":    round(float(ai_row["openai_cost"] or 0), 4),
                    "anthropic": round(float(ai_row["anthropic_cost"] or 0), 4),
                },
                "msg_by_provider": {
                    "gemini":    int(ai_row["gemini_msgs"] or 0),
                    "openai":    int(ai_row["openai_msgs"] or 0),
                    "anthropic": int(ai_row["anthropic_msgs"] or 0),
                },
            },
            sms={
                "total_usd":        round(total_sms, 4),
                "exact":            sms_exact,
                "inbound_count":    int(sms_row["inbound_count"] or 0),
                "outbound_count":   int(sms_row["outbound_count"] or 0),
                "inbound_cost_usd": round(float(sms_row["inbound_cost_usd"] or 0), 4),
                "outbound_cost_usd":round(float(sms_row["outbound_cost_usd"] or 0), 4),
            },
            phone_rental=PHONE_RENTAL,
            total_cost_usd=total_cost,
        )
    except Exception:
        logger.exception("api: cost-breakdown failed for slug=%s month=%s", slug, month)
        return jsonify(error="internal error"), 500


@api_bp.route("/api/audience/frequency")
@login_required
def audience_frequency():
    """
    Step 10 — Blast frequency view.
    Returns per-tier fan counts + when each tier was last blasted,
    plus the 50 most recently blasted fans with tier and days-since.

    Tenant-scoped: a team member only sees frequency stats for fans that
    belong to their project (``contacts.creator_slug == user.creator_slug``).
    """
    _require_performer_account()
    user = current_user() or {}
    is_super = bool(user.get("is_super_admin"))
    slug = None if is_super else (user.get("creator_slug") or None)
    if not is_super and not slug:
        return jsonify(tiers=[], recent_blasted=[])
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            slug_sql = " AND c.creator_slug = %s" if slug else ""
            slug_params: tuple = (slug,) if slug else ()
            cur.execute("""
                SELECT
                    c.fan_tier,
                    COUNT(DISTINCT c.phone_number)                          AS fan_count,
                    MAX(br.sent_at)                                         AS last_blasted_at,
                    ROUND(AVG(
                        EXTRACT(EPOCH FROM (NOW() - br.sent_at)) / 86400
                    ))::int                                                 AS avg_days_since
                FROM contacts c
                LEFT JOIN blast_recipients br ON br.phone_number = c.phone_number
                WHERE c.fan_tier IS NOT NULL
                  AND c.phone_number NOT LIKE 'whatsapp:%%'
                """ + slug_sql + """
                GROUP BY c.fan_tier
                ORDER BY CASE c.fan_tier
                    WHEN 'superfan' THEN 1 WHEN 'engaged' THEN 2
                    WHEN 'lurker'   THEN 3 WHEN 'dormant' THEN 4
                    ELSE 5 END
            """, slug_params)
            tier_rows = cur.fetchall()

            cur.execute("""
                SELECT
                    RIGHT(c.phone_number, 4)                                AS phone_last4,
                    c.fan_tier,
                    c.fan_tags,
                    c.fan_name,
                    MAX(br.sent_at)                                         AS last_blasted_at,
                    EXTRACT(EPOCH FROM (NOW() - MAX(br.sent_at)))::int / 86400 AS days_since
                FROM contacts c
                JOIN blast_recipients br ON br.phone_number = c.phone_number
                WHERE c.phone_number NOT LIKE 'whatsapp:%%'
                """ + slug_sql + """
                GROUP BY c.phone_number, c.fan_tier, c.fan_tags, c.fan_name
                ORDER BY last_blasted_at DESC
                LIMIT 50
            """, slug_params)
            fan_rows = cur.fetchall()
        conn.close()

        tier_icons = {"superfan": "⭐", "engaged": "✅", "lurker": "👀", "dormant": "😴"}
        return jsonify(
            tiers=[
                {
                    "tier": r["fan_tier"],
                    "icon": tier_icons.get(r["fan_tier"], ""),
                    "fan_count": r["fan_count"],
                    "last_blasted_at": r["last_blasted_at"].isoformat() if r["last_blasted_at"] else None,
                    "avg_days_since": r["avg_days_since"],
                }
                for r in tier_rows
            ],
            recent_blasted=[
                {
                    "phone_last4": r["phone_last4"],
                    "fan_tier": r["fan_tier"],
                    "fan_tags": list(r["fan_tags"] or []),
                    "fan_name": r["fan_name"] or "",
                    "last_blasted_at": r["last_blasted_at"].isoformat() if r["last_blasted_at"] else None,
                    "days_since": r["days_since"],
                }
                for r in fan_rows
            ],
        )
    except Exception:
        logger.exception("api: audience/frequency failed")
        return jsonify(tiers=[], recent_blasted=[])


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
    _require_performer_account()
    _slug = _slug_or_abort()
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
                WHERE role = 'user' AND creator_slug = %s
            """, (_slug,))
            total = cur.fetchone()["total"]

            # One row per fan: most recent message, message counts, fan info
            cur.execute("""
                SELECT
                    m.phone_number,
                    RIGHT(m.phone_number, 4) AS phone_last4,
                    MAX(m.created_at) AS last_message_at,
                    MIN(m.created_at) AS first_message_at,
                    COUNT(*) FILTER (WHERE m.role = 'user') AS fan_messages,
                    COUNT(*) FILTER (WHERE m.role = 'assistant') AS bot_messages,
                    (
                        SELECT text FROM messages m2
                        WHERE m2.phone_number = m.phone_number AND m2.creator_slug = m.creator_slug
                        ORDER BY m2.created_at DESC LIMIT 1
                    ) AS last_body,
                    (
                        SELECT role FROM messages m2
                        WHERE m2.phone_number = m.phone_number AND m2.creator_slug = m.creator_slug
                        ORDER BY m2.created_at DESC LIMIT 1
                    ) AS last_role,
                    c.fan_tier,
                    c.fan_tags,
                    c.fan_location,
                    c.fan_name,
                    LEFT(c.fan_memory, 200) AS fan_memory_preview
                FROM messages m
                LEFT JOIN contacts c ON c.phone_number = m.phone_number AND c.creator_slug = m.creator_slug
                WHERE m.creator_slug = %s
                GROUP BY m.phone_number, c.fan_tier, c.fan_tags, c.fan_location, c.fan_name, c.fan_memory
                ORDER BY last_message_at DESC
                LIMIT %s OFFSET %s
            """, (_slug, per_page, offset))
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
            "first_message_at": r["first_message_at"].isoformat() if r["first_message_at"] else None,
            "last_body": (r["last_body"] or "")[:120],
            "last_role": r["last_role"],
            "fan_messages": r["fan_messages"],
            "bot_messages": r["bot_messages"],
            "fan_tier": r["fan_tier"],
            "fan_tags": tags[:5],
            "fan_location": r["fan_location"] or "",
            "fan_name": r["fan_name"] or "",
            "fan_memory_preview": (r["fan_memory_preview"] or "")[:200],
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
    Scoped to the logged-in user's authorized creator_slug.
    """
    _require_performer_account()
    _slug = _slug_or_abort()
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Find the full phone number (most recent match, scoped to this creator)
            cur.execute("""
                SELECT phone_number FROM messages
                WHERE RIGHT(phone_number, 4) = %s AND creator_slug = %s
                GROUP BY phone_number
                ORDER BY MAX(created_at) DESC
                LIMIT 1
            """, (phone_last4, _slug))
            row = cur.fetchone()
            if not row:
                return jsonify(messages=[], fan={}), 404

            phone = row["phone_number"]

            cur.execute("""
                SELECT role, text AS body, created_at, intent, tone_mode, sell_variant
                FROM messages
                WHERE phone_number = %s AND creator_slug = %s
                ORDER BY created_at ASC
            """, (phone, _slug))
            messages = [
                {
                    "role": r["role"],
                    "body": r["body"],
                    "created_at": r["created_at"].isoformat(),
                    "intent": r.get("intent"),
                    "tone_mode": r.get("tone_mode"),
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT fan_tier, fan_tags, fan_location, fan_memory, fan_score, fan_name, created_at
                FROM contacts WHERE phone_number = %s AND creator_slug = %s
            """, (phone, _slug))
            fan_row = cur.fetchone()
            fan = {}
            if fan_row:
                fan = {
                    "fan_tier": fan_row["fan_tier"],
                    "fan_tags": fan_row["fan_tags"] or [],
                    "fan_location": fan_row["fan_location"] or "",
                    "fan_name": fan_row["fan_name"] or "",
                    "fan_memory": fan_row["fan_memory"] or "",
                    "fan_score": fan_row["fan_score"],
                    "joined_at": fan_row["created_at"].isoformat() if fan_row["created_at"] else None,
                }
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch thread for %s", phone_last4)
        return jsonify(messages=[], fan={}), 500

    return jsonify(messages=messages, fan=fan, phone_last4=phone_last4)


@api_bp.route("/api/inbox/<phone_last4>/send", methods=["POST"])
@login_required
def api_inbox_send(phone_last4):
    """
    Send a manual message from the operator to a specific fan.
    Delivers via Twilio, then logs it to the messages table as role='assistant'
    so it appears inline in the thread history.

    Body: { "text": "Hey, great to hear from you!" }
    Returns: { success, message_id, sent_at }
    """
    _require_performer_account()
    from datetime import datetime, timezone
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()

    if not text:
        return jsonify(success=False, error="Message text is required."), 400
    if len(text) > 1600:
        return jsonify(success=False, error="Message too long (max 1600 chars)."), 400

    # ── Credit gate ─────────────────────────────────────────────────────
    # Estimate segments for this message so the soft-grace decision uses the
    # actual credit cost, not just "1". Callers with MMS (images) go through
    # the blast flow, not this endpoint.
    from ..billing.credits import check_send_quota, count_segments
    segments = count_segments(text, has_media=False)

    user = current_user()
    try:
        allowed, status = check_send_quota(user_id=user["id"], requested=segments)
        if not allowed:
            return jsonify(
                success=False,
                error="credit_limit_exceeded",
                message=(
                    "You're out of credits. Upgrade to keep sending."
                    if status.get("is_trial")
                    else "You're past the overage limit. Buy a booster or upgrade."
                ),
                status=status,
            ), 402
    except Exception:
        logger.exception("api_inbox_send: credit gate failed — allowing send (fail-open)")

    # Resolve the full phone number from last-4, scoped to this creator
    _slug_for_send = _slug_or_abort()
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT phone_number FROM messages
                WHERE RIGHT(phone_number, 4) = %s AND creator_slug = %s
                GROUP BY phone_number
                ORDER BY MAX(created_at) DESC
                LIMIT 1
            """, (phone_last4, _slug_for_send))
            row = cur.fetchone()
        conn.close()
    except Exception as e:
        logger.exception("api_inbox_send: phone lookup failed")
        return jsonify(success=False, error="Could not resolve phone number."), 500

    if not row:
        return jsonify(success=False, error=f"No fan found with last-4 '{phone_last4}'."), 404

    phone = row[0]

    # Send via SlickText (all outbound messages use SlickText until Twilio inbound is live)
    try:
        from ..blast_sender import _send_one
        ok = _send_one(phone, text, channel="slicktext")
        if not ok:
            return jsonify(success=False, error="SlickText send failed — check credentials."), 500
        logger.info("api_inbox_send: sent to ***%s via slicktext", phone_last4)
    except Exception as e:
        logger.exception("api_inbox_send: send failed for ***%s", phone_last4)
        return jsonify(success=False, error=f"Send failed: {e}"), 500

    # Consume credits after successful send
    try:
        from ..billing.credits import consume_credit, KIND_SMS_OUTBOUND
        consume_credit(
            user_id=user["id"],
            kind=KIND_SMS_OUTBOUND,
            credits=segments,
            source_id=f"inbox:{phone_last4}",
        )
    except Exception:
        logger.warning("api_inbox_send: consume_credit failed (send succeeded)", exc_info=True)

    # Log to messages table so it shows in thread history
    sent_at = datetime.now(timezone.utc)
    inbox_slug = (user or {}).get("creator_slug") or ""
    message_id = None
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO messages (phone_number, role, text, created_at, source, creator_slug)
                    VALUES (%s, 'assistant', %s, %s, 'manual_operator', %s)
                    RETURNING id
                """, (phone, text, sent_at, inbox_slug))
                message_id = cur.fetchone()[0]
        conn.close()
        logger.info("api_inbox_send: logged message id=%s for ***%s", message_id, phone_last4)
    except Exception as e:
        logger.warning("api_inbox_send: failed to log message (send already happened): %s", e)

    return jsonify(
        success=True,
        message_id=message_id,
        sent_at=sent_at.isoformat(),
        phone_last4=phone_last4,
    )


# ── Shows (write) ─────────────────────────────────────────────────────────────

@api_bp.route("/api/shows/create", methods=["POST"])
@login_required
def api_create_show():
    """Create a new live show draft. Returns {success, show_id, error}."""
    from ..routes.shows import _create_show, _parse_local_dt, EVENT_TIMEZONE_CHOICES, _ALLOWED_TZ
    _require_performer_account()
    data = request.get_json(silent=True) or {}

    name = (data.get("name") or "").strip()
    keyword = " ".join((data.get("keyword") or "").upper().split())
    mode = data.get("signup_mode", "keyword")
    use_kw = (mode == "keyword")
    etz = data.get("event_timezone") or "America/New_York"
    if etz not in _ALLOWED_TZ:
        etz = "America/New_York"
    ws = _parse_local_dt(data.get("window_start"), etz)
    we = _parse_local_dt(data.get("window_end"), etz)
    deliver = (data.get("deliver_as") or "sms").lower()
    if deliver not in ("sms", "whatsapp"):
        deliver = "sms"
    event_cat = (data.get("event_category") or "comedy").lower()
    if event_cat not in ("comedy", "live_stream", "other"):
        event_cat = "comedy"

    if not name:
        return jsonify(success=False, error="Show name is required."), 400
    if use_kw and not keyword:
        return jsonify(success=False, error="Keyword is required for keyword mode."), 400
    if not use_kw and (ws is None or we is None):
        return jsonify(success=False, error="Window start and end are required for time-window mode."), 400

    try:
        user = current_user() or {}
        show_id = _create_show(
            name, keyword, use_kw, ws, we, deliver, event_cat, etz,
            creator_slug=(user.get("creator_slug") or None),
            created_by=(user.get("email") or None),
        )
        return jsonify(success=True, show_id=show_id, message=f'Show "{name}" created as a draft.')
    except Exception as e:
        logger.exception("api_create_show error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/shows/<int:show_id>/activate", methods=["POST"])
@login_required
def api_activate_show(show_id):
    """Activate a show (set status=live, ends any currently live show)."""
    from ..routes.shows import _update_show_status
    user = current_user()
    if not _user_owns_show(show_id, user):
        return jsonify(success=False, error="Show not found."), 404
    try:
        _update_show_status(show_id, "live")
        return jsonify(success=True, status="live", show_id=show_id)
    except Exception as e:
        logger.exception("api_activate_show error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/shows/<int:show_id>/end", methods=["POST"])
@login_required
def api_end_show(show_id):
    """End a live show."""
    from ..routes.shows import _update_show_status
    user = current_user()
    if not _user_owns_show(show_id, user):
        return jsonify(success=False, error="Show not found."), 404
    try:
        _update_show_status(show_id, "ended")
        return jsonify(success=True, status="ended", show_id=show_id)
    except Exception as e:
        logger.exception("api_end_show error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/shows/<int:show_id>/blast-init", methods=["POST"])
@login_required
def api_show_blast_init(show_id):
    """
    Create a fresh blast draft pre-targeted to a specific live show's signup list.
    Returns { success, draft_id, show_name, signup_count } so the frontend can
    navigate directly to the Blast Composer with the draft pre-loaded.
    """
    from ..queries import save_blast_draft, list_shows
    user = current_user()
    slug = None if (user or {}).get("is_super_admin") else (user or {}).get("creator_slug")
    try:
        shows = list_shows(creator_slug=slug) if slug else list_shows()
        show = next((s for s in shows if s["id"] == show_id), None)
        if not show:
            return jsonify(success=False, error="Show not found."), 404

        show_name    = show.get("name") or f"Show {show_id}"
        signup_count = show.get("signup_count", 0)

        draft_id = save_blast_draft(
            name=f"Blast – {show_name}",
            body="",
            channel="slicktext",
            audience_type="show",
            audience_filter=str(show_id),
            sample_pct=100,
            media_url="",
            link_url="",
            tracked_link_slug="",
            created_by=user["email"] if user else "",
            creator_slug=(user.get("creator_slug") if user else None),
            draft_id=None,
        )
        return jsonify(
            success=True,
            draft_id=draft_id,
            show_name=show_name,
            signup_count=signup_count,
        )
    except Exception as e:
        logger.exception("api_show_blast_init error show_id=%s", show_id)
        return jsonify(success=False, error=str(e)), 500


# ── Blasts (read helpers) ──────────────────────────────────────────────────────

CADENCE_DAYS = {"superfan": 5, "engaged": 7, "lurker": 14, "dormant": 30}
TIER_LABELS  = {"superfan": "Superfan ⭐", "engaged": "Engaged ✅", "lurker": "Lurker 💬", "dormant": "Dormant 😴"}


@api_bp.route("/api/blasts/<int:draft_id>", methods=["GET"])
@login_required
def api_get_blast(draft_id):
    """
    Return a single blast draft by id.

    Used by the Lovable frontend when a draft is deep-linked (e.g. from a live
    show's "Blast Message" button) and hasn't loaded yet via the /api/blasts
    list endpoint.
    """
    user = current_user()
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="Not found."), 404
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, status, body, channel,
                       audience_type, audience_filter, audience_sample_pct,
                       media_url, link_url, tracked_link_slug, blast_context_note,
                       sent_count, failed_count, total_recipients,
                       scheduled_at, sent_at, created_at
                FROM   blast_drafts
                WHERE  id=%s
                """,
                (draft_id,),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify(success=False, error="Not found."), 404
        d = dict(row)
        draft = {
            "id": d["id"],
            "name": d.get("name") or "Untitled",
            "status": d["status"],
            "body": d.get("body", ""),
            "channel": d.get("channel", "twilio"),
            "audience_type": d.get("audience_type"),
            "audience_filter": d.get("audience_filter"),
            "audience_sample_pct": d.get("audience_sample_pct"),
            "media_url": d.get("media_url", ""),
            "link_url": d.get("link_url", ""),
            "tracked_link_slug": d.get("tracked_link_slug", ""),
            "blast_context_note": d.get("blast_context_note", ""),
            "sent_count": d.get("sent_count", 0),
            "failed_count": d.get("failed_count", 0),
            "total_recipients": d.get("total_recipients", 0),
            "scheduled_at": d["scheduled_at"].isoformat() if d.get("scheduled_at") else None,
            "sent_at": d["sent_at"].isoformat() if d.get("sent_at") else None,
            "created_at": d["created_at"].isoformat() if d.get("created_at") else None,
        }
        return jsonify(success=True, draft=draft)
    except Exception:
        logger.exception("api_get_blast: failed for id=%s", draft_id)
        return jsonify(success=False, error="Failed to load draft"), 500


@api_bp.route("/api/blasts/tier-counts")
@login_required
def api_blast_tier_counts():
    """
    Returns per-tier fan counts and Smart Send cadence rules so the
    Blast Composer can display subscriber counts under each tier option.

    Response:
      {
        "tiers": [
          {"tier": "superfan", "label": "Superfan ⭐", "count": 42, "cadence_days": 5},
          ...
        ]
      }
    """
    _require_performer_account()
    _slug = _slug_or_abort()
    counts = {}
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor() as cur:
            cur.execute(
                "SELECT fan_tier, COUNT(*) FROM contacts "
                "WHERE creator_slug = %s AND fan_tier IS NOT NULL AND phone_number NOT LIKE 'whatsapp:%%' "
                "GROUP BY fan_tier",
                (_slug,)
            )
            counts = {row[0]: row[1] for row in cur.fetchall()}
        conn.close()
    except Exception:
        logger.exception("api_blast_tier_counts: db error")

    tiers = [
        {
            "tier": tier,
            "label": TIER_LABELS.get(tier, tier.title()),
            "count": counts.get(tier, 0),
            "cadence_days": cadence,
        }
        for tier, cadence in CADENCE_DAYS.items()
    ]
    return jsonify(success=True, tiers=tiers)


@api_bp.route("/api/contacts/engaged")
@login_required
def api_contacts_engaged():
    """Top-N most-engaged contacts for Smart Send audience picking.

    Query: ?top=100 (clamped to 5000)
    Returns: { success, count, contacts: [{ phone_number, fan_tier, engagement_score, last_replied_at }] }
    """
    from ..engagement import top_engaged

    _require_performer_account()
    slug = _slug_or_abort()

    try:
        limit = int(request.args.get("top", "100"))
    except (TypeError, ValueError):
        limit = 100

    try:
        contacts = top_engaged(slug=slug, limit=limit)
        return jsonify(success=True, count=len(contacts), contacts=contacts)
    except Exception:
        logger.exception("api_contacts_engaged: failed")
        return jsonify(success=False, error="Failed to load engaged contacts"), 500


@api_bp.route("/api/admin/engagement/recompute", methods=["POST"])
@login_required
def api_engagement_recompute():
    """Admin-only trigger to recompute engagement scores.

    Normally run via cron nightly, but this endpoint lets an operator kick
    off a refresh on demand (e.g. right after a big blast).
    """
    from ..engagement import recompute_all

    user = current_user()
    if not user.get("is_super_admin"):
        return jsonify(error="Admin only"), 403

    try:
        count = recompute_all()
        return jsonify(success=True, rows_updated=count)
    except Exception as e:
        logger.exception("api_engagement_recompute: failed")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/smart-send-preview", methods=["POST"])
@login_required
def api_smart_send_preview():
    """
    Returns how many fans per tier would receive the blast today vs be
    suppressed by Smart Send cadence rules.

    Response:
      {
        "tiers": {
          "superfan": {"total": N, "suppressed": N, "sending": N, "cadence_days": 5},
          ...
        },
        "total_sending": N,
        "total_suppressed": N
      }
    """
    _require_performer_account()
    _slug_ssp = _slug_or_abort()
    result = {"tiers": {}, "total_sending": 0, "total_suppressed": 0}
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT phone_number FROM broadcast_optouts")
            optouts = {r[0] for r in cur.fetchall()}

            for tier, cadence in CADENCE_DAYS.items():
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts "
                    "WHERE creator_slug = %s AND fan_tier = %s AND phone_number NOT LIKE 'whatsapp:%%'",
                    (_slug_ssp, tier),
                )
                all_phones = {r[0] for r in cur.fetchall()} - optouts

                # Cadence join must be tenant-scoped on both sides — a fan
                # that Zarna's project blasted yesterday should not suppress
                # another tenant's send today, and vice versa.
                cur.execute(
                    """
                    SELECT DISTINCT br.phone_number
                    FROM   blast_recipients br
                    JOIN   blast_drafts bd ON bd.id = br.blast_id
                    JOIN   contacts c ON c.phone_number = br.phone_number
                    WHERE  c.fan_tier = %s
                      AND  c.creator_slug = %s
                      AND  bd.creator_slug = %s
                      AND  br.sent_at >= NOW() - (%s || ' days')::INTERVAL
                    """,
                    (tier, _slug_ssp, _slug_ssp, str(cadence)),
                )
                recently_blasted = {r[0] for r in cur.fetchall()}

                suppressed = len(all_phones & recently_blasted)
                sending    = len(all_phones - recently_blasted)
                result["tiers"][tier] = {
                    "total":        len(all_phones),
                    "suppressed":   suppressed,
                    "sending":      sending,
                    "cadence_days": cadence,
                }
                result["total_sending"]    += sending
                result["total_suppressed"] += suppressed
        conn.close()
    except Exception:
        logger.exception("api_smart_send_preview: db error")
        return jsonify(success=False, error="Failed to compute smart send preview"), 500

    return jsonify(success=True, **result)


# ── Blasts (write) ─────────────────────────────────────────────────────────────

# ── Ownership helpers ──────────────────────────────────────────────────────────

def _user_owns_draft(draft_id: int, user: dict) -> bool:
    """
    Returns True if the current user is allowed to operate on this blast draft.

    Authorization model (post multi-tenant migration):
      • Super-admins pass unconditionally.
      • Everyone else must belong to the draft's tenant — i.e. the draft's
        ``creator_slug`` must match the caller's ``creator_slug``. Team
        members (admin / member) can therefore view, edit, send, schedule,
        cancel and delete every draft inside their project, not just the
        drafts they personally created.

    Legacy rows with a NULL/empty ``creator_slug`` fall back to the prior
    email-based ``created_by`` check so pre-migration data stays accessible
    to the original author while the backfill propagates.
    """
    if user.get("is_super_admin"):
        return True
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT creator_slug, created_by FROM blast_drafts WHERE id=%s",
                (draft_id,),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            return False
        draft_slug = (row[0] or "").lower()
        created_by = (row[1] or "").lower()
        user_slug = (user.get("creator_slug") or "").lower()
        user_email = (user.get("email") or "").lower()

        if draft_slug:
            return bool(user_slug) and draft_slug == user_slug
        if created_by:
            return created_by == user_email
        return True
    except Exception:
        logger.exception("_user_owns_draft check failed for draft_id=%s", draft_id)
        return False


def _user_owns_show(show_id: int, user: dict) -> bool:
    """
    Returns True if the current user is allowed to operate on this live show.

    Authorization model (post multi-tenant migration):
      • Super-admins pass unconditionally.
      • Everyone else must belong to the show's tenant — i.e. their
        creator_slug must match live_shows.creator_slug. Team members
        (admin / member roles) can therefore activate, end, or delete
        shows inside their project, not just shows they personally created.

    Legacy rows with a NULL creator_slug fall back to the old email-based
    created_by check so we don't accidentally lock out accounts that still
    have historical shows from before the backfill ran.
    """
    if user.get("is_super_admin"):
        return True
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT creator_slug, created_by FROM live_shows WHERE id=%s",
                (show_id,),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            return False
        show_slug = (row[0] or "").lower()
        created_by = (row[1] or "").lower()
        user_slug = (user.get("creator_slug") or "").lower()
        user_email = (user.get("email") or "").lower()

        if show_slug:
            return bool(user_slug) and show_slug == user_slug

        # Legacy fallback: no slug stamped yet. Prefer email match if we have
        # it, otherwise (truly pre-auth rows) allow the request through.
        if created_by:
            return created_by == user_email
        return True
    except Exception:
        logger.exception("_user_owns_show check failed for show_id=%s", show_id)
        return False


@api_bp.route("/api/blasts/create", methods=["POST"])
@login_required
def api_create_blast():
    """Create a blank blast draft. Returns {success, draft_id}."""
    from ..queries import save_blast_draft
    _require_performer_account()
    user = current_user()
    try:
        draft_id = save_blast_draft(
            name="Untitled draft",
            body="",
            channel="twilio",
            audience_type="all",
            audience_filter="",
            sample_pct=100,
            created_by=user["email"] if user else "",
            creator_slug=(user.get("creator_slug") if user else None),
        )
        return jsonify(success=True, draft_id=draft_id)
    except Exception as e:
        logger.exception("api_create_blast error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/<int:draft_id>/save", methods=["POST"])
@login_required
def api_save_blast(draft_id):
    """Save blast draft fields including media_url, link_url, and tracked link creation."""
    from ..queries import save_blast_draft
    from ..routes.blast import _create_tracked_link
    data = request.get_json(silent=True) or {}
    user = current_user()
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="Not found."), 404

    body = (data.get("body") or "").strip()
    if not body:
        return jsonify(success=False, error="Message body is required."), 400

    name = (data.get("name") or "Untitled draft").strip()[:120]
    channel = data.get("channel", "twilio")
    if channel not in ("twilio", "slicktext"):
        channel = "twilio"
    audience_type = data.get("audience_type", "all")
    if audience_type not in ("all", "tag", "location", "random", "show", "tier", "engaged"):
        audience_type = "all"
    audience_filter = (data.get("audience_filter") or "").strip()[:200]
    sample_pct = max(1, min(100, int(data.get("sample_pct", 100) or 100)))
    media_url = (data.get("media_url") or "").strip()[:1000]
    link_url  = (data.get("link_url") or "").strip()[:2000]
    tracked_link_slug = (data.get("tracked_link_slug") or "").strip()
    blast_context_note = (data.get("blast_context_note") or "").strip()[:1000]

    # Auto-create a tracked link the first time a link_url is saved on this draft
    if link_url and not tracked_link_slug:
        tracked_link_slug = _create_tracked_link(link_url, name) or ""
        logger.info("api_save_blast: created tracked_link_slug=%r for draft %s", tracked_link_slug, draft_id)

    try:
        new_id = save_blast_draft(
            name=name, body=body, channel=channel,
            audience_type=audience_type, audience_filter=audience_filter,
            sample_pct=sample_pct, media_url=media_url,
            link_url=link_url, tracked_link_slug=tracked_link_slug,
            blast_context_note=blast_context_note,
            created_by=user["email"] if user else "",
            creator_slug=(user.get("creator_slug") if user else None),
            draft_id=draft_id,
        )
        tracked_url = ""
        if tracked_link_slug:
            scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
            host   = request.headers.get("X-Forwarded-Host", request.host)
            tracked_url = f"{scheme}://{host}/t/{tracked_link_slug}"
        return jsonify(success=True, draft_id=new_id,
                       tracked_link_slug=tracked_link_slug, tracked_url=tracked_url)
    except Exception as e:
        logger.exception("api_save_blast error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/upload-image", methods=["POST"])
@login_required
def api_upload_image():
    """
    Upload a blast image (MMS). Stores in Postgres and returns a public URL.
    Accepts multipart/form-data with field name 'image'.
    Returns { success, url, size }.
    """
    import uuid, base64

    f = request.files.get("image")
    if not f or not f.filename:
        return jsonify(success=False, error="No file received."), 400

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else "jpg"
    if ext not in ("jpg", "jpeg", "png", "gif", "webp"):
        return jsonify(success=False, error=f"Unsupported format .{ext} — use jpg, png, gif, or webp."), 400

    filename = f"{uuid.uuid4().hex}.{ext}"
    mime_type = f.content_type or f"image/{ext}"

    try:
        import secrets as _secrets
        data = f.read()
        if not data:
            return jsonify(success=False, error="Uploaded file is empty."), 400

        data_b64 = base64.b64encode(data).decode("ascii")
        access_token = _secrets.token_hex(16)
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO operator_blast_images (filename, mime_type, data_b64, access_token) "
                        "VALUES (%s, %s, %s, %s) RETURNING id",
                        (filename, mime_type, data_b64, access_token),
                    )
                    image_id = cur.fetchone()[0]
        finally:
            conn.close()

        scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
        host   = request.headers.get("X-Forwarded-Host", request.host)
        url = f"{scheme}://{host}/operator/blast/img/{image_id}/{access_token}/{filename}"
        logger.info("api_upload_image: stored id=%s size=%d url=%s", image_id, len(data), url)
        return jsonify(success=True, url=url, size=len(data), image_id=image_id)
    except Exception as e:
        logger.exception("api_upload_image error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/preview-count", methods=["POST"])
@login_required
def api_blast_preview_count():
    """Returns audience count for given filter as JSON."""
    from ..queries import count_audience
    _require_performer_account()
    data = request.get_json(silent=True) or {}
    audience_type = data.get("audience_type", "all")
    if audience_type not in ("all", "tag", "location", "random", "show", "tier", "engaged"):
        audience_type = "all"
    audience_filter = (data.get("audience_filter") or "").strip()
    sample_pct = max(1, min(100, int(data.get("sample_pct", 100) or 100)))
    user = current_user() or {}
    slug = None if user.get("is_super_admin") else (user.get("creator_slug") or None)
    try:
        count = count_audience(audience_type, audience_filter, sample_pct, creator_slug=slug)
        return jsonify(success=True, count=count)
    except Exception as e:
        logger.exception("api_blast_preview_count error")
        return jsonify(success=False, count=0, error=str(e)), 500


@api_bp.route("/api/blasts/<int:draft_id>/test", methods=["POST"])
@login_required
def api_blast_test(draft_id):
    """Send a [TEST] copy of the blast to a single phone number."""
    from ..blast_sender import _send_one
    from ..queries import get_blast_draft
    user = current_user()
    # Ownership gate matches the rest of the /api/blasts/<id>/* routes
    # (save, send, delete) — without it a logged-in user could /test any
    # other tenant's draft id by guessing.
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="Blast not found"), 404
    data = request.get_json(silent=True) or {}

    test_phone = (data.get("test_phone") or "").strip()
    body = (data.get("body") or "").strip()

    if not test_phone:
        return jsonify(success=False, error="test_phone is required."), 400
    if not body:
        return jsonify(success=False, error="Message body is required."), 400

    channel = data.get("channel", "twilio")
    if channel not in ("twilio", "slicktext"):
        channel = "twilio"

    test_body = f"[TEST] {body}"
    try:
        ok = _send_one(test_phone, test_body, channel)
        if ok:
            return jsonify(success=True, message=f"Test sent to ***{test_phone[-4:]}")
        else:
            return jsonify(success=False, error="Send failed — check Twilio credentials."), 500
    except Exception as e:
        logger.exception("api_blast_test error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/<int:draft_id>/send", methods=["POST"])
@login_required
def api_blast_send(draft_id):
    """
    Send a blast immediately. Accepts optional fields in the JSON body to
    auto-save the latest UI state (channel, body, audience, etc.) before
    firing, so toggles like channel are never stale.
    """
    from ..queries import get_blast_draft, save_blast_draft
    from ..blast_sender import execute_blast_async
    user = current_user()
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="Draft not found."), 404

    draft = get_blast_draft(draft_id)
    if not draft:
        return jsonify(success=False, error="Draft not found."), 404
    if draft["status"] in ("sent", "cancelled"):
        return jsonify(success=False, error="This blast has already been sent or cancelled."), 400

    # Accept any UI-state overrides in the request body and auto-save before sending
    data = request.get_json(silent=True) or {}
    body          = (data.get("body")           or draft.get("body")           or "").strip()
    channel       = (data.get("channel")        or draft.get("channel")        or "twilio")
    name          = (data.get("name")           or draft.get("name")           or "Untitled draft").strip()[:120]
    audience_type = (data.get("audience_type")  or draft.get("audience_type")  or "all")
    audience_filter = (data.get("audience_filter") or draft.get("audience_filter") or "").strip()[:200]
    sample_pct    = int(data.get("sample_pct")  or draft.get("audience_sample_pct") or 100)
    media_url     = (data.get("media_url")      or draft.get("media_url")      or "").strip()
    link_url      = (data.get("link_url")       or draft.get("link_url")       or "").strip()
    tracked_link_slug = (data.get("tracked_link_slug") or draft.get("tracked_link_slug") or "").strip()
    blast_context_note = (data.get("blast_context_note") or draft.get("blast_context_note") or "").strip()[:1000]

    if channel not in ("twilio", "slicktext"):
        channel = "twilio"
    if audience_type not in ("all", "tag", "location", "random", "show", "tier", "engaged"):
        audience_type = "all"

    if not body:
        return jsonify(success=False, error="Message body is required before sending."), 400

    # Credit gate: block trial accounts that are at zero and paid accounts past
    # the soft-grace ceiling. A real per-recipient consume_credit runs inside
    # blast_sender.py, so the worst case here is we block when there are zero
    # credits left (or <= overage ceiling for paid).
    try:
        from ..billing.credits import check_send_quota, count_segments
        segs = count_segments(body, has_media=bool(media_url))
        allowed, status = check_send_quota(user_id=user["id"], requested=segs)
        if not allowed:
            reason = "trial_credits_exhausted" if status.get("is_trial") else "credit_limit_exceeded"
            return jsonify(
                success=False,
                error=reason,
                message=("You're out of trial credits. Upgrade to keep sending."
                         if status.get("is_trial")
                         else "You've reached your credit limit. Buy a booster or upgrade."),
                upgrade_url="/plans",
                status=status,
            ), 402
    except Exception:
        logger.warning("api_blast_send: credit gate check failed (allowing send)", exc_info=True)

    # Persist latest UI state so execute_blast reads fresh values from DB
    if data:
        try:
            save_blast_draft(
                name=name, body=body, channel=channel,
                audience_type=audience_type, audience_filter=audience_filter,
                sample_pct=sample_pct, media_url=media_url,
                link_url=link_url, tracked_link_slug=tracked_link_slug,
                blast_context_note=blast_context_note,
                created_by=user["email"] if user else "",
                creator_slug=(user.get("creator_slug") if user else None),
                draft_id=draft_id,
            )
            logger.info("api_blast_send: auto-saved draft %s channel=%s before send", draft_id, channel)
        except Exception as e:
            logger.warning("api_blast_send: auto-save failed (non-fatal): %s", e)

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET status='sending', updated_at=NOW() WHERE id=%s",
                    (draft_id,),
                )
    except Exception as e:
        logger.exception("api_blast_send: failed to mark sending")
        return jsonify(success=False, error=str(e)), 500
    finally:
        conn.close()

    execute_blast_async(draft_id)
    logger.info("api_blast_send: queued draft %s via channel=%s", draft_id, channel)
    return jsonify(success=True, message="Blast queued — sending in background.", draft_id=draft_id)


@api_bp.route("/api/blasts/<int:draft_id>/schedule", methods=["POST"])
@login_required
def api_blast_schedule(draft_id):
    """Schedule a blast for a future UTC datetime."""
    from ..queries import get_blast_draft, schedule_blast
    from datetime import datetime, timezone
    user = current_user()
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="Draft not found."), 404

    data = request.get_json(silent=True) or {}

    send_at_str = (data.get("send_at") or "").strip()
    if not send_at_str:
        return jsonify(success=False, error="send_at (ISO datetime) is required."), 400

    draft = get_blast_draft(draft_id)
    if not draft:
        return jsonify(success=False, error="Draft not found."), 404

    try:
        send_at = datetime.fromisoformat(send_at_str).replace(tzinfo=timezone.utc)
    except ValueError:
        return jsonify(success=False, error="Invalid datetime format — use ISO 8601."), 400

    try:
        schedule_blast(draft_id, send_at)
        return jsonify(success=True, scheduled_at=send_at.isoformat(), draft_id=draft_id)
    except Exception as e:
        logger.exception("api_blast_schedule error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/<int:draft_id>/cancel", methods=["POST"])
@login_required
def api_blast_cancel(draft_id):
    """Cancel a scheduled or draft blast."""
    from ..queries import mark_blast_cancelled
    user = current_user()
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="Draft not found."), 404
    try:
        mark_blast_cancelled(draft_id)
        return jsonify(success=True, draft_id=draft_id)
    except Exception as e:
        logger.exception("api_blast_cancel error")
        return jsonify(success=False, error=str(e)), 500


@api_bp.route("/api/blasts/<int:draft_id>/status")
@login_required
def api_blast_status(draft_id):
    """Poll blast send progress."""
    from ..queries import get_blast_draft
    user = current_user()
    if not _user_owns_draft(draft_id, user):
        return jsonify(success=False, error="not found"), 404
    draft = get_blast_draft(draft_id)
    if not draft:
        return jsonify(success=False, error="not found"), 404
    return jsonify(
        success=True,
        status=draft["status"],
        sent_count=draft["sent_count"] or 0,
        failed_count=draft["failed_count"] or 0,
        total_recipients=draft["total_recipients"] or 0,
    )


# ── Fan of the Week ───────────────────────────────────────────────────────────

_FOTW_CANDIDATES_SQL = """
    WITH
    -- recent fan activity signals (last 7 days)
    recent_replies AS (
        SELECT phone_number, COUNT(*) AS reply_count
        FROM   messages
        WHERE  role = 'user'
          AND  creator_slug = %s
          AND  created_at >= NOW() - INTERVAL '7 days'
          AND  did_user_reply = true
        GROUP  BY phone_number
    ),
    came_back AS (
        SELECT phone_number, COUNT(*) > 0 AS did_come_back
        FROM   conversation_sessions
        WHERE  came_back_within_7d = true
          AND  started_at >= NOW() - INTERVAL '7 days'
        GROUP  BY phone_number
    ),
    -- best qualifying message per fan (not blast reply, not opt-out, 50-400 chars)
    best_msg AS (
        SELECT DISTINCT ON (m.phone_number)
            m.phone_number,
            m.text AS message_text,
            m.created_at AS msg_at
        FROM messages m
        WHERE m.role = 'user'
          AND m.creator_slug = %s
          AND m.created_at >= NOW() - INTERVAL '1 day' * %s
          AND LENGTH(m.text) BETWEEN 15 AND 400
          AND m.text NOT ILIKE 'stop%%'
          AND m.text NOT ILIKE 'yes%%'
          AND m.text NOT ILIKE 'no%%'
          AND m.text NOT ILIKE 'ok%%'
          AND (m.intent IS NULL OR m.intent NOT IN ('STOP', 'OPTOUT'))
        ORDER BY m.phone_number, LENGTH(m.text) DESC
    )
    SELECT
        bm.phone_number,
        RIGHT(bm.phone_number, 4)                AS phone_last4,
        bm.message_text,
        bm.msg_at,
        c.fan_tier,
        COALESCE(c.fan_score, 0)                 AS fan_score,
        c.fan_tags,
        c.fan_location,
        c.fan_name,
        c.fan_memory,
        COALESCE(rr.reply_count, 0)              AS reply_count,
        COALESCE(cb.did_come_back, false)        AS came_back,
        (
            COALESCE(c.fan_score, 0) * 0.4
          + CASE c.fan_tier
                WHEN 'superfan' THEN 30
                WHEN 'engaged'  THEN 15
                ELSE 0
            END
          + LEAST(COALESCE(rr.reply_count, 0) * 5, 25)
          + CASE WHEN COALESCE(cb.did_come_back, false) THEN 20 ELSE 0 END
          + CASE WHEN c.fan_memory IS NOT NULL AND c.fan_memory != '' THEN 10 ELSE 0 END
          + RANDOM() * 5
        )                                        AS candidate_score
    FROM best_msg bm
    LEFT JOIN contacts c  ON c.phone_number = bm.phone_number AND c.creator_slug = %s
    LEFT JOIN recent_replies rr ON rr.phone_number = bm.phone_number
    LEFT JOIN came_back cb      ON cb.phone_number = bm.phone_number
    WHERE bm.phone_number NOT IN (
        SELECT phone_number FROM fan_of_the_week
        WHERE week_of >= CURRENT_DATE - INTERVAL '8 weeks'
          AND creator_slug = %s
    )
      AND bm.phone_number NOT IN (
        '+16466406086', '+16467244908', '+16467242012'
    )
    ORDER BY candidate_score DESC
    LIMIT 5
"""


@api_bp.route("/api/fan-of-the-week")
@login_required
def fan_of_the_week():
    """
    Returns this week's saved Fan of the Week if one has been selected,
    otherwise falls back to the top dynamic candidate.
    Scoped to the logged-in user's authorized creator_slug.
    """
    _require_performer_account()
    _slug = _slug_or_abort()
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Check if a pick is already saved for the current week
            cur.execute("""
                SELECT f.phone_number, RIGHT(f.phone_number, 4) AS phone_last4,
                       f.message_text, f.week_of, f.selected_at,
                       c.fan_tier, c.fan_tags, c.fan_location, c.fan_memory, c.fan_score, c.fan_name
                FROM fan_of_the_week f
                LEFT JOIN contacts c ON c.phone_number = f.phone_number
                WHERE f.week_of = DATE_TRUNC('week', CURRENT_DATE)::date
                  AND f.creator_slug = %s
                LIMIT 1
            """, (_slug,))
            saved = cur.fetchone()
            if saved:
                conn.close()
                tags = saved["fan_tags"] or []
                return jsonify(
                    found=True,
                    saved=True,
                    phone_last4=saved["phone_last4"],
                    body=saved["message_text"] or "",
                    week_of=saved["week_of"].isoformat(),
                    selected_at=saved["selected_at"].isoformat(),
                    fan_tier=saved["fan_tier"],
                    fan_tags=tags[:5],
                    fan_location=saved["fan_location"] or "",
                    fan_name=saved["fan_name"] or "",
                    fan_memory=saved["fan_memory"] or "",
                    fan_score=saved["fan_score"],
                )
            # No saved pick — return top dynamic candidate scoped to this creator
            row = None
            for days_back in (7, 30, 90):
                cur.execute(_FOTW_CANDIDATES_SQL, (_slug, _slug, days_back, _slug, _slug))
                row = cur.fetchone()
                if row:
                    break
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch fan of the week")
        return jsonify(found=False), 500

    if not row:
        return jsonify(found=False)

    tags = row["fan_tags"] or []
    return jsonify(
        found=True,
        saved=False,
        phone_last4=row["phone_last4"],
        body=row["message_text"],
        created_at=row["msg_at"].isoformat(),
        fan_tier=row["fan_tier"],
        fan_tags=tags[:5],
        fan_location=row["fan_location"] or "",
        fan_name=row["fan_name"] or "",
        fan_memory=row["fan_memory"] or "",
        fan_score=row["fan_score"],
        days_back=days_back,
    )


@api_bp.route("/api/fan-of-the-week/candidates")
@login_required
def fan_of_the_week_candidates():
    """
    Returns up to 5 smart-ranked candidates for Fan of the Week,
    excluding anyone picked in the last 8 weeks.
    Scoped to the logged-in user's authorized creator_slug.
    """
    _require_performer_account()
    _slug = _slug_or_abort()
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            rows = []
            for days_back in (7, 30, 90):
                cur.execute(_FOTW_CANDIDATES_SQL, (_slug, _slug, days_back, _slug, _slug))
                rows = cur.fetchall()
                if rows:
                    break
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch fan of the week candidates")
        return jsonify(candidates=[], days_back=0), 500

    candidates = []
    for r in rows:
        tags = r["fan_tags"] or []
        candidates.append({
            "phone_last4": r["phone_last4"],
            "message_text": r["message_text"],
            "msg_at": r["msg_at"].isoformat(),
            "fan_tier": r["fan_tier"],
            "fan_score": r["fan_score"],
            "fan_tags": tags[:5],
            "fan_location": r["fan_location"] or "",
            "fan_memory": r["fan_memory"] or "",
            "reply_count": r["reply_count"],
            "came_back": r["came_back"],
            "candidate_score": round(float(r["candidate_score"]), 1),
        })
    return jsonify(candidates=candidates, days_back=days_back)


@api_bp.route("/api/fan-of-the-week/select", methods=["POST"])
@login_required
def fan_of_the_week_select():
    """
    Save the chosen Fan of the Week for the current week.
    Body: { "phone_last4": "1234", "message_text": "..." }
    Also tags the contact with 'fan_of_the_week'.
    """
    _require_performer_account()
    _slug_fotw = _slug_or_abort()
    import psycopg2.extras
    data = request.get_json(silent=True) or {}
    phone_last4 = (data.get("phone_last4") or "").strip()
    message_text = (data.get("message_text") or "").strip()[:500]

    if not phone_last4:
        return jsonify(ok=False, error="phone_last4 required"), 400

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Resolve last-4 to full phone number, scoped to this creator
            cur.execute("""
                SELECT phone_number FROM messages
                WHERE RIGHT(phone_number, 4) = %s AND creator_slug = %s
                GROUP BY phone_number
                ORDER BY MAX(created_at) DESC
                LIMIT 1
            """, (phone_last4, _slug_fotw))
            row = cur.fetchone()
            if not row:
                conn.close()
                return jsonify(ok=False, error="Fan not found"), 404

            phone = row["phone_number"]

            # Upsert the pick for this week, scoped to this creator
            cur.execute("""
                INSERT INTO fan_of_the_week (phone_number, week_of, message_text, creator_slug)
                VALUES (%s, DATE_TRUNC('week', CURRENT_DATE)::date, %s, %s)
                ON CONFLICT (creator_slug, week_of) DO UPDATE
                    SET phone_number = EXCLUDED.phone_number,
                        message_text = EXCLUDED.message_text,
                        selected_at  = NOW()
            """, (phone, message_text, _slug_fotw))

            # Add 'fan_of_the_week' tag to the contact (avoid duplicates)
            cur.execute("""
                UPDATE contacts
                SET fan_tags = array_append(
                    COALESCE(fan_tags, '{}'),
                    'fan_of_the_week'
                )
                WHERE phone_number = %s AND creator_slug = %s
                  AND NOT ('fan_of_the_week' = ANY(COALESCE(fan_tags, '{}')))
            """, (phone, _slug_fotw))

        conn.commit()
        conn.close()
    except Exception:
        logger.exception("api: failed to save fan of the week selection")
        return jsonify(ok=False, error="DB error"), 500

    return jsonify(ok=True, phone_last4=phone_last4)


@api_bp.route("/api/fan-of-the-week/history")
@login_required
def fan_of_the_week_history():
    """
    Returns all past Fan of the Week picks, newest first.
    Scoped to the logged-in user's authorized creator_slug.
    """
    _require_performer_account()
    _slug = _slug_or_abort()
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    f.week_of,
                    f.message_text,
                    f.selected_at,
                    RIGHT(f.phone_number, 4)  AS phone_last4,
                    c.fan_tier,
                    c.fan_tags,
                    c.fan_location,
                    c.fan_name,
                    c.fan_score
                FROM fan_of_the_week f
                LEFT JOIN contacts c ON c.phone_number = f.phone_number AND c.creator_slug = f.creator_slug
                WHERE f.creator_slug = %s
                ORDER BY f.week_of DESC
                LIMIT 52
            """, (_slug,))
            rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("api: failed to fetch fan of the week history")
        return jsonify(history=[]), 500

    history = []
    for r in rows:
        tags = r["fan_tags"] or []
        history.append({
            "week_of": r["week_of"].isoformat(),
            "phone_last4": r["phone_last4"],
            "message_text": (r["message_text"] or "")[:200],
            "selected_at": r["selected_at"].isoformat() if r["selected_at"] else None,
            "fan_tier": r["fan_tier"],
            "fan_tags": tags[:5],
            "fan_location": r["fan_location"] or "",
            "fan_name": r["fan_name"] or "",
            "fan_score": r["fan_score"],
        })
    return jsonify(history=history)


# ── SMB Customer of the Week ──────────────────────────────────────────────────

_COTW_CANDIDATES_SQL = """
    WITH
    attendance AS (
        SELECT
            phone_number,
            COUNT(*)                                          AS shows_attended,
            MAX(checked_in_at)                                AS last_checkin,
            BOOL_OR(checked_in_at >= NOW() - INTERVAL '30 days') AS attended_recently
        FROM smb_show_checkins
        WHERE tenant_slug = %s
        GROUP BY phone_number
    ),
    engagement AS (
        SELECT phone_number, COUNT(*) AS msg_count
        FROM smb_messages
        WHERE tenant_slug = %s
          AND created_at >= NOW() - INTERVAL '30 days'
          AND role = 'user'
        GROUP BY phone_number
    )
    SELECT
        s.phone_number,
        RIGHT(s.phone_number, 4)                             AS phone_last4,
        COALESCE(a.shows_attended, 0)                        AS shows_attended,
        COALESCE(a.last_checkin, s.created_at)               AS last_active,
        s.created_at                                         AS subscribed_at,
        COALESCE(e.msg_count, 0)                             AS recent_msgs,
        (
            LEAST(COALESCE(a.shows_attended, 0) * 10, 50)
          + CASE WHEN s.created_at <= NOW() - INTERVAL '90 days' THEN 20 ELSE 0 END
          + CASE WHEN COALESCE(a.attended_recently, false) THEN 15 ELSE 0 END
          + LEAST(COALESCE(e.msg_count, 0) * 3, 15)
          + RANDOM() * 5
        )                                                    AS candidate_score
    FROM smb_subscribers s
    LEFT JOIN attendance a  ON a.phone_number = s.phone_number
    LEFT JOIN engagement e  ON e.phone_number = s.phone_number
    WHERE s.tenant_slug = %s
      AND s.status = 'active'
      AND s.phone_number NOT IN (
          SELECT phone_number FROM smb_customer_of_the_week
          WHERE tenant_slug = %s
            AND week_of >= CURRENT_DATE - INTERVAL '8 weeks'
      )
    ORDER BY candidate_score DESC
    LIMIT 5
"""


def _ensure_slug_authorized(slug: str):
    """
    Abort 403 unless the current user is authorized for this tenant slug.

    Used by all /api/smb/<slug>/... endpoints — without this check the slug
    in the URL is trusted, which would let any logged-in business owner read
    or mutate another tenant's Customer of the Week records.
    """
    from flask import abort
    user = current_user()
    if not user:
        abort(401)
    if user.get("is_super_admin"):
        return
    own_slug = user.get("creator_slug") or ""
    authorized = get_authorized_slugs(user.get("id"), own_slug)
    if slug not in authorized:
        abort(403)


@api_bp.route("/api/smb/<slug>/customer-of-the-week")
@login_required
def smb_customer_of_the_week(slug: str):
    """
    Returns this week's saved Customer of the Week for an SMB tenant,
    or falls back to the top dynamic candidate.
    """
    _ensure_slug_authorized(slug)
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT RIGHT(phone_number, 4) AS phone_last4, message_text,
                       week_of, selected_at, shows_attended
                FROM smb_customer_of_the_week
                WHERE tenant_slug = %s
                  AND week_of = DATE_TRUNC('week', CURRENT_DATE)::date
                LIMIT 1
            """, (slug,))
            saved = cur.fetchone()
            if saved:
                conn.close()
                return jsonify(
                    found=True, saved=True,
                    phone_last4=saved["phone_last4"],
                    message_text=saved["message_text"] or "",
                    week_of=saved["week_of"].isoformat(),
                    selected_at=saved["selected_at"].isoformat(),
                    shows_attended=saved["shows_attended"],
                )
            cur.execute(_COTW_CANDIDATES_SQL, (slug, slug, slug, slug))
            row = cur.fetchone()
        conn.close()
    except Exception:
        logger.exception("api: smb_customer_of_the_week failed for %s", slug)
        return jsonify(found=False), 500

    if not row:
        return jsonify(found=False)

    return jsonify(
        found=True, saved=False,
        phone_last4=row["phone_last4"],
        shows_attended=row["shows_attended"],
        last_active=row["last_active"].isoformat() if row["last_active"] else None,
        subscribed_at=row["subscribed_at"].isoformat() if row["subscribed_at"] else None,
        recent_msgs=row["recent_msgs"],
    )


@api_bp.route("/api/smb/<slug>/customer-of-the-week/candidates")
@login_required
def smb_customer_of_the_week_candidates(slug: str):
    """Top 5 Customer of the Week candidates for an SMB tenant."""
    _ensure_slug_authorized(slug)
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(_COTW_CANDIDATES_SQL, (slug, slug, slug, slug))
            rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("api: smb_cotw_candidates failed for %s", slug)
        return jsonify(candidates=[]), 500

    return jsonify(candidates=[
        {
            "phone_last4": r["phone_last4"],
            "shows_attended": r["shows_attended"],
            "last_active": r["last_active"].isoformat() if r["last_active"] else None,
            "subscribed_at": r["subscribed_at"].isoformat() if r["subscribed_at"] else None,
            "recent_msgs": r["recent_msgs"],
        }
        for r in rows
    ])


@api_bp.route("/api/smb/<slug>/customer-of-the-week/select", methods=["POST"])
@login_required
def smb_customer_of_the_week_select(slug: str):
    """
    Save the chosen Customer of the Week for the current week.
    Body: { "phone_last4": "1234", "message_text": "..." }
    """
    _ensure_slug_authorized(slug)
    import psycopg2.extras
    data = request.get_json(silent=True) or {}
    phone_last4 = (data.get("phone_last4") or "").strip()
    message_text = (data.get("message_text") or "").strip()[:500]

    if not phone_last4:
        return jsonify(ok=False, error="phone_last4 required"), 400

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT phone_number FROM smb_subscribers
                WHERE tenant_slug = %s AND RIGHT(phone_number, 4) = %s
                ORDER BY created_at DESC LIMIT 1
            """, (slug, phone_last4))
            row = cur.fetchone()
            if not row:
                conn.close()
                return jsonify(ok=False, error="Customer not found"), 404
            phone = row["phone_number"]

            cur.execute("""
                SELECT COUNT(*) AS cnt FROM smb_show_checkins
                WHERE tenant_slug = %s AND phone_number = %s
            """, (slug, phone))
            shows_attended = cur.fetchone()["cnt"]

            cur.execute("""
                INSERT INTO smb_customer_of_the_week
                    (tenant_slug, phone_number, week_of, message_text, shows_attended)
                VALUES (%s, %s, DATE_TRUNC('week', CURRENT_DATE)::date, %s, %s)
                ON CONFLICT (tenant_slug, week_of) DO UPDATE
                    SET phone_number   = EXCLUDED.phone_number,
                        message_text   = EXCLUDED.message_text,
                        shows_attended = EXCLUDED.shows_attended,
                        selected_at    = NOW()
            """, (slug, phone, message_text, shows_attended))
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("api: smb_cotw_select failed for %s", slug)
        return jsonify(ok=False, error="DB error"), 500

    return jsonify(ok=True, phone_last4=phone_last4)


@api_bp.route("/api/smb/<slug>/customer-of-the-week/history")
@login_required
def smb_customer_of_the_week_history(slug: str):
    """Returns last 52 weeks of Customer of the Week picks for an SMB tenant."""
    _ensure_slug_authorized(slug)
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT RIGHT(phone_number, 4) AS phone_last4,
                       week_of, selected_at, message_text, shows_attended
                FROM smb_customer_of_the_week
                WHERE tenant_slug = %s
                ORDER BY week_of DESC
                LIMIT 52
            """, (slug,))
            rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("api: smb_cotw_history failed for %s", slug)
        return jsonify(history=[]), 500

    return jsonify(history=[
        {
            "phone_last4": r["phone_last4"],
            "week_of": r["week_of"].isoformat(),
            "selected_at": r["selected_at"].isoformat() if r["selected_at"] else None,
            "message_text": (r["message_text"] or "")[:200],
            "shows_attended": r["shows_attended"],
        }
        for r in rows
    ])


# ── Bot Data ──────────────────────────────────────────────────────────────────

def _resolve_bot_context() -> tuple[str | None, str, int | None]:
    """
    Resolve (creator_slug, account_type, http_error_or_None) for /api/bot-data.

    Honors viewing_as (header-first, then session) so that:
      - Super-admins see/edit the project they've selected via the project picker.
      - Team members see/edit the tenant they're switched into.
      - Regular users get their own creator_slug + account_type.
    """
    user = current_user()
    if not user:
        return (None, "performer", 401)

    own_slug = user.get("creator_slug") or ""
    own_type = user.get("account_type") or "performer"

    va_slug, va_type = _get_viewing_as()
    if user.get("is_super_admin") and va_slug:
        return (va_slug, va_type or "performer", None)

    slug, err = resolve_slug()
    if err is not None:
        return (slug or None, own_type, err)
    if not slug:
        return (None, own_type, None)

    if slug == own_slug:
        return (slug, own_type, None)

    # Team-member viewing-as another tenant — look up that tenant's account_type.
    account_type = session.get("viewing_as_account_type")
    if not account_type:
        try:
            dbc = get_conn()
            with dbc.cursor() as cur:
                cur.execute(
                    "SELECT account_type FROM operator_users "
                    "WHERE creator_slug=%s AND is_active=TRUE LIMIT 1",
                    (slug,),
                )
                row = cur.fetchone()
                account_type = (row[0] if row else None) or "performer"
            dbc.close()
        except Exception:
            logger.exception("_resolve_bot_context: lookup failed for slug=%s", slug)
            account_type = "performer"
    return (slug, account_type, None)


def _load_performer_config_from_db(slug: str) -> dict | None:
    """
    Load a performer's bot config from bot_configs.config_json.
    Returns the parsed dict, or None if no row exists.
    """
    import psycopg2.extras
    try:
        dbc = get_conn()
        with dbc.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT config_json FROM bot_configs WHERE creator_slug=%s",
                (slug,),
            )
            row = cur.fetchone()
        dbc.close()
        return dict(row["config_json"]) if row and row["config_json"] else None
    except Exception:
        logger.exception("api: failed to load bot_configs for slug=%s", slug)
        return None


@api_bp.route("/api/bot-data")
@login_required
def bot_data():
    """
    Returns the current bot configuration for the logged-in user.

    Business accounts: merge file defaults with DB overrides from smb_bot_config.
    Performer accounts: prefer bot_configs DB row; fall back to creator_config JSON
                        for legacy installs (e.g. Zarna's hand-crafted file).
    """
    import json

    slug, account_type, err = _resolve_bot_context()
    if err == 401:
        return jsonify(authenticated=False, error="Login required"), 401
    if err == 403:
        return jsonify(error="Not authorized for this account."), 403
    if not slug:
        return jsonify(error="Onboarding not complete — no bot configured yet."), 400

    if account_type == "business":
        config_path = _BUSINESS_CONFIGS_DIR / f"{slug}.json"
        try:
            with open(config_path) as f:
                cfg = json.load(f)
        except Exception:
            logger.warning("api: no file config for business slug=%s, using empty base", slug)
            cfg = {}

        # Merge DB overrides on top of file defaults so edits survive deploys
        import psycopg2.extras
        db_overrides = {}
        try:
            dbc = get_conn()
            with dbc.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT config_json FROM smb_bot_config WHERE tenant_slug=%s",
                    (slug,),
                )
                row = cur.fetchone()
                if row:
                    db_overrides = row["config_json"] or {}
            dbc.close()
        except Exception:
            logger.exception("api: failed to load smb_bot_config for slug=%s", slug)

        def _get(key, default=""):
            return db_overrides.get(key, cfg.get(key, default))

        knowledge = cfg.get("knowledge_base", {})
        tracked_links = db_overrides.get("tracked_links", cfg.get("tracked_links", {}))

        return jsonify(
            display_name=_get("display_name", cfg.get("display_name", "")),
            business_type=cfg.get("business_type", ""),
            location=cfg.get("location", ""),
            tone=_get("tone"),
            welcome_message=_get("welcome_message"),
            signup_question=_get("signup_question"),
            outreach_invite_message=_get("outreach_invite_message"),
            send_contact_card=db_overrides.get("send_contact_card", cfg.get("send_contact_card", True)),
            tracked_links=tracked_links,
            address=db_overrides.get("address", knowledge.get("address", "")),
            hours=db_overrides.get("hours", knowledge.get("hours", "")),
            website=_get("website"),
            logo_url=cfg.get("logo_url", ""),
            edits_used=0,
            edits_limit=20,
        )

    # ── Performer bot config ──
    # 1. Try DB first (self-serve accounts always have a bot_configs row)
    db_cfg = _load_performer_config_from_db(slug)
    if db_cfg is not None:
        links = db_cfg.get("links", {})
        return jsonify(
            name=db_cfg.get("name", db_cfg.get("display_name", "")),
            bio=db_cfg.get("bio", ""),
            description=db_cfg.get("description", ""),
            voice_style=db_cfg.get("voice_style", ""),
            tone=db_cfg.get("tone", "casual"),
            website_url=db_cfg.get("website_url", ""),
            podcast_url=db_cfg.get("podcast_url", ""),
            media_urls=db_cfg.get("media_urls", []),
            links={
                "tickets": links.get("tickets", ""),
                "merch": links.get("merch", ""),
                "book": links.get("book", ""),
                "youtube": links.get("youtube", ""),
            },
            banned_words=db_cfg.get("banned_words", []),
            name_variants=db_cfg.get("name_variants", []),
            edits_used=0,
            edits_limit=20,
        )

    # 2. Fall back to legacy file-based config (e.g. Zarna's hand-crafted JSON)
    config_path = Path(__file__).parents[3] / "creator_config" / f"{slug}.json"
    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except Exception:
        logger.exception("api: no DB row or file config for performer slug=%s", slug)
        return jsonify(error="Config not found"), 404

    links = cfg.get("links", {})
    return jsonify(
        name=cfg.get("name", ""),
        bio=cfg.get("bio", ""),
        description=cfg.get("description", ""),
        voice_style=cfg.get("voice_style", ""),
        tone=cfg.get("tone", "casual"),
        website_url=cfg.get("website_url", cfg.get("links", {}).get("website", "")),
        podcast_url=cfg.get("podcast_url", ""),
        media_urls=cfg.get("media_urls", []),
        links={
            "tickets": links.get("tickets", ""),
            "merch": links.get("merch", ""),
            "book": links.get("book", ""),
            "youtube": links.get("youtube", ""),
        },
        banned_words=cfg.get("banned_words", []),
        name_variants=cfg.get("name_variants", []),
        edits_used=0,
        edits_limit=20,
    )


@api_bp.route("/api/bot-data", methods=["POST"])
@login_required
def save_bot_data():
    """
    Save editable bot config fields. Persists to DB so changes survive deploys.

    Business accounts: upserts into smb_bot_config.
    Performer accounts: upserts into bot_configs.config_json.
    """
    import json
    import psycopg2.extras

    user = current_user()
    slug, account_type, err = _resolve_bot_context()
    if err == 401:
        return jsonify(authenticated=False, error="Login required"), 401
    if err == 403:
        return jsonify(error="Not authorized for this account."), 403
    if not slug:
        return jsonify(error="Onboarding not complete — no bot configured yet."), 400

    data = request.get_json(silent=True) or {}

    if account_type == "business":
        allowed = {
            "tone", "welcome_message", "signup_question",
            "outreach_invite_message", "address", "hours",
            "website", "tracked_links", "display_name",
            "logo_url", "send_contact_card",
        }
        updates = {k: v for k, v in data.items() if k in allowed}
        if not updates:
            return jsonify(error="No valid fields provided"), 400

        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO smb_bot_config (tenant_slug, config_json, updated_at)
                           VALUES (%s, %s::jsonb, NOW())
                           ON CONFLICT (tenant_slug) DO UPDATE
                           SET config_json = smb_bot_config.config_json || %s::jsonb,
                               updated_at  = NOW()""",
                        (slug, json.dumps(updates), json.dumps(updates)),
                    )
            return jsonify(success=True)
        except Exception:
            logger.exception("save_bot_data: business failed for slug=%s", slug)
            return jsonify(error="Failed to save config"), 500
        finally:
            conn.close()

    # ── Performer save ──
    allowed_performer = {
        "name", "bio", "description", "tone", "voice_style",
        "website_url", "podcast_url", "media_urls", "banned_words", "links",
    }
    updates = {k: v for k, v in data.items() if k in allowed_performer}
    if not updates:
        return jsonify(error="No valid fields provided"), 400

    # When a super-admin / team member is viewing-as a different tenant,
    # attribute a brand-new bot_configs INSERT to that tenant's actual owner
    # (not the impersonator). For existing rows, ON CONFLICT only updates
    # config_json, so this only matters on first save.
    owner_user_id = user["id"]
    if slug != (user.get("creator_slug") or ""):
        try:
            dbc2 = get_conn()
            with dbc2.cursor() as cur:
                cur.execute(
                    "SELECT id FROM operator_users "
                    "WHERE creator_slug=%s AND is_active=TRUE "
                    "ORDER BY id LIMIT 1",
                    (slug,),
                )
                row = cur.fetchone()
                if row:
                    owner_user_id = row[0]
            dbc2.close()
        except Exception:
            logger.exception("save_bot_data: owner lookup failed for slug=%s", slug)

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO bot_configs
                           (operator_user_id, creator_slug, account_type, config_json, status)
                       VALUES (%s, %s, 'performer', %s::jsonb, 'active')
                       ON CONFLICT (creator_slug) DO UPDATE
                       SET config_json = bot_configs.config_json || %s::jsonb,
                           updated_at  = NOW()""",
                    (owner_user_id, slug, json.dumps(updates), json.dumps(updates)),
                )
        return jsonify(success=True)
    except Exception:
        logger.exception("save_bot_data: performer failed for slug=%s", slug)
        return jsonify(error="Failed to save config"), 500
    finally:
        conn.close()


# ── User ──────────────────────────────────────────────────────────────────────

@api_bp.route("/api/user")
@login_required
def user_info():
    """Returns the current logged-in user's info including account_type."""
    user = current_user()
    return jsonify(
        email=user["email"],
        name=user["name"],
        is_owner=user["is_owner"],
        account_type=user.get("account_type") or "performer",
        creator_slug=user.get("creator_slug") or "",
        is_super_admin=bool(user.get("is_super_admin")),
    )


@api_bp.route("/api/user", methods=["PATCH"])
@login_required
def update_user():
    """
    Update display name and/or email for the current user.
    Body: { "name": "...", "email": "..." }  (either or both)
    """
    data = request.get_json(silent=True) or {}
    new_name  = (data.get("name") or "").strip()
    new_email = (data.get("email") or "").strip().lower()

    if not new_name and not new_email:
        return jsonify(error="Provide name or email to update"), 400
    if new_email and "@" not in new_email:
        return jsonify(error="Invalid email address"), 400

    user = current_user()
    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if new_email and new_email != user["email"]:
                cur.execute("SELECT id FROM operator_users WHERE email=%s AND id!=%s",
                            (new_email, user["id"]))
                if cur.fetchone():
                    return jsonify(error="That email is already in use"), 409

        updates = []
        params  = []
        if new_name:
            updates.append("name=%s")
            params.append(new_name)
        if new_email:
            updates.append("email=%s")
            params.append(new_email)
        params.append(user["id"])

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE operator_users SET {', '.join(updates)} WHERE id=%s",
                    params,
                )

        # Refresh session with updated values
        from flask import session as flask_session
        if new_name:
            flask_session["user_name"] = new_name
        if new_email:
            flask_session["user_email"] = new_email

        return jsonify(success=True,
                       name=new_name or user["name"],
                       email=new_email or user["email"])
    except Exception:
        logger.exception("update_user: failed for user_id=%s", user["id"])
        return jsonify(error="Update failed"), 500
    finally:
        conn.close()


@api_bp.route("/api/billing/status")
@login_required
def billing_status():
    """
    Billing + credit summary for the current user.

    Reads from operator_credit_usage (period totals) + operator_users (plan tier)
    + Stripe price IDs (booster catalog). See billing/credits.py for the real
    consumption logic — this endpoint is read-only.

    Returns:
      plan_name, plan_label, is_trial,
      credits_used, credits_total, credits_included, credits_remaining,
      credits_warning, overage_credits,
      period_start, period_end,
      boosters (static catalog),
      replies_this_month, blasts_this_month, fans_reached_this_month
    """
    import psycopg2.extras
    from datetime import date

    from ..billing.credits import get_credit_status
    from ..billing.plans import BOOSTERS, ALL_PLANS

    user = current_user()
    slug = user.get("creator_slug") or ""
    account_type = user.get("account_type") or "performer"
    month = date.today().strftime("%Y-%m")

    # Fetch billing fields not included in current_user() to keep that query light.
    stripe_customer_id = None
    billing_cycle_db = None
    try:
        _sc_conn = get_conn()
        with _sc_conn.cursor() as _cur:
            _cur.execute(
                "SELECT stripe_customer_id, billing_cycle FROM operator_users WHERE id=%s",
                (user["id"],),
            )
            _scr = _cur.fetchone()
            if _scr:
                stripe_customer_id = _scr[0]
                billing_cycle_db = _scr[1]
        _sc_conn.close()
    except Exception:
        pass

    # Team members should inherit the tenant owner's plan — an invited user on
    # Zarna (grandfathered/unlimited) must NOT see the "Free Trial" banner just
    # because their personal operator_users row still defaults to trial. When a
    # creator_slug is available we always look the owner up by slug; otherwise
    # we fall back to the user's own id.
    if slug:
        status = get_credit_status(slug=slug)
    else:
        status = get_credit_status(user_id=user["id"])
    plan_tier = status.get("plan_tier") or "trial"
    plan = ALL_PLANS.get(plan_tier)
    if status.get("unlimited"):
        plan_label = "Unlimited"
    elif plan:
        plan_label = plan.label
    elif plan_tier == "trial":
        plan_label = "Free Trial"
    else:
        plan_label = plan_tier.replace("_", " ").title()

    # Static booster catalog — order + prices from plans.py
    boosters = [
        {
            "key": b.key,
            "credits": b.credits,
            "price_usd": b.price_usd,
            "label": b.label,
        }
        for b in BOOSTERS.values()
    ]

    # Activity breakdown (AI replies + blasts) — approximate, month-to-date.
    # Kept separate from credit totals so UI can show "where credits went".
    replies_this_month = 0
    blasts_this_month = 0
    fans_reached_count = 0
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if account_type == "performer" and slug:
                cur.execute(
                    """SELECT COUNT(*) AS cnt
                       FROM messages m
                       JOIN contacts c ON c.phone_number = m.phone_number
                       WHERE c.creator_slug = %s
                         AND m.role = 'assistant'
                         AND m.created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
                r = cur.fetchone()
                replies_this_month = int(r["cnt"]) if r else 0

                # Scope monthly activity to the whole project (every team
                # member's blasts count toward the project's totals) rather
                # than the signed-in user's personal outbox.
                cur.execute(
                    """SELECT COUNT(*) AS blasts,
                              COALESCE(SUM(sent_count), 0) AS fans_reached
                       FROM   blast_drafts
                       WHERE  status = 'sent'
                         AND  sent_at >= DATE_TRUNC('month', NOW())
                         AND  creator_slug = %s""",
                    (slug,),
                )
                b = cur.fetchone()
                if b:
                    blasts_this_month = int(b["blasts"] or 0)
                    fans_reached_count = int(b["fans_reached"] or 0)
            elif account_type == "business" and slug:
                cur.execute(
                    """SELECT COUNT(*) AS cnt FROM smb_messages
                       WHERE tenant_slug=%s AND role='assistant'
                         AND created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
                r = cur.fetchone()
                replies_this_month = int(r["cnt"]) if r else 0
        conn.close()
    except Exception:
        logger.warning("billing_status: activity breakdown failed for slug=%s", slug, exc_info=True)

    try:
        return jsonify(
            slug=slug,
            month=month,
            plan_name=plan_tier,
            plan_label=plan_label,
            billing_cycle=billing_cycle_db or (plan and "monthly") or None,
            is_trial=status.get("is_trial", False),
            unlimited=status.get("unlimited", False),
            stripe_customer_id=stripe_customer_id,
            credits_used=status.get("used", 0),
            credits_total=status.get("total", 0),
            credits_included=status.get("included", 0),
            credits_remaining=status.get("remaining", 0),
            credits_warning=status.get("warning"),
            overage_credits=status.get("overage", 0),
            boosters_purchased=status.get("boosters_purchased", 0),
            period_start=status.get("period_start"),
            period_end=status.get("period_end"),
            boosters=boosters,
            replies_this_month=replies_this_month,
            blasts_this_month=blasts_this_month,
            fans_reached_this_month=fans_reached_count,
        )
    except Exception:
        logger.exception("billing_status: response build failed for slug=%s", slug)
        return jsonify(error="internal error"), 500


# ── Business (multi-tenant SMB) ────────────────────────────────────────────────

def _get_tenant_slug() -> str | None:
    """
    Returns the effective tenant_slug for the current request.
    Delegates to resolve_slug() so all authorization checks (own slug +
    team membership) are applied consistently.
    """
    slug, err = resolve_slug()
    if err:
        return None
    return slug or None


# Module-level cache: slug → pre-built vCard string.
# Built once on first request per process; Twilio retries and subsequent
# opt-ins return instantly without re-downloading or re-processing the logo.
_vcard_cache: dict[str, str] = {}


def _build_vcard(slug: str) -> str | None:
    """
    Build the full vCard text for a slug, embedding the logo as base64.
    Returns None if the config doesn't exist.
    Downloads and processes the logo only once per process lifetime.
    """
    import base64, io, json

    config_path = _BUSINESS_CONFIGS_DIR / f"{slug}.json"
    if not config_path.exists():
        return None

    try:
        cfg = json.loads(config_path.read_text())
    except Exception:
        return None

    display_name = cfg.get("display_name") or slug
    logo_url = cfg.get("logo_url") or ""
    sms_number = cfg.get("sms_number") or ""

    lines = [
        "BEGIN:VCARD",
        "VERSION:3.0",
        f"FN:{display_name}",
        "N:;;;;",
        f"ORG:{display_name}",
    ]
    if sms_number:
        lines.append(f"TEL;TYPE=CELL:{sms_number}")

    if logo_url:
        try:
            from PIL import Image
            import urllib.request
            req = urllib.request.Request(
                logo_url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; ZarBotVCard/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                img_bytes = resp.read()
            img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            w, h = img.size
            side = min(w, h)
            img = img.crop(((w - side) // 2, (h - side) // 2,
                             (w + side) // 2, (h + side) // 2))
            img = img.resize((300, 300), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=85)
            b64 = base64.b64encode(buf.getvalue()).decode("ascii")
            lines.append(f"PHOTO;TYPE=JPEG;ENCODING=BASE64:{b64}")
        except Exception:
            logger.warning("operator vCard: failed to embed logo for slug=%s", slug, exc_info=True)

    lines.append("END:VCARD")
    return "\r\n".join(lines) + "\r\n"


@api_bp.route("/smb/vcard/<slug>.vcf", methods=["GET"])
def operator_smb_vcard(slug: str):
    """
    Serve a vCard (.vcf) for a business tenant so subscribers can save the
    contact with one tap. No auth required — Twilio/iOS fetches this URL
    directly when displaying the MMS.

    The vCard is built once and cached in _vcard_cache so Twilio's fetch
    (and any retries) return in microseconds instead of re-downloading and
    re-processing the logo image on every request.
    """
    from flask import Response as FlaskResponse

    if slug not in _vcard_cache:
        vcf = _build_vcard(slug)
        if vcf is None:
            return ("Not found", 404)
        _vcard_cache[slug] = vcf

    return FlaskResponse(
        _vcard_cache[slug],
        mimetype="text/vcard",
        headers={"Content-Disposition": f'attachment; filename="{slug}.vcf"'},
    )


@api_bp.route("/api/business/stats")
@login_required
def business_stats():
    """Dashboard stats for a business account from smb_* tables."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug=%s", (slug,))
            total_subscribers = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug=%s AND status='active'",
                (slug,),
            )
            active_subscribers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM smb_messages WHERE tenant_slug=%s", (slug,))
            total_messages = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM smb_messages WHERE tenant_slug=%s AND role='user'",
                (slug,),
            )
            inbound_messages = cur.fetchone()[0]

            cur.execute(
                """SELECT COUNT(*) FROM smb_messages
                   WHERE tenant_slug=%s AND created_at >= NOW() - INTERVAL '7 days'""",
                (slug,),
            )
            messages_week = cur.fetchone()[0]

            cur.execute(
                """SELECT COUNT(*) FROM smb_messages
                   WHERE tenant_slug=%s AND DATE(created_at) = CURRENT_DATE""",
                (slug,),
            )
            messages_today = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM smb_blasts WHERE tenant_slug=%s", (slug,))
            total_blasts = cur.fetchone()[0]

            cur.execute(
                """SELECT DATE(created_at) AS day, COUNT(*) AS cnt
                   FROM smb_messages
                   WHERE tenant_slug=%s AND created_at >= NOW() - INTERVAL '7 days'
                   GROUP BY day ORDER BY day""",
                (slug,),
            )
            messages_by_day = [
                {"date": str(r["day"]), "count": r["cnt"]} for r in cur.fetchall()
            ]

        import os as _os
        env_key = f"SMB_{slug.upper()}_SMS_NUMBER"
        sms_number = _os.getenv(env_key, "")

        return jsonify(
            total_subscribers=total_subscribers,
            active_subscribers=active_subscribers,
            total_messages=total_messages,
            inbound_messages=inbound_messages,
            messages_today=messages_today,
            messages_week=messages_week,
            total_blasts=total_blasts,
            messages_by_day=messages_by_day,
            sms_number=sms_number,
        )
    finally:
        conn.close()


@api_bp.route("/api/business/inbox")
@login_required
def business_inbox():
    """Latest conversations for the business tenant, grouped by subscriber."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT DISTINCT ON (phone_number)
                       phone_number, body AS last_message, role AS last_role, created_at
                   FROM smb_messages
                   WHERE tenant_slug=%s
                   ORDER BY phone_number, created_at DESC""",
                (slug,),
            )
            latest_rows = cur.fetchall()

            threads = []
            for r in latest_rows:
                phone = r["phone_number"]

                cur.execute(
                    """SELECT status, created_at AS joined_at
                       FROM smb_subscribers
                       WHERE tenant_slug=%s AND phone_number=%s""",
                    (slug, phone),
                )
                sub = cur.fetchone()

                cur.execute(
                    "SELECT COUNT(*) FROM smb_messages WHERE tenant_slug=%s AND phone_number=%s",
                    (slug, phone),
                )
                msg_count = cur.fetchone()[0]

                cur.execute(
                    "SELECT MIN(created_at) FROM smb_messages WHERE tenant_slug=%s AND phone_number=%s",
                    (slug, phone),
                )
                first_msg_at = cur.fetchone()[0]

                # Never send the full E.164 to the browser — last 4 is all
                # the UI uses, and exposing the full number means a stolen
                # session leaks every customer's phone number to attackers.
                threads.append({
                    "phone_last4": phone[-4:],
                    "last_message": r["last_message"] or "",
                    "last_role": r["last_role"],
                    "last_message_at": r["created_at"].isoformat(),
                    "message_count": msg_count,
                    "first_message_at": first_msg_at.isoformat() if first_msg_at else None,
                    "status": sub["status"] if sub else "unknown",
                    "joined_at": sub["joined_at"].isoformat() if sub and sub["joined_at"] else None,
                })

            threads.sort(key=lambda x: x["last_message_at"], reverse=True)
            return jsonify(threads=threads)
    finally:
        conn.close()


@api_bp.route("/api/business/inbox/<phone_last4>/thread")
@login_required
def business_inbox_thread(phone_last4):
    """Full conversation thread for a business subscriber."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT phone_number FROM smb_subscribers
                   WHERE tenant_slug=%s AND phone_number LIKE %s LIMIT 1""",
                (slug, f"%{phone_last4}"),
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    """SELECT DISTINCT phone_number FROM smb_messages
                       WHERE tenant_slug=%s AND phone_number LIKE %s LIMIT 1""",
                    (slug, f"%{phone_last4}"),
                )
                row = cur.fetchone()
            if not row:
                return jsonify(error="Subscriber not found"), 404
            phone = row["phone_number"]

            cur.execute(
                """SELECT body, role, created_at FROM smb_messages
                   WHERE tenant_slug=%s AND phone_number=%s
                   ORDER BY created_at ASC""",
                (slug, phone),
            )
            messages = [
                {"role": r["role"], "text": r["body"], "created_at": r["created_at"].isoformat()}
                for r in cur.fetchall()
            ]

            cur.execute(
                """SELECT status, onboarding_step, created_at AS joined_at
                   FROM smb_subscribers
                   WHERE tenant_slug=%s AND phone_number=%s""",
                (slug, phone),
            )
            sub = cur.fetchone()

            prefs = {}
            if sub:
                cur.execute(
                    """SELECT p.question_key, p.answer
                       FROM smb_preferences p
                       JOIN smb_subscribers s ON s.id = p.subscriber_id
                       WHERE s.tenant_slug=%s AND s.phone_number=%s""",
                    (slug, phone),
                )
                prefs = {r["question_key"]: r["answer"] for r in cur.fetchall()}

            profile = {
                "phone_last4": phone[-4:],
                "status": sub["status"] if sub else "unknown",
                "joined_at": sub["joined_at"].isoformat() if sub and sub["joined_at"] else None,
                "preferences": prefs,
            }

            return jsonify(messages=messages, profile=profile)
    finally:
        conn.close()


@api_bp.route("/api/business/inbox/<phone_last4>/send", methods=["POST"])
@login_required
def business_inbox_send(phone_last4):
    """Send a manual message to a business subscriber via the tenant's Twilio number."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    data = request.get_json(silent=True) or {}
    text = (data.get("message") or "").strip()
    if not text:
        return jsonify(error="Message text is required"), 400

    conn = get_conn()
    try:
        import os, psycopg2.extras
        from twilio.rest import Client as TwilioClient

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT phone_number FROM smb_subscribers
                   WHERE tenant_slug=%s AND phone_number LIKE %s LIMIT 1""",
                (slug, f"%{phone_last4}"),
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    """SELECT DISTINCT phone_number FROM smb_messages
                       WHERE tenant_slug=%s AND phone_number LIKE %s LIMIT 1""",
                    (slug, f"%{phone_last4}"),
                )
                row = cur.fetchone()
            if not row:
                return jsonify(error="Subscriber not found"), 404
            phone = row["phone_number"]

        slug_upper = slug.upper()
        from_number = os.getenv(f"SMB_{slug_upper}_SMS_NUMBER")
        if not from_number:
            return jsonify(error="SMS number not configured for this account"), 500

        twilio_client = TwilioClient(
            os.getenv("TWILIO_ACCOUNT_SID"),
            os.getenv("TWILIO_AUTH_TOKEN"),
        )
        msg = twilio_client.messages.create(body=text, from_=from_number, to=phone)

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO smb_messages (tenant_slug, phone_number, role, body, created_at)
                       VALUES (%s, %s, 'assistant', %s, NOW())""",
                    (slug, phone, text),
                )

        return jsonify(success=True, sid=msg.sid)
    except Exception:
        logger.exception("business_inbox_send: failed tenant=%s phone=%s", slug, phone_last4)
        return jsonify(error="Failed to send message"), 500
    finally:
        conn.close()


@api_bp.route("/api/business/promos")
@login_required
def business_promos():
    """List past promotional blasts and outreach invite campaigns for this business tenant."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Regular blasts
            cur.execute(
                """SELECT id, owner_message, body, attempted, succeeded, sent_at, segment
                   FROM smb_blasts
                   WHERE tenant_slug=%s
                   ORDER BY sent_at DESC LIMIT 50""",
                (slug,),
            )
            promos = [
                {
                    "id": f"blast-{r['id']}",
                    "type": "blast",
                    "message": r["owner_message"] or r["body"],
                    "sent_body": r["body"],
                    "attempted": r["attempted"],
                    "succeeded": r["succeeded"],
                    "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
                    "segment": r["segment"],
                }
                for r in cur.fetchall()
            ]

            # Outreach invite batches (grouped by batch_name)
            cur.execute(
                """SELECT
                       batch_name,
                       offer,
                       COUNT(*) AS attempted,
                       COUNT(claimed_at) AS claimed,
                       MIN(sent_at) AS sent_at
                   FROM smb_outreach_invites
                   WHERE tenant_slug=%s
                   GROUP BY batch_name, offer
                   ORDER BY sent_at DESC""",
                (slug,),
            )
            for r in cur.fetchall():
                promos.append({
                    "id": f"invite-{r['batch_name']}",
                    "type": "outreach_invite",
                    "message": f"Outreach invite — {r['offer'].replace('_', ' ')} offer",
                    "sent_body": None,
                    "attempted": r["attempted"],
                    "succeeded": r["attempted"],   # all were sent; claimed is tracked separately
                    "claimed": r["claimed"],
                    "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
                    "segment": r["batch_name"],
                })

            # Sort combined list by sent_at descending
            promos.sort(key=lambda x: x["sent_at"] or "", reverse=True)

        return jsonify(promos=promos)
    finally:
        conn.close()


@api_bp.route("/api/business/customers")
@login_required
def business_customers():
    """Subscriber tier/segment breakdown for the business customers page."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras, json
        from pathlib import Path

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug=%s AND status='active'",
                (slug,),
            )
            total_active = cur.fetchone()[0]

            # Load segment definitions from creator config
            try:
                cfg = json.loads((_BUSINESS_CONFIGS_DIR / f"{slug}.json").read_text())
                segments_def = cfg.get("segments", [])
            except Exception:
                segments_def = []

            # Count subscribers per segment
            tiers = []
            for seg in segments_def:
                name = seg.get("name", "")
                description = seg.get("description", "")
                question_key = seg.get("question_key", "")
                answers = seg.get("answers", [])
                if not question_key or not answers:
                    continue
                placeholders = ",".join(["%s"] * len(answers))
                cur.execute(
                    f"""SELECT COUNT(DISTINCT s.id)
                        FROM smb_subscribers s
                        JOIN smb_preferences p ON p.subscriber_id = s.id
                        WHERE s.tenant_slug=%s
                          AND p.question_key=%s
                          AND p.answer IN ({placeholders})""",
                    (slug, question_key, *answers),
                )
                count = cur.fetchone()[0]
                tiers.append({
                    "name": name,
                    "description": description,
                    "count": count,
                })

            # Always include an "All Subscribers" tier
            tiers.insert(0, {
                "name": "ALL",
                "description": "All active subscribers",
                "count": total_active,
            })

        return jsonify(tiers=tiers, total_subscribers=total_active)
    finally:
        conn.close()


@api_bp.route("/api/business/customer-of-week")
@login_required
def business_customer_of_week():
    """The most engaged subscriber in the last 30 days for this business tenant."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT
                       phone_number,
                       COUNT(*) FILTER (WHERE role='user') AS inbound_count,
                       MAX(created_at) AS last_seen,
                       MIN(created_at) AS first_seen
                   FROM smb_messages
                   WHERE tenant_slug=%s AND created_at >= NOW() - INTERVAL '30 days'
                   GROUP BY phone_number
                   ORDER BY inbound_count DESC LIMIT 1""",
                (slug,),
            )
            best = cur.fetchone()
            if not best:
                return jsonify(phone_last4=None, best_message=None, inbound_count=0)

            phone = best["phone_number"]

            # body_length_chars is added by ensure_smb_engagement_schema()
            # in the main app and is missing from older SMB databases. Order
            # by char_length(body) directly so this endpoint never crashes
            # on a fresh provision before the migration runs.
            cur.execute(
                """SELECT body FROM smb_messages
                   WHERE tenant_slug=%s AND phone_number=%s AND role='user'
                   ORDER BY char_length(body) DESC NULLS LAST LIMIT 1""",
                (slug, phone),
            )
            msg_row = cur.fetchone()

            cur.execute(
                "SELECT status, created_at FROM smb_subscribers WHERE tenant_slug=%s AND phone_number=%s",
                (slug, phone),
            )
            sub = cur.fetchone()

            return jsonify(
                phone_last4=phone[-4:],
                best_message=msg_row["body"] if msg_row else "",
                inbound_count=best["inbound_count"],
                first_seen=best["first_seen"].isoformat() if best["first_seen"] else None,
                last_seen=best["last_seen"].isoformat() if best["last_seen"] else None,
                status=sub["status"] if sub else "active",
            )
    finally:
        conn.close()


@api_bp.route("/api/business/blast/tier-counts")
@login_required
def business_blast_tier_counts():
    """
    Per-tier subscriber counts so the business blast composer can show how
    many fans would receive each tier-targeted promo (mirrors the performer
    /api/blasts/tier-counts shape so the React component can be reused).
    """
    from .. import business_blast as _bb

    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        tiers = _bb.compute_tier_counts(slug, conn)
    except Exception:
        logger.exception("business_blast_tier_counts: failed for slug=%s", slug)
        return jsonify(success=False, error="Failed to compute tier counts"), 500
    finally:
        conn.close()
    return jsonify(success=True, tiers=tiers)


@api_bp.route("/api/business/blast/smart-send-preview", methods=["POST"])
@login_required
def business_blast_smart_send_preview():
    """
    For each tier, return how many fans would actually receive this blast vs
    be suppressed by the cadence rule. Used by the business Smart Send card.
    """
    from .. import business_blast as _bb

    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        result = _bb.compute_smart_send_preview(slug, conn)
    except Exception:
        logger.exception("business_blast_smart_send_preview: failed for slug=%s", slug)
        return jsonify(success=False, error="Failed to compute Smart Send preview"), 500
    finally:
        conn.close()
    return jsonify(success=True, **result)


@api_bp.route("/api/business/blast/send", methods=["POST"])
@login_required
def business_blast_send():
    """
    Fire a promo blast for the business tenant.

    Body: {
      "message":    "...",                 # required
      "audience":   "all" | "tier:<tier>" | "smart-send"
                  | "segment:<NAME>" | "customer_of_the_week",
      "ai_cleanup": true | false           # default true
    }
    Sends in a background thread; returns immediately with the resolved
    recipient count and the AI-cleaned body so the UI can show what actually
    went out.
    """
    from .. import business_blast as _bb

    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    audience = (data.get("audience") or "all").strip()
    ai_cleanup = data.get("ai_cleanup", True)
    if isinstance(ai_cleanup, str):
        ai_cleanup = ai_cleanup.lower() not in ("false", "0", "no", "")

    if not message:
        return jsonify(error="Message is required"), 400

    try:
        result = _bb.send_blast(
            slug=slug,
            raw_message=message,
            audience=audience,
            ai_cleanup=bool(ai_cleanup),
            business_configs_dir=_BUSINESS_CONFIGS_DIR,
            get_conn=get_conn,
        )
    except _bb.UnknownAudience as exc:
        return jsonify(success=False, error=str(exc)), 400
    except Exception:
        logger.exception("business_blast_send: failed for slug=%s audience=%s", slug, audience)
        return jsonify(success=False, error="Failed to queue blast"), 500

    if not result.get("success"):
        # Helper returns success=False with a user-safe error string when the
        # audience is empty or the SMS number isn't configured.
        return jsonify(result), 400

    return jsonify(
        success=True,
        status=(
            f"Blast queued for {result['recipient_count']:,} "
            f"{result['audience_label']} subscriber"
            f"{'' if result['recipient_count'] == 1 else 's'}. "
            "You'll see it in Promos when it completes."
        ),
        recipient_count=result["recipient_count"],
        audience_label=result["audience_label"],
        ai_cleaned=result["ai_cleaned"],
        body_preview=result["body_preview"],
        blast_id=result["blast_id"],
    )


@api_bp.route("/api/business/blast/test", methods=["POST"])
@login_required
def business_blast_test():
    """
    Send a [TEST] copy of a promo message to a single phone number using
    the tenant's own SMS number. The message is prefixed with [TEST] so
    it is clearly distinguishable from a real blast.

    Body: { "message": "...", "test_phone": "+12125551234" }
    """
    import os
    import re

    slug = _get_tenant_slug()
    if not slug:
        return jsonify(success=False, error="No tenant configured for this account."), 400

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    test_phone = (data.get("test_phone") or "").strip()

    if not message:
        return jsonify(success=False, error="message is required."), 400
    if not test_phone:
        return jsonify(success=False, error="test_phone is required."), 400

    # Normalise to E.164 if the user typed a bare 10-digit US number
    digits_only = re.sub(r"\D", "", test_phone)
    if len(digits_only) == 10:
        test_phone = f"+1{digits_only}"
    elif len(digits_only) == 11 and digits_only.startswith("1"):
        test_phone = f"+{digits_only}"

    env_key = f"SMB_{slug.upper()}_SMS_NUMBER"
    from_number = os.getenv(env_key, "").strip()
    if not from_number:
        return jsonify(success=False, error=f"SMS number not configured ({env_key})."), 400

    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
    if not all([account_sid, auth_token]):
        return jsonify(success=False, error="Twilio credentials not configured."), 500

    body = f"[TEST] {message}"
    try:
        from twilio.rest import Client as TwilioClient
        client = TwilioClient(account_sid, auth_token)
        client.messages.create(body=body, from_=from_number, to=test_phone)
        logger.info(
            "business_blast_test: test sent to ...%s for slug=%s", test_phone[-4:], slug
        )
        return jsonify(success=True, sent_to=f"...{test_phone[-4:]}")
    except Exception as exc:
        logger.exception("business_blast_test: failed for slug=%s", slug)
        return jsonify(success=False, error=str(exc)), 500


@api_bp.route("/api/business/outreach/send", methods=["POST"])
@login_required
def business_outreach_send():
    """
    Send a cold outreach message to a list of raw phone numbers.

    Unlike /api/business/blast/send (which targets existing subscribers),
    this endpoint targets people who have NOT yet opted in. Used to convert
    existing customer lists into SMS subscribers.

    Compliance rules enforced server-side:
      - Skip any number already in smb_subscribers with status='stopped'
        (they previously opted out and must not be re-contacted).
      - Append a mandatory opt-in / opt-out footer so the message meets
        TCPA / CTIA A2P 10DLC requirements for cold outreach.
      - Log every send to smb_outreach_invites for auditing and claimed tracking.

    Body: {
      "phones":      ["2125551234", "+12125554567", ...],   // raw or E.164
      "message":     "West Side Comedy Club here! ...",     // custom or bot default
      "batch_name":  "April 2026 Email List"               // optional label
    }
    """
    import os
    import re
    import threading

    slug = _get_tenant_slug()
    if not slug:
        return jsonify(success=False, error="No tenant configured for this account."), 400

    data = request.get_json(silent=True) or {}
    raw_phones = data.get("phones") or []
    message = (data.get("message") or "").strip()
    batch_name = (data.get("batch_name") or "").strip() or None

    if not raw_phones:
        return jsonify(success=False, error="No phone numbers provided."), 400
    if not message:
        return jsonify(success=False, error="message is required."), 400
    if len(raw_phones) > 5000:
        return jsonify(success=False, error="Maximum 5,000 numbers per send."), 400

    # Normalise to E.164 US numbers, drop obvious non-numbers
    def _normalise(raw: str) -> str | None:
        digits = re.sub(r"\D", "", raw.strip())
        if len(digits) == 10:
            return f"+1{digits}"
        if len(digits) == 11 and digits.startswith("1"):
            return f"+{digits}"
        return None

    phones = list(dict.fromkeys(p for p in (_normalise(r) for r in raw_phones) if p))
    if not phones:
        return jsonify(success=False, error="No valid US phone numbers found."), 400

    env_key = f"SMB_{slug.upper()}_SMS_NUMBER"
    from_number = os.getenv(env_key, "").strip()
    if not from_number:
        return jsonify(success=False, error=f"SMS number not configured ({env_key})."), 400

    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    auth_token  = os.getenv("TWILIO_AUTH_TOKEN", "")
    if not all([account_sid, auth_token]):
        return jsonify(success=False, error="Twilio credentials not configured."), 500

    # Filter out numbers that previously opted out (STOP'd)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT phone_number FROM smb_subscribers
                   WHERE tenant_slug=%s AND status='stopped'""",
                (slug,),
            )
            stopped = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    phones_to_send = [p for p in phones if p not in stopped]
    skipped_stopped = len(phones) - len(phones_to_send)

    if not phones_to_send:
        return jsonify(
            success=False,
            error="All provided numbers have previously opted out.",
            skipped_stopped=skipped_stopped,
        ), 400

    # Append mandatory TCPA / CTIA cold-outreach footer (non-negotiable)
    footer = "Reply YES to join our text club. Reply STOP to end."
    full_body = f"{message}\n\n{footer}"

    result = {
        "success": True,
        "total_provided": len(phones),
        "skipped_stopped": skipped_stopped,
        "recipient_count": len(phones_to_send),
        "batch_name": batch_name,
    }

    def _dispatch():
        try:
            from twilio.rest import Client as TwilioClient
            client = TwilioClient(account_sid, auth_token)
        except Exception:
            logger.exception("business_outreach: failed to init Twilio client")
            return

        sent = 0
        dbc = get_conn()
        try:
            for phone in phones_to_send:
                try:
                    client.messages.create(body=full_body, from_=from_number, to=phone)
                    sent += 1
                    # Log to smb_outreach_invites — plain INSERT so each campaign
                    # creates its own row even if the number was contacted before.
                    # The old (tenant_slug, phone_number) unique constraint was
                    # dropped via migration; dedup for the free-ticket flow is now
                    # handled application-side in storage.upsert_outreach_invite().
                    with dbc:
                        with dbc.cursor() as cur:
                            cur.execute(
                                """INSERT INTO smb_outreach_invites
                                       (tenant_slug, phone_number, offer, sent_at, batch_name)
                                   VALUES (%s, %s, %s, NOW(), %s)""",
                                (slug, phone, "custom_outreach", batch_name),
                            )
                except Exception:
                    logger.warning("business_outreach: send to %s failed", phone[-4:])
        finally:
            dbc.close()

        logger.info(
            "business_outreach: sent %d/%d for slug=%s batch=%s",
            sent, len(phones_to_send), slug, batch_name,
        )

    threading.Thread(target=_dispatch, daemon=True).start()
    return jsonify(**result)


@api_bp.route("/api/business/blast/preview-count", methods=["POST"])
@login_required
def business_blast_preview_count():
    """
    Return how many subscribers would receive a blast for the given audience.
    Body: { "audience": "all|tier:<tier>|smart-send|segment:<NAME>|customer_of_the_week" }
    """
    from .. import business_blast as _bb

    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    data = request.get_json(silent=True) or {}
    audience = (data.get("audience") or "all").strip()

    try:
        count = _bb.preview_count(
            slug=slug,
            audience=audience,
            business_configs_dir=_BUSINESS_CONFIGS_DIR,
            get_conn=get_conn,
        )
    except _bb.UnknownAudience as exc:
        return jsonify(error=str(exc)), 400
    except Exception:
        logger.exception("business_blast_preview_count: failed for slug=%s audience=%s", slug, audience)
        return jsonify(error="Failed to preview audience"), 500

    return jsonify(count=count, audience=audience)


# ── Password change (all account types) ──────────────────────────────────────

@api_bp.route("/api/auth/change-password", methods=["POST"])
@login_required
def change_password():
    """
    Change the current user's password.
    Body: { "current_password": "...", "new_password": "..." }
    """
    from werkzeug.security import check_password_hash, generate_password_hash

    data = request.get_json(silent=True) or {}
    current_pw = data.get("current_password") or ""
    new_pw = data.get("new_password") or ""

    if not current_pw or not new_pw:
        return jsonify(error="Both current and new password are required"), 400
    if len(new_pw) < 8:
        return jsonify(error="New password must be at least 8 characters"), 400

    user = current_user()
    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT password_hash FROM operator_users WHERE id=%s",
                (user["id"],),
            )
            row = cur.fetchone()
            if not row:
                return jsonify(error="Account not found"), 404
            stored_hash = row["password_hash"] or ""
            # OAuth-only accounts (Google signups) have an empty password_hash
            # and must set a password via the forgot-password flow first.
            if not stored_hash:
                return jsonify(
                    error="This account signed in with Google. Use 'Forgot password' to set a password first.",
                ), 400
            if not check_password_hash(stored_hash, current_pw):
                return jsonify(error="Current password is incorrect"), 401

        new_hash = generate_password_hash(new_pw)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE operator_users SET password_hash=%s WHERE id=%s",
                    (new_hash, user["id"]),
                )
        return jsonify(success=True)
    except Exception:
        logger.exception("change_password: failed for user_id=%s", user["id"])
        return jsonify(error="Password change failed"), 500
    finally:
        conn.close()


# ── Delete endpoints ───────────────────────────────────────────────────────────

@api_bp.route("/api/blasts/<int:blast_id>", methods=["DELETE"])
@login_required
def delete_blast(blast_id):
    """Delete a blast draft. Refuses to delete blasts that are currently sending."""
    user = current_user()
    if not _user_owns_draft(blast_id, user):
        return jsonify(error="Blast not found"), 404
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM blast_drafts WHERE id=%s", (blast_id,))
            row = cur.fetchone()
            if not row:
                return jsonify(error="Blast not found"), 404
            if row[0] == "sending":
                return jsonify(error="Cannot delete a blast that is currently sending. Cancel it first."), 409
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM blast_drafts WHERE id=%s", (blast_id,))
        return jsonify(success=True)
    except Exception:
        logger.exception("delete_blast: failed for id=%s", blast_id)
        return jsonify(error="Failed to delete blast"), 500
    finally:
        conn.close()


@api_bp.route("/api/shows/<int:show_id>", methods=["DELETE"])
@login_required
def delete_show(show_id):
    """Delete a live show. Refuses to delete shows that are currently live."""
    user = current_user()
    if not _user_owns_show(show_id, user):
        return jsonify(error="Show not found"), 404
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM live_shows WHERE id=%s", (show_id,))
            row = cur.fetchone()
            if not row:
                return jsonify(error="Show not found"), 404
            if row[0] == "live":
                return jsonify(error="Cannot delete a show that is currently live. End it first."), 409
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM live_show_signups WHERE show_id=%s", (show_id,))
                cur.execute("DELETE FROM live_shows WHERE id=%s", (show_id,))
        return jsonify(success=True)
    except Exception:
        logger.exception("delete_show: failed for id=%s", show_id)
        return jsonify(error="Failed to delete show"), 500
    finally:
        conn.close()


@api_bp.route("/api/business/promos/<promo_id>/stats")
@login_required
def business_promo_stats(promo_id):
    """
    Detailed stats for a single promo.
    promo_id is a string: "blast-8" or "invite-wscc-blast-2"
    """
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            if promo_id.startswith("blast-"):
                blast_id = int(promo_id[6:])
                cur.execute(
                    """SELECT owner_message, body, attempted, succeeded, sent_at, segment
                       FROM smb_blasts WHERE id=%s AND tenant_slug=%s""",
                    (blast_id, slug),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify(error="Promo not found"), 404

                attempted = row["attempted"] or 0
                succeeded = row["succeeded"] or 0
                failed = attempted - succeeded

                # Reply rate must be scoped to actual recipients within a
                # reasonable response window — otherwise an unrelated DM
                # from a random subscriber the day after a blast inflates the
                # number. We prefer the per-recipient log added with
                # smb_blast_recipients; we fall back to the legacy "any
                # message after sent_at" query for blasts that pre-date the
                # migration so historical promos still show a number.
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT m.phone_number) AS cnt
                    FROM   smb_blast_recipients r
                    JOIN   smb_messages m
                           ON m.tenant_slug = r.tenant_slug
                          AND m.phone_number = r.phone_number
                          AND m.role = 'user'
                          AND m.created_at >  r.sent_at
                          AND m.created_at <= r.sent_at + INTERVAL '24 hours'
                    WHERE  r.blast_id = %s
                    """,
                    (blast_id,),
                )
                replies = cur.fetchone()["cnt"] or 0

                if replies == 0:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM smb_blast_recipients WHERE blast_id=%s",
                        (blast_id,),
                    )
                    if (cur.fetchone()["cnt"] or 0) == 0:
                        cur.execute(
                            """SELECT COUNT(DISTINCT phone_number) AS cnt
                               FROM smb_messages
                               WHERE tenant_slug=%s AND role='user'
                                 AND created_at >  %s
                                 AND created_at <= %s + INTERVAL '24 hours'""",
                            (slug, row["sent_at"], row["sent_at"]),
                        )
                        replies = cur.fetchone()["cnt"] or 0

                reply_rate = round(replies / succeeded * 100, 1) if succeeded else 0

                # Sample reply messages — same join, same window
                cur.execute(
                    """
                    SELECT m.body
                    FROM   smb_blast_recipients r
                    JOIN   smb_messages m
                           ON m.tenant_slug = r.tenant_slug
                          AND m.phone_number = r.phone_number
                          AND m.role = 'user'
                          AND m.created_at >  r.sent_at
                          AND m.created_at <= r.sent_at + INTERVAL '24 hours'
                    WHERE  r.blast_id = %s
                    ORDER  BY m.created_at ASC
                    LIMIT  5
                    """,
                    (blast_id,),
                )
                sample_replies = [r["body"] for r in cur.fetchall()]
                if not sample_replies:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM smb_blast_recipients WHERE blast_id=%s",
                        (blast_id,),
                    )
                    if (cur.fetchone()["cnt"] or 0) == 0:
                        cur.execute(
                            """SELECT body FROM smb_messages
                               WHERE tenant_slug=%s AND role='user'
                                 AND created_at >  %s
                                 AND created_at <= %s + INTERVAL '24 hours'
                               ORDER BY created_at ASC LIMIT 5""",
                            (slug, row["sent_at"], row["sent_at"]),
                        )
                        sample_replies = [r["body"] for r in cur.fetchall()]

                return jsonify(
                    type="blast",
                    message=row["owner_message"] or row["body"],
                    sent_at=row["sent_at"].isoformat() if row["sent_at"] else None,
                    segment=row["segment"],
                    attempted=attempted,
                    succeeded=succeeded,
                    failed=failed,
                    delivery_rate=round(succeeded / attempted * 100, 1) if attempted else 0,
                    replies=replies,
                    reply_rate=reply_rate,
                    sample_replies=sample_replies,
                )

            elif promo_id.startswith("invite-"):
                batch_name = promo_id[7:]
                cur.execute(
                    """SELECT
                           COUNT(*) AS total_sent,
                           COUNT(claimed_at) AS claimed,
                           MIN(sent_at) AS sent_at
                       FROM smb_outreach_invites
                       WHERE tenant_slug=%s AND batch_name=%s""",
                    (slug, batch_name),
                )
                row = cur.fetchone()
                if not row or not row["total_sent"]:
                    return jsonify(error="Promo not found"), 404

                total_sent = row["total_sent"]
                claimed = row["claimed"]
                sent_at = row["sent_at"]

                # Replies from invite recipients after the campaign
                cur.execute(
                    """SELECT COUNT(DISTINCT m.phone_number) AS cnt
                       FROM smb_outreach_invites oi
                       JOIN smb_messages m
                         ON m.phone_number = oi.phone_number
                        AND m.tenant_slug = oi.tenant_slug
                        AND m.created_at > oi.sent_at
                        AND m.role = 'user'
                       WHERE oi.tenant_slug=%s AND oi.batch_name=%s""",
                    (slug, batch_name),
                )
                replies = cur.fetchone()["cnt"]

                # Sample what the new subscribers said
                cur.execute(
                    """SELECT m.body FROM smb_outreach_invites oi
                       JOIN smb_messages m
                         ON m.phone_number = oi.phone_number
                        AND m.tenant_slug = oi.tenant_slug
                        AND m.created_at > oi.sent_at
                        AND m.role = 'user'
                       WHERE oi.tenant_slug=%s AND oi.batch_name=%s
                       ORDER BY m.created_at ASC LIMIT 5""",
                    (slug, batch_name),
                )
                sample_replies = [r["body"] for r in cur.fetchall()]

                return jsonify(
                    type="outreach_invite",
                    message=f"Outreach invite campaign — {batch_name}",
                    sent_at=sent_at.isoformat() if sent_at else None,
                    segment=batch_name,
                    attempted=total_sent,
                    succeeded=total_sent,
                    claimed=claimed,
                    claim_rate=round(claimed / total_sent * 100, 1) if total_sent else 0,
                    replies=replies,
                    reply_rate=round(replies / total_sent * 100, 1) if total_sent else 0,
                    sample_replies=sample_replies,
                )

            else:
                return jsonify(error="Invalid promo ID format"), 400

    finally:
        conn.close()


@api_bp.route("/api/business/promos/<int:promo_id>", methods=["DELETE"])
@login_required
def delete_business_promo(promo_id):
    """Delete a business promo blast, scoped to the logged-in tenant."""
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM smb_blasts WHERE id=%s AND tenant_slug=%s",
                (promo_id, slug),
            )
            if not cur.fetchone():
                return jsonify(error="Promo not found"), 404
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM smb_blasts WHERE id=%s AND tenant_slug=%s",
                    (promo_id, slug),
                )
        return jsonify(success=True)
    except Exception:
        logger.exception("delete_business_promo: failed for id=%s tenant=%s", promo_id, slug)
        return jsonify(error="Failed to delete promo"), 500
    finally:
        conn.close()


# ── Super-admin project switcher ───────────────────────────────────────────────

def _require_super_admin():
    """Returns the user dict if super admin, else None."""
    user = current_user()
    if not user or not user.get("is_super_admin"):
        return None
    return user


@api_bp.route("/api/admin/projects")
@login_required
def admin_projects():
    """
    Returns all projects (tenants + performers) the super-admin can view.
    Each entry has enough metadata to render a project card.
    """
    if not _require_super_admin():
        return jsonify(error="Super-admin access required"), 403

    conn = get_conn()
    try:
        import psycopg2.extras, json
        from pathlib import Path

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Get distinct creator slugs and their account types
            cur.execute("""
                SELECT DISTINCT creator_slug, account_type
                FROM operator_users
                WHERE creator_slug IS NOT NULL AND is_active=TRUE
                ORDER BY creator_slug
            """)
            slugs = cur.fetchall()

        projects = []
        for row in slugs:
            slug = row["creator_slug"]
            account_type = row["account_type"]

            # Load display metadata from local config
            display_name = slug.replace("_", " ").title()
            logo_url = ""
            location = ""

            if account_type == "business":
                config_path = _BUSINESS_CONFIGS_DIR / f"{slug}.json"
            else:
                config_path = Path(__file__).parents[3] / "creator_config" / f"{slug}.json"

            try:
                cfg = json.loads(config_path.read_text())
                display_name = cfg.get("display_name") or cfg.get("name") or display_name
                logo_url = cfg.get("logo_url") or ""
                location = cfg.get("location") or ""
            except Exception:
                pass

            # Live subscriber/fan counts.
            # Business: real per-tenant count from smb_subscribers.
            # Performer: contacts table is shared across all performers on the
            # same deployment — there is no per-slug filter, so we show None
            # and let the frontend display a dash rather than a misleading total.
            try:
                with conn.cursor() as cur:
                    if account_type == "business":
                        cur.execute(
                            "SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug=%s AND status='active'",
                            (slug,),
                        )
                        count = cur.fetchone()[0]
                    else:
                        count = None
            except Exception:
                count = None

            projects.append({
                "slug": slug,
                "display_name": display_name,
                "account_type": account_type,
                "logo_url": logo_url,
                "location": location,
                "subscriber_count": count,  # None for performers (shared table)
            })

        return jsonify(projects=projects)
    finally:
        conn.close()


@api_bp.route("/api/admin/select-project", methods=["POST"])
@login_required
def admin_select_project():
    """Set the super-admin's active viewing context to a specific project."""
    from flask import session
    if not _require_super_admin():
        return jsonify(error="Super-admin access required"), 403

    data = request.get_json(silent=True) or {}
    slug = (data.get("slug") or "").strip()
    if not slug:
        return jsonify(error="slug is required"), 400

    # Verify the slug exists
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT account_type FROM operator_users WHERE creator_slug=%s AND is_active=TRUE LIMIT 1",
                (slug,),
            )
            row = cur.fetchone()
        if not row:
            return jsonify(error="Project not found"), 404
        session["viewing_as"] = slug
        session["viewing_as_account_type"] = row[0]
        return jsonify(success=True, slug=slug, account_type=row[0])
    finally:
        conn.close()


@api_bp.route("/api/admin/exit-project", methods=["POST"])
@login_required
def admin_exit_project():
    """Clear the super-admin's viewing context — returns to project selector."""
    from flask import session
    if not _require_super_admin():
        return jsonify(error="Super-admin access required"), 403
    session.pop("viewing_as", None)
    session.pop("viewing_as_account_type", None)
    return jsonify(success=True)


@api_bp.route("/api/admin/project-info/<slug>")
@login_required
def admin_project_info(slug: str):
    """Lightweight metadata lookup for a single project slug.

    Used by the frontend SlugGuard when a super-admin opens a bookmarked
    /{slug}/dashboard URL in a new tab — we need to know account_type before
    rendering the page so the correct dashboard is shown.
    """
    if not _require_super_admin():
        return jsonify(error="Super-admin access required"), 403
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT ou.account_type, bc.display_name, bc.logo_url, ou.location,
                          (SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug = ou.creator_slug) AS subscriber_count
                   FROM operator_users ou
                   LEFT JOIN bot_configs bc ON bc.creator_slug = ou.creator_slug
                   WHERE ou.creator_slug = %s AND ou.is_active = TRUE
                   LIMIT 1""",
                (slug,),
            )
            row = cur.fetchone()
        if not row:
            return jsonify(error="Project not found"), 404
        return jsonify(
            slug=slug,
            account_type=row[0] or "performer",
            display_name=row[1] or slug,
            logo_url=row[2],
            location=row[3],
            subscriber_count=int(row[4]) if row[4] is not None else None,
        )
    finally:
        conn.close()


@api_bp.route("/api/admin/current-project")
@login_required
def admin_current_project():
    """Returns which project the super-admin is currently viewing, if any.

    The frontend (account-type.tsx) expects `viewing_as` to be an object with
    { slug, display_name, account_type, logo_url, location, subscriber_count }.
    """
    user = current_user()
    if not user or not user.get("is_super_admin"):
        return jsonify(viewing_as=None)
    slug, account_type = _get_viewing_as()
    if not slug:
        return jsonify(viewing_as=None, is_super_admin=True)
    account_type = account_type or "performer"

    # Enrich with display metadata from bot_configs so the header shows
    # the project name rather than a raw slug.
    display_name = slug
    logo_url = None
    location = None
    subscriber_count = None
    try:
        _conn = get_conn()
        with _conn.cursor() as _cur:
            _cur.execute(
                """SELECT bc.display_name, bc.logo_url, ou.location
                   FROM bot_configs bc
                   LEFT JOIN operator_users ou ON ou.id = bc.operator_user_id
                   WHERE bc.creator_slug = %s LIMIT 1""",
                (slug,),
            )
            _row = _cur.fetchone()
            if _row:
                display_name = _row[0] or slug
                logo_url = _row[1]
                location = _row[2]
        _conn.close()
    except Exception:
        pass

    return jsonify(
        viewing_as={
            "slug": slug,
            "display_name": display_name,
            "account_type": account_type,
            "logo_url": logo_url,
            "location": location,
            "subscriber_count": subscriber_count,
        },
        is_super_admin=True,
    )


@api_bp.route("/api/admin/billing-overview")
@login_required
def admin_billing_overview():
    """Super-admin only: revenue, subscriber, and trial metrics across all accounts.

    Returns:
      total_accounts, active_subscriptions, trial_accounts, cancelled_accounts,
      grandfathered_accounts, mrr_usd (sum of plan monthly prices for active subs),
      arr_usd (annualised), trial_exhausted_count, recent_upgrades (last 30d),
      recent_cancellations (last 30d), accounts_by_tier (breakdown).
    """
    if not _require_super_admin():
        return jsonify(error="Super-admin access required"), 403

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Tier breakdown
            cur.execute("""
                SELECT plan_tier, COUNT(*) AS cnt
                FROM   operator_users
                WHERE  creator_slug IS NOT NULL AND creator_slug <> ''
                GROUP  BY plan_tier
                ORDER  BY cnt DESC
            """)
            tier_rows = cur.fetchall()
            by_tier = {r["plan_tier"] or "unknown": int(r["cnt"]) for r in tier_rows}

            # Active paid (has stripe_subscription_id and not cancelled/trial)
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM   operator_users
                WHERE  stripe_subscription_id IS NOT NULL
                  AND  plan_tier NOT IN ('trial', 'cancelled', 'grandfathered', 'founder', 'internal')
                  AND  creator_slug IS NOT NULL AND creator_slug <> ''
            """)
            active_subs = int((cur.fetchone() or {}).get("cnt", 0))

            # Trial with credits remaining
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM   operator_users
                WHERE  plan_tier = 'trial'
                  AND  (trial_credits_remaining IS NULL OR trial_credits_remaining > 0)
                  AND  creator_slug IS NOT NULL AND creator_slug <> ''
            """)
            trial_active = int((cur.fetchone() or {}).get("cnt", 0))

            # Trial exhausted
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM   operator_users
                WHERE  plan_tier = 'trial'
                  AND  trial_credits_remaining IS NOT NULL
                  AND  trial_credits_remaining <= 0
                  AND  creator_slug IS NOT NULL AND creator_slug <> ''
            """)
            trial_exhausted = int((cur.fetchone() or {}).get("cnt", 0))

            # Cancelled
            cancelled = int(by_tier.get("cancelled", 0))
            grandfathered = sum(
                by_tier.get(t, 0) for t in ("grandfathered", "founder", "internal")
            )
            total_accounts = sum(by_tier.values())

            # Recent plan changes (last 30 days from credit_events)
            cur.execute("""
                SELECT kind, COUNT(*) AS cnt
                FROM   credit_events
                WHERE  kind IN ('plan_changed', 'plan_reset')
                  AND  created_at >= NOW() - INTERVAL '30 days'
                GROUP  BY kind
            """)
            event_rows = {r["kind"]: int(r["cnt"]) for r in cur.fetchall()}

    finally:
        conn.close()

    # Estimate MRR from active plans
    from ..billing.plans import ALL_PLANS
    mrr = 0
    for tier, count in by_tier.items():
        p = ALL_PLANS.get(tier)
        if p and tier not in ("trial", "cancelled", "grandfathered", "founder", "internal"):
            mrr += p.monthly_price_usd * count

    return jsonify(
        total_accounts=total_accounts,
        active_subscriptions=active_subs,
        trial_active=trial_active,
        trial_exhausted=trial_exhausted,
        cancelled=cancelled,
        grandfathered=grandfathered,
        mrr_usd=mrr,
        arr_usd=mrr * 12,
        recent_upgrades=event_rows.get("plan_reset", 0),
        recent_plan_changes=event_rows.get("plan_changed", 0),
        accounts_by_tier=by_tier,
    )


# ── Team management ────────────────────────────────────────────────────────────

@api_bp.route("/api/team/members")
@login_required
def team_members():
    """
    List all members and pending invites for the current project.

    Backed by the `team_members` table (one row per user-in-tenant with an
    explicit role). Pending invites still live in `operator_invites` until
    the invitee completes signup.
    """
    slug = _slug_or_abort()

    if not slug:
        return jsonify(error="No project context"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.email, u.name, u.account_type, u.is_super_admin,
                       u.last_login_at, u.created_at, tm.role, tm.accepted_at
                FROM   team_members tm
                JOIN   operator_users u ON u.id = tm.user_id
                WHERE  tm.tenant_slug = %s
                  AND  u.is_active = TRUE
                ORDER BY
                    CASE tm.role WHEN 'owner' THEN 0
                                 WHEN 'admin' THEN 1
                                 ELSE 2 END,
                    u.created_at
                """,
                (slug,),
            )
            members = [
                {
                    "id": r["id"],
                    "email": r["email"],
                    "name": r["name"] or "",
                    "account_type": r["account_type"],
                    "role": r["role"] or "member",
                    "is_super_admin": bool(r["is_super_admin"]),
                    "last_login_at": r["last_login_at"].isoformat() if r["last_login_at"] else None,
                    "status": "active",
                }
                for r in cur.fetchall()
            ]

            cur.execute(
                """SELECT id, email, account_type, created_at
                   FROM operator_invites
                   WHERE creator_slug=%s AND accepted_at IS NULL
                   ORDER BY created_at""",
                (slug,),
            )
            invites = [
                {
                    "id": f"invite-{r['id']}",
                    "email": r["email"],
                    "name": "",
                    "account_type": r["account_type"],
                    "role": "member",
                    "is_super_admin": False,
                    "last_login_at": None,
                    "status": "pending",
                }
                for r in cur.fetchall()
            ]

            # Surface seat limit so the UI can show "3 of 4 used" correctly.
            # Try owner role first; fall back to any member of this slug so that
            # grandfathered accounts without an explicit owner row still get
            # their real plan tier instead of defaulting to "trial".
            cur.execute(
                "SELECT plan_tier FROM operator_users WHERE creator_slug=%s AND id=( "
                "SELECT user_id FROM team_members WHERE tenant_slug=%s AND role='owner' LIMIT 1)",
                (slug, slug),
            )
            owner_row = cur.fetchone()
            if not owner_row:
                cur.execute(
                    "SELECT plan_tier FROM operator_users WHERE creator_slug=%s LIMIT 1",
                    (slug,),
                )
                owner_row = cur.fetchone()
            owner_plan = (owner_row["plan_tier"] if owner_row else None) or "trial"

        try:
            from ..billing.plans import get_plan_seats
            seats_limit = get_plan_seats(owner_plan)
        except Exception:
            seats_limit = 1

        return jsonify(
            members=members + invites,
            slug=slug,
            plan_tier=owner_plan,
            seats_limit=seats_limit,  # None = unlimited
            seats_used=len(members) + len(invites),
        )
    finally:
        conn.close()


def _send_invite_email(
    to_email: str,
    inviter_name: str,
    project_name: str,
    *,
    account_type: str = "performer",
) -> None:
    """Send a team invite email via Resend.

    The body wording adapts to whether the project is a performer or business
    account so we don't tell a restaurant's new manager they're getting access
    to "fan conversations".
    """
    import os
    import resend

    resend.api_key = os.getenv("RESEND_API_KEY", "")
    from_addr = os.getenv("RESEND_FROM", "hello@zar.bot")
    login_url = os.getenv("FRONTEND_URL", "https://zar.bot") + "/login"

    is_business = (account_type or "performer").lower() == "business"
    inbox_label = "customer conversations" if is_business else "fan conversations"
    closing_line = (
        "Your customers are texting in. Don't keep them waiting."
        if is_business
        else "Your fans are waiting. Don't ghost them."
    )

    resend.Emails.send({
        "from": f"Zar <{from_addr}>",
        "to": [to_email],
        "subject": "Someone slid into your inbox (professionally)",
        "html": f"""
        <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:32px 24px;color:#111">
          <div style="margin-bottom:24px">
            <span style="font-size:22px;font-weight:800;color:#f97316">ZarBot</span>
          </div>
          <h2 style="font-size:20px;font-weight:700;margin:0 0 12px">
            You have been invited to join {project_name} on Zar
          </h2>
          <p style="color:#555;margin:0 0 8px;line-height:1.6">
            <strong>{inviter_name}</strong> has added you as a team member.
            Sign in with Google to get access to the dashboard, {inbox_label}, and more.
          </p>
          <p style="color:#555;margin:0 0 28px;line-height:1.6">
            Use <strong>{to_email}</strong> when signing in so your invite is recognized automatically.
          </p>
          <a href="{login_url}"
             style="display:inline-block;background:#f97316;color:#fff;font-weight:700;
                    padding:13px 32px;border-radius:8px;text-decoration:none;font-size:15px">
            Accept invite
          </a>
          <p style="color:#aaa;font-size:12px;margin-top:36px;line-height:1.6">
            {closing_line}<br>
            If you were not expecting this invite, you can safely ignore this email.
          </p>
        </div>
        """,
    })


@api_bp.route("/api/team/invite", methods=["POST"])
@login_required
def team_invite():
    """
    Invite someone to the current project by email.
    On their first Google login the account is auto-provisioned.
    """
    user = current_user()
    slug = _slug_or_abort()

    if not slug:
        return jsonify(error="No project context"), 400

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify(error="Valid email is required"), 400

    conn = get_conn()

    # Resolve the account_type AND display_name for the target project —
    # not the logged-in user's own type. Critical for super-admins: when
    # brij@zarnagarg.com (performer) invites Felecia to WSCC (business),
    # the invite must carry account_type="business" so Felecia lands on
    # the business dashboard, not the performer dashboard.
    try:
        with conn.cursor() as _cur:
            _cur.execute(
                "SELECT account_type, name FROM operator_users WHERE creator_slug=%s AND is_active=TRUE LIMIT 1",
                (slug,),
            )
            _row = _cur.fetchone()
            account_type_for_project = (_row[0] if _row else None) or "performer"
            project_display_name = (_row[1] if _row else None) or slug.replace("_", " ").title()
    except Exception:
        logger.exception("team_invite: failed to look up account_type for slug=%s", slug)
        account_type_for_project = user.get("account_type") or "performer"
        project_display_name = slug.replace("_", " ").title()
    try:
        with conn:
            with conn.cursor() as cur:
                # Seat enforcement — check plan tier's seat limit vs current
                # members + pending invites. None = unlimited.
                # Try owner role first; fall back to any user with this slug so
                # that grandfathered accounts without an explicit owner row
                # still get their real plan tier instead of defaulting to "trial".
                cur.execute(
                    "SELECT plan_tier FROM operator_users WHERE id=( "
                    "SELECT user_id FROM team_members WHERE tenant_slug=%s AND role='owner' LIMIT 1)",
                    (slug,),
                )
                owner_row = cur.fetchone()
                if not owner_row:
                    cur.execute(
                        "SELECT plan_tier FROM operator_users WHERE creator_slug=%s LIMIT 1",
                        (slug,),
                    )
                    owner_row = cur.fetchone()
                owner_plan = (owner_row[0] if owner_row else None) or "trial"
                try:
                    from ..billing.plans import get_plan_seats
                    seats_limit = get_plan_seats(owner_plan)
                except Exception:
                    seats_limit = 1

                if seats_limit is not None:
                    cur.execute(
                        """SELECT
                              (SELECT COUNT(*) FROM team_members tm
                               JOIN operator_users u ON u.id = tm.user_id
                               WHERE tm.tenant_slug=%s AND u.is_active=TRUE)
                            + (SELECT COUNT(*) FROM operator_invites
                               WHERE creator_slug=%s AND accepted_at IS NULL
                                 AND email <> %s)""",
                        (slug, slug, email),
                    )
                    used = cur.fetchone()[0] or 0
                    if used >= seats_limit:
                        return jsonify(
                            error="seat_limit_reached",
                            message=f"Your plan includes {seats_limit} seat(s). Upgrade to add more teammates.",
                            upgrade_url="/plans",
                        ), 402

                cur.execute(
                    "SELECT u.id FROM team_members tm JOIN operator_users u ON u.id=tm.user_id "
                    "WHERE tm.tenant_slug=%s AND lower(u.email)=lower(%s) AND u.is_active=TRUE",
                    (slug, email),
                )
                if cur.fetchone():
                    return jsonify(error="This person is already a team member"), 409

                cur.execute(
                    """INSERT INTO operator_invites (email, creator_slug, account_type, invited_by)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (email, creator_slug) DO UPDATE
                       SET accepted_at=NULL, invited_by=%s, created_at=NOW()
                       RETURNING id""",
                    (email, slug, account_type_for_project, user["id"], user["id"]),
                )
                invite_id = cur.fetchone()[0]

        # Send invite email (non-blocking, failure does not break the invite)
        inviter_name = user.get("name") or user.get("email") or "Your teammate"
        try:
            _send_invite_email(
                email,
                inviter_name,
                project_display_name,
                account_type=account_type_for_project,
            )
        except Exception:
            logger.exception("team_invite: failed to send email to %s", email)

        return jsonify(success=True, invite_id=invite_id, email=email)
    except Exception:
        logger.exception("team_invite: failed for slug=%s email=%s", slug, email)
        return jsonify(error="Failed to create invite"), 500
    finally:
        conn.close()


@api_bp.route("/api/team/invite/<int:invite_id>", methods=["DELETE"])
@login_required
def team_revoke_invite(invite_id):
    """Revoke a pending invite."""
    slug = _slug_or_abort()

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM operator_invites WHERE id=%s AND creator_slug=%s",
                    (invite_id, slug),
                )
                if cur.rowcount == 0:
                    return jsonify(error="Invite not found"), 404
        return jsonify(success=True)
    finally:
        conn.close()


@api_bp.route("/api/team/members/<int:member_id>", methods=["DELETE"])
@login_required
def team_remove_member(member_id):
    """Remove an active team member.

    Owners cannot be removed via this endpoint (they must transfer ownership
    first or cancel the project). Everyone else gets their team_members row
    dropped and their operator_users record deactivated.
    """
    user = current_user()
    slug = _slug_or_abort()

    if member_id == user["id"]:
        return jsonify(error="You cannot remove yourself"), 400

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role FROM team_members WHERE tenant_slug=%s AND user_id=%s",
                    (slug, member_id),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify(error="Member not found"), 404
                if row[0] == "owner":
                    return jsonify(error="Owner cannot be removed"), 400

                cur.execute(
                    "DELETE FROM team_members WHERE tenant_slug=%s AND user_id=%s",
                    (slug, member_id),
                )
                # Remove project association but keep the account active — the
                # person can still log in to ZarBot and create their own bot.
                # We only strip their slug so resolve_slug() returns empty and
                # they land on onboarding instead of this dashboard.
                cur.execute(
                    """UPDATE operator_users SET creator_slug=NULL
                       WHERE id=%s AND is_super_admin=FALSE
                         AND creator_slug=%s""",
                    (member_id, slug),
                )
                # Cancel any outstanding invites for this user on this project so
                # they can't use a dangling invite to immediately re-join.
                cur.execute(
                    """DELETE FROM operator_invites
                       WHERE creator_slug=%s
                         AND email=(SELECT email FROM operator_users WHERE id=%s)""",
                    (slug, member_id),
                )
        return jsonify(success=True)
    finally:
        conn.close()


# ── Onboarding ─────────────────────────────────────────────────────────────────

@api_bp.route("/api/onboarding/status")
@login_required
def api_onboarding_status():
    """
    Returns whether the current user has completed bot onboarding.
    Lovable calls this on load to decide whether to show the wizard
    or redirect straight to the dashboard.

    Super-admins who have selected a project via /api/admin/select-project
    are always considered "completed" for that viewing context — they should
    never be bounced to the onboarding wizard while impersonating a tenant.

    Response:
      { "completed": true,  "account_type": "performer", "creator_slug": "zarna" }
      { "completed": false, "account_type": null,        "creator_slug": null }
    """
    user = current_user()

    # Super-admin impersonating a project → always treat as completed
    va_slug, va_type = _get_viewing_as()
    if user.get("is_super_admin") and va_slug:
        return jsonify(completed=True, account_type=va_type or "performer", creator_slug=va_slug)

    completed = bool(user.get("creator_slug") and user.get("account_type"))
    return jsonify(
        completed=completed,
        account_type=user.get("account_type"),
        creator_slug=user.get("creator_slug"),
    )


@api_bp.route("/api/onboarding/submit", methods=["POST"])
@login_required
def api_onboarding_submit():
    """
    Save the bot creation wizard data. Called on Step 4 of onboarding.

    Accepts:
    {
      "account_type":  "performer" | "business",
      "display_name":  "Zarna Garg",
      "slug":          "zarna",          // suggested from name, user-editable
      "bio":           "...",
      "tone":          "casual" | "professional" | "hype" | "warm",
      "website_url":   "https://...",
      "podcast_url":   "https://...",
      "media_urls":    ["https://...", ...],
      "extra_context": "Anything else the AI should know..."
    }

    Actions:
    1. Validate slug uniqueness.
    2. Insert into bot_configs (status=submitted).
    3. Set operator_users.creator_slug + account_type.

    Returns: { success, creator_slug, account_type }
    """
    import re
    user = current_user()
    data = request.get_json(silent=True) or {}

    account_type  = data.get("account_type", "performer")
    if account_type not in ("performer", "business"):
        account_type = "performer"

    display_name  = (data.get("display_name") or "").strip()[:120]
    slug          = re.sub(r"[^a-z0-9_]", "", (data.get("slug") or "").strip().lower())[:40]
    bio           = (data.get("bio") or "").strip()[:2000]
    tone          = (data.get("tone") or "casual").strip()[:50]
    website_url   = (data.get("website_url") or "").strip()[:500]
    podcast_url   = (data.get("podcast_url") or "").strip()[:500]
    media_urls    = [u.strip() for u in (data.get("media_urls") or []) if u.strip()][:20]
    extra_context = (data.get("extra_context") or "").strip()[:5000]

    if not display_name:
        return jsonify(success=False, error="Display name is required."), 400
    if not slug:
        # Auto-generate from display_name
        slug = re.sub(r"[^a-z0-9]", "_", display_name.lower()).strip("_")[:40]
    if not slug:
        return jsonify(success=False, error="Could not generate a valid slug from the name provided."), 400

    config_json = {
        "display_name": display_name,
        "bio": bio,
        "tone": tone,
        "website_url": website_url,
        "podcast_url": podcast_url,
        "media_urls": media_urls,
        "extra_context": extra_context,
    }

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Check slug uniqueness across both operator_users and bot_configs
            cur.execute(
                "SELECT id FROM operator_users WHERE creator_slug=%s AND id != %s",
                (slug, user["id"]),
            )
            if cur.fetchone():
                conn.close()
                return jsonify(success=False, error=f"The name '{slug}' is already taken. Try a different one."), 409
            cur.execute(
                "SELECT id FROM bot_configs WHERE creator_slug=%s",
                (slug,),
            )
            if cur.fetchone():
                conn.close()
                return jsonify(success=False, error=f"The name '{slug}' is already taken. Try a different one."), 409

        with conn:
            with conn.cursor() as cur:
                # Upsert bot_configs
                cur.execute(
                    """INSERT INTO bot_configs
                           (operator_user_id, creator_slug, account_type, config_json, status)
                       VALUES (%s, %s, %s, %s, 'submitted')
                       ON CONFLICT (creator_slug) DO UPDATE
                       SET config_json=%s, account_type=%s, status='submitted', updated_at=NOW()""",
                    (user["id"], slug, account_type,
                     psycopg2.extras.Json(config_json),
                     psycopg2.extras.Json(config_json), account_type),
                )
                # Stamp operator_users with slug + account_type
                cur.execute(
                    """UPDATE operator_users
                       SET creator_slug=%s, account_type=%s
                       WHERE id=%s""",
                    (slug, account_type, user["id"]),
                )
                # Seed smb_bot_config for business accounts so GET /api/bot-data
                # always finds a row without requiring a manual file deploy.
                if account_type == "business":
                    import json as _json
                    seed = _json.dumps({
                        "display_name": display_name,
                        "tone": tone,
                        "website": website_url,
                        "welcome_message": "",
                        "signup_question": "",
                        "outreach_invite_message": "",
                    })
                    cur.execute(
                        """INSERT INTO smb_bot_config (tenant_slug, config_json, updated_at)
                           VALUES (%s, %s::jsonb, NOW())
                           ON CONFLICT (tenant_slug) DO NOTHING""",
                        (slug, seed),
                    )

        conn.close()
        logger.info("onboarding_submit: user=%s slug=%s type=%s", user["email"], slug, account_type)

        # Seed 1,000 free trial credits for this newly-onboarded user so they
        # can immediately start sending. Stripe checkout later replaces the
        # trial with a paid plan via the billing webhook.
        try:
            from ..billing.credits import seed_trial_credits
            seed_trial_credits(user_id=user["id"], slug=slug)
        except Exception:
            logger.exception("onboarding_submit: seed_trial_credits failed (non-fatal)")

        # Ensure this user is the 'owner' in team_members (backfill covers
        # existing users; new users miss the db.py backfill block).
        try:
            tm_conn = get_conn()
            with tm_conn:
                with tm_conn.cursor() as tm_cur:
                    tm_cur.execute(
                        """
                        INSERT INTO team_members (tenant_slug, user_id, role, invited_at, accepted_at)
                        VALUES (%s, %s, 'owner', NOW(), NOW())
                        ON CONFLICT (tenant_slug, user_id) DO UPDATE SET role='owner'
                        """,
                        (slug, user["id"]),
                    )
            tm_conn.close()
        except Exception:
            logger.exception("onboarding_submit: team_members seed failed (non-fatal)")

        # Fire async Notion CRM record creation
        try:
            from ..notion_crm import create_customer_async
            create_customer_async(user["id"], user["email"], account_type, slug, config_json)
        except Exception:
            logger.warning("onboarding_submit: notion_crm import failed — skipping", exc_info=True)

        # Fire async universal-bot provisioning pipeline.
        #   Only for performer accounts today — business (SMB) accounts use a
        #   separate tenant-scoped code path (smb_bot_config) that doesn't
        #   need a creator_configs / creator_embeddings row.
        # Runs in a background thread so the API response isn't blocked by
        # Gemini calls + embedding batches (can take 30-60s).
        if account_type == "performer":
            try:
                import threading
                from ..provisioning import provision_new_creator

                provisioning_form = {
                    "display_name":   display_name,
                    "bio":            bio,
                    "tone":           tone,
                    "sms_keyword":    slug.upper()[:14],
                    "account_type":   account_type,
                    "website_url":    website_url,
                    "podcast_url":    podcast_url,
                    "media_urls":     media_urls,
                    "extra_context":  extra_context,
                    "uploaded_files": [],
                }
                thread = threading.Thread(
                    target=provision_new_creator,
                    args=(user["id"], slug, provisioning_form),
                    name=f"provision-{slug}",
                    daemon=True,
                )
                thread.start()
                logger.info("onboarding_submit: provisioning thread started for slug=%s", slug)
            except Exception:
                logger.exception("onboarding_submit: could not start provisioning thread")

        return jsonify(success=True, creator_slug=slug, account_type=account_type)

    except Exception:
        logger.exception("api_onboarding_submit error")
        try:
            conn.close()
        except Exception:
            pass
        return jsonify(success=False, error="Failed to save — please try again."), 500


@api_bp.route("/api/provisioning/status")
@login_required
def api_provisioning_status():
    """
    Polling endpoint used by the onboarding UI after Step 4 submits.

    Returns the current state of the universal-bot provisioning pipeline
    for the logged-in user's slug (or the slug being impersonated).

    Response:
      {
        "status":        "pending" | "in_progress" | "live" | "failed",
        "phone_number":  "+1..." | null,
        "error_message": "..." | null,      // only when status == "failed"
        "creator_slug":  "haley"
      }

    States:
      pending      — row exists, pipeline hasn't started yet (or was rolled
                     back). Shouldn't happen in the happy path; shown just
                     in case.
      in_progress  — background thread is running (phone → config → ingest)
      live         — bot is ready; fan texts will be answered
      failed       — pipeline raised; inspect error_message for the traceback
    """
    user = current_user()

    slug = None
    va_slug, _va_type = _get_viewing_as()
    if user.get("is_super_admin") and va_slug:
        slug = va_slug
    else:
        slug = user.get("creator_slug")

    if not slug:
        return jsonify(
            status="pending",
            phone_number=None,
            error_message=None,
            creator_slug=None,
        )

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Check plan tier first — grandfathered/founder/internal accounts
            # pre-date the self-serve provisioning pipeline. Their bots are
            # already live on manually-configured numbers, so return "legacy"
            # regardless of what bot_configs says (a stale pending/in_progress
            # row would otherwise keep the "Setting up your bot…" banner alive).
            cur.execute(
                """SELECT ou.plan_tier, ou.phone_number
                   FROM operator_users ou
                   WHERE ou.creator_slug = %s
                   LIMIT 1""",
                (slug,),
            )
            tier_row = cur.fetchone()
            if tier_row:
                plan_tier_val, legacy_phone_val = tier_row
                from ..billing.plans import is_unlimited_tier
                if is_unlimited_tier(plan_tier_val):
                    return jsonify(
                        status="legacy",
                        phone_number=legacy_phone_val,
                        error_message=None,
                        creator_slug=slug,
                    )

            cur.execute(
                """
                SELECT bc.provisioning_status, bc.error_message, ou.phone_number
                FROM bot_configs bc
                LEFT JOIN operator_users ou ON ou.id = bc.operator_user_id
                WHERE bc.creator_slug=%s
                """,
                (slug,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        # No bot_configs row for this slug. This happens for accounts that
        # pre-date the self-serve provisioning pipeline (e.g. Zarna, WSCC —
        # numbers were configured manually) or operators impersonating a
        # slug without one. Surface a dedicated "legacy" state so the UI
        # can hide the banner entirely instead of implying a setup is
        # in progress.
        legacy_phone = None
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phone_number FROM operator_users WHERE creator_slug=%s LIMIT 1",
                    (slug,),
                )
                lr = cur.fetchone()
                if lr:
                    legacy_phone = lr[0]
            conn.close()
        except Exception:
            pass
        return jsonify(
            status="legacy",
            phone_number=legacy_phone,
            error_message=None,
            creator_slug=slug,
        )

    raw_status, err, phone_number = row
    status = (raw_status or "pending").lower()
    if status not in ("pending", "in_progress", "live", "failed"):
        status = "pending"

    return jsonify(
        status=status,
        phone_number=phone_number,
        error_message=(err if status == "failed" else None),
        creator_slug=slug,
    )


@api_bp.route("/api/provisioning/retry", methods=["POST"])
@login_required
def api_provisioning_retry():
    """Kick off provisioning again after a failure.

    Only valid when the current slug's bot_configs row is in status='failed'.
    Re-fires the same background thread used by onboarding_submit so the UI
    "Retry" button actually restarts the pipeline instead of just refetching
    status.
    """
    import threading
    user = current_user()

    va_slug, _va_type = _get_viewing_as()
    if user.get("is_super_admin") and va_slug:
        slug = va_slug
    else:
        slug = user.get("creator_slug")

    if not slug:
        return jsonify(success=False, error="no_slug"), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bc.provisioning_status, bc.config_json, ou.id
                FROM   bot_configs bc
                LEFT   JOIN operator_users ou ON ou.id = bc.operator_user_id
                WHERE  bc.creator_slug=%s
                """,
                (slug,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify(success=False, error="no_bot_config"), 404

    current_status, config_json, user_id = row
    if (current_status or "").lower() in ("in_progress", "live"):
        return jsonify(success=False, error="provisioning_already_running"), 409

    try:
        from ..provisioning import provision_new_creator
        import json as _json
        config = config_json if isinstance(config_json, dict) else _json.loads(config_json or "{}")
        thread = threading.Thread(
            target=provision_new_creator,
            args=(user_id or user["id"], slug, config),
            name=f"provision-retry-{slug}",
            daemon=True,
        )
        thread.start()
        logger.info("api_provisioning_retry: restarted provisioning for slug=%s", slug)
        return jsonify(success=True, status="in_progress", creator_slug=slug)
    except Exception:
        logger.exception("api_provisioning_retry: failed to start thread")
        return jsonify(success=False, error="retry_failed"), 500


# NOTE: simulate-message / dev brain-call endpoint intentionally lives on the
# main (Zarna) service, not the operator service — the operator Docker image
# doesn't ship the root `app/` package. Run scripts/e2e_voice_test.py locally
# to exercise the pipeline for a given slug.
