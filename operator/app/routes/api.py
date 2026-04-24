"""
JSON API routes consumed by the Zar marketing site / Lovable React dashboard.

All routes require an active session (login via /api/auth/login first).
All routes return JSON — no HTML rendering.
"""

from pathlib import Path

from flask import Blueprint, jsonify, request
from ..routes.auth import login_required, current_user

_BUSINESS_CONFIGS_DIR = Path(__file__).parent.parent / "business_configs"
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


@api_bp.route("/api/billing/cost-breakdown")
@login_required
def api_cost_breakdown():
    """
    GET /api/billing/cost-breakdown?slug=zarna&month=2026-04
    Returns exact AI cost (from messages.ai_cost_usd), SMS cost (from sms_cost_log),
    and phone rental for a given creator and calendar month.
    Falls back to flat estimates for any source not yet populated.
    """
    import psycopg2.extras
    slug  = request.args.get("slug", "").strip().lower()
    month = request.args.get("month", "").strip()  # e.g. "2026-04"
    if not slug:
        return jsonify(error="slug is required"), 400
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
    """
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Tier counts + last blast date per tier
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
                GROUP BY c.fan_tier
                ORDER BY CASE c.fan_tier
                    WHEN 'superfan' THEN 1 WHEN 'engaged' THEN 2
                    WHEN 'lurker'   THEN 3 WHEN 'dormant' THEN 4
                    ELSE 5 END
            """)
            tier_rows = cur.fetchall()

            # 50 fans with most recent blast date (for the frequency table)
            cur.execute("""
                SELECT
                    RIGHT(c.phone_number, 4)                                AS phone_last4,
                    c.fan_tier,
                    c.fan_tags,
                    MAX(br.sent_at)                                         AS last_blasted_at,
                    EXTRACT(EPOCH FROM (NOW() - MAX(br.sent_at)))::int / 86400 AS days_since
                FROM contacts c
                JOIN blast_recipients br ON br.phone_number = c.phone_number
                WHERE c.phone_number NOT LIKE 'whatsapp:%%'
                GROUP BY c.phone_number, c.fan_tier, c.fan_tags
                ORDER BY last_blasted_at DESC
                LIMIT 50
            """)
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
                    MIN(m.created_at) AS first_message_at,
                    COUNT(*) FILTER (WHERE m.role = 'user') AS fan_messages,
                    COUNT(*) FILTER (WHERE m.role = 'assistant') AS bot_messages,
                    (
                        SELECT text FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                        ORDER BY m2.created_at DESC LIMIT 1
                    ) AS last_body,
                    (
                        SELECT role FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                        ORDER BY m2.created_at DESC LIMIT 1
                    ) AS last_role,
                    c.fan_tier,
                    c.fan_tags,
                    c.fan_location,
                    LEFT(c.fan_memory, 200) AS fan_memory_preview
                FROM messages m
                LEFT JOIN contacts c ON c.phone_number = m.phone_number
                GROUP BY m.phone_number, c.fan_tier, c.fan_tags, c.fan_location, c.fan_memory
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
            "first_message_at": r["first_message_at"].isoformat() if r["first_message_at"] else None,
            "last_body": (r["last_body"] or "")[:120],
            "last_role": r["last_role"],
            "fan_messages": r["fan_messages"],
            "bot_messages": r["bot_messages"],
            "fan_tier": r["fan_tier"],
            "fan_tags": tags[:5],
            "fan_location": r["fan_location"] or "",
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
                SELECT role, text AS body, created_at, intent, tone_mode, sell_variant
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
                    "tone_mode": r.get("tone_mode"),
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT fan_tier, fan_tags, fan_location, fan_memory, fan_score, created_at
                FROM contacts WHERE phone_number = %s
            """, (phone,))
            fan_row = cur.fetchone()
            fan = {}
            if fan_row:
                fan = {
                    "fan_tier": fan_row["fan_tier"],
                    "fan_tags": fan_row["fan_tags"] or [],
                    "fan_location": fan_row["fan_location"] or "",
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
    from datetime import datetime, timezone
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()

    if not text:
        return jsonify(success=False, error="Message text is required."), 400
    if len(text) > 1600:
        return jsonify(success=False, error="Message too long (max 1600 chars)."), 400

    # Resolve the full phone number from last-4
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT phone_number FROM messages
                WHERE RIGHT(phone_number, 4) = %s
                GROUP BY phone_number
                ORDER BY MAX(created_at) DESC
                LIMIT 1
            """, (phone_last4,))
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

    # Log to messages table so it shows in thread history
    sent_at = datetime.now(timezone.utc)
    message_id = None
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO messages (phone_number, role, text, created_at, source)
                    VALUES (%s, 'assistant', %s, %s, 'manual_operator')
                    RETURNING id
                """, (phone, text, sent_at))
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
        show_id = _create_show(name, keyword, use_kw, ws, we, deliver, event_cat, etz)
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
    try:
        shows = list_shows()
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
    counts = {}
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor() as cur:
            cur.execute(
                "SELECT fan_tier, COUNT(*) FROM contacts "
                "WHERE fan_tier IS NOT NULL AND phone_number NOT LIKE 'whatsapp:%%' "
                "GROUP BY fan_tier"
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
    result = {"tiers": {}, "total_sending": 0, "total_suppressed": 0}
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT phone_number FROM broadcast_optouts")
            optouts = {r[0] for r in cur.fetchall()}

            for tier, cadence in CADENCE_DAYS.items():
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts "
                    "WHERE fan_tier = %s AND phone_number NOT LIKE 'whatsapp:%%'",
                    (tier,),
                )
                all_phones = {r[0] for r in cur.fetchall()} - optouts

                cur.execute(
                    """
                    SELECT DISTINCT br.phone_number
                    FROM   blast_recipients br
                    JOIN   blast_drafts bd ON bd.id = br.blast_id
                    JOIN   contacts c ON c.phone_number = br.phone_number
                    WHERE  c.fan_tier = %s
                      AND  br.sent_at >= NOW() - (%s || ' days')::INTERVAL
                    """,
                    (tier, str(cadence)),
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
    Super-admins pass unconditionally. Regular users must match created_by email.
    """
    if user.get("is_super_admin"):
        return True
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT created_by FROM blast_drafts WHERE id=%s", (draft_id,))
            row = cur.fetchone()
        conn.close()
        if not row:
            return False
        return (row[0] or "").lower() == (user.get("email") or "").lower()
    except Exception:
        logger.exception("_user_owns_draft check failed for draft_id=%s", draft_id)
        return False


def _user_owns_show(show_id: int, user: dict) -> bool:
    """
    Returns True if the current user is allowed to operate on this live show.
    Super-admins pass unconditionally. Regular users check creator_slug scoping
    via the shows table (created_by column if present, otherwise slug-level gate).
    """
    if user.get("is_super_admin"):
        return True
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT created_by FROM live_shows WHERE id=%s",
                (show_id,),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            return False
        created_by = (row[0] or "").lower()
        # If show has no created_by (legacy rows), allow any authenticated user
        # for backward compatibility — tightened once creator_slug is on shows.
        if not created_by:
            return True
        return created_by == (user.get("email") or "").lower()
    except Exception:
        logger.exception("_user_owns_show check failed for show_id=%s", show_id)
        return False


@api_bp.route("/api/blasts/create", methods=["POST"])
@login_required
def api_create_blast():
    """Create a blank blast draft. Returns {success, draft_id}."""
    from ..queries import save_blast_draft
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
    if audience_type not in ("all", "tag", "location", "random", "show", "tier"):
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
    data = request.get_json(silent=True) or {}
    audience_type = data.get("audience_type", "all")
    if audience_type not in ("all", "tag", "location", "random", "show", "tier"):
        audience_type = "all"
    audience_filter = (data.get("audience_filter") or "").strip()
    sample_pct = max(1, min(100, int(data.get("sample_pct", 100) or 100)))
    try:
        count = count_audience(audience_type, audience_filter, sample_pct)
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
    if audience_type not in ("all", "tag", "location", "random", "show", "tier"):
        audience_type = "all"

    if not body:
        return jsonify(success=False, error="Message body is required before sending."), 400

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
    LEFT JOIN contacts c  ON c.phone_number = bm.phone_number
    LEFT JOIN recent_replies rr ON rr.phone_number = bm.phone_number
    LEFT JOIN came_back cb      ON cb.phone_number = bm.phone_number
    WHERE bm.phone_number NOT IN (
        SELECT phone_number FROM fan_of_the_week
        WHERE week_of >= CURRENT_DATE - INTERVAL '8 weeks'
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
    """
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Check if a pick is already saved for the current week
            cur.execute("""
                SELECT f.phone_number, RIGHT(f.phone_number, 4) AS phone_last4,
                       f.message_text, f.week_of, f.selected_at,
                       c.fan_tier, c.fan_tags, c.fan_location, c.fan_memory, c.fan_score
                FROM fan_of_the_week f
                LEFT JOIN contacts c ON c.phone_number = f.phone_number
                WHERE f.week_of = DATE_TRUNC('week', CURRENT_DATE)::date
                LIMIT 1
            """)
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
                    fan_memory=saved["fan_memory"] or "",
                    fan_score=saved["fan_score"],
                )
            # No saved pick — return top dynamic candidate
            row = None
            for days_back in (7, 30, 90):
                cur.execute(_FOTW_CANDIDATES_SQL, (days_back,))
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
    """
    import psycopg2.extras
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            rows = []
            for days_back in (7, 30, 90):
                cur.execute(_FOTW_CANDIDATES_SQL, (days_back,))
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
    import psycopg2.extras
    data = request.get_json(silent=True) or {}
    phone_last4 = (data.get("phone_last4") or "").strip()
    message_text = (data.get("message_text") or "").strip()[:500]

    if not phone_last4:
        return jsonify(ok=False, error="phone_last4 required"), 400

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Resolve last-4 to full phone number
            cur.execute("""
                SELECT phone_number FROM messages
                WHERE RIGHT(phone_number, 4) = %s
                GROUP BY phone_number
                ORDER BY MAX(created_at) DESC
                LIMIT 1
            """, (phone_last4,))
            row = cur.fetchone()
            if not row:
                conn.close()
                return jsonify(ok=False, error="Fan not found"), 404

            phone = row["phone_number"]

            # Upsert the pick for this week
            cur.execute("""
                INSERT INTO fan_of_the_week (phone_number, week_of, message_text)
                VALUES (%s, DATE_TRUNC('week', CURRENT_DATE)::date, %s)
                ON CONFLICT (week_of) DO UPDATE
                    SET phone_number = EXCLUDED.phone_number,
                        message_text = EXCLUDED.message_text,
                        selected_at  = NOW()
            """, (phone, message_text))

            # Add 'fan_of_the_week' tag to the contact (avoid duplicates)
            cur.execute("""
                UPDATE contacts
                SET fan_tags = array_append(
                    COALESCE(fan_tags, '{}'),
                    'fan_of_the_week'
                )
                WHERE phone_number = %s
                  AND NOT ('fan_of_the_week' = ANY(COALESCE(fan_tags, '{}')))
            """, (phone,))

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
    """
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
                    c.fan_score
                FROM fan_of_the_week f
                LEFT JOIN contacts c ON c.phone_number = f.phone_number
                ORDER BY f.week_of DESC
                LIMIT 52
            """)
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


@api_bp.route("/api/smb/<slug>/customer-of-the-week")
@login_required
def smb_customer_of_the_week(slug: str):
    """
    Returns this week's saved Customer of the Week for an SMB tenant,
    or falls back to the top dynamic candidate.
    """
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

    user = current_user()
    slug = user.get("creator_slug") if user else None
    account_type = (user.get("account_type") or "performer") if user else "performer"

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
    account_type = (user.get("account_type") or "performer")
    slug = user.get("creator_slug") or ""
    if not slug:
        return jsonify(error="Onboarding not complete — no bot configured yet."), 400

    data = request.get_json(silent=True) or {}

    if account_type == "business":
        allowed = {
            "tone", "welcome_message", "signup_question",
            "outreach_invite_message", "address", "hours",
            "website", "tracked_links", "display_name",
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
                    (user["id"], slug, json.dumps(updates), json.dumps(updates)),
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


# Plan definitions: name → monthly credit allotment (None = unlimited)
_PLAN_CREDITS = {
    "starter": 3200,
    "growth":  6200,
    "pro":     12500,
    "scale":   25200,
    "custom":  None,   # unlimited
}

# Credit booster packs (UI-only until Stripe is wired)
_CREDIT_BOOSTERS = [
    {"credits": 1000,  "price_usd": 9,  "label": "+1,000 credits"},
    {"credits": 2500,  "price_usd": 19, "label": "+2,500 credits"},
    {"credits": 5000,  "price_usd": 35, "label": "+5,000 credits"},
    {"credits": 10000, "price_usd": 59, "label": "+10,000 credits"},
]


@api_bp.route("/api/billing/status")
@login_required
def billing_status():
    """
    Monthly credit usage summary — segment-based billing.

    Credits are counted by SMS segments (not flat messages):
      - Fan inbound / AI reply ≤ 160 chars  → 1 credit
      - AI reply 161-306 chars              → 2 credits
      - AI reply 307-459 chars              → 3 credits (etc.)
      - Text blast                          → CEIL(body_length / 160) × fans_reached
      - MMS blast (media attached)          → 3 × fans_reached

    Returns credits_used, credits_total (null = unlimited), plan_name, and breakdown stats.
    """
    import psycopg2.extras
    from datetime import date

    user = current_user()
    slug = user.get("creator_slug") or ""
    account_type = user.get("account_type") or "performer"
    month = date.today().strftime("%Y-%m")

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Fetch plan info for this user
            cur.execute(
                """SELECT plan_name, monthly_credits
                   FROM operator_users
                   WHERE creator_slug=%s AND is_active=TRUE
                   LIMIT 1""",
                (slug,),
            )
            plan_row = cur.fetchone()
            plan_name      = (plan_row["plan_name"] if plan_row else "starter") or "starter"
            credits_total  = plan_row["monthly_credits"] if plan_row else _PLAN_CREDITS.get(plan_name, 3200)

            if account_type == "performer":
                # Conversation credits — segment-based:
                #   fan message : CEIL(LENGTH(text) / 160)   (avg ~52 chars = always 1)
                #   AI reply    : CEIL(reply_length_chars / 160)  (avg ~154 chars, some = 2)
                # Unicode/emoji messages use 70-char single / 67-char multi thresholds,
                # but for billing we use the conservative 160/153 GSM-7 limits.
                cur.execute(
                    """SELECT
                          COUNT(*) FILTER (WHERE m.role='assistant') AS ai_replies,
                          COUNT(*) FILTER (WHERE m.ai_cost_usd IS NOT NULL) AS tracked_cnt,
                          COALESCE(SUM(m.ai_cost_usd), 0) AS exact_ai_cost,
                          -- segments: use reply_length_chars for AI replies (pre-computed),
                          -- fall back to LENGTH(text) for fan messages
                          COALESCE(SUM(
                              GREATEST(1, CEIL(
                                  COALESCE(m.reply_length_chars, LENGTH(m.text), 1)::float / 160
                              ))
                          ), 0) AS convo_credits
                       FROM messages m
                       JOIN contacts c ON c.phone_number = m.phone_number
                       WHERE c.creator_slug=%s
                         AND m.created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
                msg_row            = cur.fetchone()
                convo_credits      = int(msg_row["convo_credits"] or 0)
                replies_this_month = msg_row["ai_replies"]
                untracked_cnt      = replies_this_month - msg_row["tracked_cnt"]
                ai_cost            = round(float(msg_row["exact_ai_cost"]) + untracked_cnt * 0.004, 4)
                ai_cost_fully_exact = (untracked_cnt == 0)

                # Blast credits — segment-based:
                #   MMS blast (media_url set) → 3 credits per fan
                #   Text-only blast           → CEIL(LENGTH(body) / 160) credits per fan
                cur.execute(
                    """SELECT
                          COUNT(*) AS blasts,
                          COALESCE(SUM(sent_count), 0) AS fans_reached,
                          COALESCE(SUM(
                              sent_count * CASE
                                  WHEN media_url IS NOT NULL THEN 3
                                  ELSE GREATEST(1, CEIL(LENGTH(body)::float / 160))
                              END
                          ), 0) AS blast_credits
                       FROM blast_drafts
                       WHERE status='sent'
                         AND sent_at >= DATE_TRUNC('month', NOW())""",
                )
                blast_row          = cur.fetchone()
                blast_credits      = int(blast_row["blast_credits"] or 0)
                blasts_this_month  = int(blast_row["blasts"] or 0)
                fans_reached_count = int(blast_row["fans_reached"] or 0)

                # Total credits = conversation segments + blast segments
                credits_used = convo_credits + blast_credits

                # SMS cost (internal, not shown to client)
                cur.execute(
                    """SELECT COALESCE(SUM(inbound_cost_usd + outbound_cost_usd), -1) AS sms_cost
                       FROM sms_cost_log
                       WHERE creator_slug=%s AND TO_CHAR(log_date,'YYYY-MM')=%s""",
                    (slug, month),
                )
                sms_row  = cur.fetchone()
                sms_cost = float(sms_row["sms_cost"]) if sms_row["sms_cost"] >= 0 else None
                total_cost = round(
                    1.15 + ai_cost +
                    (sms_cost if sms_cost is not None else replies_this_month * 0.0079), 2
                )

            else:
                # Business account
                cur.execute(
                    """SELECT COUNT(*) AS cnt FROM smb_messages
                       WHERE tenant_slug=%s AND role='assistant'
                         AND created_at >= DATE_TRUNC('month', NOW())""",
                    (slug,),
                )
                replies_this_month  = cur.fetchone()["cnt"]
                credits_used        = replies_this_month * 2  # rough: 1 in + 1 out
                blasts_this_month   = 0
                blast_credits       = 0
                fans_reached_count  = 0
                ai_cost             = None
                sms_cost            = None
                total_cost          = round(1.15 + replies_this_month * 0.004 +
                                            replies_this_month * 0.0079, 2)
                ai_cost_fully_exact = False

        conn.close()

        # Warning thresholds (null credits_total = unlimited, no warning)
        credits_warning = None
        if credits_total:
            remaining = credits_total - credits_used
            pct_remaining = remaining / credits_total if credits_total else 1
            if pct_remaining <= 0.10:
                credits_warning = "critical"
            elif pct_remaining <= 0.20:
                credits_warning = "low"

        return jsonify(
            slug=slug,
            month=month,
            plan_name=plan_name,
            credits_used=credits_used,
            credits_total=credits_total,        # null = unlimited
            credits_warning=credits_warning,    # null | "low" | "critical"
            boosters=_CREDIT_BOOSTERS,
            replies_this_month=replies_this_month,
            blasts_this_month=blasts_this_month,
            fans_reached_this_month=fans_reached_count,  # actual people, not segment-weighted
            blast_credits=blast_credits,        # segment-weighted credits from blasts
            ai_cost_usd=ai_cost,
            sms_cost_usd=sms_cost,
            total_cost_usd=total_cost,
            cost_exact=(ai_cost_fully_exact and sms_cost is not None),
        )
    except Exception:
        logger.exception("billing_status: failed for slug=%s", slug)
        return jsonify(error="internal error"), 500


# ── Business (multi-tenant SMB) ────────────────────────────────────────────────

def _get_tenant_slug() -> str | None:
    """
    Returns the effective tenant_slug for the current request.
    Super-admins can override via session['viewing_as']; everyone else
    gets their own creator_slug.
    """
    from flask import session
    user = current_user()
    if not user:
        return None
    if user.get("is_super_admin"):
        return session.get("viewing_as") or user.get("creator_slug")
    return user.get("creator_slug")


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

        return jsonify(
            total_subscribers=total_subscribers,
            active_subscribers=active_subscribers,
            total_messages=total_messages,
            inbound_messages=inbound_messages,
            messages_week=messages_week,
            total_blasts=total_blasts,
            messages_by_day=messages_by_day,
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

                threads.append({
                    "phone_last4": phone[-4:],
                    "phone_number": phone,
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
                "phone_number": phone,
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

            cur.execute(
                """SELECT body FROM smb_messages
                   WHERE tenant_slug=%s AND phone_number=%s AND role='user'
                   ORDER BY body_length_chars DESC NULLS LAST LIMIT 1""",
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


@api_bp.route("/api/business/blast/send", methods=["POST"])
@login_required
def business_blast_send():
    """
    Fire a promo blast for the business tenant.
    Body: { "message": "...", "audience": "all|segment:LOCAL|segment:ENGAGED|..." }
    Sends in a background thread; returns immediately.
    """
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    audience = (data.get("audience") or "all").strip()

    if not message:
        return jsonify(error="Message is required"), 400

    import os, threading, time as _time, json
    from twilio.rest import Client as TwilioClient

    slug_upper = slug.upper()
    from_number = os.getenv(f"SMB_{slug_upper}_SMS_NUMBER")
    if not from_number:
        return jsonify(error="SMS number not configured for this account"), 500

    # Load segment definitions from local business config
    try:
        cfg = json.loads((_BUSINESS_CONFIGS_DIR / f"{slug}.json").read_text())
        segments_def = cfg.get("segments", [])
    except Exception:
        segments_def = []

    def _run():
        conn = get_conn()
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if audience.lower() == "all":
                    cur.execute(
                        "SELECT phone_number FROM smb_subscribers WHERE tenant_slug=%s AND status='active'",
                        (slug,),
                    )
                elif audience.lower().startswith("segment:"):
                    seg_name = audience[8:].strip().upper()
                    seg = next((s for s in segments_def if s["name"].upper() == seg_name), None)
                    if not seg:
                        logger.error("business_blast: unknown segment %s for tenant %s", seg_name, slug)
                        return
                    ph = ",".join(["%s"] * len(seg["answers"]))
                    cur.execute(
                        f"""SELECT DISTINCT s.phone_number
                            FROM smb_subscribers s
                            JOIN smb_preferences p ON p.subscriber_id = s.id
                            WHERE s.tenant_slug=%s AND s.status='active'
                              AND p.question_key=%s AND p.answer IN ({ph})""",
                        (slug, seg["question_key"], *seg["answers"]),
                    )
                else:
                    logger.error("business_blast: invalid audience=%s", audience)
                    return

                phones = [r["phone_number"] for r in cur.fetchall()]

            if not phones:
                logger.info("business_blast: no subscribers for audience=%s tenant=%s", audience, slug)
                return

            twilio = TwilioClient(
                os.getenv("TWILIO_ACCOUNT_SID"),
                os.getenv("TWILIO_AUTH_TOKEN"),
            )
            attempted = succeeded = 0
            for phone in phones:
                attempted += 1
                try:
                    twilio.messages.create(body=message, from_=from_number, to=phone)
                    succeeded += 1
                except Exception as e:
                    logger.warning("business_blast: send to %s failed: %s", phone[-4:], e)
                if len(phones) > 1:
                    _time.sleep(0.35)

            seg_label = audience if audience != "all" else None
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO smb_blasts
                               (tenant_slug, owner_message, body, attempted, succeeded, segment)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (slug, message[:500], message[:500], attempted, succeeded, seg_label),
                    )
            logger.info(
                "business_blast done: tenant=%s audience=%s attempted=%d succeeded=%d",
                slug, audience, attempted, succeeded,
            )
        except Exception:
            logger.exception("business_blast: thread failed for tenant=%s", slug)
        finally:
            conn.close()

    threading.Thread(target=_run, daemon=True).start()

    audience_label = audience.replace("segment:", "").replace("all", "all subscribers").lower()
    return jsonify(success=True, status=f"Blast queued for {audience_label}. You'll see it in Promos when it completes.")


@api_bp.route("/api/business/blast/preview-count", methods=["POST"])
@login_required
def business_blast_preview_count():
    """
    Return how many subscribers would receive a blast for the given audience.
    Body: { "audience": "all|segment:LOCAL|..." }
    """
    slug = _get_tenant_slug()
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    data = request.get_json(silent=True) or {}
    audience = (data.get("audience") or "all").strip().lower()

    conn = get_conn()
    try:
        import psycopg2.extras, json
        from pathlib import Path

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if audience == "all":
                cur.execute(
                    "SELECT COUNT(*) FROM smb_subscribers WHERE tenant_slug=%s AND status='active'",
                    (slug,),
                )
                count = cur.fetchone()[0]
            elif audience.startswith("segment:"):
                seg_name = audience[8:].strip().upper()
                cfg = json.loads((_BUSINESS_CONFIGS_DIR / f"{slug}.json").read_text())
                seg = next((s for s in cfg.get("segments", []) if s["name"].upper() == seg_name), None)
                if not seg:
                    return jsonify(error=f"Unknown segment: {seg_name}"), 400
                ph = ",".join(["%s"] * len(seg["answers"]))
                cur.execute(
                    f"""SELECT COUNT(DISTINCT s.id)
                        FROM smb_subscribers s
                        JOIN smb_preferences p ON p.subscriber_id = s.id
                        WHERE s.tenant_slug=%s AND p.question_key=%s AND p.answer IN ({ph})""",
                    (slug, seg["question_key"], *seg["answers"]),
                )
                count = cur.fetchone()[0]
            else:
                return jsonify(error="Invalid audience type"), 400

        return jsonify(count=count, audience=audience)
    finally:
        conn.close()


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
            if not row or not check_password_hash(row["password_hash"], current_pw):
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
                cur.execute("DELETE FROM blast_recipients WHERE blast_draft_id=%s", (blast_id,))
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

                # Replies from any subscriber after this blast was sent
                cur.execute(
                    """SELECT COUNT(DISTINCT phone_number) AS cnt
                       FROM smb_messages
                       WHERE tenant_slug=%s AND role='user' AND created_at > %s""",
                    (slug, row["sent_at"]),
                )
                replies = cur.fetchone()["cnt"]
                reply_rate = round(replies / succeeded * 100, 1) if succeeded else 0

                # Sample reply messages
                cur.execute(
                    """SELECT body FROM smb_messages
                       WHERE tenant_slug=%s AND role='user' AND created_at > %s
                       ORDER BY created_at ASC LIMIT 5""",
                    (slug, row["sent_at"]),
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


@api_bp.route("/api/admin/current-project")
@login_required
def admin_current_project():
    """Returns which project the super-admin is currently viewing, if any."""
    from flask import session
    user = current_user()
    if not user or not user.get("is_super_admin"):
        return jsonify(viewing_as=None)
    slug = session.get("viewing_as")
    account_type = session.get("viewing_as_account_type")
    return jsonify(
        viewing_as=slug,
        viewing_as_account_type=account_type,
        is_super_admin=True,
    )


# ── Team management ────────────────────────────────────────────────────────────

@api_bp.route("/api/team/members")
@login_required
def team_members():
    """
    List all members and pending invites for the current project.
    Super-admins see the project they're currently viewing.
    Regular users see their own project.
    """
    user = current_user()
    from flask import session
    if user.get("is_super_admin"):
        slug = session.get("viewing_as") or user.get("creator_slug")
    else:
        slug = user.get("creator_slug")

    if not slug:
        return jsonify(error="No project context"), 400

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Active members
            cur.execute(
                """SELECT id, email, name, account_type, is_super_admin,
                          last_login_at, created_at
                   FROM operator_users
                   WHERE creator_slug=%s AND is_active=TRUE
                   ORDER BY created_at""",
                (slug,),
            )
            members = [
                {
                    "id": r["id"],
                    "email": r["email"],
                    "name": r["name"] or "",
                    "account_type": r["account_type"],
                    "is_super_admin": bool(r["is_super_admin"]),
                    "last_login_at": r["last_login_at"].isoformat() if r["last_login_at"] else None,
                    "status": "active",
                }
                for r in cur.fetchall()
            ]

            # Pending invites (not yet accepted)
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
                    "is_super_admin": False,
                    "last_login_at": None,
                    "status": "pending",
                }
                for r in cur.fetchall()
            ]

        return jsonify(members=members + invites, slug=slug)
    finally:
        conn.close()


def _send_invite_email(to_email: str, inviter_name: str, project_name: str) -> None:
    """Send a team invite email via Resend."""
    import os
    import resend

    resend.api_key = os.getenv("RESEND_API_KEY", "")
    from_addr = os.getenv("RESEND_FROM", "hello@zar.bot")
    login_url = os.getenv("FRONTEND_URL", "https://zar.bot") + "/login"

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
            Sign in with Google to get access to the dashboard, fan conversations, and more.
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
            Your fans are waiting. Don't ghost them.<br>
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
    from flask import session
    if user.get("is_super_admin"):
        slug = session.get("viewing_as") or user.get("creator_slug")
        account_type_for_project = session.get("viewing_as_account_type") or user.get("account_type") or "performer"
    else:
        slug = user.get("creator_slug")
        account_type_for_project = user.get("account_type") or "performer"

    if not slug:
        return jsonify(error="No project context"), 400

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify(error="Valid email is required"), 400

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Check if already an active member
                cur.execute(
                    "SELECT id FROM operator_users WHERE email=%s AND creator_slug=%s AND is_active=TRUE",
                    (email, slug),
                )
                if cur.fetchone():
                    return jsonify(error="This person is already a team member"), 409

                # Upsert invite
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
        project_name = slug.replace("_", " ").title()
        try:
            _send_invite_email(email, inviter_name, project_name)
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
    user = current_user()
    from flask import session
    slug = session.get("viewing_as") if user.get("is_super_admin") else user.get("creator_slug")

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
    """Remove an active team member (deactivate their account)."""
    user = current_user()
    from flask import session
    slug = session.get("viewing_as") if user.get("is_super_admin") else user.get("creator_slug")

    if member_id == user["id"]:
        return jsonify(error="You cannot remove yourself"), 400

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE operator_users SET is_active=FALSE
                       WHERE id=%s AND creator_slug=%s AND is_super_admin=FALSE""",
                    (member_id, slug),
                )
                if cur.rowcount == 0:
                    return jsonify(error="Member not found or cannot be removed"), 404
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
    from flask import session
    user = current_user()

    # Super-admin impersonating a project → always treat as completed
    if user.get("is_super_admin") and session.get("viewing_as"):
        slug = session["viewing_as"]
        account_type = session.get("viewing_as_account_type") or "performer"
        return jsonify(completed=True, account_type=account_type, creator_slug=slug)

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
    from flask import session
    user = current_user()

    slug = None
    if user.get("is_super_admin") and session.get("viewing_as"):
        slug = session["viewing_as"]
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
        return jsonify(
            status="pending",
            phone_number=None,
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
