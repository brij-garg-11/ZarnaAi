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
    try:
        _update_show_status(show_id, "ended")
        return jsonify(success=True, status="ended", show_id=show_id)
    except Exception as e:
        logger.exception("api_end_show error")
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
        data = f.read()
        if not data:
            return jsonify(success=False, error="Uploaded file is empty."), 400

        data_b64 = base64.b64encode(data).decode("ascii")
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO operator_blast_images (filename, mime_type, data_b64) "
                        "VALUES (%s, %s, %s) RETURNING id",
                        (filename, mime_type, data_b64),
                    )
                    image_id = cur.fetchone()[0]
        finally:
            conn.close()

        scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
        host   = request.headers.get("X-Forwarded-Host", request.host)
        url = f"{scheme}://{host}/operator/blast/img/{image_id}/{filename}"
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

@api_bp.route("/api/fan-of-the-week")
@login_required
def fan_of_the_week():
    """
    Surfaces one real, engaging fan message from the past 7 days.
    Picks the longest fan message (most expressive) that isn't a blast reply,
    opt-out, or single-word response. Falls back to past 30 days if quiet week.
    """
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for days_back in (7, 30, 90):
                cur.execute("""
                    SELECT
                        m.text  AS body,
                        m.created_at,
                        RIGHT(m.phone_number, 4) AS phone_last4,
                        c.fan_tier,
                        c.fan_tags,
                        c.fan_location,
                        c.fan_memory
                    FROM messages m
                    LEFT JOIN contacts c ON c.phone_number = m.phone_number
                    WHERE
                        m.role = 'user'
                        AND m.created_at >= NOW() - INTERVAL '%s days'
                        AND LENGTH(m.text) > 30
                        AND m.text NOT ILIKE 'stop%%'
                        AND m.text NOT ILIKE 'yes%%'
                        AND m.text NOT ILIKE 'no%%'
                        AND m.text NOT ILIKE 'ok%%'
                        AND (m.intent IS NULL OR m.intent NOT IN ('STOP', 'OPTOUT'))
                    ORDER BY LENGTH(m.text) DESC, RANDOM()
                    LIMIT 1
                """, (days_back,))
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
        body=row["body"],
        phone_last4=row["phone_last4"],
        created_at=row["created_at"].isoformat(),
        fan_tier=row["fan_tier"],
        fan_tags=tags[:3],
        fan_location=row["fan_location"] or "",
        fan_memory=row["fan_memory"] or "",
        days_back=days_back,
    )


# ── Bot Data ──────────────────────────────────────────────────────────────────

@api_bp.route("/api/bot-data")
@login_required
def bot_data():
    """
    Returns the current bot configuration for the logged-in user.
    Stub: reads from creator_config/<slug>.json.
    Future: will read from DB per user's creator_slug.
    """
    import json

    user = current_user()
    slug = (user.get("creator_slug") or "zarna") if user else "zarna"
    account_type = (user.get("account_type") or "performer") if user else "performer"

    # Business configs live in operator/app/business_configs/
    # Performer configs live in creator_config/ at repo root (not available in Railway container)
    if account_type == "business":
        config_path = _BUSINESS_CONFIGS_DIR / f"{slug}.json"
    else:
        config_path = Path(__file__).parents[3] / "creator_config" / f"{slug}.json"

    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except Exception:
        logger.exception("api: failed to load creator config for slug=%s", slug)
        return jsonify(error="Config not found"), 404

    if account_type == "business":
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
            display_name=cfg.get("display_name", ""),
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
        edits_used=0,
        edits_limit=20,
    )


@api_bp.route("/api/bot-data", methods=["POST"])
@login_required
def save_bot_data():
    """
    Save editable bot config fields for a business account.
    Persists to smb_bot_config (DB) so changes survive Railway deploys.
    Allowed fields: tone, welcome_message, signup_question,
                    outreach_invite_message, address, hours, website, tracked_links
    """
    user = current_user()
    if (user.get("account_type") or "performer") != "business":
        return jsonify(error="Only business accounts can edit bot config via this endpoint"), 403

    slug = user.get("creator_slug") or ""
    if not slug:
        return jsonify(error="No tenant slug configured for this account"), 400

    data = request.get_json(silent=True) or {}

    allowed = {
        "tone", "welcome_message", "signup_question",
        "outreach_invite_message", "address", "hours",
        "website", "tracked_links",
    }
    updates = {k: v for k, v in data.items() if k in allowed}

    if not updates:
        return jsonify(error="No valid fields provided"), 400

    import json, psycopg2.extras
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
        logger.exception("save_bot_data: failed for slug=%s", slug)
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
    )


# ── Business (multi-tenant SMB) ────────────────────────────────────────────────

def _get_tenant_slug() -> str | None:
    """Returns the tenant_slug for the current logged-in business user."""
    user = current_user()
    return user.get("creator_slug") if user else None


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
