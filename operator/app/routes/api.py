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
                        m.body,
                        m.created_at,
                        RIGHT(m.phone_number, 4) AS phone_last4,
                        c.fan_tier,
                        c.fan_tags,
                        c.fan_memory
                    FROM messages m
                    LEFT JOIN contacts c ON c.phone_number = m.phone_number
                    WHERE
                        m.role = 'user'
                        AND m.created_at >= NOW() - INTERVAL '%s days'
                        AND LENGTH(m.body) > 30
                        AND m.body NOT ILIKE 'stop%%'
                        AND m.body NOT ILIKE 'yes%%'
                        AND m.body NOT ILIKE 'no%%'
                        AND m.body NOT ILIKE 'ok%%'
                        AND m.intent NOT IN ('STOP', 'OPTOUT')
                    ORDER BY LENGTH(m.body) DESC, RANDOM()
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
