"""
Admin analytics dashboard — password-protected read-only view of all activity.

Access: https://your-railway-url.app/admin
Login: HTTP Basic Auth — username anything, password = ADMIN_PASSWORD env var

Tabs:
  /admin              → Overview (stats + charts)
  /admin?tab=audience → Audience (tags, fan profiles, location)
  /admin?tab=convos   → Inbox (per-member list) + click for threaded conversation
"""

import csv
import hashlib
import io
import os
import secrets as _secrets
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

from flask import Blueprint, Response, jsonify, redirect as _redirect, request

from app.admin_auth import (
    admin_password_configured,
    check_admin_auth,
    get_db_connection,
    no_admin_password_response,
    require_admin_auth_response,
)

admin_bp = Blueprint("admin", __name__)

INBOX_PAGE_SIZE = 60
THREAD_PAGE_SIZE = 120
_VALID_CHART_DAYS = frozenset((7, 14, 30, 90))


def _safe_chart_days(raw) -> int:
    try:
        v = int(raw)
        return v if v in _VALID_CHART_DAYS else 14
    except (TypeError, ValueError):
        return 14


def _esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _check_auth():
    return check_admin_auth()


def _require_auth():
    return require_admin_auth_response()


def _no_password_configured():
    return no_admin_password_response()


def _get_db():
    return get_db_connection()


def _init_tracking_tables():
    """Idempotent — create tracked_links / tracked_link_clicks tables if absent."""
    conn = _get_db()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tracked_links (
                        id            BIGSERIAL PRIMARY KEY,
                        slug          TEXT UNIQUE NOT NULL,
                        label         TEXT NOT NULL DEFAULT '',
                        campaign_type TEXT NOT NULL DEFAULT 'other',
                        destination   TEXT NOT NULL,
                        created_by    TEXT DEFAULT '',
                        created_at    TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tracked_link_clicks (
                        id         BIGSERIAL PRIMARY KEY,
                        link_id    BIGINT NOT NULL REFERENCES tracked_links(id) ON DELETE CASCADE,
                        clicked_at TIMESTAMPTZ DEFAULT NOW(),
                        ip_hash    TEXT DEFAULT '',
                        ua_short   TEXT DEFAULT ''
                    )
                """)
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tlc_link_id ON tracked_link_clicks(link_id)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tlc_clicked_at ON tracked_link_clicks(clicked_at)"
                )
                # sent_to: total recipients across all blasts that used this link
                cur.execute(
                    "ALTER TABLE tracked_links ADD COLUMN IF NOT EXISTS sent_to INT DEFAULT 0"
                )
                # Seed the two permanent bot-tracking rows — insert if missing, then
                # fix destination if it was stored without https:// (old bug).
                cur.execute("""
                    INSERT INTO tracked_links (slug, label, campaign_type, destination)
                    VALUES ('bot-website', 'Bot → Website / Tickets', 'ticket', 'https://zarnagarg.com')
                    ON CONFLICT (slug) DO NOTHING
                """)
                cur.execute("""
                    UPDATE tracked_links
                    SET destination = 'https://zarnagarg.com'
                    WHERE slug = 'bot-website' AND destination NOT LIKE 'https://%'
                """)
                cur.execute("""
                    INSERT INTO tracked_links (slug, label, campaign_type, destination)
                    VALUES ('bot-podcast', 'Bot → Podcast', 'podcast', 'https://open.spotify.com')
                    ON CONFLICT (slug) DO NOTHING
                """)
                cur.execute("""
                    UPDATE tracked_links
                    SET destination = 'https://open.spotify.com'
                    WHERE slug = 'bot-podcast' AND destination NOT LIKE 'https://%'
                """)
    except Exception as e:
        import logging
        logging.warning("_init_tracking_tables error: %s", e)
    finally:
        conn.close()


def _fetch_export(tag_filter="", location_filter=""):
    conn = _get_db()
    if not conn:
        return []
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if tag_filter and location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                      AND LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (tag_filter.lower(), f"%{location_filter.lower()}%"))
            elif tag_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                """, (tag_filter.lower(),))
            elif location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (f"%{location_filter.lower()}%",))
            else:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts ORDER BY created_at DESC
                """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@admin_bp.route("/admin/export")
def admin_export():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    tag_filter = request.args.get("tag", "").strip().lower()
    location_filter = request.args.get("location", "").strip()

    rows = _fetch_export(tag_filter=tag_filter, location_filter=location_filter)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["phone_number", "source", "fan_memory", "fan_tags", "fan_location", "joined_at"])
    for r in rows:
        writer.writerow([
            r["phone_number"],
            r.get("source") or "",
            r.get("fan_memory") or "",
            ", ".join(r.get("fan_tags") or []),
            r.get("fan_location") or "",
            r["created_at"].strftime("%Y-%m-%d") if r.get("created_at") else "",
        ])

    parts = []
    if tag_filter:
        parts.append(tag_filter)
    if location_filter:
        parts.append(location_filter.replace(" ", "-").lower())
    if not parts:
        parts.append("all-fans")
    filename = f"zarna-fans-{'_'.join(parts)}-{datetime.now().strftime('%Y%m%d')}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@admin_bp.route("/admin/export/thread")
def admin_export_thread():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    thread_phone = request.args.get("thread", "").strip()
    if not thread_phone:
        return Response("Missing thread (phone) parameter.", 400)

    conn = _get_db()
    if not conn:
        return Response("No database.", 503)
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT phone_number, role, text, created_at
                FROM messages
                WHERE phone_number = %s
                ORDER BY created_at ASC
            """, (thread_phone,))
            rows = cur.fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["phone_number", "role", "text", "created_at_utc"])
    for r in rows:
        writer.writerow([
            r["phone_number"],
            r["role"],
            r["text"],
            r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else "",
        ])

    tail = "".join(c for c in thread_phone if c.isdigit())[-4:] or "thread"
    filename = f"zarna-conversation-{tail}-{datetime.now().strftime('%Y%m%d')}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


_tlog = __import__("logging").getLogger(__name__)


@admin_bp.route("/t/<slug>")
def track_redirect(slug: str):
    """
    Public tracked-link redirect — no auth required.
    Logs an anonymous click then 302s to the real destination.
    """
    _init_tracking_tables()
    destination = None
    conn = _get_db()
    if not conn:
        _tlog.error("track_redirect: no DB connection for slug=%r", slug)
        return "Link not found", 404
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, destination FROM tracked_links WHERE slug=%s", (slug,)
            )
            row = cur.fetchone()
        if not row:
            _tlog.warning("track_redirect: slug=%r not found in tracked_links", slug)
            conn.close()
            return "Link not found", 404
        link_id, destination = row[0], row[1]
    except Exception as e:
        _tlog.error("track_redirect: DB lookup error for slug=%r: %s", slug, e)
        conn.close()
        return "Link not found", 404

    # Always redirect — log the click separately so an error there never breaks the redirect
    try:
        ip_raw = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        ip_hash = hashlib.sha256(ip_raw.encode()).hexdigest()[:16] if ip_raw else ""
        ua_short = (request.user_agent.string or "")[:120]
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tracked_link_clicks (link_id, ip_hash, ua_short) VALUES (%s,%s,%s)",
                    (link_id, ip_hash, ua_short),
                )
        _tlog.info("track_redirect: logged click for slug=%r link_id=%s", slug, link_id)
    except Exception as e:
        _tlog.error("track_redirect: failed to log click for slug=%r link_id=%s: %s", slug, link_id, e)
    finally:
        conn.close()

    return _redirect(destination, 302)


@admin_bp.route("/admin/conversions/new", methods=["POST"])
def conversions_new():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()
    _init_tracking_tables()

    label = request.form.get("label", "").strip()[:200]
    campaign_type = request.form.get("campaign_type", "other").strip()
    if campaign_type not in ("ticket", "podcast", "promo", "other"):
        campaign_type = "other"
    destination = request.form.get("destination", "").strip()

    if not label or not destination:
        return _redirect("/admin?tab=conversions&cerr=missing")
    if not destination.startswith(("http://", "https://")):
        return _redirect("/admin?tab=conversions&cerr=badurl")

    slug = _secrets.token_urlsafe(6)
    conn = _get_db()
    if not conn:
        return "No DB", 503
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tracked_links (slug, label, campaign_type, destination) "
                    "VALUES (%s, %s, %s, %s)",
                    (slug, label, campaign_type, destination),
                )
    finally:
        conn.close()
    return _redirect(f"/admin?tab=conversions&cnew={slug}")


@admin_bp.route("/admin/conversions/<int:link_id>/delete", methods=["POST"])
def conversions_delete(link_id: int):
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    conn = _get_db()
    if not conn:
        return "No DB", 503
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM tracked_links WHERE id=%s", (link_id,))
    finally:
        conn.close()
    return _redirect("/admin?tab=conversions")


def _fetch_dashboard(
    tab: str,
    chart_days: int,
    tag_filter: str,
    location_filter: str,
    thread_phone: str,
    inbox_phone_q: str,
    msg_body_q: str,
    inbox_page: int,
    thread_page: int,
    insights_days: int = 30,
    insights_era: str = "post",
):
    conn = _get_db()
    if not conn:
        return None
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            total_subscribers = total_messages = messages_today = messages_week = 0
            messages_prev_week = new_subscribers_week = new_subscribers_prev_week = 0
            profiled_fans = 0
            messages_by_day = []
            messages_by_hour = []
            top_messages = []
            top_area_codes = []
            tag_breakdown = []
            fan_profiles = []
            messages_last_hour = 0
            inbox_rows = []
            thread_rows = []
            thread_total = 0

            # ── Core counts & charts (overview + shared) ──────────────────
            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
            total_subscribers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role = 'user'")
            total_messages = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '1 hour'"
            )
            messages_last_hour = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '24 hours'"
            )
            messages_today = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '7 days'"
            )
            messages_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE role='user' AND created_at >= NOW()-INTERVAL '14 days' AND created_at < NOW()-INTERVAL '7 days'
            """)
            messages_prev_week = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE created_at >= NOW()-INTERVAL '7 days'"
            )
            new_subscribers_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(DISTINCT phone_number) FROM contacts
                WHERE created_at >= NOW()-INTERVAL '14 days' AND created_at < NOW()-INTERVAL '7 days'
            """)
            new_subscribers_prev_week = cur.fetchone()[0]

            cur.execute("""
                SELECT DATE(created_at AT TIME ZONE 'America/New_York') as day, COUNT(*) as cnt
                FROM messages
                WHERE role='user' AND created_at >= NOW()- make_interval(days => %s)
                GROUP BY day ORDER BY day
            """, (chart_days,))
            messages_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]

            cur.execute("""
                SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int as hr, COUNT(*) as cnt
                FROM messages
                WHERE role='user' AND created_at >= NOW()- make_interval(days => %s)
                GROUP BY hr ORDER BY hr
            """, (chart_days,))
            hour_map = {r["hr"]: r["cnt"] for r in cur.fetchall()}
            messages_by_hour = [hour_map.get(h, 0) for h in range(24)]

            cur.execute("""
                SELECT LOWER(TRIM(text)) as msg, COUNT(*) as cnt
                FROM messages
                WHERE role='user' AND created_at >= NOW()- make_interval(days => %s)
                GROUP BY LOWER(TRIM(text))
                ORDER BY cnt DESC LIMIT 20
            """, (chart_days,))
            top_messages = [(r["msg"], r["cnt"]) for r in cur.fetchall()]

            cur.execute("SELECT phone_number FROM contacts")
            all_phones = [r[0] for r in cur.fetchall()]
            area_codes = Counter()
            for p in all_phones:
                digits = "".join(c for c in p if c.isdigit())
                if len(digits) == 11 and digits[0] == "1":
                    area_codes[digits[1:4]] += 1
                elif len(digits) == 10:
                    area_codes[digits[:3]] += 1
            top_area_codes = area_codes.most_common(15)

            cur.execute("""
                SELECT UNNEST(fan_tags) as tag, COUNT(*) as cnt
                FROM contacts
                WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                GROUP BY tag ORDER BY cnt DESC LIMIT 30
            """)
            tag_breakdown = [(r["tag"], r["cnt"]) for r in cur.fetchall()]

            if tag_filter and location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE %s = ANY(fan_tags) AND LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (tag_filter.lower(), f"%{location_filter.lower()}%"))
            elif tag_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                """, (tag_filter.lower(),))
            elif location_filter:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                """, (f"%{location_filter.lower()}%",))
            else:
                cur.execute("""
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at, source
                    FROM contacts
                    WHERE fan_memory IS NOT NULL AND fan_memory != ''
                    ORDER BY created_at DESC LIMIT 100
                """)
            fan_profiles = [dict(r) for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*) FROM contacts WHERE fan_memory IS NOT NULL AND fan_memory != ''"
            )
            profiled_fans = cur.fetchone()[0]

            # ── Conversions tab ───────────────────────────────────────────
            tracked_links_rows = []
            conv_clicks_by_day = []
            conv_summary = {"total_links": 0, "total_clicks": 0, "clicks_week": 0, "top_label": "—"}
            if tab == "conversions":
                _init_tracking_tables()
                try:
                    cur.execute("""
                        SELECT tl.id, tl.slug, tl.label, tl.campaign_type, tl.destination,
                               tl.created_at, COALESCE(tl.sent_to, 0) AS sent_to,
                               COUNT(tlc.id)                                                AS total_clicks,
                               COUNT(tlc.id) FILTER (WHERE tlc.clicked_at >= NOW()-INTERVAL '7 days') AS clicks_7d
                        FROM   tracked_links tl
                        LEFT JOIN tracked_link_clicks tlc ON tlc.link_id = tl.id
                        GROUP  BY tl.id
                        ORDER  BY total_clicks DESC, tl.created_at DESC
                    """)
                    tracked_links_rows = [dict(r) for r in cur.fetchall()]
                except Exception:
                    tracked_links_rows = []

                conv_summary["total_links"] = len(tracked_links_rows)
                conv_summary["total_clicks"] = sum(r["total_clicks"] for r in tracked_links_rows)
                conv_summary["clicks_week"] = sum(r["clicks_7d"] for r in tracked_links_rows)
                if tracked_links_rows:
                    top = tracked_links_rows[0]
                    conv_summary["top_label"] = (top["label"] or top["slug"])[:40]

                try:
                    cur.execute("""
                        SELECT DATE(clicked_at AT TIME ZONE 'America/New_York') AS day,
                               COUNT(*) AS cnt
                        FROM   tracked_link_clicks
                        WHERE  clicked_at >= NOW() - INTERVAL '30 days'
                        GROUP  BY day ORDER BY day
                    """)
                    conv_clicks_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]
                except Exception:
                    conv_clicks_by_day = []

            # ── Insights tab ──────────────────────────────────────────────
            insights_summary = {}
            insights_intent = []
            insights_tone = []
            insights_dropoff = []
            insights_session = {}
            insights_scored_total = 0
            insights_impact = {}
            insights_blasts = []
            _BOT_LAUNCH = "2026-03-27"  # first bot reply date — used for pre/post comparison
            if tab == "insights":
                # ── Bot engagement impact ─────────────────────────────────
                try:
                    cur.execute(
                        """
                        SELECT
                          -- Pre-bot list (subscribed before launch)
                          (SELECT COUNT(*) FROM contacts
                           WHERE created_at < %s)                              AS pre_bot_list,

                          -- New subscribers (subscribed on/after launch)
                          (SELECT COUNT(*) FROM contacts
                           WHERE created_at >= %s)                             AS post_bot_list,

                          -- Legacy subs (pre-March 27) who have since texted the bot
                          (SELECT COUNT(DISTINCT m.phone_number)
                           FROM messages m JOIN contacts c ON c.phone_number = m.phone_number
                           WHERE m.role = 'user' AND m.created_at >= %s
                             AND c.created_at < %s)                           AS legacy_engaged,

                          -- New subs (post-March 27) who have texted the bot
                          (SELECT COUNT(DISTINCT m.phone_number)
                           FROM messages m JOIN contacts c ON c.phone_number = m.phone_number
                           WHERE m.role = 'user' AND m.created_at >= %s
                             AND c.created_at >= %s)                          AS new_sub_engaged,

                          -- Pre-bot: fans with 3+ messages (csv_import = same filter as pre-bot era)
                          (SELECT COUNT(*) FROM (
                               SELECT phone_number FROM messages
                               WHERE role = 'user' AND source = 'csv_import'
                               GROUP BY phone_number HAVING COUNT(*) >= 3
                           ) AS pre_deep_fans)                                AS pre_deep_convo_fans,

                          -- Post-bot: fans with 3+ user messages
                          (SELECT COUNT(*) FROM (
                               SELECT phone_number FROM messages
                               WHERE role = 'user' AND created_at >= %s
                               GROUP BY phone_number HAVING COUNT(*) >= 3
                           ) AS post_deep_fans)                               AS post_deep_convo_fans,

                          -- Pre-bot: fans with 5+ messages (csv_import)
                          (SELECT COUNT(*) FROM (
                               SELECT phone_number FROM messages
                               WHERE role = 'user' AND source = 'csv_import'
                               GROUP BY phone_number HAVING COUNT(*) >= 5
                           ) AS pre_super_fans)                               AS pre_super_deep_convo_fans,

                          -- Post-bot: fans with 5+ user messages
                          (SELECT COUNT(*) FROM (
                               SELECT phone_number FROM messages
                               WHERE role = 'user' AND created_at >= %s
                               GROUP BY phone_number HAVING COUNT(*) >= 5
                           ) AS post_super_fans)                              AS post_super_deep_convo_fans,

                          -- Pre-bot: unique fans who sent at least 1 message (csv_import)
                          (SELECT COUNT(DISTINCT phone_number) FROM messages
                           WHERE role = 'user' AND source = 'csv_import')     AS pre_engaging_fans,

                          -- Post-bot: unique fans who sent at least 1 message
                          (SELECT COUNT(DISTINCT phone_number) FROM messages
                           WHERE role = 'user' AND created_at >= %s)          AS post_engaging_fans,

                          -- Unique fans who received a bot reply
                          (SELECT COUNT(DISTINCT phone_number) FROM messages
                           WHERE role = 'assistant' AND created_at >= %s)     AS bot_replied_fans,

                          -- Earliest subscriber date for context
                          (SELECT MIN(created_at)::date FROM contacts)        AS earliest_sub_date
                        """,
                        (
                            _BOT_LAUNCH, _BOT_LAUNCH,   # pre/post_bot_list
                            _BOT_LAUNCH, _BOT_LAUNCH,   # legacy_engaged
                            _BOT_LAUNCH, _BOT_LAUNCH,   # new_sub_engaged
                            _BOT_LAUNCH,                # post_deep_convo_fans
                            _BOT_LAUNCH,                # post_super_deep_convo_fans
                            _BOT_LAUNCH,                # post_engaging_fans
                            _BOT_LAUNCH,                # bot_replied_fans
                        ),
                    )
                    row = cur.fetchone()
                    if row:
                        pre_list        = row[0] or 0
                        post_list       = row[1] or 1
                        legacy_engaged  = row[2] or 0
                        new_engaged     = row[3] or 0
                        pre_deep_convos       = row[4] or 0
                        post_deep_convos      = row[5] or 0
                        pre_super_convos      = row[6] or 0
                        post_super_convos     = row[7] or 0
                        pre_engaging_fans     = row[8] or 1
                        post_engaging_fans    = row[9] or 1
                        bot_replied           = row[10] or 1
                        earliest_date         = row[11]
                        earliest_year         = earliest_date.year if earliest_date else 2022
                        insights_impact = {
                            "pre_bot_list": pre_list,
                            "post_bot_list": post_list,
                            "legacy_engaged": legacy_engaged,
                            "new_engaged": new_engaged,
                            "legacy_pct": round(legacy_engaged / max(pre_list, 1) * 100, 1),
                            "new_pct": round(new_engaged / max(post_list, 1) * 100, 1),
                            "pre_deep_convo_fans": pre_deep_convos,
                            "post_deep_convo_fans": post_deep_convos,
                            "pre_deep_convo_pct": round(pre_deep_convos / max(pre_engaging_fans, 1) * 100, 1),
                            "post_deep_convo_pct": round(post_deep_convos / max(post_engaging_fans, 1) * 100, 1),
                            "pre_super_deep_fans": pre_super_convos,
                            "post_super_deep_fans": post_super_convos,
                            "pre_super_deep_pct": round(pre_super_convos / max(pre_engaging_fans, 1) * 100, 1),
                            "post_super_deep_pct": round(post_super_convos / max(post_engaging_fans, 1) * 100, 1),
                            "pre_engaging_fans": pre_engaging_fans,
                            "post_engaging_fans": post_engaging_fans,
                            "bot_replied_fans": bot_replied,
                            "earliest_year": earliest_year,
                        }
                except Exception:
                    conn.rollback()

                # Build date filter clause based on era toggle
                _BOT_LAUNCH_STR = "2026-03-27"
                _idays = int(insights_days)
                if insights_era == "pre":
                    # Pre-bot: only CSV-imported rows
                    _date_filter = "source = 'csv_import'"
                    _session_date_filter = f"started_at < '{_BOT_LAUNCH_STR}'"
                else:
                    # Post-bot: exclude CSV imports and blast messages (not AI conversations)
                    _date_filter = (
                        f"created_at >= NOW() - INTERVAL '{_idays} days' "
                        f"AND source IS DISTINCT FROM 'csv_import' "
                        f"AND source IS DISTINCT FROM 'blast'"
                    )
                    _session_date_filter = f"started_at >= NOW() - INTERVAL '{_idays} days'"

                try:
                    cur.execute(
                        f"""
                        SELECT
                          COUNT(*)                                           AS scored_bot_replies,
                          ROUND(AVG(did_user_reply::int) * 100, 1)          AS reply_rate_pct,
                          ROUND(
                            100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                            / NULLIF(COUNT(*), 0),
                            1
                          )                                                  AS dropoff_rate_pct,
                          ROUND(AVG(reply_delay_seconds), 0)                 AS avg_reply_delay_s,
                          ROUND(AVG(reply_length_chars), 0)                  AS avg_bot_reply_length
                        FROM messages
                        WHERE role = 'assistant'
                          AND did_user_reply IS NOT NULL
                          {"AND COALESCE(intent, 'general') != 'general'" if insights_era == 'post' else ""}
                          AND {_date_filter}
                        """
                    )
                    row = cur.fetchone()
                    if row:
                        insights_summary = dict(zip(
                            ["scored_bot_replies", "reply_rate_pct", "dropoff_rate_pct",
                             "avg_reply_delay_s", "avg_bot_reply_length"],
                            row,
                        ))
                        insights_scored_total = insights_summary.get("scored_bot_replies") or 0
                except Exception:
                    conn.rollback()

                try:
                    cur.execute(
                        f"""
                        SELECT
                          COALESCE(intent, 'unknown')                   AS intent,
                          COUNT(*)                                       AS total,
                          ROUND(AVG(did_user_reply::int) * 100, 1)      AS reply_rate_pct,
                          ROUND(
                            100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                            / NULLIF(COUNT(*), 0),
                            1
                          )                                              AS dropoff_rate_pct,
                          ROUND(AVG(reply_delay_seconds), 0)             AS avg_delay_s
                        FROM messages
                        WHERE role = 'assistant'
                          AND did_user_reply IS NOT NULL
                          AND source IS DISTINCT FROM 'csv_import'
                          AND source IS DISTINCT FROM 'blast'
                          AND {_date_filter}
                        GROUP BY COALESCE(intent, 'unknown')
                        ORDER BY reply_rate_pct DESC NULLS LAST
                        """
                    )
                    insights_intent = [
                        dict(zip(["intent", "total", "reply_rate_pct", "dropoff_rate_pct", "avg_delay_s"], r))
                        for r in cur.fetchall()
                    ]
                except Exception:
                    conn.rollback()

                try:
                    cur.execute(
                        f"""
                        SELECT
                          COALESCE(tone_mode, 'unknown')                AS tone_mode,
                          COUNT(*)                                       AS total,
                          ROUND(AVG(did_user_reply::int) * 100, 1)      AS reply_rate_pct,
                          ROUND(
                            100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                            / NULLIF(COUNT(*), 0),
                            1
                          )                                              AS dropoff_rate_pct
                        FROM messages
                        WHERE role = 'assistant'
                          AND did_user_reply IS NOT NULL
                          AND source IS DISTINCT FROM 'csv_import'
                          AND source IS DISTINCT FROM 'blast'
                          AND {_date_filter}
                        GROUP BY COALESCE(tone_mode, 'unknown')
                        ORDER BY reply_rate_pct DESC NULLS LAST
                        """
                    )
                    insights_tone = [
                        dict(zip(["tone_mode", "total", "reply_rate_pct", "dropoff_rate_pct"], r))
                        for r in cur.fetchall()
                    ]
                except Exception:
                    conn.rollback()

                try:
                    cur.execute(
                        f"""
                        SELECT LEFT(text, 180) AS preview, intent, tone_mode, reply_length_chars
                        FROM messages
                        WHERE role = 'assistant'
                          AND went_silent_after = TRUE
                          AND {_date_filter}
                        ORDER BY created_at DESC
                        LIMIT 15
                        """
                    )
                    insights_dropoff = [
                        dict(zip(["preview", "intent", "tone_mode", "reply_length_chars"], r))
                        for r in cur.fetchall()
                    ]
                except Exception:
                    conn.rollback()

                try:
                    cur.execute(
                        f"""
                        SELECT
                          COUNT(*)                                                AS total_sessions,
                          ROUND(AVG(user_message_count), 1)                      AS avg_user_msgs,
                          MAX(user_message_count + bot_message_count)             AS max_depth,
                          COUNT(*) FILTER (WHERE came_back_within_7d = TRUE)      AS came_back_7d,
                          COUNT(*) FILTER (WHERE ended_at IS NOT NULL)            AS closed_sessions
                        FROM conversation_sessions
                        WHERE {_session_date_filter}
                        """
                    )
                    row = cur.fetchone()
                    if row:
                        insights_session = dict(zip(
                            ["total_sessions", "avg_user_msgs", "max_depth",
                             "came_back_7d", "closed_sessions"], row,
                        ))
                    else:
                        insights_session = {}
                except Exception:
                    conn.rollback()
                    insights_session = {}

                # ── Blast performance ─────────────────────────────────────
                insights_blasts = []
                try:
                    cur.execute(
                        f"""
                        SELECT
                          bd.id,
                          bd.name,
                          bd.sent_at,
                          bd.sent_count,
                          bd.tracked_link_slug,
                          COALESCE(bd.opt_out_count, 0) AS opt_out_count,
                          bd.manual_link_clicks,
                          bd.blast_category,
                          -- replies within 24h, only from contacts who existed at blast time.
                          -- csv_import is included so pre-bot blasts count replies from the CSV history.
                          (SELECT COUNT(DISTINCT m.phone_number)
                           FROM messages m
                           JOIN contacts c ON c.phone_number = m.phone_number
                           WHERE m.role = 'user'
                             AND m.source IS DISTINCT FROM 'blast'
                             AND m.created_at >= bd.sent_at
                             AND m.created_at <  bd.sent_at + INTERVAL '24 hours'
                             AND c.created_at  <= bd.sent_at
                          ) AS replies_24h,
                          -- tracked link clicks (for our own blasts)
                          COALESCE((
                            SELECT COUNT(*)
                            FROM tracked_links tl
                            JOIN tracked_link_clicks tlc ON tlc.link_id = tl.id
                            WHERE tl.slug = bd.tracked_link_slug
                              AND bd.tracked_link_slug <> ''
                          ), 0) AS tracked_clicks,
                          COALESCE((
                            SELECT tl.sent_to FROM tracked_links tl
                            WHERE tl.slug = bd.tracked_link_slug
                              AND bd.tracked_link_slug <> ''
                          ), 0) AS link_sent_to
                        FROM blast_drafts bd
                        WHERE bd.status = 'sent'
                          AND bd.sent_at IS NOT NULL
                          AND bd.sent_at {'<' if insights_era == 'pre' else '>='} %s
                        ORDER BY bd.sent_at DESC
                        LIMIT 50
                        """,
                        (_BOT_LAUNCH,)
                    )
                    cols = ["id", "name", "sent_at", "sent_count", "tracked_link_slug",
                            "opt_out_count", "manual_link_clicks", "blast_category",
                            "replies_24h", "tracked_clicks", "link_sent_to"]
                    for r in cur.fetchall():
                        row = dict(zip(cols, r))
                        sc  = row["sent_count"] or 0
                        rep = min(row["replies_24h"] or 0, sc)  # cap at sent_count
                        row["replies_24h"]    = rep
                        row["reply_rate_pct"] = round(rep / sc * 100, 1) if sc else 0
                        # Prefer manual_link_clicks (external blasts) over tracked DB clicks
                        mlc = row["manual_link_clicks"]
                        if mlc is not None:
                            row["link_clicks"] = mlc
                            denom = sc
                        else:
                            row["link_clicks"] = row["tracked_clicks"]
                            denom = row["link_sent_to"] or sc
                        has_link = (mlc is not None) or row["tracked_link_slug"]
                        row["ctr_pct"] = round(row["link_clicks"] / denom * 100, 1) if (denom and has_link) else None
                        oc = row["opt_out_count"] or 0
                        row["unsub_rate_pct"] = round(oc / sc * 100, 2) if sc else None
                        row["sent_at_str"] = row["sent_at"].strftime("%b %-d, %Y") if row["sent_at"] else "—"
                        insights_blasts.append(row)
                except Exception:
                    conn.rollback()
                    insights_blasts = []

            # ── Conversations tab ─────────────────────────────────────────
            if tab == "convos":
                inbox_off = max(0, inbox_page) * INBOX_PAGE_SIZE
                thread_off = max(0, thread_page) * THREAD_PAGE_SIZE

                if thread_phone:
                    cur.execute(
                        "SELECT COUNT(*) FROM messages WHERE phone_number = %s",
                        (thread_phone,),
                    )
                    thread_total = cur.fetchone()[0]
                    cur.execute("""
                        SELECT phone_number, role, text, created_at
                        FROM messages
                        WHERE phone_number = %s
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, (thread_phone, THREAD_PAGE_SIZE, thread_off))
                    rows_desc = [dict(r) for r in cur.fetchall()]
                    thread_rows = list(reversed(rows_desc))
                else:
                    phone_like = f"%{inbox_phone_q}%" if inbox_phone_q else "%"
                    if msg_body_q:
                        pat = f"%{msg_body_q}%"
                        cur.execute("""
                            WITH latest AS (
                                SELECT DISTINCT ON (m.phone_number)
                                    m.phone_number, m.role, m.text, m.created_at
                                FROM messages m
                                INNER JOIN (
                                    SELECT DISTINCT phone_number FROM messages WHERE text ILIKE %s
                                ) hit ON hit.phone_number = m.phone_number
                                ORDER BY m.phone_number, m.created_at DESC
                            )
                            SELECT * FROM latest
                            WHERE phone_number LIKE %s
                            ORDER BY created_at DESC
                            LIMIT %s OFFSET %s
                        """, (pat, phone_like, INBOX_PAGE_SIZE, inbox_off))
                    else:
                        cur.execute("""
                            SELECT * FROM (
                                SELECT DISTINCT ON (phone_number)
                                    phone_number, role, text, created_at
                                FROM messages
                                WHERE phone_number LIKE %s
                                ORDER BY phone_number, created_at DESC
                            ) sub
                            ORDER BY created_at DESC
                            LIMIT %s OFFSET %s
                        """, (phone_like, INBOX_PAGE_SIZE, inbox_off))
                    inbox_rows = [dict(r) for r in cur.fetchall()]

        return {
            "total_subscribers": total_subscribers,
            "total_messages": total_messages,
            "messages_today": messages_today,
            "messages_week": messages_week,
            "messages_prev_week": messages_prev_week,
            "new_subscribers_week": new_subscribers_week,
            "new_subscribers_prev_week": new_subscribers_prev_week,
            "profiled_fans": profiled_fans,
            "messages_by_day": messages_by_day,
            "messages_by_hour": messages_by_hour,
            "top_messages": top_messages,
            "top_area_codes": top_area_codes,
            "tag_breakdown": tag_breakdown,
            "fan_profiles": fan_profiles,
            "tag_filter": tag_filter,
            "messages_last_hour": messages_last_hour,
            "inbox_rows": inbox_rows,
            "thread_rows": thread_rows,
            "thread_total": thread_total,
            "chart_days": chart_days,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            # conversions
            "tracked_links_rows": tracked_links_rows,
            "conv_clicks_by_day": conv_clicks_by_day,
            "conv_summary": conv_summary,
            # insights
            "insights_summary": insights_summary,
            "insights_intent": insights_intent,
            "insights_tone": insights_tone,
            "insights_dropoff": insights_dropoff,
            "insights_scored_total": insights_scored_total,
            "insights_session": insights_session,
            "insights_impact": insights_impact,
            "insights_blasts": insights_blasts,
        }
    finally:
        conn.close()


def _trend_html(current, previous, label="vs last week"):
    if previous == 0:
        return f'<span style="color:#64748b;font-size:12px">{label}</span>'
    diff = current - previous
    pct = round(abs(diff) / previous * 100)
    if diff > 0:
        return f'<span style="color:#10b981;font-size:12px">▲ {pct}% {label}</span>'
    elif diff < 0:
        return f'<span style="color:#f87171;font-size:12px">▼ {pct}% {label}</span>'
    else:
        return f'<span style="color:#64748b;font-size:12px">— same {label}</span>'


def _range_links(active: int) -> str:
    parts = []
    for d in (7, 14, 30, 90):
        cls = "range-pill-active" if d == active else "range-pill"
        parts.append(f'<a class="{cls}" href="/admin?tab=overview&range={d}">{d}d</a>')
    return " ".join(parts)


def _render_impact_section(impact: dict, era: str = "post") -> str:
    """Before/after bot impact banner."""
    if not impact:
        return ""
    pre_list        = impact.get("pre_bot_list", 0)
    post_list       = impact.get("post_bot_list", 0)
    legacy_engaged  = impact.get("legacy_engaged", 0)
    new_engaged     = impact.get("new_engaged", 0)
    legacy_pct      = impact.get("legacy_pct", 0)
    new_pct         = impact.get("new_pct", 0)
    pre_deep_pct         = impact.get("pre_deep_convo_pct", 0)
    post_deep_pct        = impact.get("post_deep_convo_pct", 0)
    pre_deep_fans        = impact.get("pre_deep_convo_fans", 0)
    post_deep_fans       = impact.get("post_deep_convo_fans", 0)
    pre_super_pct        = impact.get("pre_super_deep_pct", 0)
    post_super_pct       = impact.get("post_super_deep_pct", 0)
    pre_super_fans       = impact.get("pre_super_deep_fans", 0)
    post_super_fans      = impact.get("post_super_deep_fans", 0)
    pre_engaging_fans    = impact.get("pre_engaging_fans", 0)
    post_engaging_fans   = impact.get("post_engaging_fans", 0)
    bot_replied          = impact.get("bot_replied_fans", 0)

    # Show era-appropriate values for deep/super deep convos
    deep_pct      = pre_deep_pct   if era == "pre" else post_deep_pct
    deep_fans     = pre_deep_fans  if era == "pre" else post_deep_fans
    super_pct     = pre_super_pct  if era == "pre" else post_super_pct
    super_fans    = pre_super_fans if era == "pre" else post_super_fans
    engaging_fans = pre_engaging_fans if era == "pre" else post_engaging_fans
    earliest_year   = impact.get("earliest_year", 2022)

    def _bar(pct, color):
        w = min(100, max(0, float(pct or 0)))
        return (
            f'<div style="background:#1f2937;border-radius:4px;height:8px;margin-top:6px;">'
            f'<div style="width:{w}%;background:{color};height:8px;border-radius:4px;'
            f'transition:width .4s;"></div></div>'
        )

    return f"""
    <div style="background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 100%);
                border:1px solid #312e81;border-radius:14px;padding:22px 24px;margin-bottom:22px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;color:#818cf8;
                  text-transform:uppercase;margin-bottom:14px;">
        Bot Impact — Before vs After March 27
      </div>
      <div style="display:grid;grid-template-columns:1fr 1px 1fr 1px 1fr 1px 1fr 1px 1fr;gap:0;align-items:start;">

        <div style="padding-right:20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Legacy subscribers (since {earliest_year})</div>
          <div style="font-size:28px;font-weight:800;color:#f87171;">{legacy_pct}%</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            {legacy_engaged:,} of {pre_list:,} SMS-only fans tried the bot
          </div>
          {_bar(legacy_pct, "#f87171")}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding:0 20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">New subscribers (post-March 27)</div>
          <div style="font-size:28px;font-weight:800;color:#4ade80;">{new_pct}%</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            {new_engaged:,} of {post_list:,} new subs texted the bot
          </div>
          {_bar(new_pct, "#4ade80")}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding:0 20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Deep convos (3+ msgs)</div>
          <div style="font-size:28px;font-weight:800;color:#a78bfa;">{deep_pct}%</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            {deep_fans:,} of {engaging_fans:,} fans who replied
          </div>
          {_bar(deep_pct, "#a78bfa")}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding:0 20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Super deep convos (5+ msgs)</div>
          <div style="font-size:28px;font-weight:800;color:#f472b6;">{super_pct}%</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            {super_fans:,} of {engaging_fans:,} fans who replied
          </div>
          {_bar(super_pct, "#f472b6")}
        </div>

        <div style="background:#1f2937;"></div>

        <div style="padding-left:20px;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.06em;
                      margin-bottom:4px;">Bot replied to</div>
          <div style="font-size:28px;font-weight:800;color:#60a5fa;">{bot_replied:,}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px;">
            unique fans since launch
          </div>
        </div>

      </div>
      <div style="margin-top:14px;padding-top:12px;border-top:1px solid #1f2937;
                  font-size:11px;color:#4b5563;">
        Goal: pre-bot % stays low, post-bot penetration % and deep convo % grow show over show.
      </div>
    </div>"""


def _render_insights_tab(stats: dict, insights_days: int = 30, insights_era: str = "post") -> str:
    """Return the inner HTML for the 🧠 Insights tab."""
    s = stats["insights_summary"]
    scored = stats["insights_scored_total"]

    if not scored:
        if insights_era == "pre":
            empty_msg = (
                "<div style='font-size:16px;font-weight:600;color:#9ca3af;margin-bottom:8px;'>"
                "No AI conversations before March 27</div>"
                "<p style='font-size:13px;max-width:480px;margin:0 auto;line-height:1.6;'>"
                "Before the bot launched, there were no AI-driven conversations — only one-way SMS blasts. "
                "This is the baseline: 0% reply rate, 0 scored replies. Switch to <b>Post-bot</b> to see what the AI changed."
                "</p>"
            )
        else:
            empty_msg = (
                "<div style='font-size:16px;font-weight:600;color:#9ca3af;margin-bottom:8px;'>"
                "No engagement data yet</div>"
                "<p style='font-size:13px;max-width:420px;margin:0 auto;line-height:1.6;'>"
                "Data starts accumulating as fans text in. Come back after your next show."
                "</p>"
            )
        _era_bar = (
            f'<div style="display:flex;gap:8px;justify-content:center;margin-top:16px;">'
            f'<a href="/admin?tab=insights&era=pre" style="padding:6px 18px;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;'
            f'{"background:#f87171;color:#fff;" if insights_era == "pre" else "background:#1f2937;color:#94a3b8;"}">'
            f'Pre-bot</a>'
            f'<a href="/admin?tab=insights&era=post" style="padding:6px 18px;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;'
            f'{"background:#6366f1;color:#fff;" if insights_era == "post" else "background:#1f2937;color:#94a3b8;"}">'
            f'Post-bot</a></div>'
        )
        return f"""
        <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;
                    padding:40px;text-align:center;color:#6b7280;margin-top:8px;">
          <div style="font-size:32px;margin-bottom:12px;">🧠</div>
          {empty_msg}
          {_era_bar}
        </div>"""

    def _pct_color(v):
        if v is None:
            return "#6b7280"
        return "#4ade80" if v >= 60 else ("#fbbf24" if v >= 40 else "#f87171")

    def _drop_color(v):
        if v is None:
            return "#6b7280"
        return "#f87171" if v >= 30 else ("#fbbf24" if v >= 15 else "#4ade80")

    reply_rate = s.get("reply_rate_pct")
    dropoff    = s.get("dropoff_rate_pct")
    delay      = s.get("avg_reply_delay_s")
    avg_len    = s.get("avg_bot_reply_length")

    _era_btn_style = "padding:6px 18px;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;border:none;cursor:pointer;"
    _pre_active  = insights_era == "pre"
    _post_active = insights_era == "post"
    era_toggle_html = (
        f'<a href="/admin?tab=insights&era=pre&days={insights_days}" style="{_era_btn_style}'
        f'{"background:#f87171;color:#fff;" if _pre_active else "background:#1f2937;color:#94a3b8;"}">'
        f'Pre-bot (before Mar 27)</a>'
        f'<a href="/admin?tab=insights&era=post&days={insights_days}" style="{_era_btn_style}'
        f'{"background:#6366f1;color:#fff;" if _post_active else "background:#1f2937;color:#94a3b8;"}">'
        f'Post-bot (after Mar 27)</a>'
    )
    day_picker_html = "".join(
        f'<a href="/admin?tab=insights&era={insights_era}&days={d}" style="'
        f'padding:5px 12px;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;'
        f'{"background:#6366f1;color:#fff;" if d == insights_days else "background:#1f2937;color:#94a3b8;"}'
        f'">{d}d</a>'
        for d in (7, 14, 30)
    ) if not _pre_active else ""
    date_filter_bar = f"""
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:18px;flex-wrap:wrap;">
      <span style="color:#6b7280;font-size:13px;margin-right:4px;">Era:</span>
      {era_toggle_html}
      {"<span style='color:#374151;margin:0 6px;'>|</span><span style='color:#6b7280;font-size:13px;'>Window:</span>" + day_picker_html if day_picker_html else ""}
    </div>"""

    impact_html = _render_impact_section(stats.get("insights_impact", {}), era=insights_era)
    summary_html = impact_html + date_filter_bar + f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px;">
      <div class="stat-card">
        <div class="stat-label">Scored Replies</div>
        <div class="stat-value">{scored:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">{"before Mar 27" if insights_era == "pre" else f"last {insights_days} days"} · excl. unclassified</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Reply Rate</div>
        <div class="stat-value" style="color:{_pct_color(reply_rate)}">{reply_rate if reply_rate is not None else '—'}{'%' if reply_rate is not None else ''}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">fans who texted back · excl. unclassified</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Drop-off Rate</div>
        <div class="stat-value" style="color:{_drop_color(dropoff)}">{dropoff if dropoff is not None else '—'}{'%' if dropoff is not None else ''}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">bot msg then silence · excl. unclassified</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Reply Delay</div>
        <div class="stat-value purple">{int(delay) if delay is not None else '—'}{'s' if delay is not None else ''}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">fan response time</div>
      </div>
    </div>"""

    # Intent breakdown table
    intent_rows_html = ""
    for r in stats["insights_intent"]:
        rr = r.get("reply_rate_pct")
        dr = r.get("dropoff_rate_pct")
        d  = r.get("avg_delay_s")
        intent_rows_html += f"""
        <tr>
          <td style="padding:10px 14px;font-weight:600;color:#e2e8f0;">{_esc(str(r.get("intent","?")).upper())}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">{r.get("total",0):,}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_pct_color(rr)}">{rr if rr is not None else '—'}{'%' if rr is not None else ''}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_drop_color(dr)}">{dr if dr is not None else '—'}{'%' if dr is not None else ''}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">{int(d) if d is not None else '—'}{'s' if d is not None else ''}</td>
        </tr>"""
    if not intent_rows_html:
        intent_rows_html = f'<tr><td colspan="5" style="padding:24px;text-align:center;color:#6b7280;font-style:italic;">No data yet for last {insights_days} days.</td></tr>'

    intent_table = f"""
    <div class="card" style="margin-bottom:20px;padding:0;overflow:hidden;">
      <div style="padding:16px 20px 12px;border-bottom:1px solid #1f2937;">
        <div class="card-title" style="margin:0;">Engagement by Intent — {"Before Mar 27 (Pre-bot)" if insights_era == "pre" else f"Last {insights_days} Days"}</div>
      </div>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="border-bottom:1px solid #1f2937;">
            <th style="padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Intent</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Scored</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Reply Rate ↑</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Drop-off ↓</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Avg Delay</th>
          </tr>
        </thead>
        <tbody>{intent_rows_html}</tbody>
      </table>
    </div>"""

    # Tone breakdown table
    tone_rows_html = ""
    for r in stats["insights_tone"]:
        rr = r.get("reply_rate_pct")
        dr = r.get("dropoff_rate_pct")
        tone_rows_html += f"""
        <tr>
          <td style="padding:10px 14px;font-weight:600;color:#e2e8f0;">{_esc(str(r.get("tone_mode","?"))).title()}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">{r.get("total",0):,}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_pct_color(rr)}">{rr if rr is not None else '—'}{'%' if rr is not None else ''}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:{_drop_color(dr)}">{dr if dr is not None else '—'}{'%' if dr is not None else ''}</td>
        </tr>"""
    if not tone_rows_html:
        tone_rows_html = '<tr><td colspan="4" style="padding:24px;text-align:center;color:#6b7280;font-style:italic;">No data yet.</td></tr>'

    tone_table = f"""
    <div class="card" style="margin-bottom:20px;padding:0;overflow:hidden;">
      <div style="padding:16px 20px 12px;border-bottom:1px solid #1f2937;">
        <div class="card-title" style="margin:0;">Engagement by Tone — {"Before Mar 27 (Pre-bot)" if insights_era == "pre" else f"Last {insights_days} Days"}</div>
      </div>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="border-bottom:1px solid #1f2937;">
            <th style="padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Tone</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Scored</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Reply Rate ↑</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Drop-off ↓</th>
          </tr>
        </thead>
        <tbody>{tone_rows_html}</tbody>
      </table>
    </div>"""

    # Drop-off trigger list
    dropoff_items_html = ""
    for r in stats["insights_dropoff"]:
        preview = _esc(str(r.get("preview") or ""))
        intent  = _esc(str(r.get("intent") or "—").upper())
        tone    = _esc(str(r.get("tone_mode") or "—"))
        chars   = r.get("reply_length_chars")
        dropoff_items_html += f"""
        <div style="padding:12px 0;border-bottom:1px solid #1f2937;">
          <div style="display:flex;gap:8px;margin-bottom:6px;flex-wrap:wrap;">
            <span style="background:#1f2937;color:#94a3b8;padding:2px 8px;border-radius:8px;font-size:11px;">{intent}</span>
            <span style="background:#1f2937;color:#94a3b8;padding:2px 8px;border-radius:8px;font-size:11px;">{tone}</span>
            {'<span style="background:#1f2937;color:#94a3b8;padding:2px 8px;border-radius:8px;font-size:11px;">' + str(chars) + ' chars</span>' if chars else ''}
          </div>
          <div style="color:#d1d5db;font-size:13px;line-height:1.45;">{preview}</div>
        </div>"""

    if not dropoff_items_html:
        dropoff_items_html = '<p class="empty-note">No drop-off triggers recorded yet. Run the nightly backfill script to score older messages.</p>'

    dropoff_section = f"""
    <div class="card" style="margin-bottom:20px;">
      <div class="card-title">Drop-off Triggers — Last Bot Message Before Fan Went Silent (last 30d)</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:12px;">These are the bot messages that ended in silence — patterns here tell you what to avoid.</p>
      {dropoff_items_html}
    </div>"""

    # Session stats section
    sess = stats.get("insights_session", {})
    total_sess = sess.get("total_sessions") or 0
    avg_msgs   = sess.get("avg_user_msgs")
    max_depth  = sess.get("max_depth")
    came_back  = sess.get("came_back_7d") or 0
    closed_s   = sess.get("closed_sessions") or 0
    ret_7d     = round(came_back / closed_s * 100, 1) if closed_s else None

    session_html = ""
    if total_sess:
        session_html = f"""
        <div class="card" style="margin-bottom:20px;">
          <div class="card-title">Conversation Sessions — Last 30 Days</div>
          <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:4px;">
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">Total Sessions</div>
              <div class="stat-value">{total_sess:,}</div>
            </div>
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">Avg Fan Messages</div>
              <div class="stat-value purple">{avg_msgs if avg_msgs is not None else '—'}</div>
              <div style="color:#64748b;font-size:12px">per session</div>
            </div>
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">Deepest Session</div>
              <div class="stat-value teal">{max_depth if max_depth is not None else '—'}</div>
              <div style="color:#64748b;font-size:12px">total messages</div>
            </div>
            <div class="stat-card" style="padding:14px 16px;">
              <div class="stat-label">7-Day Return Rate</div>
              <div class="stat-value" style="color:{_pct_color(ret_7d)}">{ret_7d if ret_7d is not None else '—'}{'%' if ret_7d is not None else ''}</div>
              <div style="color:#64748b;font-size:12px">fans who came back</div>
            </div>
          </div>
          <p style="color:#64748b;font-size:12px;margin-top:12px;">
            Session = contiguous conversation. New session after {_esc(str(os.getenv('SESSION_GAP_HOURS', '24')))}h of silence.
            Run <code style="color:#a5b4fc">python scripts/backfill_silence.py</code> nightly to close stale sessions.
          </p>
        </div>"""

    api_hint = f"""
    <div class="card" style="margin-bottom:0;background:#0d0d1a;border-color:#1a1a3a;">
      <div class="card-title">JSON API — programmatic access</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:10px;">Same data in JSON format, useful for scripts and external tools. All require HTTP Basic Auth (same password).</p>
      <div style="display:flex;flex-direction:column;gap:6px;font-size:12px;">
        <code style="color:#a5b4fc;">/analytics/engagement-summary</code>
        <code style="color:#a5b4fc;">/analytics/intent-breakdown</code>
        <code style="color:#a5b4fc;">/analytics/tone-breakdown</code>
        <code style="color:#a5b4fc;">/analytics/dropoff-triggers</code>
        <code style="color:#a5b4fc;">/analytics/top-bot-replies</code>
        <code style="color:#a5b4fc;">/analytics/reply-length-buckets</code>
      </div>
      <p style="color:#4b5563;font-size:11px;margin-top:10px;">Append <code>?days=7</code> (or 14, 30, 90) to any endpoint to change the window.</p>
    </div>"""

    # ── Blasts section ────────────────────────────────────────────────────
    blasts = stats.get("insights_blasts", [])
    _admin_b64 = __import__('base64').b64encode(f'admin:{os.getenv("ADMIN_PASSWORD","")}'.encode()).decode()

    # Build all blast rows as JSON for client-side tab filtering
    import json as _json
    blasts_json = _json.dumps([{
        "id":            b["id"],
        "name":          b.get("name", ""),
        "sent_at_str":   b.get("sent_at_str", ""),
        "sent_count":    b.get("sent_count") or 0,
        "replies_24h":   b.get("replies_24h") or 0,
        "reply_rate_pct":b.get("reply_rate_pct", 0),
        "ctr_pct":       b.get("ctr_pct"),
        "link_clicks":   b.get("link_clicks") or 0,
        "unsub_rate_pct":b.get("unsub_rate_pct"),
        "opt_out_count": b.get("opt_out_count") or 0,
        "blast_category":b.get("blast_category") or "",
    } for b in blasts])

    _cat_colors = {
        "friendly": ("#22d3ee", "#0e7490"),   # cyan
        "sales":    ("#a78bfa", "#6d28d9"),   # purple
        "show":     ("#fb923c", "#c2410c"),   # orange
        "":         ("#4b5563", "#374151"),   # grey (uncategorized)
    }

    blast_section = f"""
    <script>
    const _blastData = {blasts_json};
    const _blastAuth = 'Basic {_admin_b64}';
    const _catLabels = {{ friendly:'💬 Friendly', sales:'🛒 Sales', show:'🎤 Shows', '':'Uncategorized' }};
    const _catColors = {{
      friendly:['#22d3ee','#0e7490'], sales:['#a78bfa','#6d28d9'],
      show:['#fb923c','#c2410c'],    '':['#4b5563','#374151']
    }};

    let _activeBlastTab = 'all';

    function _pctColor(p) {{
      if (p >= 30) return '#22c55e';
      if (p >= 10) return '#eab308';
      if (p >= 5)  return '#f97316';
      return '#ef4444';
    }}

    function renderBlastTable() {{
      const filter = _activeBlastTab;
      const rows = _blastData.filter(b => filter === 'all' || b.blast_category === filter);
      const tbody = document.getElementById('blast-tbody');
      if (!tbody) return;

      // Tab counts
      ['all','friendly','sales','show'].forEach(cat => {{
        const cnt = cat === 'all' ? _blastData.length
                                  : _blastData.filter(b => b.blast_category === cat).length;
        const el = document.getElementById('blast-tab-' + cat);
        if (el) el.querySelector('.btab-cnt').textContent = cnt;
        if (el) {{
          const active = cat === _activeBlastTab;
          el.style.borderBottomColor = active ? (cat === 'all' ? '#818cf8' : (_catColors[cat]||['#818cf8'])[0]) : 'transparent';
          el.style.color = active ? '#e2e8f0' : '#6b7280';
        }}
      }});

      if (!rows.length) {{
        tbody.innerHTML = '<tr><td colspan="8" style="padding:24px;text-align:center;color:#4b5563;font-size:13px;">No blasts in this category yet — assign them using the dropdown on each row.</td></tr>';
        return;
      }}

      tbody.innerHTML = rows.map(b => {{
        const rr = b.reply_rate_pct;
        const rrColor = _pctColor(rr);
        const ctrCell = b.ctr_pct == null ? '—'
          : `<span style="font-weight:700;color:#818cf8">${{b.ctr_pct}}%</span> <span style="color:#4b5563;font-size:11px">(${{b.link_clicks.toLocaleString()}} clicks)</span>`;
        const unsubCell = b.unsub_rate_pct == null ? '—'
          : `<span style="font-weight:700;color:#f87171">${{b.unsub_rate_pct}}%</span> <span style="color:#4b5563;font-size:11px">(${{b.opt_out_count}})</span>`;
        const cat = b.blast_category || '';
        const [cc, cd] = _catColors[cat] || _catColors[''];
        const catLabel = _catLabels[cat] || 'Uncategorized';
        return `<tr id="blast-row-${{b.id}}">
          <td style="padding:10px 14px;">
            <div style="font-weight:600;color:#e2e8f0;margin-bottom:4px;">${{b.name}}</div>
            <select onchange="setBlastCategory(${{b.id}}, this.value)"
              style="background:#1e293b;border:1px solid ${{cd}};color:${{cc}};
                     padding:2px 6px;border-radius:5px;font-size:11px;cursor:pointer;">
              <option value="" ${{cat===''?'selected':''}}>Uncategorized</option>
              <option value="friendly" ${{cat==='friendly'?'selected':''}}>💬 Friendly</option>
              <option value="sales" ${{cat==='sales'?'selected':''}}>🛒 Sales</option>
              <option value="show" ${{cat==='show'?'selected':''}}>🎤 Shows</option>
            </select>
          </td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{b.sent_at_str}}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{b.sent_count.toLocaleString()}}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{b.replies_24h.toLocaleString()}}</td>
          <td style="padding:10px 14px;text-align:center;font-weight:700;color:${{rrColor}}">${{rr}}%</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{ctrCell}}</td>
          <td style="padding:10px 14px;text-align:center;color:#94a3b8;">${{unsubCell}}</td>
          <td style="padding:10px 14px;text-align:center;">
            <button onclick="deleteBlast(${{b.id}})"
              style="background:transparent;border:1px solid #374151;color:#6b7280;padding:3px 10px;
                     border-radius:6px;font-size:12px;cursor:pointer;"
              onmouseover="this.style.borderColor='#f87171';this.style.color='#f87171'"
              onmouseout="this.style.borderColor='#374151';this.style.color='#6b7280'">
              Delete
            </button>
          </td>
        </tr>`;
      }}).join('');
    }}

    function setBlastCategory(id, cat) {{
      const blast = _blastData.find(b => b.id === id);
      if (blast) blast.blast_category = cat;
      fetch('/admin/actions/set-blast-category/' + id, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json', 'Authorization': _blastAuth }},
        body: JSON.stringify({{ category: cat }}),
      }}).then(r => {{ if (!r.ok) alert('Save failed'); else renderBlastTable(); }});
    }}

    function deleteBlast(id) {{
      if (!confirm('Delete this blast from analytics?')) return;
      fetch('/admin/actions/delete-blast/' + id, {{
        method: 'POST', headers: {{ 'Authorization': _blastAuth }},
      }}).then(r => {{
        if (r.ok) {{ const i = _blastData.findIndex(b => b.id===id); if (i>=0) _blastData.splice(i,1); renderBlastTable(); }}
        else alert('Delete failed');
      }});
    }}

    function submitExternalBlast() {{
      const name = document.getElementById('eb-name').value.trim();
      const date = document.getElementById('eb-date').value;
      const sent = parseInt(document.getElementById('eb-sent').value) || 0;
      const optouts = parseInt(document.getElementById('eb-optouts').value) || 0;
      const clicks = document.getElementById('eb-clicks').value.trim();
      const link_clicks = clicks !== '' ? parseInt(clicks) : null;
      if (!name || !date || !sent) {{ alert('Name, date, and sent count are required.'); return; }}
      fetch('/admin/actions/add-external-blast', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json', 'Authorization': _blastAuth }},
        body: JSON.stringify({{ name, date, sent_count: sent, opt_out_count: optouts, link_clicks }}),
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
        else alert('Error: ' + (d.error || 'unknown'));
      }});
    }}

    document.addEventListener('DOMContentLoaded', renderBlastTable);
    </script>

    <div class="card" style="margin-bottom:20px;padding:0;overflow:hidden;">
      <!-- Header -->
      <div style="padding:16px 20px 0;border-bottom:1px solid #1f2937;">
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px;">
          <div class="card-title" style="margin:0;">📣 Blast Performance</div>
          <span style="font-size:12px;color:#6b7280;">reply rate = subscribers who texted back within 24h</span>
          <button onclick="document.getElementById('add-blast-panel').style.display='block'"
            style="margin-left:auto;background:#1e293b;border:1px solid #374151;color:#94a3b8;
                   padding:5px 12px;border-radius:6px;font-size:12px;cursor:pointer;white-space:nowrap;"
            onmouseover="this.style.borderColor='#4f46e5';this.style.color='#818cf8'"
            onmouseout="this.style.borderColor='#374151';this.style.color='#94a3b8'">
            + Add external blast
          </button>
        </div>
        <!-- Category tabs -->
        <div style="display:flex;gap:0;">
          {"".join(f'''<button id="blast-tab-{cat}" onclick="_activeBlastTab='{cat}';renderBlastTable()"
            style="padding:8px 16px;background:transparent;border:none;border-bottom:2px solid transparent;
                   color:#6b7280;font-size:13px;font-weight:500;cursor:pointer;transition:all .15s;">
            {label} <span class="btab-cnt" style="background:#1e293b;border-radius:10px;
                    padding:1px 7px;font-size:11px;margin-left:4px;">0</span>
          </button>''' for cat, label in [("all","All"), ("friendly","💬 Friendly"), ("sales","🛒 Sales"), ("show","🎤 Shows")])}
        </div>
      </div>

      <!-- Add external blast form -->
      <div id="add-blast-panel" style="display:none;padding:16px 20px;border-bottom:1px solid #1f2937;background:#0f172a;">
        <div style="font-size:13px;color:#94a3b8;margin-bottom:12px;">Add an external blast (e.g. from SlickText)</div>
        <div style="display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr 1fr;gap:10px;align-items:end;">
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Blast Name</label>
            <input id="eb-name" type="text" placeholder="e.g. Zarna Voice Note"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Date Sent</label>
            <input id="eb-date" type="date"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Sent Count</label>
            <input id="eb-sent" type="number" min="0" placeholder="4238"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Opt-outs</label>
            <input id="eb-optouts" type="number" min="0" placeholder="0"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div>
            <label style="font-size:11px;color:#6b7280;display:block;margin-bottom:4px;">Link Clicks</label>
            <input id="eb-clicks" type="number" min="0" placeholder="0"
              style="width:100%;background:#1e293b;border:1px solid #374151;color:#e2e8f0;
                     padding:6px 10px;border-radius:6px;font-size:13px;box-sizing:border-box;">
          </div>
          <div style="display:flex;gap:8px;">
            <button onclick="submitExternalBlast()"
              style="flex:1;background:#4f46e5;border:none;color:#fff;padding:7px 14px;
                     border-radius:6px;font-size:13px;cursor:pointer;font-weight:600;">Add</button>
            <button onclick="document.getElementById('add-blast-panel').style.display='none'"
              style="background:transparent;border:1px solid #374151;color:#6b7280;padding:7px 12px;
                     border-radius:6px;font-size:13px;cursor:pointer;">✕</button>
          </div>
        </div>
      </div>

      <!-- Table -->
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="border-bottom:1px solid #1f2937;">
            <th style="padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Blast</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Date</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Sent</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Replies (24h)</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Reply Rate</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Link CTR</th>
            <th style="padding:10px 14px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;">Unsub Rate</th>
            <th style="padding:10px 14px;"></th>
          </tr>
        </thead>
        <tbody id="blast-tbody"><tr><td colspan="8" style="padding:20px;text-align:center;color:#4b5563;">Loading…</td></tr></tbody>
      </table>
    </div>"""

    return summary_html + intent_table + tone_table + session_html + blast_section + dropoff_section + api_hint


@admin_bp.route("/admin")
def admin():
    if not admin_password_configured():
        return _no_password_configured()
    if not _check_auth():
        return _require_auth()

    tab = request.args.get("tab", "overview").strip().lower()
    tag_filter = request.args.get("tag", "").strip().lower()
    location_filter = request.args.get("location", "").strip()
    chart_days = _safe_chart_days(request.args.get("range"))
    thread_phone = request.args.get("thread", "").strip()
    inbox_phone_q = request.args.get("phone", "").strip()
    msg_body_q = request.args.get("q", "").strip()
    try:
        insights_days = int(request.args.get("days", "30"))
        if insights_days not in (7, 14, 30):
            insights_days = 30
    except ValueError:
        insights_days = 30
    insights_era = request.args.get("era", "post").strip().lower()
    if insights_era not in ("pre", "post"):
        insights_era = "post"
    try:
        inbox_page = max(0, int(request.args.get("inbox_page", "0")))
    except ValueError:
        inbox_page = 0
    try:
        thread_page = max(0, int(request.args.get("thread_page", "0")))
    except ValueError:
        thread_page = 0

    if (tag_filter or location_filter) and tab == "overview":
        tab = "audience"

    stats = _fetch_dashboard(
        tab=tab,
        chart_days=chart_days,
        tag_filter=tag_filter,
        location_filter=location_filter,
        thread_phone=thread_phone,
        inbox_phone_q=inbox_phone_q,
        msg_body_q=msg_body_q,
        inbox_page=inbox_page,
        thread_page=thread_page,
        insights_days=insights_days,
        insights_era=insights_era,
    )
    if stats is None:
        return "<h2 style='font-family:sans-serif;padding:40px'>No database configured (DATABASE_URL not set).</h2>", 503

    days_labels = [d for d, _ in stats["messages_by_day"]]
    days_data = [c for _, c in stats["messages_by_day"]]
    hour_labels = [f"{h}" for h in range(24)]
    hour_data = stats["messages_by_hour"]

    week_trend = _trend_html(stats["messages_week"], stats["messages_prev_week"])
    sub_trend = _trend_html(
        stats["new_subscribers_week"],
        stats["new_subscribers_prev_week"],
        "new vs last week",
    )

    top_msgs_html = ""
    for i, (msg, cnt) in enumerate(stats["top_messages"], 1):
        msg_e = _esc(msg)
        top_msgs_html += f"""
        <div class="top-msg-row">
          <span class="top-msg-rank">#{i}</span>
          <span class="top-msg-text">{msg_e}</span>
          <span class="top-msg-badge">{cnt}</span>
        </div>"""

    area_html = ""
    for ac, cnt in stats["top_area_codes"]:
        area_html += f"""
        <div class="area-row">
          <span>({ac})</span>
          <span class="area-cnt">{cnt}</span>
        </div>"""

    tag_breakdown_html = ""
    if stats["tag_breakdown"]:
        for tag, cnt in stats["tag_breakdown"]:
            active = "tag-pill-active" if tag == tag_filter else ""
            tag_breakdown_html += f'<a href="/admin?tab=audience&tag={tag}" class="tag-pill {active}">{tag} <span class="tag-cnt">{cnt}</span></a>'
    else:
        tag_breakdown_html = '<p class="empty-note">Tags build automatically as fans text in after the next show.</p>'

    fan_profiles_html = ""
    for fan in stats["fan_profiles"]:
        phone_d = fan["phone_number"][-4:]
        mem_e = _esc(fan["fan_memory"] or "")
        loc = fan.get("fan_location") or ""
        loc_html = f'<span class="fan-loc">📍 {_esc(loc)}</span>' if loc else ""
        src = fan.get("source") or "—"
        joined = fan["created_at"].strftime("%Y-%m-%d") if fan.get("created_at") else "—"
        th_q = quote(fan["phone_number"], safe="")
        tags_html = " ".join(
            f'<a href="/admin?tab=audience&tag={t}" class="fan-tag">{_esc(t)}</a>'
            for t in (fan["fan_tags"] or [])
        )
        fan_profiles_html += f"""
        <div class="fan-card">
          <div class="fan-header">
            <span class="fan-phone">…{phone_d}</span>
            <span class="fan-meta">source: {_esc(src)} · joined {joined}</span>
            {loc_html}
            <a class="fan-open-convo" href="/admin?tab=convos&thread={th_q}">Open conversation →</a>
          </div>
          <p class="fan-memory">{mem_e or "<em class='empty-note'>No profile yet</em>"}</p>
          <div class="fan-tags">{tags_html}</div>
        </div>"""

    if not fan_profiles_html:
        fan_profiles_html = '<p class="empty-note">Fan profiles build automatically as fans text in. Check back after the next show.</p>'

    # Conversations: thread view OR inbox
    convos_inner_html = ""
    chart_days = stats["chart_days"]
    mh = stats["messages_last_hour"]

    if tab == "convos":
        if thread_phone:
            phone_d = thread_phone[-4:] if len(thread_phone) >= 4 else thread_phone
            th_enc = quote(thread_phone, safe="")
            export_href = f"/admin/export/thread?thread={th_enc}"
            total = stats["thread_total"]
            n_show = len(stats["thread_rows"])
            end_i = total - thread_page * THREAD_PAGE_SIZE if total else 0
            start_i = (end_i - n_show + 1) if n_show and end_i else 0
            nav_parts = ['<a class="back-inbox" href="/admin?tab=convos">← Inbox</a>']
            if thread_page > 0:
                q = urlencode(
                    {"tab": "convos", "thread": thread_phone, "thread_page": thread_page - 1}
                )
                nav_parts.append(f'<a class="page-link" href="/admin?{q}">Newer messages ↑</a>')
            if start_i > 1:
                q = urlencode(
                    {"tab": "convos", "thread": thread_phone, "thread_page": thread_page + 1}
                )
                nav_parts.append(f'<a class="page-link" href="/admin?{q}">Earlier messages ↓</a>')
            nav_html = " · ".join(nav_parts)
            bubble_html = ""
            for r in stats["thread_rows"]:
                is_fan = r["role"] == "user"
                cls = "bubble-fan" if is_fan else "bubble-bot"
                lbl = "Fan" if is_fan else "Zarna AI"
                ts = r["created_at"].strftime("%m/%d %I:%M %p ET") if r["created_at"] else ""
                bubble_html += f"""
                <div class="bubble-row {cls}">
                  <div class="bubble-meta"><span class="bubble-who">{lbl}</span> · {ts}</div>
                  <div class="bubble-text">{_esc(r["text"])}</div>
                </div>"""
            if not bubble_html:
                bubble_html = '<p class="empty-note">No messages for this number.</p>'
            convos_inner_html = f"""
            <div class="thread-toolbar">
              <div class="thread-title">Conversation <span class="mono">…{phone_d}</span></div>
              <div class="thread-sub">{nav_html}</div>
              <a class="export-btn thread-export" href="{export_href}">⬇ Export this thread (CSV)</a>
            </div>
            <p class="thread-range-note">Messages {start_i}–{end_i} of {total} (oldest → newest in this chunk)</p>
            <div class="bubble-stack card">{bubble_html}</div>
            """
        else:
            q_val = _esc(request.args.get("q", ""))
            ph_val = _esc(inbox_phone_q)
            convos_inner_html = f"""
            <form method="get" action="/admin" class="search-row convo-filters">
              <input type="hidden" name="tab" value="convos">
              <input type="text" name="phone" class="search-input"
                     placeholder="Filter inbox by phone (partial / last 4)…" value="{ph_val}">
              <input type="text" name="q" class="search-input"
                     placeholder="Search message text…" value="{q_val}">
              <button type="submit" class="search-btn">Apply</button>
              {'<a href="/admin?tab=convos" class="search-btn secondary-btn">Clear</a>' if (inbox_phone_q or msg_body_q) else ''}
            </form>
            <p class="inbox-hint">Click a row to open the full conversation. Page size {INBOX_PAGE_SIZE}.</p>
            """
            inbox_body = ""
            for row in stats["inbox_rows"]:
                p = row["phone_number"]
                p4 = p[-4:] if len(p) >= 4 else p
                preview = _esc(row["text"][:120] + ("…" if len(row["text"]) > 120 else ""))
                ts = row["created_at"].strftime("%m/%d %I:%M %p") if row["created_at"] else ""
                role_lbl = "Fan" if row["role"] == "user" else "Bot"
                href = f"/admin?tab=convos&thread={quote(p, safe='')}"
                inbox_body += f"""
                <a href="{href}" class="inbox-row">
                  <span class="inbox-phone mono">…{p4}</span>
                  <span class="inbox-preview">{preview}</span>
                  <span class="inbox-meta"><span class="badge {"badge-fan" if row["role"]=="user" else "badge-bot"}">{role_lbl}</span> {ts}</span>
                </a>"""
            if not inbox_body:
                inbox_body = '<p class="empty-note">No conversations match your filters.</p>'
            next_prev = ""
            if inbox_page > 0 or len(stats["inbox_rows"]) == INBOX_PAGE_SIZE:
                links = []
                if inbox_page > 0:
                    links.append(
                        f'<a class="page-link" href="/admin?{urlencode({"tab": "convos", "phone": inbox_phone_q, "q": msg_body_q, "inbox_page": inbox_page - 1})}">← Newer chats</a>'
                    )
                if len(stats["inbox_rows"]) == INBOX_PAGE_SIZE:
                    links.append(
                        f'<a class="page-link" href="/admin?{urlencode({"tab": "convos", "phone": inbox_phone_q, "q": msg_body_q, "inbox_page": inbox_page + 1})}">Older chats →</a>'
                    )
                next_prev = f'<div class="inbox-pagination">{" · ".join(links)}</div>'
            convos_inner_html += f'<div class="inbox-list card">{inbox_body}</div>{next_prev}'

    filter_banner = ""
    if tag_filter:
        filter_banner = f"""
        <div class="filter-banner">
          <span>Filtering by tag: <strong>{_esc(tag_filter)}</strong> — {len(stats["fan_profiles"])} fan{"s" if len(stats["fan_profiles"]) != 1 else ""}</span>
          <a href="/admin?tab=audience" class="filter-clear">✕ Clear</a>
        </div>"""
    elif tab == "convos" and thread_phone:
        filter_banner = f"""
        <div class="filter-banner">
          <span>Viewing conversation <strong class="mono">…{thread_phone[-4:]}</strong></span>
          <a href="/admin?tab=convos" class="filter-clear">✕ Back to inbox</a>
        </div>"""

    range_pills = _range_links(chart_days)
    health_note = f'<span class="health-pill">{mh} fan msg in last hour</span>'

    # ── Conversions tab HTML ──────────────────────────────────────────────────
    cnew_slug = request.args.get("cnew", "")
    cerr      = request.args.get("cerr", "")
    base_url  = request.host_url.rstrip("/")
    # Override to https when behind Railway proxy
    scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
    host   = request.headers.get("X-Forwarded-Host", request.host)
    base_url = f"{scheme}://{host}"

    conv_notice_html = ""
    if cnew_slug:
        short_url = f"{base_url}/t/{cnew_slug}"
        conv_notice_html = f"""
        <div style="background:#064e3b;border:1px solid #065f46;border-radius:8px;padding:14px 18px;margin-bottom:16px;display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
          <span style="color:#6ee7b7;font-size:14px;">✅ Link created!</span>
          <code style="background:#1f2937;color:#a5b4fc;padding:5px 12px;border-radius:6px;font-size:13px;flex:1;word-break:break-all;">{_esc(short_url)}</code>
          <button onclick="navigator.clipboard.writeText('{_esc(short_url)}');this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',1500)"
                  style="background:#7c3aed;color:white;border:none;border-radius:6px;padding:6px 14px;font-size:13px;cursor:pointer;">Copy</button>
        </div>"""
    elif cerr == "missing":
        conv_notice_html = '<div style="background:#450a0a;border:1px solid #dc2626;border-radius:8px;padding:12px 18px;margin-bottom:16px;color:#fca5a5;font-size:13px;">⚠️ Label and destination URL are both required.</div>'
    elif cerr == "badurl":
        conv_notice_html = '<div style="background:#450a0a;border:1px solid #dc2626;border-radius:8px;padding:12px 18px;margin-bottom:16px;color:#fca5a5;font-size:13px;">⚠️ Destination must start with http:// or https://</div>'

    type_colors = {"ticket": ("#7c3aed","#c4b5fd"), "podcast": ("#0891b2","#67e8f9"), "promo": ("#d97706","#fcd34d"), "other": ("#374151","#9ca3af")}

    conv_rows_html = ""
    for lnk in stats["tracked_links_rows"]:
        short = f"{base_url}/t/{lnk['slug']}"
        short_e = _esc(short)
        label_e = _esc(lnk["label"] or lnk["slug"])
        dest_e  = _esc(lnk["destination"][:60] + ("…" if len(lnk["destination"]) > 60 else ""))
        ct = lnk["campaign_type"] or "other"
        bg, fg = type_colors.get(ct, type_colors["other"])
        created = lnk["created_at"].strftime("%b %d %Y") if lnk.get("created_at") else "—"
        tc      = lnk["total_clicks"]
        wc      = lnk["clicks_7d"]
        sent_to = lnk["sent_to"]
        ctr_html = "—"
        if sent_to > 0:
            ctr_pct = round(tc / sent_to * 100, 1)
            ctr_color = "#4ade80" if ctr_pct >= 5 else ("#fbbf24" if ctr_pct >= 1 else "#f87171")
            ctr_html = f'<span style="color:{ctr_color};font-weight:700;">{ctr_pct}%</span>'
        conv_rows_html += f"""
        <tr class="conv-row">
          <td style="padding:12px 14px;color:#e2e8f0;font-weight:500;">{label_e}</td>
          <td style="padding:12px 14px;"><span style="background:{bg};color:{fg};padding:2px 10px;border-radius:10px;font-size:11px;font-weight:600;">{ct}</span></td>
          <td style="padding:12px 14px;">
            <div style="display:flex;align-items:center;gap:8px;">
              <code style="color:#a5b4fc;font-size:12px;">{_esc('/t/' + lnk['slug'])}</code>
              <button onclick="navigator.clipboard.writeText('{short_e}');this.textContent='✓';setTimeout(()=>this.textContent='Copy',1500)"
                      style="background:#1f2937;color:#9ca3af;border:1px solid #374151;border-radius:5px;padding:3px 9px;font-size:11px;cursor:pointer;">Copy</button>
            </div>
            <div style="color:#64748b;font-size:11px;margin-top:2px;">{dest_e}</div>
          </td>
          <td style="padding:12px 14px;text-align:center;font-size:20px;font-weight:800;color:#a78bfa;">{tc:,}</td>
          <td style="padding:12px 14px;text-align:center;font-size:16px;font-weight:700;color:#4ade80;">{wc:,}</td>
          <td style="padding:12px 14px;text-align:center;color:#94a3b8;font-size:14px;">{sent_to:,}</td>
          <td style="padding:12px 14px;text-align:center;font-size:14px;">{ctr_html}</td>
          <td style="padding:12px 14px;color:#64748b;font-size:12px;">{created}</td>
          <td style="padding:12px 14px;">
            <form method="post" action="/admin/conversions/{lnk['id']}/delete"
                  onsubmit="return confirm('Delete this link and all its click history?')">
              <button type="submit" style="background:transparent;border:1px solid #6b7280;color:#9ca3af;border-radius:5px;padding:3px 9px;font-size:11px;cursor:pointer;">Delete</button>
            </form>
          </td>
        </tr>"""

    if not conv_rows_html:
        conv_rows_html = '<tr><td colspan="7" style="padding:30px;text-align:center;color:#6b7280;font-style:italic;">No tracked links yet — create your first one above.</td></tr>'

    conv_clicks_labels = [d for d, _ in stats["conv_clicks_by_day"]]
    conv_clicks_data   = [c for _, c in stats["conv_clicks_by_day"]]

    from app.ops_metrics import snapshot as ops_snapshot

    ops = ops_snapshot()
    deploy_ref = (os.getenv("RAILWAY_GIT_COMMIT_SHA") or os.getenv("GIT_COMMIT") or "").strip()[:12]
    deploy_disp = deploy_ref if deploy_ref else "—"
    ops_signals_html = f"""
    <div class="card" style="margin-bottom:20px">
      <div class="card-title">Service signals (this worker · resets on deploy)</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:10px">Deploy ref: <code style="color:#e2e8f0">{_esc(deploy_disp)}</code>
        · Active AI replies: <strong>{ops.get("active_ai_replies", 0)}</strong></p>
      <ul style="color:#cbd5e1;font-size:13px;line-height:1.75;list-style:none;padding:0;margin:0">
        <li>SlickText webhook 401: {ops.get("slicktext_webhook_401", 0)}</li>
        <li>Twilio signature fail: {ops.get("twilio_signature_fail", 0)}</li>
        <li>AI / brain errors: {ops.get("ai_reply_error", 0)}</li>
        <li>Dropped (at capacity): {ops.get("ai_reply_capacity_reject", 0)}</li>
      </ul>
      <p style="color:#64748b;font-size:12px;margin-top:10px">Set <code>AI_REPLY_MAX_CONCURRENT</code> (default 16) to tune load.</p>
    </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zarna AI — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: #0a0f1e; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; min-height: 100vh; }}

.header {{ background: linear-gradient(135deg, #7c3aed 0%, #4f46e5 60%, #0891b2 100%); padding: 20px 28px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
.header-left {{ display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }}
.header-logo {{ font-size: 22px; font-weight: 800; color: white; letter-spacing: -0.5px; }}
.header-logo span {{ color: rgba(255,255,255,0.6); font-weight: 400; font-size: 14px; margin-left: 8px; }}
.header-right {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
.refresh-btn {{ background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.25); color: white; border-radius: 8px; padding: 7px 14px; font-size: 13px; cursor: pointer; transition: background 0.2s; }}
.refresh-btn:hover {{ background: rgba(255,255,255,0.25); }}
.refresh-btn.active {{ background: rgba(16,185,129,0.3); border-color: #10b981; }}
.updated-time {{ color: rgba(255,255,255,0.55); font-size: 12px; }}
.health-pill {{ background: rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.2); color: rgba(255,255,255,0.85); border-radius: 20px; padding: 4px 12px; font-size: 12px; }}

.nav-tabs {{ position: sticky; top: 0; z-index: 50; background: #111827; border-bottom: 1px solid #1f2937; padding: 0 28px; display: flex; gap: 4px; flex-wrap: wrap; }}
.nav-tab {{ padding: 14px 20px; font-size: 14px; font-weight: 500; color: #64748b; text-decoration: none; border-bottom: 2px solid transparent; transition: all 0.15s; white-space: nowrap; }}
.nav-tab:hover {{ color: #e2e8f0; }}
.nav-tab.active {{ color: #a78bfa; border-bottom-color: #7c3aed; }}

.container {{ max-width: 1400px; margin: 0 auto; padding: 24px 28px; }}

.stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 24px; }}
.stat-card {{ background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 20px 22px; transition: border-color 0.2s; }}
.stat-card:hover {{ border-color: #374151; }}
.stat-label {{ color: #6b7280; font-size: 12px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 10px; }}
.stat-value {{ font-size: 34px; font-weight: 800; color: white; line-height: 1; margin-bottom: 8px; }}
.stat-value.purple {{ color: #a78bfa; }}
.stat-value.teal {{ color: #2dd4bf; }}
.stat-value.green {{ color: #4ade80; }}
.stat-trend {{ font-size: 12px; }}

.range-toolbar {{ display: flex; align-items: center; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }}
.range-toolbar span {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }}
.range-pill, .range-pill-active {{
  display: inline-block; padding: 6px 12px; border-radius: 8px; font-size: 13px; text-decoration: none;
  background: #1f2937; color: #94a3b8; border: 1px solid #374151;
}}
.range-pill:hover {{ color: #e2e8f0; border-color: #7c3aed; }}
.range-pill-active {{ background: #312e81; color: #c4b5fd; border-color: #7c3aed; }}

.card {{ background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 20px 22px; margin-bottom: 20px; }}
.card-title {{ color: #9ca3af; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 16px; }}
.grid-2 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 14px; margin-bottom: 20px; }}
.grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; margin-bottom: 20px; }}

.top-msg-row {{ display: flex; align-items: center; gap: 10px; padding: 9px 0; border-bottom: 1px solid #1f2937; }}
.top-msg-rank {{ color: #4b5563; font-size: 11px; min-width: 24px; }}
.top-msg-text {{ color: #d1d5db; font-size: 13px; flex: 1; }}
.top-msg-badge {{ background: #312e81; color: #a5b4fc; padding: 2px 9px; border-radius: 10px; font-size: 12px; font-weight: 600; }}

.area-row {{ display: flex; justify-content: space-between; padding: 7px 0; border-bottom: 1px solid #1f2937; font-size: 13px; }}
.area-cnt {{ color: #2dd4bf; font-weight: 600; }}

.tag-pill {{ display: inline-flex; align-items: center; gap: 6px; background: #1e3a5f; color: #93c5fd; padding: 5px 12px; border-radius: 20px; font-size: 13px; text-decoration: none; margin: 4px; transition: background 0.15s; }}
.tag-pill:hover {{ background: #1d4ed8; color: white; }}
.tag-pill-active {{ background: #1d4ed8; color: white; }}
.tag-cnt {{ background: #3b82f6; color: white; border-radius: 10px; padding: 1px 7px; font-size: 11px; }}

.fan-card {{ padding: 14px 0; border-bottom: 1px solid #1f2937; }}
.fan-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; flex-wrap: wrap; }}
.fan-phone {{ color: #6b7280; font-size: 12px; font-family: monospace; }}
.fan-meta {{ color: #64748b; font-size: 11px; }}
.fan-loc {{ color: #fbbf24; font-size: 12px; }}
.fan-open-convo {{ margin-left: auto; font-size: 12px; color: #a78bfa; text-decoration: none; white-space: nowrap; }}
.fan-open-convo:hover {{ text-decoration: underline; }}
.fan-memory {{ color: #d1d5db; font-size: 14px; margin-bottom: 8px; line-height: 1.4; }}
.fan-tags {{ display: flex; flex-wrap: wrap; gap: 4px; }}
.fan-tag {{ background: #1e3a5f; color: #93c5fd; padding: 2px 8px; border-radius: 8px; font-size: 11px; text-decoration: none; }}
.fan-tag:hover {{ background: #1d4ed8; color: white; }}

.search-row {{ display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }}
.convo-filters .search-input {{ flex: 1; min-width: 140px; }}
.search-input {{ flex: 1; background: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 9px 14px; color: #e2e8f0; font-size: 14px; outline: none; }}
.search-input:focus {{ border-color: #7c3aed; }}
.search-btn {{ background: #7c3aed; color: white; border: none; border-radius: 8px; padding: 9px 18px; font-size: 14px; cursor: pointer; white-space: nowrap; }}
.search-btn:hover {{ background: #6d28d9; }}
.secondary-btn {{ background: #374151; text-decoration: none; display: inline-flex; align-items: center; }}
.export-btn {{ display:inline-flex;align-items:center;gap:6px;background:#064e3b;color:#6ee7b7;border:1px solid #065f46;border-radius:8px;padding:9px 18px;font-size:14px;text-decoration:none;transition:background 0.15s; }}
.export-btn:hover {{ background:#065f46;color:white; }}

.inbox-hint {{ color: #64748b; font-size: 13px; margin-bottom: 12px; }}
.inbox-list {{ padding: 0 !important; overflow: hidden; }}
.inbox-row {{
  display: flex; align-items: flex-start; gap: 14px; padding: 14px 18px; border-bottom: 1px solid #1f2937;
  text-decoration: none; color: inherit; transition: background 0.12s;
}}
.inbox-row:hover {{ background: rgba(124,58,237,0.08); }}
.inbox-row:last-child {{ border-bottom: none; }}
.inbox-phone {{ flex: 0 0 auto; color: #a78bfa; font-weight: 600; }}
.inbox-preview {{ flex: 1; color: #d1d5db; font-size: 14px; line-height: 1.35; word-break: break-word; }}
.inbox-meta {{ flex: 0 0 auto; text-align: right; font-size: 12px; color: #6b7280; min-width: 120px; }}
.inbox-pagination {{ margin-top: 14px; }}

.thread-toolbar {{ display: flex; flex-wrap: wrap; align-items: center; gap: 12px; margin-bottom: 10px; }}
.thread-title {{ font-size: 18px; font-weight: 700; color: #f1f5f9; }}
.thread-sub {{ color: #94a3b8; font-size: 13px; flex: 1; }}
.thread-export {{ font-size: 13px; padding: 7px 14px; }}
.thread-range-note {{ color: #64748b; font-size: 12px; margin-bottom: 12px; }}
.back-inbox, .page-link {{ color: #a78bfa; text-decoration: none; }}
.back-inbox:hover, .page-link:hover {{ text-decoration: underline; }}
.mono {{ font-family: ui-monospace, monospace; }}

.bubble-stack {{ padding: 16px 20px !important; max-width: 720px; }}
.bubble-row {{ margin-bottom: 16px; }}
.bubble-row.bubble-fan {{ text-align: left; }}
.bubble-row.bubble-bot {{ text-align: right; }}
.bubble-meta {{ font-size: 11px; color: #64748b; margin-bottom: 4px; }}
.bubble-who {{ font-weight: 600; color: #94a3b8; }}
.bubble-text {{
  display: inline-block; max-width: 85%; text-align: left;
  padding: 10px 14px; border-radius: 12px; font-size: 14px; line-height: 1.45; word-break: break-word;
}}
.bubble-fan .bubble-text {{ background: #312e81; color: #e0e7ff; border-bottom-left-radius: 4px; }}
.bubble-bot .bubble-text {{ background: #064e3b; color: #d1fae5; border-bottom-right-radius: 4px; }}

.badge {{ padding: 2px 8px; border-radius: 8px; font-size: 11px; font-weight: 600; }}
.badge-fan {{ background: #312e81; color: #a5b4fc; }}
.badge-bot {{ background: #064e3b; color: #6ee7b7; }}

.filter-banner {{ background: #1e3a5f; border: 1px solid #2563eb; border-radius: 8px; padding: 10px 18px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; font-size: 13px; color: #93c5fd; flex-wrap: wrap; gap: 8px; }}
.filter-clear {{ color: #6b7280; text-decoration: none; font-size: 13px; }}
.filter-clear:hover {{ color: #e2e8f0; }}

.empty-note {{ color: #6b7280; font-size: 13px; font-style: italic; padding: 8px 0; }}
.tab-content {{ display: none; }}
.tab-content.active {{ display: block; }}

/* Conversions tab */
.conv-table {{ width:100%;border-collapse:collapse; }}
.conv-table th {{ padding:10px 14px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #1f2937; }}
.conv-table th.center {{ text-align:center; }}
.conv-row {{ border-bottom:1px solid #1f2937;transition:background .1s; }}
.conv-row:hover {{ background:rgba(124,58,237,.07); }}
.conv-row:last-child {{ border-bottom:none; }}
.conv-form-row {{ display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end; }}
.conv-form-row .search-input {{ flex:1;min-width:140px; }}
.conv-chart-toggle {{ background:#1f2937;color:#94a3b8;border:1px solid #374151;border-radius:8px;padding:8px 16px;font-size:13px;cursor:pointer;transition:all .15s; }}
.conv-chart-toggle:hover {{ color:#e2e8f0;border-color:#7c3aed; }}
.conv-chart-toggle.active {{ background:#312e81;color:#c4b5fd;border-color:#7c3aed; }}

@media (max-width: 768px) {{
  .header {{ padding: 16px; }}
  .header-logo {{ font-size: 18px; }}
  .nav-tab {{ padding: 12px 14px; font-size: 13px; }}
  .container {{ padding: 16px; }}
  .stats-grid {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
  .stat-value {{ font-size: 26px; }}
  .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
  .inbox-row {{ flex-direction: column; gap: 6px; }}
  .inbox-meta {{ text-align: left; }}
  .fan-open-convo {{ margin-left: 0; }}
  .updated-time {{ display: none; }}
}}
@media (max-width: 480px) {{
  .stats-grid {{ grid-template-columns: 1fr 1fr; }}
  .nav-tabs {{ padding: 0 12px; gap: 0; }}
  .nav-tab {{ padding: 10px 10px; font-size: 12px; }}
}}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="header-logo">Zarna AI <span>Dashboard</span></div>
    {health_note}
  </div>
  <div class="header-right">
    <button class="refresh-btn" id="autoRefreshBtn" onclick="toggleAutoRefresh()">⟳ Auto-refresh: Off</button>
    <span class="updated-time">Updated: {stats["generated_at"]}</span>
  </div>
</div>

<nav class="nav-tabs">
  <a href="/admin?tab=overview&amp;range={chart_days}" class="nav-tab {'active' if tab == 'overview' else ''}">📊 Overview</a>
  <a href="/admin?tab=audience" class="nav-tab {'active' if tab == 'audience' else ''}">👥 Audience</a>
  <a href="/admin?tab=convos" class="nav-tab {'active' if tab == 'convos' else ''}">💬 Conversations</a>
  <a href="/admin?tab=conversions" class="nav-tab {'active' if tab == 'conversions' else ''}">🔗 Conversions</a>
  <a href="/admin?tab=insights" class="nav-tab {'active' if tab == 'insights' else ''}">🧠 Insights</a>
  <a href="/admin/live-shows" class="nav-tab">🎤 Live shows</a>
</nav>

<div class="container">

  {filter_banner}

  <div class="tab-content {'active' if tab == 'overview' else ''}" id="tab-overview">
    {ops_signals_html}

    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Total Subscribers</div>
        <div class="stat-value">{stats["total_subscribers"]:,}</div>
        <div class="stat-trend">{sub_trend}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Messages</div>
        <div class="stat-value purple">{stats["total_messages"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">from fans (all time)</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Messages Today</div>
        <div class="stat-value teal">{stats["messages_today"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">last 24 hours</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">This Week</div>
        <div class="stat-value green">{stats["messages_week"]:,}</div>
        <div class="stat-trend">{week_trend}</div>
      </div>
    </div>

    <div class="range-toolbar">
      <span>Chart window</span>
      {range_pills}
    </div>

    <div class="grid-2">
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Messages Per Day — Last {chart_days} Days</div>
        <canvas id="dayChart" height="110"></canvas>
      </div>
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Activity by Hour (ET) — Same Window</div>
        <canvas id="hourChart" height="110"></canvas>
      </div>
    </div>

    <div style="margin-bottom:20px"></div>

    <div class="grid-3">
      <div class="card" style="grid-column:span 2;margin-bottom:0">
        <div class="card-title">Top Messages from Fans (same window)</div>
        {top_msgs_html}
      </div>
      <div class="card" style="margin-bottom:0">
        <div class="card-title">Top Area Codes</div>
        {area_html}
      </div>
    </div>

  </div>

  <div class="tab-content {'active' if tab == 'audience' else ''}" id="tab-audience">

    <div class="stats-grid" style="grid-template-columns:repeat(2,1fr);max-width:500px">
      <div class="stat-card">
        <div class="stat-label">Total Fans</div>
        <div class="stat-value">{stats["total_subscribers"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">unique numbers</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Profiled Fans</div>
        <div class="stat-value purple">{stats["profiled_fans"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">with memory built</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">Audience Tags — click any to filter</div>
      <div style="margin-top:4px">{tag_breakdown_html}</div>
    </div>

    <div class="card">
      <div class="card-title">Search & Export by Location or Tag</div>
      <form method="get" action="/admin" style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px">
        <input type="hidden" name="tab" value="audience">
        <input type="text" name="tag" class="search-input" style="flex:1;min-width:160px"
               placeholder="Tag: doctor, lawyer, married…"
               value="{tag_filter}">
        <input type="text" name="location" class="search-input" style="flex:1;min-width:160px"
               placeholder="Location: Rhode Island, Boston, Chicago…"
               value="{_esc(request.args.get('location', ''))}">
        <button type="submit" class="search-btn">Filter</button>
        {'<a href="/admin?tab=audience" class="search-btn" style="background:#374151;text-decoration:none">Clear</a>' if tag_filter or request.args.get('location') else ''}
      </form>
      {'<a href="/admin/export?tag=' + tag_filter + '&location=' + request.args.get("location", "") + '" class="export-btn">⬇ Export CSV (' + str(len(stats["fan_profiles"])) + ' fans)</a>' if tag_filter or request.args.get("location") else '<a href="/admin/export" class="export-btn">⬇ Export All Fans</a>'}
    </div>

    <div class="card">
      <div class="card-title">
        {'Fan Profiles — ' + tag_filter + (' in ' + request.args.get('location','') if request.args.get('location') else '') if tag_filter else ('Fans in ' + request.args.get('location','') if request.args.get('location') else 'Fan Profiles (most recent 100 with memory)')}
      </div>
      {fan_profiles_html}
    </div>

  </div>

  <div class="tab-content {'active' if tab == 'convos' else ''}" id="tab-convos">
    {convos_inner_html}
  </div>

  <div class="tab-content {'active' if tab == 'insights' else ''}" id="tab-insights">
    {_render_insights_tab(stats, insights_days, insights_era)}
  </div>

  <div class="tab-content {'active' if tab == 'conversions' else ''}" id="tab-conversions">

    {conv_notice_html}

    <!-- Summary stats -->
    <div class="stats-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:24px;">
      <div class="stat-card">
        <div class="stat-label">Total Links</div>
        <div class="stat-value">{stats["conv_summary"]["total_links"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">tracked destinations</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Clicks</div>
        <div class="stat-value purple">{stats["conv_summary"]["total_clicks"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">all time</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">This Week</div>
        <div class="stat-value teal">{stats["conv_summary"]["clicks_week"]:,}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">last 7 days</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Top Performer</div>
        <div class="stat-value green" style="font-size:18px;padding-top:4px;">{_esc(stats["conv_summary"]["top_label"])}</div>
        <div class="stat-trend" style="color:#64748b;font-size:12px">most clicks</div>
      </div>
    </div>

    <!-- New link form -->
    <div class="card" style="margin-bottom:20px;">
      <div class="card-title">Create a new tracked link</div>
      <p style="color:#94a3b8;font-size:13px;margin-bottom:14px;">
        Paste your real URL — we generate a short <code style="color:#a5b4fc">/t/…</code> link. Use it in bot messages, blasts, or anywhere else. Every click is logged.
      </p>
      <form method="post" action="/admin/conversions/new">
        <div class="conv-form-row">
          <input type="text" name="label" class="search-input" placeholder="Label — e.g. Phoenix ticket link, April podcast ep…"
                 required maxlength="200" style="flex:2;min-width:200px;">
          <select name="campaign_type" class="search-input" style="flex:0 0 140px;">
            <option value="ticket">🎟 Ticket</option>
            <option value="podcast">🎙 Podcast</option>
            <option value="promo">🎁 Promo</option>
            <option value="other">🔗 Other</option>
          </select>
          <input type="url" name="destination" class="search-input" placeholder="https://your-destination.com/…"
                 required style="flex:3;min-width:240px;">
          <button type="submit" class="search-btn" style="flex:0 0 auto;">Generate link →</button>
        </div>
      </form>
    </div>

    <!-- Chart toggle -->
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;">
      <div class="card-title" style="margin:0;">All Tracked Links</div>
      <button class="conv-chart-toggle" id="convChartToggle" onclick="toggleConvChart()">📈 Show Charts</button>
    </div>

    <!-- Charts (hidden by default) -->
    <div id="convChartSection" style="display:none;margin-bottom:20px;">
      <div class="card">
        <div class="card-title">Total Clicks Per Day — Last 30 Days (all links)</div>
        <canvas id="convDayChart" height="80"></canvas>
      </div>
    </div>

    <!-- Links table -->
    <div class="card" style="padding:0;overflow:hidden;">
      <table class="conv-table">
          <thead>
          <tr>
            <th>Label</th>
            <th>Type</th>
            <th>Tracked URL</th>
            <th class="center">All-time clicks</th>
            <th class="center">Last 7 days</th>
            <th class="center">Sent to</th>
            <th class="center">CTR</th>
            <th>Created</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {conv_rows_html}
        </tbody>
      </table>
    </div>

  </div>

</div>

<script>
const chartCfg = {{
  plugins: {{ legend: {{ display: false }} }},
  scales: {{
    x: {{ ticks: {{ color:'#6b7280', font:{{ size:10 }} }}, grid:{{ color:'#1f2937' }} }},
    y: {{ ticks: {{ color:'#6b7280', font:{{ size:10 }} }}, grid:{{ color:'#1f2937' }} }}
  }}
}};

const dayEl = document.getElementById('dayChart');
if (dayEl) new Chart(dayEl, {{
  type: 'bar',
  data: {{ labels: {days_labels}, datasets: [{{ data: {days_data}, backgroundColor:'#7c3aed', borderRadius:4 }}] }},
  options: chartCfg
}});

const hourEl = document.getElementById('hourChart');
if (hourEl) new Chart(hourEl, {{
  type: 'bar',
  data: {{ labels: {hour_labels}, datasets: [{{ data: {hour_data}, backgroundColor:'#0891b2', borderRadius:4 }}] }},
  options: chartCfg
}});

let _refreshTimer = null;
let _refreshOn = false;

function toggleAutoRefresh() {{
  const btn = document.getElementById('autoRefreshBtn');
  if (_refreshOn) {{
    clearInterval(_refreshTimer);
    _refreshOn = false;
    btn.textContent = '⟳ Auto-refresh: Off';
    btn.classList.remove('active');
  }} else {{
    _refreshOn = true;
    btn.textContent = '⟳ Auto-refresh: On (30s)';
    btn.classList.add('active');
    _refreshTimer = setInterval(() => location.reload(), 30000);
  }}
}}

// ── Conversions chart ───────────────────────────────────────────────────────
let _convChart = null;
let _convChartVisible = false;

function toggleConvChart() {{
  const sec = document.getElementById('convChartSection');
  const btn = document.getElementById('convChartToggle');
  _convChartVisible = !_convChartVisible;
  sec.style.display = _convChartVisible ? 'block' : 'none';
  btn.textContent = _convChartVisible ? '📈 Hide Charts' : '📈 Show Charts';
  btn.classList.toggle('active', _convChartVisible);
  if (_convChartVisible && !_convChart) {{
    const el = document.getElementById('convDayChart');
    if (el) {{
      _convChart = new Chart(el, {{
        type: 'bar',
        data: {{
          labels: {conv_clicks_labels},
          datasets: [{{
            data: {conv_clicks_data},
            backgroundColor: '#7c3aed',
            borderRadius: 4,
            label: 'Clicks'
          }}]
        }},
        options: {{
          plugins: {{ legend: {{ display: false }} }},
          scales: {{
            x: {{ ticks: {{ color:'#6b7280', font:{{ size:10 }} }}, grid:{{ color:'#1f2937' }} }},
            y: {{ ticks: {{ color:'#6b7280', font:{{ size:10 }}, stepSize:1 }}, grid:{{ color:'#1f2937' }} }}
          }}
        }}
      }});
    }}
  }}
}}
</script>

</body>
</html>"""

    return html


@admin_bp.route("/admin/actions/sync-slicktext-dates", methods=["POST"])
def sync_slicktext_dates():
    """One-off job: backfill contacts.created_at from SlickText subscribedDate."""
    if not check_admin_auth():
        return require_admin_auth_response()

    import time

    import requests as _req

    pub  = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
    priv = os.getenv("SLICKTEXT_PRIVATE_KEY", "")
    if not pub or not priv:
        return Response("SLICKTEXT_PUBLIC_KEY / SLICKTEXT_PRIVATE_KEY not set", status=503, mimetype="text/plain")

    conn = get_db_connection()
    if not conn:
        return Response("DB not configured", status=503, mimetype="text/plain")

    _BACKFILL_THRESHOLD = "2026-03-26"
    _TEXTWORDS = [(3185378, "zarna"), (4633842, "hello")]
    _PAGE_SIZE = 200

    def _parse_date(raw):
        if not raw:
            return None
        try:
            datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
            return raw.strip()
        except ValueError:
            return None

    def _stream():
        yield "SlickText → Postgres date sync starting…\n\n"
        seen = {}
        for tw_id, label in _TEXTWORDS:
            yield f"Fetching textword '{label}' (id={tw_id})…\n"
            offset, total = 0, None
            while True:
                resp = _req.get(
                    "https://api.slicktext.com/v1/contacts/",
                    params={"textword": tw_id, "limit": _PAGE_SIZE, "offset": offset},
                    auth=(pub, priv),
                    timeout=30,
                )
                if resp.status_code != 200:
                    yield f"  API error {resp.status_code}: {resp.text[:200]}\n"
                    break
                data = resp.json()
                if total is None:
                    total = data["meta"]["total"]
                    yield f"  Total subscribers: {total:,}\n"
                contacts = data.get("contacts", [])
                if not contacts:
                    break
                for c in contacts:
                    number = (c.get("number") or "").strip()
                    if number and number not in seen:
                        seen[number] = _parse_date(c.get("subscribedDate"))
                offset += _PAGE_SIZE
                yield f"  Fetched {min(offset, total):,} / {total:,}\n"
                if offset >= total:
                    break
                time.sleep(0.1)

        yield f"\nTotal unique contacts: {len(seen):,}\n"
        yield "Upserting into Postgres…\n"

        inserted = skipped = 0
        try:
            with conn:
                with conn.cursor() as cur:
                    for number, sub_date in seen.items():
                        if sub_date:
                            cur.execute(
                                """
                                INSERT INTO contacts (phone_number, source, created_at)
                                VALUES (%s, 'slicktext', %s::timestamp)
                                ON CONFLICT (phone_number) DO UPDATE
                                  SET created_at = EXCLUDED.created_at
                                WHERE contacts.created_at::date >= %s::date
                                """,
                                (number, sub_date, _BACKFILL_THRESHOLD),
                            )
                        else:
                            cur.execute(
                                "INSERT INTO contacts (phone_number, source) VALUES (%s, 'slicktext') ON CONFLICT DO NOTHING",
                                (number,),
                            )
                        if cur.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1
        except Exception as exc:
            conn.rollback()
            yield f"\nDB error: {exc}\n"
            conn.close()
            return
        conn.close()

        yield f"\nInserted / updated : {inserted:,}\n"
        yield f"Already correct    : {skipped:,}\n"
        yield "\nDone. Reload the Insights tab to see updated pre-bot metrics.\n"

    return Response(_stream(), mimetype="text/plain")


@admin_bp.route("/admin/actions/delete-blast/<int:blast_id>", methods=["POST"])
def delete_blast(blast_id: int):
    """Delete a blast draft record (for removing tests/junk from analytics)."""
    if not check_admin_auth():
        return require_admin_auth_response()
    conn = get_db_connection()
    if not conn:
        return Response("DB not configured", status=503, mimetype="text/plain")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM blast_drafts WHERE id = %s", (blast_id,))
                deleted = cur.rowcount
        conn.close()
        if deleted:
            return Response("deleted", status=200, mimetype="text/plain")
        return Response("not found", status=404, mimetype="text/plain")
    except Exception as e:
        conn.rollback()
        conn.close()
        return Response(f"Error: {e}", status=500, mimetype="text/plain")


@admin_bp.route("/admin/actions/set-blast-category/<int:blast_id>", methods=["POST"])
def set_blast_category(blast_id: int):
    """Set the category (friendly / sales / show) on a blast_drafts record."""
    if not check_admin_auth():
        return require_admin_auth_response()
    conn = get_db_connection()
    if not conn:
        return jsonify({"ok": False, "error": "DB not configured"}), 503
    try:
        data     = request.get_json(force=True)
        category = (data.get("category") or "").strip().lower()
        if category not in ("friendly", "sales", "show", ""):
            return jsonify({"ok": False, "error": "category must be friendly, sales, show, or empty"}), 400
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET blast_category = %s WHERE id = %s",
                    (category or None, blast_id),
                )
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.route("/admin/actions/add-external-blast", methods=["POST"])
def add_external_blast():
    """Insert a manually-entered blast record (e.g. from SlickText) into blast_drafts."""
    if not check_admin_auth():
        return require_admin_auth_response()
    conn = get_db_connection()
    if not conn:
        return jsonify({"ok": False, "error": "DB not configured"}), 503
    try:
        data = request.get_json(force=True)
        name          = (data.get("name") or "").strip()
        date_str      = (data.get("date") or "").strip()
        sent_count    = int(data.get("sent_count") or 0)
        opt_out_count = int(data.get("opt_out_count") or 0)
        lc            = data.get("link_clicks")
        manual_link_clicks = int(lc) if lc is not None else None
        if not name or not date_str or sent_count <= 0:
            return jsonify({"ok": False, "error": "name, date, and sent_count are required"}), 400
        # Parse the date and treat it as noon UTC so it shows the right calendar day
        from datetime import datetime as _dt
        sent_at = _dt.strptime(date_str, "%Y-%m-%d").replace(hour=12)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO blast_drafts
                      (name, body, status, sent_at, sent_count, total_recipients,
                       opt_out_count, manual_link_clicks, created_by, channel)
                    VALUES (%s, '', 'sent', %s, %s, %s, %s, %s, 'external', 'slicktext')
                    RETURNING id
                    """,
                    (name, sent_at, sent_count, sent_count, opt_out_count, manual_link_clicks),
                )
                new_id = cur.fetchone()[0]
        conn.close()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.route("/admin/actions/mark-blasts", methods=["POST"])
def mark_blasts():
    """One-off: mark existing preseed/blast messages (same text to 50+ people) as source='blast'."""
    if not check_admin_auth():
        return require_admin_auth_response()
    conn = get_db_connection()
    if not conn:
        return Response("DB not configured", status=503, mimetype="text/plain")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE messages
                    SET source = 'blast'
                    WHERE source IS NULL
                      AND role = 'assistant'
                      AND text IN (
                        SELECT text FROM messages
                        WHERE role = 'assistant' AND source IS NULL
                        GROUP BY text
                        HAVING COUNT(DISTINCT phone_number) >= 50
                      )
                    """
                )
                marked = cur.rowcount
        conn.close()
        return Response(f"Marked {marked:,} blast rows as source='blast'.", status=200, mimetype="text/plain")
    except Exception as e:
        conn.rollback()
        conn.close()
        return Response(f"Error: {e}", status=500, mimetype="text/plain")


@admin_bp.route("/admin/actions/import-chat-transcripts", methods=["GET", "POST"])
def import_chat_transcripts():
    """Upload SlickText CSV and import pre-bot chat history into messages table."""
    if not check_admin_auth():
        return require_admin_auth_response()

    if request.method == "GET":
        return Response(
            """<!doctype html><html><body style="font-family:sans-serif;max-width:600px;margin:60px auto;padding:20px">
            <h2>Import SlickText Chat Transcripts</h2>
            <p>Upload the CSV exported from SlickText Inbox. Only messages before March 27 will be imported.</p>
            <form method="POST" enctype="multipart/form-data">
              <input type="file" name="csv_file" accept=".csv" required style="margin-bottom:16px;display:block">
              <button type="submit" style="padding:10px 24px;background:#6366f1;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:15px">
                Import CSV
              </button>
            </form></body></html>""",
            mimetype="text/html",
        )

    # POST — process uploaded file
    import csv as _csv
    import io
    from datetime import timezone as _tz, timedelta as _td

    f = request.files.get("csv_file")
    if not f:
        return Response("No file uploaded.", status=400, mimetype="text/plain")

    ZARNA_NUMBER = "+18775532629"
    BOT_LAUNCH   = datetime(2026, 3, 27, tzinfo=timezone.utc)
    REPLY_WINDOW = 48 * 3600  # seconds

    _TZ_OFF = {"EDT": -4, "EST": -5, "PDT": -7, "PST": -8, "UTC": 0}

    def _parse_ts(raw):
        raw = (raw or "").strip()
        if not raw:
            return None
        parts = raw.rsplit(" ", 1)
        offset_h = _TZ_OFF.get(parts[1].upper(), -5) if len(parts) == 2 else -5
        try:
            naive = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
            return naive.replace(tzinfo=timezone(timedelta(hours=offset_h)))
        except ValueError:
            return None

    def _stream():
        yield "SlickText Chat Transcript Import\n\n"
        text_data = f.stream.read().decode("utf-8")
        rows = []
        for r in _csv.DictReader(io.StringIO(text_data)):
            dt = _parse_ts(r.get("Sent", ""))
            if not dt or dt >= BOT_LAUNCH:
                continue
            from_num = (r.get("From") or "").strip()
            to_num   = (r.get("To")   or "").strip()
            body     = (r.get("Body") or "").strip()
            if not body:
                continue
            if from_num == ZARNA_NUMBER:
                role, phone = "assistant", to_num
            else:
                role, phone = "user", from_num
            if phone:
                rows.append((phone, role, body, dt))

        incoming = sum(1 for r in rows if r[1] == "user")
        outgoing = sum(1 for r in rows if r[1] == "assistant")
        fans     = len({r[0] for r in rows if r[1] == "user"})
        yield f"Pre-bot rows found : {len(rows):,}\n"
        yield f"Incoming (fans)    : {incoming:,} from {fans:,} unique fans\n"
        yield f"Outgoing (Zarna)   : {outgoing:,}\n\n"

        conn = get_db_connection()
        if not conn:
            yield "DB not configured.\n"
            return

        try:
            # Ensure source column
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'bot'")
            conn.commit()
            yield "DB schema ready.\n"

            # Insert rows
            inserted = 0
            with conn.cursor() as cur:
                for phone, role, body, dt in rows:
                    cur.execute(
                        "INSERT INTO messages (phone_number, role, text, created_at, source) "
                        "VALUES (%s, %s, %s, %s, 'csv_import') ON CONFLICT DO NOTHING",
                        (phone, role, body, dt),
                    )
                    if cur.rowcount > 0:
                        inserted += 1
            conn.commit()
            yield f"Inserted : {inserted:,}  (skipped {len(rows) - inserted:,} dupes)\n\n"

            # Score reply metrics
            yield "Scoring reply metrics…\n"
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE messages AS m
                    SET
                      did_user_reply = EXISTS (
                        SELECT 1 FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                          AND m2.role = 'user' AND m2.source = 'csv_import'
                          AND m2.created_at > m.created_at
                          AND m2.created_at <= m.created_at + INTERVAL '{REPLY_WINDOW} seconds'
                      ),
                      reply_delay_seconds = (
                        SELECT EXTRACT(EPOCH FROM (m2.created_at - m.created_at))::int
                        FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                          AND m2.role = 'user' AND m2.source = 'csv_import'
                          AND m2.created_at > m.created_at
                        ORDER BY m2.created_at LIMIT 1
                      ),
                      went_silent_after = NOT EXISTS (
                        SELECT 1 FROM messages m2
                        WHERE m2.phone_number = m.phone_number
                          AND m2.role = 'user' AND m2.source = 'csv_import'
                          AND m2.created_at > m.created_at
                          AND m2.created_at <= m.created_at + INTERVAL '{REPLY_WINDOW} seconds'
                      )
                    WHERE m.role = 'assistant'
                      AND m.source = 'csv_import'
                      AND m.did_user_reply IS NULL
                    """
                )
                scored = cur.rowcount
            conn.commit()
            yield f"Scored   : {scored:,} outgoing messages\n\n"
            yield "Done! Reload the Insights tab and switch to Pre-bot to see real reply rates.\n"
        except Exception as exc:
            conn.rollback()
            yield f"Error: {exc}\n"
        finally:
            conn.close()

    return Response(_stream(), mimetype="text/plain")


@admin_bp.route("/admin/quizzes/kill-all", methods=["POST"])
def kill_all_quizzes():
    """Immediately expire all active quiz sessions. Requires admin auth."""
    if not check_admin_auth():
        return require_admin_auth_response()
    conn = get_db_connection()
    if not conn:
        return Response("DB not configured", status=503)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE quiz_sessions SET expires_at = NOW() - INTERVAL '1 second' "
                    "WHERE expires_at IS NULL OR expires_at > NOW()"
                )
                killed = cur.rowcount
        conn.close()
        return Response(f"Killed {killed} active quiz session(s).", status=200, mimetype="text/plain")
    except Exception as e:
        conn.close()
        return Response(f"Error: {e}", status=500, mimetype="text/plain")
