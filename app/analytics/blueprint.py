"""
Analytics JSON API — engagement data for programmatic / external use.

All endpoints require the same HTTP Basic Auth as the admin dashboard
(ADMIN_PASSWORD env var).  They return JSON so you can query them from
scripts, Slack bots, external dashboards, etc.

Routes:
  GET /analytics/engagement-summary      30-day headline numbers
  GET /analytics/intent-breakdown        reply_rate / dropoff by intent
  GET /analytics/tone-breakdown          reply_rate / dropoff by tone_mode
  GET /analytics/dropoff-triggers        last bot msg before silence (recent 20)
  GET /analytics/top-bot-replies         fastest-reply-getting bot msgs (recent 20)
  GET /analytics/reply-length-buckets    reply rate vs reply length distribution
"""

import logging
import os

from flask import Blueprint, jsonify, request

from app.admin_auth import (
    admin_password_configured,
    check_admin_auth,
    get_db_connection,
    no_admin_password_response,
    require_admin_auth_response,
)

analytics_bp = Blueprint("analytics", __name__)
_logger = logging.getLogger(__name__)

_DAYS_DEFAULT = 30
_DAYS_MAX = 90


def _days_param() -> int:
    try:
        v = int(request.args.get("days", _DAYS_DEFAULT))
        return min(max(1, v), _DAYS_MAX)
    except (TypeError, ValueError):
        return _DAYS_DEFAULT


def _auth_guard():
    """Returns a Response if auth fails, else None."""
    if not admin_password_configured():
        return no_admin_password_response()
    if not check_admin_auth():
        return require_admin_auth_response()
    return None


def _get_db():
    return get_db_connection()


# ---------------------------------------------------------------------------
# /analytics/engagement-summary
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/engagement-summary")
def engagement_summary():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)                                           AS scored_bot_replies,
                  ROUND(AVG(did_user_reply::int) * 100, 1)          AS reply_rate_pct,
                  ROUND(
                    100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                    / NULLIF(COUNT(*), 0),
                    1
                  )                                                  AS dropoff_rate_pct,
                  ROUND(AVG(reply_delay_seconds), 0)                 AS avg_reply_delay_s,
                  PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY reply_delay_seconds)                   AS median_reply_delay_s,
                  ROUND(AVG(reply_length_chars), 0)                  AS avg_bot_reply_length,
                  COUNT(*) FILTER (WHERE has_link = TRUE)            AS bot_msgs_with_link,
                  COUNT(*) FILTER (WHERE has_link = TRUE
                                   AND link_clicked_1h = TRUE)       AS link_clicks_1h,
                  COUNT(*) FILTER (WHERE conversation_turn = 1)      AS first_turn_replies,
                  ROUND(
                    AVG(did_user_reply::int)
                      FILTER (WHERE conversation_turn = 1) * 100, 1
                  )                                                  AS first_turn_reply_rate_pct
                FROM messages
                WHERE role            = 'assistant'
                  AND did_user_reply IS NOT NULL
                  AND created_at     >= NOW() - make_interval(days => %s)
                """,
                (days,),
            )
            row = dict(cur.fetchone() or {})
            # compute link_ctr_pct safely
            with_link = row.get("bot_msgs_with_link") or 0
            clicks = row.get("link_clicks_1h") or 0
            row["link_ctr_pct"] = round(clicks / with_link * 100, 1) if with_link else None
            row["days"] = days
        return jsonify(row)
    except Exception as e:
        _logger.exception("engagement_summary error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /analytics/intent-breakdown
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/intent-breakdown")
def intent_breakdown():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                  COALESCE(intent, 'unknown')                   AS intent,
                  COUNT(*)                                       AS total_bot_replies,
                  ROUND(AVG(did_user_reply::int) * 100, 1)      AS reply_rate_pct,
                  ROUND(
                    100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                    / NULLIF(COUNT(*), 0),
                    1
                  )                                              AS dropoff_rate_pct,
                  ROUND(AVG(reply_delay_seconds), 0)             AS avg_reply_delay_s,
                  ROUND(AVG(reply_length_chars), 0)              AS avg_reply_length,
                  COUNT(*) FILTER (WHERE has_link = TRUE
                    AND link_clicked_1h = TRUE)                  AS link_clicks_1h,
                  COUNT(*) FILTER (WHERE has_link = TRUE)        AS bot_msgs_with_link
                FROM messages
                WHERE role            = 'assistant'
                  AND did_user_reply IS NOT NULL
                  AND created_at     >= NOW() - make_interval(days => %s)
                GROUP BY COALESCE(intent, 'unknown')
                ORDER BY reply_rate_pct DESC NULLS LAST
                """,
                (days,),
            )
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                with_link = d.get("bot_msgs_with_link") or 0
                clicks = d.get("link_clicks_1h") or 0
                d["link_ctr_pct"] = round(clicks / with_link * 100, 1) if with_link else None
                rows.append(d)
        return jsonify({"days": days, "breakdown": rows})
    except Exception as e:
        _logger.exception("intent_breakdown error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /analytics/tone-breakdown
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/tone-breakdown")
def tone_breakdown():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                  COALESCE(tone_mode, 'unknown')                AS tone_mode,
                  COUNT(*)                                       AS total_bot_replies,
                  ROUND(AVG(did_user_reply::int) * 100, 1)      AS reply_rate_pct,
                  ROUND(
                    100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                    / NULLIF(COUNT(*), 0),
                    1
                  )                                              AS dropoff_rate_pct,
                  ROUND(AVG(reply_delay_seconds), 0)             AS avg_reply_delay_s
                FROM messages
                WHERE role            = 'assistant'
                  AND did_user_reply IS NOT NULL
                  AND created_at     >= NOW() - make_interval(days => %s)
                GROUP BY COALESCE(tone_mode, 'unknown')
                ORDER BY reply_rate_pct DESC NULLS LAST
                """,
                (days,),
            )
            rows = [dict(r) for r in cur.fetchall()]
        return jsonify({"days": days, "breakdown": rows})
    except Exception as e:
        _logger.exception("tone_breakdown error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /analytics/dropoff-triggers
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/dropoff-triggers")
def dropoff_triggers():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    limit = min(int(request.args.get("limit", 20)), 100)
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                  LEFT(text, 200)                AS bot_text_preview,
                  intent,
                  tone_mode,
                  reply_length_chars,
                  conversation_turn,
                  created_at
                FROM messages
                WHERE role              = 'assistant'
                  AND went_silent_after = TRUE
                  AND created_at       >= NOW() - make_interval(days => %s)
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (days, limit),
            )
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                rows.append(d)
        return jsonify({"days": days, "triggers": rows})
    except Exception as e:
        _logger.exception("dropoff_triggers error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /analytics/top-bot-replies
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/top-bot-replies")
def top_bot_replies():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    metric = request.args.get("metric", "reply_delay")
    limit = min(int(request.args.get("limit", 20)), 100)
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    # Only allow safe sort columns
    order_clause = (
        "reply_delay_seconds ASC NULLS LAST"
        if metric == "reply_delay"
        else "conversation_turn DESC NULLS LAST"
    )

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                  LEFT(text, 200)                AS bot_text_preview,
                  intent,
                  tone_mode,
                  routing_tier,
                  reply_length_chars,
                  reply_delay_seconds,
                  conversation_turn,
                  created_at
                FROM messages
                WHERE role            = 'assistant'
                  AND did_user_reply  = TRUE
                  AND reply_delay_seconds IS NOT NULL
                  AND created_at     >= NOW() - make_interval(days => %s)
                ORDER BY {order_clause}
                LIMIT %s
                """,
                (days, limit),
            )
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                rows.append(d)
        return jsonify({"days": days, "metric": metric, "top_replies": rows})
    except Exception as e:
        _logger.exception("top_bot_replies error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /analytics/session-stats
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/session-stats")
def session_stats():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)                                                 AS total_sessions,
                  ROUND(AVG(user_message_count + bot_message_count), 1)   AS avg_session_depth,
                  ROUND(AVG(user_message_count), 1)                        AS avg_user_msgs,
                  MAX(user_message_count + bot_message_count)              AS max_session_depth,
                  ROUND(
                    AVG(EXTRACT(EPOCH FROM (
                      COALESCE(ended_at, last_active_at) - started_at
                    )) / 60.0), 1
                  )                                                        AS avg_session_duration_min,
                  COUNT(*) FILTER (WHERE ended_by = 'user_silence')        AS ended_by_silence,
                  COUNT(*) FILTER (WHERE came_back_within_7d = TRUE)       AS came_back_7d,
                  COUNT(*) FILTER (WHERE ended_at IS NOT NULL)             AS closed_sessions
                FROM conversation_sessions
                WHERE started_at >= NOW() - make_interval(days => %s)
                """,
                (days,),
            )
            row = dict(cur.fetchone() or {})

            # Depth distribution
            cur.execute(
                """
                SELECT
                  CASE
                    WHEN user_message_count = 1  THEN '1 msg'
                    WHEN user_message_count <= 3 THEN '2-3 msgs'
                    WHEN user_message_count <= 9 THEN '4-9 msgs'
                    ELSE '10+ msgs'
                  END                AS depth_bucket,
                  MIN(user_message_count) AS bucket_min,
                  COUNT(*)           AS sessions
                FROM conversation_sessions
                WHERE started_at >= NOW() - make_interval(days => %s)
                GROUP BY depth_bucket
                ORDER BY bucket_min NULLS LAST
                """,
                (days,),
            )
            depth_dist = [dict(r) for r in cur.fetchall()]

            # Retention: % that came back within 7d
            closed = row.get("closed_sessions") or 0
            came_back = row.get("came_back_7d") or 0
            row["retention_7d_pct"] = round(came_back / closed * 100, 1) if closed else None

        row["depth_distribution"] = depth_dist
        row["days"] = days
        return jsonify(row)
    except Exception as e:
        _logger.exception("session_stats error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /analytics/reply-length-buckets
# ---------------------------------------------------------------------------

@analytics_bp.route("/analytics/reply-length-buckets")
def reply_length_buckets():
    err = _auth_guard()
    if err:
        return err

    days = _days_param()
    conn = _get_db()
    if not conn:
        return jsonify({"error": "No database configured"}), 503

    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                  CASE
                    WHEN reply_length_chars < 50  THEN '< 50 chars'
                    WHEN reply_length_chars < 100 THEN '50-99 chars'
                    WHEN reply_length_chars < 160 THEN '100-159 chars'
                    WHEN reply_length_chars < 250 THEN '160-249 chars'
                    ELSE '250+ chars'
                  END                                            AS length_bucket,
                  MIN(reply_length_chars)                        AS bucket_min,
                  COUNT(*)                                       AS total,
                  ROUND(AVG(did_user_reply::int) * 100, 1)      AS reply_rate_pct,
                  ROUND(
                    100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                    / NULLIF(COUNT(*), 0),
                    1
                  )                                              AS dropoff_rate_pct,
                  ROUND(AVG(reply_delay_seconds), 0)             AS avg_delay_s
                FROM messages
                WHERE role            = 'assistant'
                  AND did_user_reply IS NOT NULL
                  AND reply_length_chars IS NOT NULL
                  AND created_at     >= NOW() - make_interval(days => %s)
                GROUP BY length_bucket
                ORDER BY bucket_min NULLS LAST
                """,
                (days,),
            )
            rows = [dict(r) for r in cur.fetchall()]
        return jsonify({"days": days, "buckets": rows})
    except Exception as e:
        _logger.exception("reply_length_buckets error")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
