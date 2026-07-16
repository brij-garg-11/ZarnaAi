"""
Giveaway campaigns JSON API — consumed by the Lovable React dashboard.

A giveaway campaign is one drawing with a keyword and an active window. When a
fan texts a message containing the keyword while the campaign is active, the
main app (app/giveaway/entry.py) records them once in giveaway_entries and
replies with the campaign's confirmation message. These endpoints let the
creator create campaigns, see everyone who entered each one, jump into a fan's
inbox thread, star winners, remove bad entries, and export.

All routes are tenant-scoped via _slug_or_abort() and require an active session.
The giveaway_campaigns / giveaway_entries tables are shared on the same
DATABASE_URL (created by both the main app and operator init_db).

Note on freshness: the main app caches active campaigns for ~20s per worker, so
a campaign created here starts catching entries within ~20 seconds — no cross-
process cache invalidation is needed.

Registered via register_giveaway_api_routes(api_bp) from operator/app/routes/api.py.
"""

import csv
import io
import logging
from datetime import datetime, timezone

import psycopg2.extras
from flask import jsonify, request, Response

from ..routes.auth import login_required
from ..db import get_conn

logger = logging.getLogger(__name__)


def _status_of(starts_at, ends_at) -> str:
    """Match app/giveaway/entry.py's active window: NULL bounds are open-ended."""
    now = datetime.now(timezone.utc)
    if starts_at and starts_at > now:
        return "upcoming"
    if ends_at and ends_at < now:
        return "ended"
    return "active"


def _iso(value):
    return value.isoformat() if value else None


def register_giveaway_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach giveaway campaign routes to the shared api blueprint."""

    @api_bp.route("/api/giveaway/campaigns")
    @login_required
    def giveaway_campaigns_list():
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT c.id, c.label, c.keyword, c.starts_at, c.ends_at,
                           c.confirmation_message, c.created_at,
                           COUNT(e.id) AS entries
                    FROM giveaway_campaigns c
                    LEFT JOIN giveaway_entries e
                           ON e.campaign_id = c.id AND e.creator_slug = %s
                    WHERE c.creator_slug = %s
                    GROUP BY c.id
                    ORDER BY c.created_at DESC
                    """,
                    (slug, slug),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to list giveaway campaigns")
            return jsonify(campaigns=[]), 500

        campaigns = [{
            "id": r["id"],
            "label": r["label"],
            "keyword": r["keyword"],
            "starts_at": _iso(r["starts_at"]),
            "ends_at": _iso(r["ends_at"]),
            "confirmation_message": r["confirmation_message"] or "",
            "created_at": _iso(r["created_at"]),
            "entries": r["entries"] or 0,
            "status": _status_of(r["starts_at"], r["ends_at"]),
        } for r in rows]
        return jsonify(campaigns=campaigns, total=len(campaigns))

    @api_bp.route("/api/giveaway/campaigns", methods=["POST"])
    @login_required
    def giveaway_campaign_create():
        require_performer_account()
        slug = slug_or_abort()
        data = request.get_json(force=True, silent=True) or {}
        label = (data.get("label") or "").strip()
        keyword = (data.get("keyword") or "").strip()
        starts_at = (data.get("starts_at") or "").strip() or None
        ends_at = (data.get("ends_at") or "").strip() or None
        confirmation = (data.get("confirmation_message") or "").strip()
        if not label:
            return jsonify(error="label is required"), 400
        if not keyword:
            return jsonify(error="keyword is required — fans text this to enter"), 400
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO giveaway_campaigns "
                    "(label, keyword, starts_at, ends_at, confirmation_message, creator_slug) "
                    "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                    (label[:200], keyword[:100], starts_at, ends_at,
                     confirmation[:1000], slug),
                )
                new_id = cur.fetchone()[0]
            conn.close()
        except Exception:
            logger.exception("api: failed to create giveaway campaign")
            return jsonify(error="failed to create campaign"), 500
        return jsonify(success=True, id=new_id)

    @api_bp.route("/api/giveaway/campaigns/<int:campaign_id>", methods=["DELETE"])
    @login_required
    def giveaway_campaign_delete(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM giveaway_campaigns WHERE id = %s AND creator_slug = %s",
                    (campaign_id, slug),
                )
                deleted = cur.rowcount
            conn.close()
        except Exception:
            logger.exception("api: failed to delete giveaway campaign")
            return jsonify(error="failed to delete"), 500
        if not deleted:
            return jsonify(error="campaign not found"), 404
        return jsonify(success=True)

    @api_bp.route("/api/giveaway/campaigns/<int:campaign_id>/entries")
    @login_required
    def giveaway_entries_list(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, label, keyword, starts_at, ends_at, confirmation_message "
                    "FROM giveaway_campaigns WHERE id = %s AND creator_slug = %s",
                    (campaign_id, slug),
                )
                campaign = cur.fetchone()
                if not campaign:
                    conn.close()
                    return jsonify(error="campaign not found"), 404

                cur.execute(
                    """
                    SELECT e.id, e.phone_number, e.source, e.entered_at,
                           c.fan_name AS contact_name,
                           (sf.phone_number IS NOT NULL) AS starred
                    FROM giveaway_entries e
                    LEFT JOIN contacts c
                           ON c.phone_number = e.phone_number AND c.creator_slug = e.creator_slug
                    LEFT JOIN starred_fans sf
                           ON sf.phone_number = e.phone_number AND sf.creator_slug = e.creator_slug
                    WHERE e.campaign_id = %s
                      AND e.creator_slug = %s
                    ORDER BY e.entered_at DESC
                    """,
                    (campaign_id, slug),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to list giveaway entries")
            return jsonify(error="failed to load entries"), 500

        entries = [{
            "id": r["id"],
            "phone": r["phone_number"] or "",
            "phone_last4": (r["phone_number"] or "")[-4:],
            "fan_name": r["contact_name"] or "",
            "source": r["source"] or "",
            "starred": bool(r["starred"]),
            "entered_at": _iso(r["entered_at"]),
        } for r in rows]
        return jsonify(
            campaign={
                "id": campaign["id"],
                "label": campaign["label"],
                "keyword": campaign["keyword"],
                "starts_at": _iso(campaign["starts_at"]),
                "ends_at": _iso(campaign["ends_at"]),
                "confirmation_message": campaign["confirmation_message"] or "",
                "status": _status_of(campaign["starts_at"], campaign["ends_at"]),
            },
            entries=entries,
            total=len(entries),
        )

    @api_bp.route("/api/giveaway/entries/<int:entry_id>", methods=["DELETE"])
    @login_required
    def giveaway_entry_delete(entry_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM giveaway_entries WHERE id = %s AND creator_slug = %s",
                    (entry_id, slug),
                )
                deleted = cur.rowcount
            conn.close()
        except Exception:
            logger.exception("api: failed to delete giveaway entry")
            return jsonify(error="failed to delete"), 500
        if not deleted:
            return jsonify(error="entry not found"), 404
        return jsonify(success=True)

    @api_bp.route("/api/giveaway/campaigns/<int:campaign_id>/export")
    @login_required
    def giveaway_export(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT e.phone_number, e.source, e.entered_at,
                           c.fan_name AS contact_name,
                           (sf.phone_number IS NOT NULL) AS starred
                    FROM giveaway_entries e
                    LEFT JOIN contacts c
                           ON c.phone_number = e.phone_number AND c.creator_slug = e.creator_slug
                    LEFT JOIN starred_fans sf
                           ON sf.phone_number = e.phone_number AND sf.creator_slug = e.creator_slug
                    WHERE e.campaign_id = %s
                      AND e.creator_slug = %s
                    ORDER BY e.entered_at DESC
                    """,
                    (campaign_id, slug),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to export giveaway entries")
            return jsonify(error="failed to export"), 500

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["entered_at", "fan_name", "phone", "starred", "source"])
        for r in rows:
            entered = r["entered_at"]
            date_str = entered.strftime("%Y-%m-%d %H:%M") if entered else ""
            w.writerow([
                date_str,
                r["contact_name"] or "",
                r["phone_number"] or "",
                "yes" if r["starred"] else "",
                r["source"] or "",
            ])
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=giveaway_entries_{campaign_id}.csv"},
        )
