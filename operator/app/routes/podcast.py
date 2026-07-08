"""
Podcast Q&A JSON API — consumed by the Lovable React dashboard.

Fans text in a question (about anything) plus their name for a shout-out on the
next podcast episode. These endpoints let the creator group submissions into
per-episode campaigns, review them, confirm the real ones, and export.

The dashboard can also trigger an AI "scan" that reads fan messages since the
campaign's promoted date, uses Gemini to spot real question submissions, and
files them into the campaign — all without leaving the frontend.

All routes are tenant-scoped via _slug_or_abort() and require an active session.
The podcast_campaigns / podcast_submissions tables are shared on the same
DATABASE_URL (created by both the main app and operator init_db).

Registered via register_podcast_api_routes(api_bp) from operator/app/routes/api.py.
"""

import csv
import io
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

import psycopg2.extras
from flask import jsonify, request, Response

from ..routes.auth import login_required
from ..db import get_conn

logger = logging.getLogger(__name__)

_STATUSES = ("new", "confirmed", "answered", "skip")

# Gemini config — same env vars the rest of the platform uses.
# NOTE: default is gemini-2.5-flash. gemini-2.0-flash was retired by Google
# (returns 404), which previously made every classification silently fail.
_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
_INTENT_MODEL = os.getenv("INTENT_MODEL", "gemini-2.5-flash")
# Fallback if the configured model is unavailable (e.g. retired) — keeps a
# scan from silently finding zero when a model name goes stale.
_INTENT_MODEL_FALLBACK = "gemini-2.5-flash"

# Safety cap so one scan can't run Gemini against an unbounded backlog.
_SCAN_MAX_MESSAGES = int(os.getenv("PODCAST_SCAN_MAX_MESSAGES", "3000"))
_SCAN_MIN_CHARS = 20
# How many messages to classify in parallel. Gemini calls are network-bound,
# so a modest pool cuts a 1,000-message scan from ~20 min to ~2 min with no
# change to per-message classification quality.
_SCAN_CONCURRENCY = int(os.getenv("PODCAST_SCAN_CONCURRENCY", "8"))

_PROMPT_TEMPLATE = """You are analyzing fan SMS messages sent to comedian Zarna Garg.

Zarna is running a marketing campaign: fans can text in a QUESTION (about anything — her life,
her family, relationships, advice, their own situation, random curiosities, etc.) along with
their name, and she'll shout them out and answer their question on her next podcast episode.

Your job: determine if this SMS message is a fan submitting a question for a shout-out.
The question does NOT have to be about the podcast — it can be about ANY topic.

Fan message:
"{message}"

A message IS a submission if:
- The fan is asking a genuine question they'd want answered (any topic is fine)
- They may include their name (e.g. "I'm Sarah" / "My name is Mike" / "This is Priya")
- They may mention the podcast, the shout-out, or the campaign — but they don't have to
- It reads like a real question directed at Zarna, not just chit-chat

A message is NOT a submission if:
- It's just a compliment or reaction with no question ("loved your show!", "so funny")
- It's only asking logistics like where to find the podcast/tickets/merch
- It's completely unrelated spam or gibberish
- It's too vague to be a real question

Respond in EXACTLY this format (no other text):

If it IS a submission:
SUBMISSION
QUESTION: [the actual question the fan wants answered, cleaned up and complete]
NAME: [the fan's self-reported name, or UNKNOWN if they didn't give one]

If it is NOT a submission:
NOT_SUBMISSION"""


def _parse_classification(raw: str) -> dict | None:
    if not raw or not raw.startswith("SUBMISSION"):
        return None
    question, fan_name = "", ""
    for line in raw.splitlines():
        if line.startswith("QUESTION:"):
            question = line[len("QUESTION:"):].strip()
        elif line.startswith("NAME:"):
            fan_name = line[len("NAME:"):].strip()
            if fan_name.upper() == "UNKNOWN":
                fan_name = ""
    if not question:
        return None
    return {"question": question, "fan_name": fan_name}


def _classify_message(client, message_text: str) -> dict | None:
    """Return {question, fan_name} if the message is a submission, else None."""
    prompt = _PROMPT_TEMPLATE.format(message=message_text.replace('"', '\\"'))
    for model in (_INTENT_MODEL, _INTENT_MODEL_FALLBACK):
        try:
            response = client.models.generate_content(model=model, contents=prompt)
            return _parse_classification((response.text or "").strip())
        except Exception as e:
            # If the primary model is unavailable (e.g. retired → 404), try the
            # fallback once. Any other error is logged and treated as "no match".
            if model != _INTENT_MODEL_FALLBACK and "404" in str(e):
                logger.warning("podcast scan: model %s unavailable, falling back", model)
                continue
            logger.exception("podcast scan: Gemini classify failed (model=%s)", model)
            return None
    return None


def _run_scan(campaign_id: int, slug: str, since_date):
    """Background worker: scan messages since since_date and file submissions.

    Classification is done concurrently (network-bound Gemini calls); DB inserts
    are performed serially on a single connection as results arrive.
    """
    found = 0
    try:
        from google import genai
        client = genai.Client(api_key=_GEMINI_API_KEY)

        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT m.id, m.phone_number, m.text
                FROM messages m
                WHERE m.role = 'user'
                  AND m.creator_slug = %s
                  AND m.created_at >= %s
                  AND LENGTH(m.text) >= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM podcast_submissions s
                      WHERE s.message_id = m.id
                  )
                ORDER BY m.created_at ASC
                LIMIT %s
                """,
                (slug, since_date, _SCAN_MIN_CHARS, _SCAN_MAX_MESSAGES),
            )
            rows = cur.fetchall()
        conn.close()

        def _classify_row(row):
            return row, _classify_message(client, (row["text"] or "").strip())

        insert_conn = get_conn()
        processed = 0
        try:
            with ThreadPoolExecutor(max_workers=_SCAN_CONCURRENCY) as pool:
                for row, result in pool.map(_classify_row, rows):
                    processed += 1
                    if result:
                        found += 1
                        try:
                            with insert_conn, insert_conn.cursor() as cur2:
                                cur2.execute(
                                    """
                                    INSERT INTO podcast_submissions
                                        (phone_number, message_id, campaign_id, question, fan_name, creator_slug)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (phone_number, message_id) DO NOTHING
                                    """,
                                    (row["phone_number"], row["id"], campaign_id,
                                     result["question"], result["fan_name"], slug),
                                )
                        except Exception:
                            logger.exception("podcast scan: insert failed for message %s", row["id"])
                    # Heartbeat every 25 messages so the dashboard shows progress.
                    if processed % 25 == 0:
                        _update_scan_progress(campaign_id, slug, found)
        finally:
            insert_conn.close()

        _finish_scan(campaign_id, slug, status="done", found=found, error=None)
    except Exception as e:
        logger.exception("podcast scan: run failed for campaign %s", campaign_id)
        _finish_scan(campaign_id, slug, status="error", found=found, error=str(e)[:400])


def _update_scan_progress(campaign_id: int, slug: str, found: int):
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE podcast_campaigns SET scan_found = %s "
                "WHERE id = %s AND creator_slug = %s AND scan_status = 'running'",
                (found, campaign_id, slug),
            )
        conn.close()
    except Exception:
        logger.exception("podcast scan: heartbeat update failed for campaign %s", campaign_id)


def _finish_scan(campaign_id: int, slug: str, status: str, found: int, error):
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE podcast_campaigns "
                "SET scan_status = %s, scan_found = %s, scan_error = %s, scanned_at = NOW() "
                "WHERE id = %s AND creator_slug = %s",
                (status, found, error, campaign_id, slug),
            )
        conn.close()
    except Exception:
        logger.exception("podcast scan: failed to finalize campaign %s", campaign_id)


def register_podcast_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach podcast Q&A routes to the shared api blueprint."""

    @api_bp.route("/api/podcast/campaigns")
    @login_required
    def podcast_campaigns_list():
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT c.id, c.label, c.promoted_at, c.created_at,
                           c.scan_status, c.scan_found, c.scanned_at,
                           COUNT(s.id) FILTER (WHERE s.status = 'new')                     AS to_review,
                           COUNT(s.id) FILTER (WHERE s.status IN ('confirmed','answered')) AS confirmed,
                           COUNT(s.id) FILTER (WHERE s.status = 'answered')                AS answered,
                           COUNT(s.id) FILTER (WHERE s.status != 'skip')                   AS total
                    FROM podcast_campaigns c
                    LEFT JOIN podcast_submissions s
                           ON s.campaign_id = c.id AND s.creator_slug = %s
                    WHERE c.creator_slug = %s
                    GROUP BY c.id
                    ORDER BY c.created_at DESC
                    """,
                    (slug, slug),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to list podcast campaigns")
            return jsonify(campaigns=[]), 500

        campaigns = [{
            "id": r["id"],
            "label": r["label"],
            "promoted_at": r["promoted_at"].isoformat() if r["promoted_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "scan_status": r["scan_status"],
            "scan_found": r["scan_found"],
            "scanned_at": r["scanned_at"].isoformat() if r["scanned_at"] else None,
            "to_review": r["to_review"] or 0,
            "confirmed": r["confirmed"] or 0,
            "answered": r["answered"] or 0,
            "total": r["total"] or 0,
        } for r in rows]
        return jsonify(campaigns=campaigns, total=len(campaigns))

    @api_bp.route("/api/podcast/campaigns", methods=["POST"])
    @login_required
    def podcast_campaign_create():
        require_performer_account()
        slug = slug_or_abort()
        data = request.get_json(force=True, silent=True) or {}
        label = (data.get("label") or "").strip()
        promoted_at = (data.get("promoted_at") or "").strip() or None
        if not label:
            return jsonify(error="label is required"), 400
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO podcast_campaigns (label, promoted_at, creator_slug) "
                    "VALUES (%s, %s, %s) RETURNING id",
                    (label[:200], promoted_at, slug),
                )
                new_id = cur.fetchone()[0]
            conn.close()
        except Exception:
            logger.exception("api: failed to create podcast campaign")
            return jsonify(error="failed to create campaign"), 500
        return jsonify(success=True, id=new_id)

    @api_bp.route("/api/podcast/campaigns/<int:campaign_id>/submissions")
    @login_required
    def podcast_submissions_list(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, label, promoted_at, scan_status, scan_found, scan_error, scanned_at "
                    "FROM podcast_campaigns WHERE id = %s AND creator_slug = %s",
                    (campaign_id, slug),
                )
                campaign = cur.fetchone()
                if not campaign:
                    conn.close()
                    return jsonify(error="campaign not found"), 404

                cur.execute(
                    """
                    SELECT s.id, s.phone_number, s.question, s.fan_name, s.status,
                           s.created_at, m.created_at AS message_at, c.fan_name AS contact_name
                    FROM podcast_submissions s
                    LEFT JOIN messages m
                           ON m.id = s.message_id
                    LEFT JOIN contacts c
                           ON c.phone_number = s.phone_number AND c.creator_slug = s.creator_slug
                    WHERE s.campaign_id = %s
                      AND s.creator_slug = %s
                      AND s.status != 'skip'
                    ORDER BY m.created_at ASC NULLS LAST, s.created_at ASC
                    """,
                    (campaign_id, slug),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to list podcast submissions")
            return jsonify(error="failed to load submissions"), 500

        def fmt(r):
            # Show when the fan actually texted (message_at), not when the scan
            # filed the submission (s.created_at).
            sent_at = r["message_at"] or r["created_at"]
            return {
                "id": r["id"],
                "phone": r["phone_number"] or "",
                "phone_last4": (r["phone_number"] or "")[-4:],
                "question": r["question"],
                "fan_name": r["fan_name"] or r["contact_name"] or "",
                "status": r["status"],
                "created_at": sent_at.isoformat() if sent_at else None,
            }

        to_review = [fmt(r) for r in rows if r["status"] == "new"]
        confirmed = [fmt(r) for r in rows if r["status"] in ("confirmed", "answered")]
        return jsonify(
            campaign={
                "id": campaign["id"],
                "label": campaign["label"],
                "promoted_at": campaign["promoted_at"].isoformat() if campaign["promoted_at"] else None,
                "scan_status": campaign["scan_status"],
                "scan_found": campaign["scan_found"],
                "scan_error": campaign["scan_error"],
                "scanned_at": campaign["scanned_at"].isoformat() if campaign["scanned_at"] else None,
            },
            to_review=to_review,
            confirmed=confirmed,
        )

    @api_bp.route("/api/podcast/submissions/<int:sub_id>/status", methods=["POST"])
    @login_required
    def podcast_submission_status(sub_id):
        require_performer_account()
        slug = slug_or_abort()
        data = request.get_json(force=True, silent=True) or {}
        status = (data.get("status") or "").strip()
        if status not in _STATUSES:
            return jsonify(error="invalid status"), 400
        try:
            conn = get_conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE podcast_submissions SET status = %s "
                    "WHERE id = %s AND creator_slug = %s",
                    (status, sub_id, slug),
                )
                updated = cur.rowcount
            conn.close()
        except Exception:
            logger.exception("api: failed to update podcast submission status")
            return jsonify(error="failed to update"), 500
        if not updated:
            return jsonify(error="submission not found"), 404
        return jsonify(success=True)

    @api_bp.route("/api/podcast/campaigns/<int:campaign_id>/scan", methods=["POST"])
    @login_required
    def podcast_scan(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        if not _GEMINI_API_KEY:
            return jsonify(error="AI scanning is not configured (GEMINI_API_KEY missing)."), 503

        data = request.get_json(force=True, silent=True) or {}
        since_override = (data.get("since") or "").strip() or None

        try:
            conn = get_conn()
            # Atomically claim the scan: only start if not already running.
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT promoted_at, scan_status FROM podcast_campaigns "
                    "WHERE id = %s AND creator_slug = %s FOR UPDATE",
                    (campaign_id, slug),
                )
                row = cur.fetchone()
                if not row:
                    conn.close()
                    return jsonify(error="campaign not found"), 404
                promoted_at, scan_status = row[0], row[1]
                if scan_status == "running":
                    conn.close()
                    return jsonify(error="A scan is already running for this campaign."), 409

                since_date = since_override or (promoted_at.isoformat() if promoted_at else None)
                if not since_date:
                    conn.close()
                    return jsonify(
                        error="Set a promoted date on this campaign first so we know how far back to scan."
                    ), 400

                cur.execute(
                    "UPDATE podcast_campaigns "
                    "SET scan_status = 'running', scan_error = NULL, scan_found = 0 "
                    "WHERE id = %s AND creator_slug = %s",
                    (campaign_id, slug),
                )
            conn.close()
        except Exception:
            logger.exception("api: failed to start podcast scan")
            return jsonify(error="failed to start scan"), 500

        t = threading.Thread(
            target=_run_scan, args=(campaign_id, slug, since_date), daemon=True
        )
        t.start()
        return jsonify(success=True, status="running", since=since_date)

    @api_bp.route("/api/podcast/campaigns/<int:campaign_id>/scan-status")
    @login_required
    def podcast_scan_status(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT scan_status, scan_found, scan_error, scanned_at "
                    "FROM podcast_campaigns WHERE id = %s AND creator_slug = %s",
                    (campaign_id, slug),
                )
                row = cur.fetchone()
            conn.close()
        except Exception:
            logger.exception("api: failed to read podcast scan status")
            return jsonify(error="failed to read status"), 500
        if not row:
            return jsonify(error="campaign not found"), 404
        return jsonify(
            scan_status=row["scan_status"],
            scan_found=row["scan_found"],
            scan_error=row["scan_error"],
            scanned_at=row["scanned_at"].isoformat() if row["scanned_at"] else None,
        )

    @api_bp.route("/api/podcast/campaigns/<int:campaign_id>/export")
    @login_required
    def podcast_export(campaign_id):
        require_performer_account()
        slug = slug_or_abort()
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT s.phone_number, s.question, s.fan_name, s.status, s.created_at,
                           m.created_at AS message_at, c.fan_name AS contact_name
                    FROM podcast_submissions s
                    LEFT JOIN messages m
                           ON m.id = s.message_id
                    LEFT JOIN contacts c
                           ON c.phone_number = s.phone_number AND c.creator_slug = s.creator_slug
                    WHERE s.campaign_id = %s
                      AND s.creator_slug = %s
                      AND s.status IN ('confirmed','answered')
                    ORDER BY m.created_at ASC NULLS LAST, s.created_at ASC
                    """,
                    (campaign_id, slug),
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            logger.exception("api: failed to export podcast submissions")
            return jsonify(error="failed to export"), 500

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["date", "fan_name", "phone_last4", "question", "status"])
        for r in rows:
            name = r["fan_name"] or r["contact_name"] or ""
            sent_at = r["message_at"] or r["created_at"]
            date_str = sent_at.strftime("%Y-%m-%d") if sent_at else ""
            w.writerow([date_str, name, (r["phone_number"] or "")[-4:], r["question"], r["status"]])
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=podcast_confirmed_{campaign_id}.csv"},
        )
