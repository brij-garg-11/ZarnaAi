"""
Blast Tool — send group texts to segmented audiences.
Toggle between Twilio (SMS) and SlickText.
Draft board for composing and saving messages before sending.
Scheduled send support via the background scheduler.
"""

import base64
import hashlib
import logging
import os
import secrets as _secrets
import uuid
from datetime import datetime, timezone

import psycopg2
from flask import Blueprint, Response as _Response, flash, jsonify, redirect, render_template, request, url_for

from ..routes.auth import login_required, current_user
from ..queries import (
    count_audience,
    get_all_tags,
    list_blast_drafts,
    get_blast_draft,
    save_blast_draft,
    schedule_blast,
    mark_blast_cancelled,
    list_shows,
)
from ..blast_sender import execute_blast_async

logger = logging.getLogger(__name__)
blast_bp = Blueprint("blast", __name__)


def _get_tier_counts() -> dict:
    """Return {tier: count} for all four tiers. Fast single query."""
    from ..db import get_conn
    counts = {"superfan": 0, "engaged": 0, "lurker": 0, "dormant": 0}
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fan_tier, COUNT(*) FROM contacts "
                    "WHERE fan_tier IS NOT NULL GROUP BY fan_tier"
                )
                for tier, cnt in cur.fetchall():
                    if tier in counts:
                        counts[tier] = cnt
        conn.close()
    except Exception as e:
        logger.warning("_get_tier_counts failed: %s", e)
    return counts


@blast_bp.route("/operator/blast")
@login_required
def blast_index():
    _reset_stuck_sending_drafts()
    drafts = list_blast_drafts()
    tags = get_all_tags()
    shows = list_shows()
    return render_template(
        "blast.html",
        user=current_user(),
        drafts=drafts,
        tags=tags,
        shows=shows,
        active_draft=None,
        audience_count=None,
        image_upload_enabled=_image_upload_configured(),
        tracked_short_url="",
        tier_counts=_get_tier_counts(),
    )


def _reset_stuck_sending_drafts():
    """Reset any drafts stuck in 'sending' for >5 min back to 'draft' so they can be retried."""
    from ..db import get_conn
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE blast_drafts
                    SET status = 'draft', updated_at = NOW()
                    WHERE status = 'sending'
                      AND updated_at < NOW() - INTERVAL '5 minutes'
                """)
                if cur.rowcount:
                    logger.info("Reset %d stuck 'sending' blast drafts back to 'draft'", cur.rowcount)
    except Exception as e:
        logger.warning("Could not reset stuck drafts: %s", e)
    finally:
        conn.close()


@blast_bp.route("/operator/blast/new")
@login_required
def blast_new():
    """Auto-create a blank draft and open the compose form immediately."""
    user = current_user()
    new_id = save_blast_draft(
        name="Untitled draft",
        body="",
        channel="twilio",
        audience_type="all",
        audience_filter="",
        sample_pct=100,
        created_by=user["email"] if user else "",
    )
    return redirect(url_for("blast.blast_compose", draft_id=new_id))


@blast_bp.route("/operator/blast/new-for-show/<int:show_id>")
@login_required
def blast_new_for_show(show_id: int):
    """
    Create a pre-filled draft targeted at a specific live show audience,
    then redirect straight into the compose form — no manual setup needed.
    """
    from ..queries import get_show
    user = current_user()
    show = get_show(show_id)
    show_name = show["name"] if show else f"Show #{show_id}"
    new_id = save_blast_draft(
        name=f"{show_name} — show message",
        body="",
        channel="slicktext",
        audience_type="show",
        audience_filter=str(show_id),
        sample_pct=100,
        created_by=user["email"] if user else "",
    )
    return redirect(url_for("blast.blast_compose", draft_id=new_id))


@blast_bp.route("/operator/blast/<int:draft_id>")
@login_required
def blast_compose(draft_id: int):
    tags = get_all_tags()
    shows = list_shows()
    drafts = list_blast_drafts()

    active_draft = get_blast_draft(draft_id)
    if not active_draft:
        flash("Draft not found.", "error")
        return redirect(url_for("blast.blast_index"))

    audience_count = count_audience(
        active_draft["audience_type"],
        active_draft["audience_filter"] or "",
        int(active_draft["audience_sample_pct"] or 100),
    )

    # Build the full tracked short URL so the template can display it
    tracked_short_url = ""
    if active_draft.get("tracked_link_slug"):
        scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
        host   = request.headers.get("X-Forwarded-Host", request.host)
        tracked_short_url = f"{scheme}://{host}/t/{active_draft['tracked_link_slug']}"

    return render_template(
        "blast.html",
        user=current_user(),
        drafts=drafts,
        tags=tags,
        shows=shows,
        active_draft=active_draft,
        audience_count=audience_count,
        image_upload_enabled=_image_upload_configured(),
        tracked_short_url=tracked_short_url,
        tier_counts=_get_tier_counts(),
    )


def _s3_configured() -> bool:
    return all([
        os.getenv("IMAGE_BUCKET"),
        os.getenv("IMAGE_ENDPOINT_URL"),
        os.getenv("IMAGE_AWS_KEY_ID"),
        os.getenv("IMAGE_AWS_KEY_SECRET"),
        os.getenv("IMAGE_PUBLIC_BASE_URL"),
    ])


def _image_upload_configured() -> bool:
    return True


@blast_bp.route("/t/<slug>")
def track_redirect_operator(slug: str):
    """
    Tracked-link redirect served by the operator app — no auth required.
    Mirrors the same route in the main app; both log to the shared DB table.
    """
    from ..db import get_conn
    from flask import redirect as _redir

    destination = None
    conn = get_conn()

    # ── Slug lookup ────────────────────────────────────────────────────────
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, destination FROM tracked_links WHERE slug=%s", (slug,))
            row = cur.fetchone()
        if not row:
            logger.warning("track_redirect_operator: slug=%r not found", slug)
            conn.close()
            return "Link not found", 404
        link_id, destination = row[0], row[1]
    except Exception as e:
        logger.error("track_redirect_operator: lookup error slug=%r: %s", slug, e)
        conn.close()
        return "Link not found", 404

    # ── Click logging (non-critical — redirect always fires) ────────────────
    phone_number: str | None = None
    try:
        # Decode fan identity from ?f= query param (base64url-encoded phone number)
        fan_token = request.args.get("f", "")
        if fan_token:
            try:
                import base64 as _b64
                padding = 4 - len(fan_token) % 4
                padded = fan_token + ("=" * padding if padding != 4 else "")
                phone_number = _b64.urlsafe_b64decode(padded).decode()
            except Exception:
                phone_number = None

        ip_raw = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        ip_hash = hashlib.sha256(ip_raw.encode()).hexdigest()[:16] if ip_raw else ""
        ua_short = (request.user_agent.string or "")[:120]
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tracked_link_clicks (link_id, ip_hash, ua_short, phone_number) VALUES (%s,%s,%s,%s)",
                    (link_id, ip_hash, ua_short, phone_number),
                )
        logger.info("track_redirect_operator: logged click slug=%r link_id=%s phone=%s",
                    slug, link_id, "known" if phone_number else "anonymous")
    except Exception as e:
        logger.error("track_redirect_operator: failed to log click slug=%r link_id=%s: %s", slug, link_id, e)
    finally:
        conn.close()

    # Flip link_clicked_1h on the most recent assistant message for this fan (if within 60 min)
    if phone_number:
        try:
            upd_conn = get_conn()
            with upd_conn:
                with upd_conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE messages
                        SET link_clicked_1h = TRUE
                        WHERE id = (
                            SELECT id FROM messages
                            WHERE phone_number = %s
                              AND role = 'assistant'
                              AND created_at >= NOW() - INTERVAL '60 minutes'
                            ORDER BY created_at DESC
                            LIMIT 1
                        )
                        """,
                        (phone_number,),
                    )
            upd_conn.close()
            logger.info("track_redirect_operator: set link_clicked_1h for phone ...%s slug=%r",
                        phone_number[-4:] if len(phone_number) >= 4 else phone_number, slug)
        except Exception as e:
            logger.warning("track_redirect_operator: link_clicked_1h update failed slug=%r: %s", slug, e)

    return _redir(destination, 302)


def _create_tracked_link(raw_url: str, label: str) -> str | None:
    """
    Always create a brand-new tracked link for this specific blast draft.
    Never reuses an existing row — each blast gets its own slug so CTR
    is measured per-message, not per-destination URL.
    Returns the new slug.
    """
    if not raw_url or not raw_url.startswith(("http://", "https://")):
        return None
    from ..db import get_conn
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                slug = _secrets.token_urlsafe(6)
                cur.execute(
                    "INSERT INTO tracked_links (slug, label, campaign_type, destination) "
                    "VALUES (%s, %s, 'other', %s) RETURNING slug",
                    (slug, (label or raw_url)[:200], raw_url),
                )
                return cur.fetchone()[0]
    except Exception as e:
        logger.exception("_create_tracked_link error: %s", e)
        return None
    finally:
        conn.close()


@blast_bp.route("/operator/blast/img/<int:image_id>/<filename>")
def serve_db_image_legacy(image_id: int, filename: str):
    """
    Backward-compatible route for blast images uploaded before the access_token
    migration. Serves the image directly without token validation so existing
    Twilio MMS URLs in sent/draft blasts keep working.
    """
    from flask import Response
    from ..db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data_b64, mime_type FROM operator_blast_images WHERE id=%s",
                (image_id,),
            )
            row = cur.fetchone()
        if not row or not row[0]:
            return "Image not found", 404
        data = base64.b64decode(row[0])
        resp = Response(data, status=200, mimetype=row[1] or "image/jpeg")
        resp.headers["Cache-Control"] = "public, max-age=86400"
        return resp
    except Exception as e:
        logger.exception("serve_db_image_legacy error for id=%s: %s", image_id, e)
        return "Error serving image", 500
    finally:
        conn.close()


@blast_bp.route("/operator/blast/img/<int:image_id>/<access_token>/<filename>")
def serve_db_image(image_id: int, access_token: str, filename: str):
    """
    Serve a blast image from Postgres — NO login required (Twilio fetches during MMS).
    access_token in the URL prevents enumeration: guessing an integer id is not
    enough; the caller must also know the random token set at upload time.
    """
    from flask import Response
    from ..db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data_b64, mime_type, access_token FROM operator_blast_images WHERE id=%s",
                (image_id,),
            )
            row = cur.fetchone()
        if not row or not row[0]:
            logger.warning("serve_db_image: id=%s not found or empty", image_id)
            return "Image not found", 404
        stored_token = row[2] or ""
        if stored_token and stored_token != access_token:
            logger.warning("serve_db_image: invalid token for id=%s", image_id)
            return "Image not found", 404
        data = base64.b64decode(row[0])
        mime_type = row[1] or "image/jpeg"
        logger.info("serve_db_image: id=%s decoded_size=%d mime=%s", image_id, len(data), mime_type)
        resp = Response(data, status=200, mimetype=mime_type)
        resp.headers["Cache-Control"] = "public, max-age=86400"
        return resp
    except Exception as e:
        logger.exception("serve_db_image error for id=%s: %s", image_id, e)
        return "Error serving image", 500
    finally:
        conn.close()


@blast_bp.route("/operator/blast/upload-image", methods=["POST"])
@login_required
def upload_image():
    """
    Upload a blast image.
    Default: stores bytes in Postgres → URL survives container redeploys.
    Optional: S3/R2 when IMAGE_BUCKET etc. env vars are set.
    """
    f = request.files.get("image")
    if not f or not f.filename:
        return jsonify({"error": "No file received."}), 400

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else "jpg"
    if ext not in ("jpg", "jpeg", "png", "gif", "webp", "pdf"):
        return jsonify({"error": f"Unsupported format .{ext} — use jpg, png, gif, or webp."}), 400

    filename = f"{uuid.uuid4().hex}.{ext}"
    mime_type = f.content_type or f"image/{ext}"

    # ── S3 / R2 (optional) ──────────────────────────────────────────────────
    if _s3_configured():
        try:
            import boto3
            bucket      = os.getenv("IMAGE_BUCKET")
            endpoint    = os.getenv("IMAGE_ENDPOINT_URL")
            key_id      = os.getenv("IMAGE_AWS_KEY_ID")
            key_secret  = os.getenv("IMAGE_AWS_KEY_SECRET")
            public_base = os.getenv("IMAGE_PUBLIC_BASE_URL", "").rstrip("/")
            key = f"blast-images/{filename}"
            s3 = boto3.client("s3", endpoint_url=endpoint,
                              aws_access_key_id=key_id, aws_secret_access_key=key_secret)
            s3.upload_fileobj(f.stream, bucket, key,
                              ExtraArgs={"ContentType": mime_type, "ACL": "public-read"})
            url = f"{public_base}/{key}"
            logger.info("Uploaded blast image to S3/R2: %s", url)
            return jsonify({"url": url})
        except Exception as e:
            logger.exception("S3 upload failed, falling back to DB: %s", e)
            f.stream.seek(0)

    # ── Postgres via base64 TEXT (zero-config, survives redeploys) ──────────
    try:
        data = f.read()
        logger.info("upload_image: read %d bytes (mime=%s)", len(data), mime_type)
        if not data:
            return jsonify({"error": "Uploaded file is empty — please try again."}), 400

        data_b64 = base64.b64encode(data).decode("ascii")

        from ..db import get_conn
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
            logger.info("Stored blast image in DB: id=%s original_size=%d b64_size=%d",
                        image_id, len(data), len(data_b64))
        finally:
            conn.close()

        # Always use the public HTTPS origin so SlickText/Twilio can fetch
        # the image without being redirected (Railway terminates TLS at proxy).
        scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
        host = request.headers.get("X-Forwarded-Host", request.host)
        base_url = f"{scheme}://{host}"
        url = f"{base_url}/operator/blast/img/{image_id}/{filename}"
        logger.info("upload_image: public URL=%s", url)
        return jsonify({"url": url, "size": len(data)})
    except Exception as e:
        logger.exception("DB image store failed: %s", e)
        return jsonify({"error": f"Upload failed: {e}"}), 500


@blast_bp.route("/operator/blast/smart-send-preview", methods=["POST"])
@login_required
def smart_send_preview():
    """
    Smart Send preview — returns a JSON breakdown of how many fans per tier
    would receive the blast, accounting for per-tier frequency cadence.

    Cadence rules (days since last blast to suppress):
      superfan : 5 days
      engaged  : 7 days
      lurker   : 14 days
      dormant  : 30 days

    Response:
      {
        "tiers": {
          "superfan": {"total": N, "suppressed": N, "sending": N},
          ...
        },
        "total_sending": N,
        "total_suppressed": N
      }
    """
    from ..db import get_conn as _get_conn
    CADENCE_DAYS = {"superfan": 5, "engaged": 7, "lurker": 14, "dormant": 30}
    conn = _get_conn()
    result = {"tiers": {}, "total_sending": 0, "total_suppressed": 0}
    try:
        with conn.cursor() as cur:
            # Optouts — exclude from all tiers
            cur.execute("SELECT phone_number FROM broadcast_optouts")
            optouts = {r[0] for r in cur.fetchall()}

            for tier, cadence in CADENCE_DAYS.items():
                # All fans in this tier (excluding WhatsApp + optouts)
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts "
                    "WHERE fan_tier = %s AND phone_number NOT LIKE 'whatsapp:%%'",
                    (tier,),
                )
                all_phones = {r[0] for r in cur.fetchall()} - optouts

                # Fans in this tier blasted within cadence window
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
                    "total":      len(all_phones),
                    "suppressed": suppressed,
                    "sending":    sending,
                    "cadence_days": cadence,
                }
                result["total_sending"]    += sending
                result["total_suppressed"] += suppressed
    finally:
        conn.close()

    return jsonify(result)


@blast_bp.route("/operator/blast/preview-count", methods=["POST"])
@login_required
def preview_count():
    """HTMX or AJAX endpoint — returns audience count for current filter."""
    audience_type = request.form.get("audience_type", "all")
    if audience_type not in ("all", "tag", "location", "random", "show", "tier"):
        audience_type = "all"
    audience_filter = request.form.get("audience_filter", "").strip()
    sample_pct = _safe_int(request.form.get("audience_sample_pct"), 100, 1, 100)
    count = count_audience(audience_type, audience_filter, sample_pct)
    return f'<span class="count-badge">{count:,} recipients match</span>'


@blast_bp.route("/operator/blast/save", methods=["POST"])
@login_required
def save_draft():
    user = current_user()
    intent = request.form.get("intent", "save")

    # ── DEBUG: log everything received so we can diagnose in Railway logs ──
    logger.info("=== BLAST SAVE_DRAFT called ===")
    logger.info("  intent=%r  user=%r", intent, user.get("email") if user else None)
    logger.info("  form keys: %s", list(request.form.keys()))
    logger.info("  draft_id=%r  name=%r  channel=%r  audience_type=%r",
                request.form.get("draft_id"), request.form.get("name"),
                request.form.get("channel"), request.form.get("audience_type"))
    body_raw = request.form.get("body", "")
    logger.info("  body length=%d  body preview=%r", len(body_raw), body_raw[:80])

    name = (request.form.get("name") or "Untitled draft").strip()[:120]
    body = body_raw.strip()
    channel = request.form.get("channel", "twilio")
    if channel not in ("twilio", "slicktext"):
        channel = "twilio"
    audience_type = request.form.get("audience_type", "all")
    if audience_type not in ("all", "tag", "location", "random", "show", "tier"):
        audience_type = "all"
    audience_filter = (request.form.get("audience_filter") or "").strip()[:200]
    sample_pct = _safe_int(request.form.get("audience_sample_pct"), 100, 1, 100)
    media_url = (request.form.get("media_url") or "").strip()[:1000]
    link_url  = (request.form.get("link_url")  or "").strip()[:2000]
    tracked_link_slug = (request.form.get("tracked_link_slug") or "").strip()
    # Quiz fields — checkbox sends "1" when checked; hidden field is fallback
    is_quiz_raw = request.form.get("is_quiz") or request.form.get("is_quiz_hidden") or "0"
    is_quiz = is_quiz_raw in ("1", "true", "on")
    quiz_correct_answer = (request.form.get("quiz_correct_answer") or "").strip()[:500]
    # Blast context — operator-only note giving AI background on the blast
    blast_context_note = (request.form.get("blast_context_note") or "").strip()[:1000]
    draft_id_raw = request.form.get("draft_id")
    draft_id = int(draft_id_raw) if draft_id_raw and draft_id_raw.isdigit() else None

    logger.info("  parsed: body=%r  audience_type=%r  audience_filter=%r  draft_id=%r  media_url=%r  link_url=%r",
                body[:60] if body else "", audience_type, audience_filter, draft_id,
                media_url[:60] if media_url else "", link_url[:60] if link_url else "")

    # Create a new tracked link the first time a link_url is saved with this draft.
    # We intentionally never reuse slugs across drafts so each blast has its own CTR.
    if link_url and not tracked_link_slug:
        tracked_link_slug = _create_tracked_link(link_url, name) or ""
        logger.info("  created tracked_link_slug=%r for link_url=%r", tracked_link_slug, link_url[:60])

    if not body:
        logger.warning("  BLOCKED: body is empty")
        flash("Message body is required.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id) if draft_id else url_for("blast.blast_index"))

    try:
        new_id = save_blast_draft(
            name=name, body=body, channel=channel,
            audience_type=audience_type, audience_filter=audience_filter,
            sample_pct=sample_pct, media_url=media_url,
            link_url=link_url, tracked_link_slug=tracked_link_slug,
            is_quiz=is_quiz, quiz_correct_answer=quiz_correct_answer,
            blast_context_note=blast_context_note,
            created_by=user["email"], draft_id=draft_id,
        )
        logger.info("  saved draft id=%s", new_id)
    except Exception as e:
        logger.exception("  FAILED to save draft: %s", e)
        flash(f"Save failed: {e}", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id) if draft_id else url_for("blast.blast_index"))

    if intent == "test":
        test_phone = (request.form.get("test_phone") or "").strip()
        logger.info("  TEST intent: phone=%r", test_phone)
        if not test_phone:
            flash("Enter a phone number to send the test to.", "error")
            return redirect(url_for("blast.blast_compose", draft_id=new_id))
        from ..blast_sender import _send_one, _create_blast_context_session
        test_body = f"[TEST] {body}"
        if tracked_link_slug:
            main_base = os.getenv("MAIN_APP_BASE_URL", "").rstrip("/")
            if not main_base:
                railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
                main_base = f"https://{railway_domain}" if railway_domain else ""
            if main_base:
                test_body = f"{test_body}\n{main_base}/t/{tracked_link_slug}"
        ok = _send_one(test_phone, test_body, channel, media_url=media_url)
        logger.info("  TEST send result: ok=%s", ok)
        if ok:
            masked = test_phone[-4:].rjust(len(test_phone), "*")
            flash(f"Test sent to {masked}. Draft saved.", "success")
            if body:
                combined = f"The blast message that was sent: \"{body}\""
                if blast_context_note:
                    combined += f"\n\nAdditional context from the operator: {blast_context_note}"
                _create_blast_context_session(new_id, combined)
                logger.info("  TEST: blast_context_session created for draft %s", new_id)
        else:
            flash("Test send failed — check Twilio/SlickText credentials in Railway env vars.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=new_id))

    if intent == "schedule":
        send_at_str = (request.form.get("send_at") or "").strip()
        logger.info("  SCHEDULE intent: send_at=%r draft=%s", send_at_str, new_id)
        if not send_at_str:
            flash("Pick a send time before scheduling.", "error")
            return redirect(url_for("blast.blast_compose", draft_id=new_id))
        try:
            send_at = datetime.fromisoformat(send_at_str).replace(tzinfo=timezone.utc)
        except ValueError:
            flash("Invalid date — use the date/time picker.", "error")
            return redirect(url_for("blast.blast_compose", draft_id=new_id))
        schedule_blast(new_id, send_at)
        flash(f"Blast scheduled for {send_at.strftime('%b %d at %I:%M %p UTC')}. Draft auto-saved.", "success")
        return redirect(url_for("blast.blast_index"))

    if intent == "send":
        logger.info("  SEND intent: firing blast for draft %s", new_id)
        existing = get_blast_draft(new_id)
        if existing and existing["status"] in ("sent", "cancelled"):
            flash("This blast has already been sent or cancelled.", "error")
            return redirect(url_for("blast.blast_compose", draft_id=new_id))

        from ..db import get_conn
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE blast_drafts SET status='sending', updated_at=NOW() WHERE id=%s",
                        (new_id,),
                    )
            logger.info("  marked draft %s as sending", new_id)
        except Exception as e:
            logger.exception("  FAILED to mark sending: %s", e)
            flash(f"Failed to queue blast: {e}", "error")
            return redirect(url_for("blast.blast_compose", draft_id=new_id))
        finally:
            conn.close()

        execute_blast_async(new_id)
        logger.info("  blast thread started for draft %s", new_id)
        flash("Blast queued — sending in background. Refresh to see results.", "success")
        return redirect(url_for("blast.blast_index"))

    logger.info("  SAVE intent: redirecting to compose %s", new_id)
    flash("Draft saved.", "success")
    return redirect(url_for("blast.blast_compose", draft_id=new_id))


@blast_bp.route("/operator/blast/<int:draft_id>/send-now", methods=["POST"])
@login_required
def send_now(draft_id: int):
    draft = get_blast_draft(draft_id)
    if not draft:
        flash("Draft not found.", "error")
        return redirect(url_for("blast.blast_index"))

    if draft["status"] in ("sent", "cancelled"):
        flash("This blast has already been sent or cancelled.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    confirm = request.form.get("confirm") == "1"
    if not confirm:
        flash("Please check the confirmation box before sending.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    # Auto-save any in-form edits before sending so body/channel/audience are fresh
    body = (request.form.get("body") or draft.get("body") or "").strip()
    if not body:
        flash("Message body is required before sending.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    name = (request.form.get("name") or draft.get("name") or "Untitled draft").strip()[:120]
    channel = request.form.get("channel") or draft.get("channel") or "twilio"
    if channel not in ("twilio", "slicktext"):
        channel = "twilio"
    audience_type = request.form.get("audience_type") or draft.get("audience_type") or "all"
    if audience_type not in ("all", "tag", "location", "random", "show"):
        audience_type = "all"
    audience_filter = (request.form.get("audience_filter") or draft.get("audience_filter") or "").strip()[:200]
    sample_pct = _safe_int(request.form.get("audience_sample_pct"), int(draft.get("audience_sample_pct") or 100), 1, 100)
    # Preserve fields not present in the send-now form so the auto-save doesn't wipe them
    media_url          = (request.form.get("media_url")          or draft.get("media_url")          or "").strip()
    link_url           = (request.form.get("link_url")           or draft.get("link_url")           or "").strip()
    tracked_link_slug  = (request.form.get("tracked_link_slug")  or draft.get("tracked_link_slug")  or "").strip()
    blast_context_note = (request.form.get("blast_context_note") or draft.get("blast_context_note") or "").strip()
    user = current_user()

    save_blast_draft(
        name=name,
        body=body,
        channel=channel,
        audience_type=audience_type,
        audience_filter=audience_filter,
        sample_pct=sample_pct,
        media_url=media_url,
        link_url=link_url,
        tracked_link_slug=tracked_link_slug,
        is_quiz=bool(draft.get("is_quiz")),
        quiz_correct_answer=draft.get("quiz_correct_answer") or "",
        blast_context_note=blast_context_note,
        created_by=user["email"],
        draft_id=draft_id,
    )

    # Mark as sending and queue
    from ..db import get_conn
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET status='sending', updated_at=NOW() WHERE id=%s",
                    (draft_id,),
                )
    finally:
        conn.close()

    execute_blast_async(draft_id)
    flash("Blast queued — sending in the background. Refresh to see results.", "success")
    return redirect(url_for("blast.blast_index"))


@blast_bp.route("/operator/blast/<int:draft_id>/schedule", methods=["POST"])
@login_required
def schedule(draft_id: int):
    draft = get_blast_draft(draft_id)
    if not draft:
        flash("Draft not found.", "error")
        return redirect(url_for("blast.blast_index"))

    send_at_str = (request.form.get("send_at") or "").strip()
    if not send_at_str:
        flash("A scheduled time is required.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    try:
        # datetime-local format: "2026-04-01T14:30"
        send_at = datetime.fromisoformat(send_at_str).replace(tzinfo=timezone.utc)
    except ValueError:
        flash("Invalid date format.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    schedule_blast(draft_id, send_at)
    flash(f"Blast scheduled for {send_at.strftime('%b %d at %I:%M %p UTC')}.", "success")
    return redirect(url_for("blast.blast_index"))


@blast_bp.route("/operator/blast/<int:draft_id>/test", methods=["POST"])
@login_required
def send_test(draft_id: int):
    """Send the current message body to a single test phone number."""
    test_phone = (request.form.get("test_phone") or "").strip()
    if not test_phone:
        flash("Enter a phone number to send the test to.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    body = (request.form.get("body") or "").strip()
    channel = request.form.get("channel", "twilio")
    if channel not in ("twilio", "slicktext"):
        channel = "twilio"

    if not body:
        flash("Add a message before sending a test.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id))

    from ..blast_sender import _send_one, _create_blast_context_session
    from ..queries import get_blast_draft
    draft = get_blast_draft(draft_id)
    test_body = f"[TEST] {body}"
    if draft and draft.get("tracked_link_slug"):
        main_base = os.getenv("MAIN_APP_BASE_URL", "").rstrip("/")
        if not main_base:
            railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
            main_base = f"https://{railway_domain}" if railway_domain else ""
        if main_base:
            test_body = f"{test_body}\n{main_base}/t/{draft['tracked_link_slug']}"
    media_url = (draft.get("media_url") or "").strip() if draft else ""
    ok = _send_one(test_phone, test_body, channel, media_url=media_url)
    if ok:
        flash(f"Test message sent to {test_phone[-4:].rjust(len(test_phone), '*')}.", "success")
        if draft and body:
            combined = f"The blast message that was sent: \"{body}\""
            extra = (draft.get("blast_context_note") or "").strip()
            if extra:
                combined += f"\n\nAdditional context from the operator: {extra}"
            _create_blast_context_session(draft_id, combined)
    else:
        flash("Test send failed — check that your Twilio/SlickText credentials are set on Railway.", "error")
    return redirect(url_for("blast.blast_compose", draft_id=draft_id))


@blast_bp.route("/operator/blast/<int:draft_id>/clone", methods=["POST"])
@login_required
def clone_draft(draft_id: int):
    """Clone a sent/cancelled blast as a new draft so it can be resent."""
    original = get_blast_draft(draft_id)
    if not original:
        flash("Draft not found.", "error")
        return redirect(url_for("blast.blast_index"))

    user = current_user()
    new_id = save_blast_draft(
        name=f"{(original['name'] or 'Untitled')} (resend)",
        body=original["body"] or "",
        channel=original["channel"] or "twilio",
        audience_type=original["audience_type"] or "all",
        audience_filter=original["audience_filter"] or "",
        sample_pct=int(original["audience_sample_pct"] or 100),
        media_url=original.get("media_url") or "",
        created_by=user["email"] if user else "",
    )
    flash("Cloned as a new draft — ready to send.", "success")
    return redirect(url_for("blast.blast_compose", draft_id=new_id))


@blast_bp.route("/operator/blast/<int:draft_id>/status")
@login_required
def draft_status(draft_id: int):
    """Lightweight JSON endpoint for polling blast send progress."""
    draft = get_blast_draft(draft_id)
    if not draft:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "status":            draft["status"],
        "sent_count":        draft["sent_count"]        or 0,
        "failed_count":      draft["failed_count"]      or 0,
        "total_recipients":  draft["total_recipients"]  or 0,
    })


@blast_bp.route("/operator/blast/<int:draft_id>/cancel", methods=["POST"])
@login_required
def cancel_blast(draft_id: int):
    mark_blast_cancelled(draft_id)
    flash("Blast cancelled.", "success")
    return redirect(url_for("blast.blast_index"))


@blast_bp.route("/operator/blast/debug-state")
@login_required
def debug_state():
    """
    Hidden diagnostics page — shows scheduled blasts and stored images.
    Access at /operator/blast/debug-state to diagnose MMS/scheduling issues.
    """
    from ..db import get_conn
    conn = get_conn()
    rows_blasts = []
    rows_images = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, status, channel, audience_type, audience_filter,
                       scheduled_at, sent_at, sent_count, failed_count,
                       LEFT(body, 80) AS body_preview,
                       LEFT(COALESCE(media_url,''), 120) AS media_url_preview,
                       updated_at
                FROM blast_drafts
                ORDER BY id DESC
                LIMIT 30
            """)
            rows_blasts = cur.fetchall()
            cur.execute("""
                SELECT id, filename, mime_type, created_at,
                       CASE WHEN data_b64 IS NULL THEN 'NULL'
                            WHEN LENGTH(data_b64) = 0 THEN 'EMPTY_STRING'
                            ELSE LENGTH(data_b64)::TEXT || ' chars (≈' ||
                                 (LENGTH(data_b64)*3/4/1024)::TEXT || ' KB)'
                       END AS data_b64_status
                FROM operator_blast_images
                ORDER BY id DESC
                LIMIT 20
            """)
            rows_images = cur.fetchall()
    except Exception as e:
        logger.exception("debug_state query failed: %s", e)
    finally:
        conn.close()

    lines = ["<pre style='font-family:monospace;font-size:13px;padding:20px'>"]
    lines.append("=== BLAST DRAFTS (last 30) ===\n")
    for r in rows_blasts:
        lines.append(
            f"id={r['id']}  status={r['status']}  channel={r['channel']}\n"
            f"  scheduled_at={r['scheduled_at']}  sent_at={r['sent_at']}\n"
            f"  body: {r['body_preview']!r}\n"
            f"  media_url: {r['media_url_preview']!r}\n"
            f"  audience: {r['audience_type']}/{r['audience_filter']}\n"
            f"  sent={r['sent_count']}  failed={r['failed_count']}  updated={r['updated_at']}\n\n"
        )
    lines.append("=== STORED IMAGES (last 20) ===\n")
    for r in rows_images:
        lines.append(
            f"id={r['id']}  file={r['filename']}  mime={r['mime_type']}\n"
            f"  data_b64: {r['data_b64_status']}\n"
            f"  created: {r['created_at']}\n\n"
        )
    lines.append("</pre>")
    from flask import Response as FlaskResponse
    return FlaskResponse("".join(lines), mimetype="text/html")


def _safe_int(val, default: int, mn: int = 1, mx: int = 100) -> int:
    try:
        v = int(val)
        return max(mn, min(mx, v))
    except (TypeError, ValueError):
        return default
