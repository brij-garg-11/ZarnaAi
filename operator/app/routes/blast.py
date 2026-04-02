"""
Blast Tool — send group texts to segmented audiences.
Toggle between Twilio (SMS) and SlickText.
Draft board for composing and saving messages before sending.
Scheduled send support via the background scheduler.
"""

import logging
from datetime import datetime, timezone

from flask import Blueprint, flash, redirect, render_template, request, url_for

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


@blast_bp.route("/operator/blast")
@login_required
def blast_index():
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
    )


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

    return render_template(
        "blast.html",
        user=current_user(),
        drafts=drafts,
        tags=tags,
        shows=shows,
        active_draft=active_draft,
        audience_count=audience_count,
    )


@blast_bp.route("/operator/blast/preview-count", methods=["POST"])
@login_required
def preview_count():
    """HTMX or AJAX endpoint — returns audience count for current filter."""
    audience_type = request.form.get("audience_type", "all")
    if audience_type not in ("all", "tag", "location", "random", "show"):
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
    if audience_type not in ("all", "tag", "location", "random", "show"):
        audience_type = "all"
    audience_filter = (request.form.get("audience_filter") or "").strip()[:200]
    sample_pct = _safe_int(request.form.get("audience_sample_pct"), 100, 1, 100)
    draft_id_raw = request.form.get("draft_id")
    draft_id = int(draft_id_raw) if draft_id_raw and draft_id_raw.isdigit() else None

    logger.info("  parsed: body=%r  audience_type=%r  audience_filter=%r  draft_id=%r",
                body[:60] if body else "", audience_type, audience_filter, draft_id)

    if not body:
        logger.warning("  BLOCKED: body is empty")
        flash("Message body is required.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=draft_id) if draft_id else url_for("blast.blast_index"))

    try:
        new_id = save_blast_draft(
            name=name, body=body, channel=channel,
            audience_type=audience_type, audience_filter=audience_filter,
            sample_pct=sample_pct, created_by=user["email"], draft_id=draft_id,
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
        from ..blast_sender import _send_one
        ok = _send_one(test_phone, f"[TEST] {body}", channel)
        logger.info("  TEST send result: ok=%s", ok)
        if ok:
            masked = test_phone[-4:].rjust(len(test_phone), "*")
            flash(f"Test sent to {masked}. Draft saved.", "success")
        else:
            flash("Test send failed — check Twilio/SlickText credentials in Railway env vars.", "error")
        return redirect(url_for("blast.blast_compose", draft_id=new_id))

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
    user = current_user()

    save_blast_draft(
        name=name,
        body=body,
        channel=channel,
        audience_type=audience_type,
        audience_filter=audience_filter,
        sample_pct=sample_pct,
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

    from ..blast_sender import _send_one
    ok = _send_one(test_phone, f"[TEST] {body}", channel)
    if ok:
        flash(f"Test message sent to {test_phone[-4:].rjust(len(test_phone), '*')}.", "success")
    else:
        flash("Test send failed — check that your Twilio/SlickText credentials are set on Railway.", "error")
    return redirect(url_for("blast.blast_compose", draft_id=draft_id))


@blast_bp.route("/operator/blast/<int:draft_id>/cancel", methods=["POST"])
@login_required
def cancel_blast(draft_id: int):
    mark_blast_cancelled(draft_id)
    flash("Blast cancelled.", "success")
    return redirect(url_for("blast.blast_index"))


def _safe_int(val, default: int, mn: int = 1, mx: int = 100) -> int:
    try:
        v = int(val)
        return max(mn, min(mx, v))
    except (TypeError, ValueError):
        return default
