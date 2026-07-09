import hashlib
import json as _json
import logging
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify

from app.inbound_security import (
    log_sensitive_webhook_data,
    running_in_production,
    slicktext_ignored_log,
    slicktext_webhook_log_line,
    slicktext_webhook_secret_configured,
    timing_safe_equal,
    verify_slicktext_webhook_secret,
)
from app.brain.handler import create_brain
from app.messaging.slicktext_adapter import create_slicktext_adapter, _is_reaction as _slick_is_reaction
from app.messaging.twilio_adapter import create_twilio_adapter
from app.messaging.slicktext_migration import (
    migration_enabled as _slicktext_migration_enabled,
    handle_migration as _handle_slicktext_migration,
)
from app.admin_auth import get_db_connection as _get_db_connection
from app.admin import admin_bp
from app.analytics.blueprint import analytics_bp
from app.live_shows.blueprint import live_shows_bp
from app.smb.blueprint import smb_bp
from app.smb.portal import portal_bp
from app.verify import verify_bp
# Note: the canonical interactive client portal lives in
# operator/app/routes/smb_portal.py and runs on the operator service.
# A second unregistered copy used to live at app/smb/portal_interactive.py;
# it was deleted in the cleanup pass to remove drift risk between the two
# implementations.
from app.live_shows.signup import LiveShowSignupResult, try_live_show_signup
from app.live_shows.quiz import get_active_quiz_for_fan, record_quiz_response, build_quiz_context
from app.live_shows.blast_context import get_active_blast_context, build_blast_context_prompt
from app.ops_metrics import ai_reply_enter, ai_reply_leave, bump as ops_bump
from app.alert_writer import write_alert as _write_alert

class _ServiceFormatter(logging.Formatter):
    """Prepend a [SERVICE] tag based on logger name so Railway logs are filterable."""
    _PREFIXES = (
        ("app.smb",       "[SMB]   "),
        ("app.brain",     "[ZARNA] "),
        ("app.admin",     "[ADMIN] "),
        ("app.analytics", "[STATS] "),
        ("app.live_shows","[ZARNA] "),
        ("app.messaging", "[ZARNA] "),
        ("app.storage",   "[DB]    "),
        # main.py uses the root logger via direct `logging.info(...)` calls.
        # Tag those as [ZARNA] too — main.py IS the Zarna inbound pipeline,
        # so [WEB] is misleading. Real non-Zarna paths (Werkzeug etc.) won't
        # match any of these prefixes and still get the [WEB] default.
        ("__main__",      "[ZARNA] "),
        ("root",          "[ZARNA] "),
        ("main",          "[ZARNA] "),
    )
    def format(self, record: logging.LogRecord) -> str:
        tag = "[WEB]   "
        for prefix, label in self._PREFIXES:
            if record.name.startswith(prefix):
                tag = label
                break
        return tag + super().format(record)

_log_handler = logging.StreamHandler()
_log_handler.setFormatter(_ServiceFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(_log_handler)
logging.root.handlers = [_log_handler]  # replace any handlers basicConfig may have added


def _record_blast_optout(phone_number: str = None) -> None:
    """Record a STOP/opt-out: persist the phone to broadcast_optouts (suppresses
    future blasts) and increment opt_out_count on the most recent sent blast."""
    try:
        from app.admin_auth import get_db_connection
        conn = get_db_connection()
        if not conn:
            return
        with conn:
            with conn.cursor() as cur:
                # Persist phone to broadcast_optouts so future blast audience
                # queries exclude this number via _get_optouts().
                if phone_number:
                    cur.execute(
                        "INSERT INTO broadcast_optouts (phone_number) VALUES (%s) "
                        "ON CONFLICT DO NOTHING",
                        (phone_number,),
                    )
                cur.execute(
                    """
                    UPDATE blast_drafts
                    SET opt_out_count = COALESCE(opt_out_count, 0) + 1
                    WHERE id = (
                        SELECT id FROM blast_drafts
                        WHERE status = 'sent'
                          AND sent_at >= NOW() - INTERVAL '7 days'
                        ORDER BY sent_at DESC
                        LIMIT 1
                    )
                    """
                )
        conn.close()
    except Exception:
        logging.exception("Failed to record blast opt-out")


def _safe_try_live_show_signup(phone_number: str, message_text: str, channel: str) -> LiveShowSignupResult:
    """Never let live-show DB logic break inbound webhooks.

    Passes the current tenant's creator_slug so the contact insert is tagged
    with the right creator in multi-tenant deployments.
    """
    try:
        slug = getattr(brain, "slug", None) or os.getenv("CREATOR_SLUG") or None
        return try_live_show_signup(phone_number, message_text, channel, creator_slug=slug)
    except Exception:
        logging.exception("Live show signup failed; continuing with reply pipeline")
        return LiveShowSignupResult()


# Bounded pool for signup confirmation texts.
# 20 workers → ~20 concurrent API calls; excess jobs queue automatically.
# Handles 500-1000 signups without opening thousands of threads or overwhelming
# the SlickText / Twilio APIs. Each adapter already retries on 429.
_confirm_pool = ThreadPoolExecutor(max_workers=20, thread_name_prefix="confirm")


def _record_reaction(phone: str, message: str) -> None:
    """
    Persist an iOS/Android reaction to the DB so it counts toward reply-rate
    metrics.  No AI reply is generated — this is engagement-only bookkeeping.
    """
    try:
        brain.storage.score_previous_bot_reply(phone)
    except Exception:
        logging.exception("_record_reaction: score failed for ...%s", phone[-4:] if phone else "?")
    try:
        from app.admin_auth import get_db_connection
        conn = get_db_connection()
        if not conn:
            return
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO messages (phone_number, role, text, source) "
                    "VALUES (%s, 'user', %s, 'reaction')",
                    (phone, (message or "")[:500]),
                )
        conn.close()
    except Exception:
        logging.exception("_record_reaction: DB insert failed for ...%s", phone[-4:] if phone else "?")


# Minimal opt-in confirmation for keyword joins that produce no themed
# confirmation copy (e.g. "other"-category shows). The compliance footer is
# appended by _send_join_confirmation_async, so a brand-new fan still receives
# the required A2P disclosure rather than a silent, message-less signup.
MINIMAL_OPT_IN_CONFIRMATION = "You're on the list!"


def _send_join_confirmation_async(
    phone: str, channel: str, body: str, append_compliance: bool = False
) -> None:
    """Queue a confirmation SMS through the bounded pool so webhooks stay fast.

    When ``append_compliance`` is True (a brand-new fan's first-ever message),
    the A2P/CTIA disclosure footer is appended — for keyword-only live-show
    joins this confirmation IS the opt-in message, so it must carry the
    disclosure (the AI/welcome path that normally adds it is skipped). Returning
    fans re-joining a later show pass False and are not re-nagged.
    """
    from app.messaging.contact_card import COMPLIANCE_FOOTER

    out_body = body or ""
    if append_compliance and COMPLIANCE_FOOTER and COMPLIANCE_FOOTER.lower() not in out_body.lower():
        out_body = f"{out_body.rstrip()}\n\n{COMPLIANCE_FOOTER}"

    def run():
        try:
            ch = (channel or "").lower()
            if ch == "slicktext":
                slicktext.send_reply(phone, out_body)
            else:
                twilio.send_reply(phone, out_body)
        except Exception as e:
            logging.error("Join confirmation SMS failed (...%s): %s", phone[-4:] if phone else "?", e)

    _confirm_pool.submit(run)


def _live_show_is_new_fan(phone: str) -> bool:
    """True when this phone has no prior message on record — i.e. a keyword join
    is this fan's first-ever contact, so the confirmation must carry the A2P
    disclosure. Best-effort: any failure returns False so we never block or
    double-send (the normal first-contact path still covers a later real text).
    """
    if not phone:
        return False
    try:
        return bool(brain.storage.is_first_message(phone))
    except Exception:
        logging.warning(
            "live-show: is_first_message check failed for ...%s",
            phone[-4:] if phone else "?", exc_info=True,
        )
        return False


def _send_live_show_contact_card_async(to_number: str, from_number: str = "") -> None:
    """Send ONLY the vCard (no welcome text) to a brand-new fan who joined via a
    live-show keyword. The join confirmation acts as the welcome, so the
    ``first_message`` text is suppressed. No-op unless ``send_contact_card`` is
    enabled for the creator, so the live Zarna deployment (card off) is
    unaffected. Runs in a daemon thread so the webhook stays fast.
    """
    def run():
        try:
            from app.messaging.contact_card import maybe_send_first_contact
            from app.brain.creator_config import load_creator as _load_creator
            slug = getattr(brain, "slug", None) or os.getenv("CREATOR_SLUG") or ""
            cfg = (_load_creator(slug) if slug else None) or getattr(brain, "creator_config", None)
            maybe_send_first_contact(
                twilio,
                to_number,
                cfg,
                from_number=from_number or "",
                storage=getattr(brain, "storage", None),
                send_welcome=False,
            )
        except Exception:
            logging.warning(
                "live-show contact card send failed for ...%s",
                to_number[-4:] if to_number else "?", exc_info=True,
            )

    threading.Thread(target=run, daemon=True).start()


app = Flask(__name__)
# Required so Flask sessions (used by the SMB interactive portal and the
# admin views below) actually work. Without this any session.set() crashes
# with "The session is unavailable because no secret key was set." in prod.
_FLASK_SECRET_DEFAULT = "dev-only-do-not-use-in-prod"
app.secret_key = (
    os.getenv("FLASK_SECRET_KEY")
    or os.getenv("SECRET_KEY")
    or os.getenv("OPERATOR_SECRET_KEY")
    or _FLASK_SECRET_DEFAULT
)
if running_in_production() and app.secret_key == _FLASK_SECRET_DEFAULT:
    # Sessions signed with a public default secret can be forged. This is
    # a critical misconfiguration if it ever ships to prod.
    logging.error(
        "[ZARNA] FLASK SECRET KEY IS USING THE HARDCODED DEFAULT. "
        "Set FLASK_SECRET_KEY (or SECRET_KEY / OPERATOR_SECRET_KEY) in Railway. "
        "All session cookies are forgeable until this is fixed."
    )
app.register_blueprint(admin_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(live_shows_bp)
app.register_blueprint(smb_bp)
app.register_blueprint(portal_bp)
app.register_blueprint(verify_bp)

brain     = create_brain()
slicktext = create_slicktext_adapter()
twilio    = create_twilio_adapter()

if running_in_production() and not slicktext_webhook_secret_configured():
    logging.warning(
        "Production: SLICKTEXT_WEBHOOK_SECRET is not set — anyone who can POST /slicktext/webhook "
        "may trigger your bot. Generate a long random secret, set it in Railway, and add header "
        "X-Zarna-Webhook-Secret on SlickText's webhook (if their UI supports custom headers)."
    )

if running_in_production() and not os.getenv("TWILIO_AUTH_TOKEN"):
    logging.error(
        "Production: TWILIO_AUTH_TOKEN is not set — Twilio webhook signature validation will "
        "REJECT all inbound messages. Set TWILIO_AUTH_TOKEN in Railway env vars."
    )

# ---------------------------------------------------------------------------
# Deduplication: last 1000 message IDs (SlickText + Twilio).
# Per-process LRU — multi-worker deploys still risk processing the same
# message twice across workers; tracked in the audit (H4).
# ---------------------------------------------------------------------------

_seen_message_ids: OrderedDict = OrderedDict()
_seen_lock = threading.Lock()
_MAX_SEEN = 1000


def _already_processed(message_id: str) -> bool:
    if not message_id:
        return False
    with _seen_lock:
        if message_id in _seen_message_ids:
            return True
        _seen_message_ids[message_id] = True
        if len(_seen_message_ids) > _MAX_SEEN:
            _seen_message_ids.popitem(last=False)
    return False


# ---------------------------------------------------------------------------
# Per-phone rate limiting — AI path only (keyword-only joins skip this)
# ---------------------------------------------------------------------------

_rate_data: dict = {}
_rate_lock = threading.Lock()
_RATE_WINDOW = 60
_RATE_MAX    = 3


def _is_rate_limited(phone_number: str) -> bool:
    now = time.monotonic()
    with _rate_lock:
        timestamps = _rate_data.get(phone_number, [])
        timestamps = [t for t in timestamps if now - t < _RATE_WINDOW]
        if len(timestamps) >= _RATE_MAX:
            _rate_data[phone_number] = timestamps
            return True
        timestamps.append(now)
        _rate_data[phone_number] = timestamps
    return False


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "zarna-ai"})


@app.route("/vcard/performer/<slug>.vcf", methods=["GET"])
def performer_vcard(slug):
    """Serve a performer's contact card (Item 2). Linked as the media URL in the
    vCard MMS sent to brand-new fans. Returns 404 when the creator has no config."""
    from flask import Response
    from app.brain.creator_config import load_creator
    from app.messaging.contact_card import build_performer_vcard

    cfg = load_creator(slug)
    if cfg is None:
        return ("Not found", 404)
    # Sanitize the optional tel query param — digits and a leading + only.
    tel = "".join(ch for ch in (request.args.get("tel") or "") if ch.isdigit() or ch == "+")[:20]
    vcf = build_performer_vcard(cfg, tel=tel)
    return Response(
        vcf,
        mimetype="text/vcard",
        headers={"Content-Disposition": f'attachment; filename="{slug}.vcf"'},
    )


_API_SECRET = (os.getenv("API_SECRET_KEY") or "").strip()


@app.route("/message", methods=["POST"])
def message():
    if running_in_production() and not _API_SECRET:
        return jsonify(
            {
                "error": "Misconfigured",
                "detail": "Set API_SECRET_KEY in the host environment to use POST /message in production.",
            }
        ), 503

    got = (request.headers.get("X-Api-Key") or "").strip()
    if _API_SECRET:
        if not timing_safe_equal(_API_SECRET, got):
            return jsonify({"error": "Unauthorized"}), 403

    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
    if _is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded"}), 429

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400

    phone_number = data.get("phone_number", "").strip()
    message_text = data.get("message", "").strip()
    if not phone_number:
        return jsonify({"error": "phone_number is required"}), 400
    if not message_text:
        return jsonify({"error": "message is required"}), 400

    if not ai_reply_enter():
        ops_bump("ai_reply_capacity_reject")
        return jsonify({"error": "Server busy", "detail": "Try again in a moment."}), 503
    try:
        reply = brain.handle_incoming_message(phone_number, message_text)
    except Exception as e:
        ops_bump("ai_reply_error")
        logging.exception("Brain error on /message: %s", e)
        return jsonify({"error": "Internal error"}), 500
    finally:
        ai_reply_leave()

    return jsonify({"reply": reply, "skipped": not (reply or "").strip()})


# ---------------------------------------------------------------------------
# SlickText webhook
# ---------------------------------------------------------------------------


def _process_slicktext_message(phone_number: str, message_text: str, quiz_context: str = None, blast_context: str = None) -> None:
    if not ai_reply_enter():
        ops_bump("ai_reply_capacity_reject")
        logging.warning("AI at capacity — SlickText message dropped (...%s)", phone_number[-4:])
        return
    try:
        slug = getattr(brain, "slug", None) or ""
        if not _has_credits_remaining(slug):
            ops_bump("ai_reply_credit_exhausted")
            logging.warning(
                "AI reply blocked: credits exhausted for slug=%s (...%s)",
                slug, phone_number[-4:] if phone_number else "?",
            )
            _write_alert(
                slug, "credits_exhausted", "warning",
                "AI replies temporarily paused",
                "Your message credit limit has been reached for this billing period. "
                "Replies will resume automatically when credits reset or your plan is upgraded.",
                detail=f"credits_exhausted slug={slug}",
            )
            return
        try:
            reply = brain.handle_incoming_message(phone_number, message_text, quiz_context=quiz_context, blast_context=blast_context)
        except Exception as e:
            ops_bump("ai_reply_error")
            logging.error("Error processing SlickText message from ...%s: %s", phone_number[-4:] if phone_number else "?", e)
            _write_alert(
                slug, "ai_error", "error",
                "A message could not be processed",
                "An error occurred while generating a reply. Our team has been notified and is looking into it.",
                detail=f"SlickText ai_error phone=...{phone_number[-4:] if phone_number else '?'}: {e}",
            )
            return
        if not (reply or "").strip():
            logging.info("No reply for ...%s (conversation ender or empty)", phone_number[-4:])
            return
        slicktext.send_reply(phone_number, reply)
        _consume_message_credits(reply, message_text, source=f"slicktext:{phone_number[-4:]}")
    finally:
        ai_reply_leave()


@app.route("/slicktext/webhook", methods=["POST"])
def slicktext_webhook():
    if not verify_slicktext_webhook_secret():
        ops_bump("slicktext_webhook_401")
        logging.warning("SlickText webhook rejected: bad or missing X-Zarna-Webhook-Secret")
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict() or {}

    slicktext_webhook_log_line(payload)

    # Dedup key. Two webhook shapes from SlickText:
    #   v1 form-POST: payload['data'] is a JSON string containing
    #                 ChatMessage.ChatMessageId — use that directly.
    #   v2 JSON     : no stable message ID is sent. Fall back to a synthetic
    #                 key derived from (event_name, contact_id, message_body)
    #                 so the same retried payload can't trigger two AI replies
    #                 within the dedup window.
    message_id = ""
    try:
        if isinstance(payload.get("data"), str):
            raw_data = _json.loads(payload.get("data", "{}"))
            message_id = str(raw_data.get("ChatMessage", {}).get("ChatMessageId", ""))
        elif isinstance(payload.get("data"), dict):
            v2 = payload.get("data") or {}
            event = str(payload.get("name", "") or "")
            contact_id = str(v2.get("contact_id", "") or "")
            body = str(v2.get("last_message", "") or "")
            if contact_id and body:
                # Body is included so a fan can re-ask the same question later
                # and still get a reply — we only block exact-duplicate retries.
                message_id = f"v2:{event}:{contact_id}:{hashlib.sha1(body.encode()).hexdigest()[:16]}"
    except Exception:
        message_id = ""

    if message_id and _already_processed(message_id):
        logging.info("Duplicate SlickText webhook ignored (id=%s)", message_id)
        return jsonify({"status": "duplicate"}), 200

    raw_phone, raw_body = slicktext.peek_inbound(payload)

    # Persist iOS/Android reactions — counts toward engagement metrics, no AI reply.
    if raw_phone and raw_body and _slick_is_reaction(raw_body):
        threading.Thread(target=_record_reaction, args=(raw_phone, raw_body), daemon=True).start()

    signup_res = LiveShowSignupResult()
    if raw_phone and raw_body:
        signup_res = _safe_try_live_show_signup(raw_phone, raw_body, "slicktext")

    # For keyword-only joins the AI/welcome path (which normally carries the A2P
    # disclosure) is skipped, so compute first-contact BEFORE the early return
    # and ride the disclosure on the join confirmation itself for brand-new fans.
    ls_new_fan = False
    if raw_phone and (signup_res.join_confirmation_sms or signup_res.suppress_ai):
        ls_new_fan = _live_show_is_new_fan(raw_phone)

    if signup_res.join_confirmation_sms and signup_res.confirmation_phone:
        _send_join_confirmation_async(
            signup_res.confirmation_phone,
            signup_res.confirmation_channel or "slicktext",
            signup_res.join_confirmation_sms,
            append_compliance=ls_new_fan,
        )
    elif signup_res.suppress_ai and ls_new_fan and raw_phone:
        # Keyword join with no themed confirmation copy (e.g. "other" category) —
        # still deliver a minimal disclosure-bearing opt-in so no signup is silent.
        _send_join_confirmation_async(
            raw_phone, "slicktext", MINIMAL_OPT_IN_CONFIRMATION, append_compliance=True,
        )

    # Track opt-outs: persist phone to broadcast_optouts and increment blast counter.
    _OPT_OUT_KEYWORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}
    if raw_phone and raw_body and raw_body.strip().lower() in _OPT_OUT_KEYWORDS:
        threading.Thread(target=_record_blast_optout, args=(raw_phone,), daemon=True).start()

    phone_number, message_text = slicktext.filter_inbound_for_ai(raw_phone, raw_body)

    if signup_res.suppress_ai:
        logging.info(
            "SlickText webhook: live show keyword-only join — no AI reply (...%s)",
            raw_phone[-4:] if raw_phone else "?",
        )
        return jsonify({"status": "ok", "live_show": "join_no_reply"}), 200

    if not phone_number or not message_text:
        slicktext_ignored_log(payload)
        return jsonify({"status": "ignored"}), 200

    if _is_rate_limited(phone_number):
        logging.warning("Rate limit hit for ...%s — dropping message", phone_number[-4:] if phone_number else "?")
        return jsonify({"status": "rate_limited"}), 200

    # SlickText → Twilio migration: when enabled, the legacy SlickText number
    # stops running the AI conversation and instead pulls the fan onto the new
    # Twilio number (one-time opener FROM Twilio + a bridge reply on SlickText).
    # STOP/opt-out and live-show joins are handled above and still work.
    if _slicktext_migration_enabled():
        logging.info(
            "SlickText migration mode: redirecting ...%s to the Twilio number",
            phone_number[-4:] if phone_number else "?",
        )
        threading.Thread(
            target=_handle_slicktext_migration,
            args=(phone_number, twilio, slicktext, _get_db_connection),
            daemon=True,
        ).start()
        return jsonify({"status": "ok", "migration": "redirected"}), 200

    # Check for an active pop quiz for this fan — inject context so AI can react in character.
    quiz_ctx = None
    try:
        quiz_session = get_active_quiz_for_fan(phone_number)
        if quiz_session:
            record_quiz_response(quiz_session["id"], phone_number, message_text)
            quiz_ctx = build_quiz_context(
                quiz_session["question_text"],
                quiz_session["correct_answer"],
                message_text,
            )
            logging.info("Quiz intercept: quiz_id=%s fan=...%s", quiz_session["id"], phone_number[-4:] if phone_number else "?")
    except Exception:
        logging.exception("Quiz intercept failed — continuing with normal AI reply")

    # Check for active blast context — soft background framing if no quiz is active.
    # The Zarna performer webhook only ever handles traffic for Zarna's Twilio
    # / SlickText numbers (SMB tenants are firewalled and routed to the SMB
    # blueprint), so the performer slug is always "zarna". ZARNA_CREATOR_SLUG
    # is an env-level escape hatch for staging / future multi-performer setups.
    blast_ctx = None
    if not quiz_ctx:
        try:
            zarna_slug = os.getenv("ZARNA_CREATOR_SLUG", "zarna")
            context_note = get_active_blast_context(creator_slug=zarna_slug)
            if context_note:
                blast_ctx = build_blast_context_prompt(context_note)
                logging.info(
                    "Blast context injected for ...%s (slug=%s)",
                    phone_number[-4:] if phone_number else "?", zarna_slug,
                )
        except Exception:
            logging.exception("Blast context lookup failed — continuing with normal AI reply")

    threading.Thread(
        target=_process_slicktext_message,
        args=(phone_number, message_text, quiz_ctx),
        kwargs={"blast_context": blast_ctx},
        daemon=True,
    ).start()

    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Twilio webhook
# ---------------------------------------------------------------------------


def _process_twilio_message(phone_number: str, message_text: str, quiz_context: str = None, blast_context: str = None, brain_obj=None, from_number: str = None) -> None:
    # In the multi-tenant ("apartment building") deployment, brain_obj is the
    # creator-specific brain resolved from the destination number, and
    # from_number is that creator's own number (so the reply comes FROM them).
    # When neither is provided we fall back to the process-global brain — the
    # unchanged behaviour for dedicated single-creator deployments (e.g. Zarna).
    active_brain = brain_obj if brain_obj is not None else brain
    if not ai_reply_enter():
        ops_bump("ai_reply_capacity_reject")
        logging.warning("AI at capacity — Twilio message dropped (...%s)", phone_number[-4:])
        return
    try:
        slug = getattr(active_brain, "slug", None) or ""
        if not _has_credits_remaining(slug):
            ops_bump("ai_reply_credit_exhausted")
            logging.warning(
                "AI reply blocked: credits exhausted for slug=%s (...%s)",
                slug, phone_number[-4:] if phone_number else "?",
            )
            _write_alert(
                slug, "credits_exhausted", "warning",
                "AI replies temporarily paused",
                "Your message credit limit has been reached for this billing period. "
                "Replies will resume automatically when credits reset or your plan is upgraded.",
                detail=f"credits_exhausted slug={slug}",
            )
            return
        # Capture first-contact status BEFORE the brain saves this message, so we
        # can send the opt-in vCard + welcome to brand-new fans (Item 2).
        is_first_contact = False
        try:
            is_first_contact = active_brain.storage.is_first_message(phone_number)
        except Exception:
            is_first_contact = False
        try:
            reply = active_brain.handle_incoming_message(phone_number, message_text, quiz_context=quiz_context, blast_context=blast_context)
        except Exception as e:
            ops_bump("ai_reply_error")
            logging.error("Error processing Twilio message from ...%s: %s", phone_number[-4:] if phone_number else "?", e)
            _write_alert(
                slug, "ai_error", "error",
                "A message could not be processed",
                "An error occurred while generating a reply. Our team has been notified and is looking into it.",
                detail=f"Twilio ai_error phone=...{phone_number[-4:] if phone_number else '?'}: {e}",
            )
            return
        # First-contact sequence: vCard MMS + first_message welcome, sent before the
        # AI reply. Opt-in via send_contact_card / first_message — no-op otherwise,
        # so creators without these configured (e.g. Zarna today) are unaffected.
        if is_first_contact:
            try:
                from app.messaging.contact_card import maybe_send_first_contact
                # Load a FRESH config (file + dashboard overrides) so the operator's
                # "send contact card / first message" toggle takes effect the moment
                # they save it — the brain's cached creator_config is only read at
                # startup. Falls back to the cached config if the fresh load misses.
                from app.brain.creator_config import load_creator as _load_creator
                first_contact_cfg = (
                    _load_creator(slug) if slug else None
                ) or getattr(active_brain, "creator_config", None)
                maybe_send_first_contact(
                    twilio,
                    phone_number,
                    first_contact_cfg,
                    from_number=from_number or "",
                    storage=getattr(active_brain, "storage", None),
                )
            except Exception:
                logging.warning(
                    "first-contact sequence failed for ...%s",
                    phone_number[-4:] if phone_number else "?", exc_info=True,
                )
        if not (reply or "").strip():
            logging.info("No Twilio reply for ...%s (conversation ender or empty)", phone_number[-4:])
            return
        try:
            twilio.send_reply(phone_number, reply, from_number=from_number)
        except Exception as e:
            ops_bump("message_send_failed")
            logging.error("Twilio send_reply failed for ...%s: %s", phone_number[-4:] if phone_number else "?", e)
            _write_alert(
                slug, "message_send_failed", "error",
                "A reply failed to send",
                "One or more outbound messages could not be delivered. "
                "This is usually a temporary carrier issue and often resolves on its own.",
                detail=f"twilio send_reply failed phone=...{phone_number[-4:] if phone_number else '?'}: {e}",
            )
            return
        _consume_message_credits(reply, message_text, source=f"twilio:{phone_number[-4:]}", slug=slug)
    finally:
        ai_reply_leave()


def _has_credits_remaining(slug: str) -> bool:
    """Return True if this slug can still send AI replies.

    Used as an optional pre-flight gate (BILLING_HARD_GATE=true). Fail-open:
    on DB or schema error we return True so a billing bug never silently
    kills the bot. The audit caller logs ops_bump('ai_reply_credit_exhausted')
    when this returns False so we can monitor blocks.
    """
    if not slug:
        return True

    if (os.getenv("BILLING_HARD_GATE") or "").strip().lower() not in ("1", "true", "yes"):
        return True

    try:
        from app.admin_auth import get_db_connection  # type: ignore
        conn = get_db_connection()
        if conn is None:
            return True
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT u.id, u.plan_tier, u.trial_credits_remaining,
                               u.billing_cycle_anchor
                        FROM   operator_users u
                        LEFT JOIN team_members tm
                               ON tm.user_id = u.id AND tm.tenant_slug = %s
                        WHERE  u.creator_slug = %s
                        ORDER BY CASE WHEN tm.role = 'owner' THEN 0 ELSE 1 END, u.id
                        LIMIT 1
                        """,
                        (slug, slug),
                    )
                    row = cur.fetchone()
                    if not row:
                        return True
                    user_id, plan_tier, trial_left, billing_anchor = row
                    pt = (plan_tier or "").lower()

                    # Unlimited tiers always have credits
                    if pt in {"grandfathered", "founder", "internal"}:
                        return True

                    # Trial: fail when trial_credits_remaining hits 0
                    if pt == "trial":
                        return (trial_left or 0) > 0

                    # Paid plans: compare current period usage to credits_included.
                    if billing_anchor:
                        period_start = billing_anchor.date()
                    else:
                        import datetime as _dt
                        period_start = _dt.date.today()
                    cur.execute(
                        """
                        SELECT COALESCE(credits_included, 0), COALESCE(credits_used, 0)
                        FROM   operator_credit_usage
                        WHERE  operator_user_id=%s AND period_start=%s
                        """,
                        (user_id, period_start),
                    )
                    usage_row = cur.fetchone()
                    if not usage_row:
                        return True
                    included, used = usage_row
                    return used < included
        finally:
            conn.close()
    except Exception:
        logging.exception("_has_credits_remaining: check failed for slug=%s — fail-open", slug)
        return True


def _consume_message_credits(outbound_text: str, inbound_text: str, *, source: str, slug: str = None) -> None:
    """Charge a creator_slug for 1 inbound + N outbound segments.

    `slug` is the resolved creator for this message (multi-tenant routing); when
    omitted we fall back to the process-global brain's slug (dedicated
    deployments like Zarna). Without this, every self-serve creator's usage
    would be billed to the global default account.

    Writes directly to operator_credit_usage + credit_events so the main app
    doesn't need to import the operator package (they share a top-level
    'app/' directory name — a direct sys.path insert would shadow imports).

    Fail-open: never blocks message processing — billing is secondary to replies.
    """
    slug = (slug or "").strip() or (getattr(brain, "slug", None) or "")
    if not slug:
        return

    import math as _math

    def _segments(text: str) -> int:
        if not text:
            return 1
        length = len(text)
        if any(ord(c) > 127 for c in text):
            return 1 if length <= 70 else max(1, _math.ceil(length / 67))
        return 1 if length <= 160 else max(1, _math.ceil(length / 153))

    outbound_credits = _segments(outbound_text)

    try:
        from app.utils.sms_segments import count_sms_segments  # type: ignore
        outbound_credits = count_sms_segments(outbound_text, has_media=False)
    except Exception:
        pass

    try:
        from app.admin_auth import get_db_connection  # type: ignore
        conn = get_db_connection()
        if conn is None:
            return
        with conn:
            with conn.cursor() as cur:
                # Resolve user_id, plan tier, and billing anchor for this slug.
                cur.execute(
                    """
                    SELECT u.id, u.plan_tier, u.trial_credits_remaining,
                           u.billing_cycle_anchor
                    FROM   operator_users u
                    LEFT JOIN team_members tm
                           ON tm.user_id = u.id AND tm.tenant_slug = %s
                    WHERE  u.creator_slug = %s
                    ORDER BY CASE WHEN tm.role = 'owner' THEN 0 ELSE 1 END, u.id
                    LIMIT 1
                    """,
                    (slug, slug),
                )
                row = cur.fetchone()
                if not row:
                    return
                user_id, plan_tier, _trial_left, billing_anchor = row
                total_credits = 1 + outbound_credits  # 1 inbound + N outbound

                # Grandfathered / founder / internal tiers bypass accounting
                # (still log audit below so usage is visible in credit_events).
                unlimited_tier = (plan_tier or "").lower() in {
                    "grandfathered", "founder", "internal"
                }

                # Use billing_cycle_anchor as period_start for paid plans so
                # this path stays aligned with operator/app/billing/credits.py's
                # _current_period(). Trial + grandfathered fall back to today.
                if plan_tier == "trial" and not unlimited_tier:
                    cur.execute(
                        """UPDATE operator_users
                           SET trial_credits_remaining = GREATEST(0, trial_credits_remaining - %s)
                           WHERE id=%s""",
                        (total_credits, user_id),
                    )

                if not unlimited_tier:
                    if plan_tier != "trial" and billing_anchor:
                        period_start = billing_anchor.date()
                    else:
                        import datetime as _dt
                        period_start = _dt.date.today()
                    cur.execute(
                        """
                        INSERT INTO operator_credit_usage
                            (operator_user_id, creator_slug, period_start, credits_included, credits_used)
                        VALUES (%s, %s, %s, 0, %s)
                        ON CONFLICT (operator_user_id, period_start)
                        DO UPDATE SET credits_used = operator_credit_usage.credits_used + EXCLUDED.credits_used,
                                      updated_at = NOW()
                        """,
                        (user_id, slug, period_start, total_credits),
                    )

                cur.execute(
                    """
                    INSERT INTO credit_events
                        (operator_user_id, creator_slug, kind, credits, source_id)
                    VALUES (%s, %s, 'sms_inbound', -1, %s),
                           (%s, %s, 'sms_outbound', %s, %s)
                    """,
                    (user_id, slug, source,
                     user_id, slug, -outbound_credits, source),
                )
        conn.close()
    except Exception:
        logging.warning("consume_message_credits: DB write failed for slug=%s", slug)


def _multi_tenant_mode() -> bool:
    """
    True on the shared multi-tenant ("apartment building") deployment. When on,
    an inbound message to a number we can't map to a creator is DROPPED rather
    than answered by the process-global brain (so we never reply in the wrong
    creator's voice). Off by default → dedicated single-creator deployments
    (e.g. Zarna) keep using the global brain exactly as before.
    """
    return os.getenv("MULTI_TENANT_MODE", "").strip().lower() in ("1", "true", "yes", "on")


def _resolve_twilio_tenant(to_number: str, slug_param: str):
    """
    Resolve the creator for an inbound Twilio message.

    Returns (brain_obj, slug, reply_from_number).
      - brain_obj is None  → caller should DROP the message (strict building,
        unknown number).
      - reply_from_number is the creator's own number (so the reply comes from
        them); None means "let the adapter use its default sender".

    Resolution order: the routing registry (source of truth, keyed by the
    destination number) → the signed `?slug=` query param Twilio was configured
    with at purchase time → fall back to the process-global brain.
    """
    from app.brain.handler import get_brain

    resolved = None
    if to_number:
        try:
            from app.performer.registry import get_registry
            resolved = get_registry().get_slug_by_to_number(to_number)
        except Exception:
            logging.exception(
                "performer registry lookup failed for ...%s",
                to_number[-4:] if to_number else "?",
            )
    if not resolved and slug_param:
        resolved = (slug_param or "").strip().lower() or None

    if resolved:
        return get_brain(resolved), resolved, to_number

    if _multi_tenant_mode():
        return None, None, None

    # Dedicated deployment: process-global brain, default sender — unchanged.
    return brain, getattr(brain, "slug", None), None


@app.route("/twilio/webhook", methods=["POST"])
def twilio_webhook():
    form_data = request.form.to_dict()
    _from = form_data.get("From", "")
    if log_sensitive_webhook_data():
        logging.info(
            "Twilio webhook received: From=...%s Body=%s",
            _from[-4:] if _from else "?",
            form_data.get("Body"),
        )
    else:
        _body = form_data.get("Body") or ""
        logging.info(
            "Twilio webhook received: From=...%s body_chars=%s",
            _from[-4:] if _from else "?",
            len(str(_body)),
        )

    if os.getenv("TWILIO_VALIDATE_SIGNATURE", "true").lower() == "true":
        sig = request.headers.get("X-Twilio-Signature", "")
        forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
        url = request.url
        if forwarded_proto == "https" and url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        if not twilio.validate_signature(url, form_data, sig):
            ops_bump("twilio_signature_fail")
            _sig_from = form_data.get("From", "")
            logging.warning("Invalid Twilio signature from ...%s", _sig_from[-4:] if _sig_from else "?")
            return ("Forbidden", 403)

    message_sid = form_data.get("MessageSid", "")
    if _already_processed(message_sid):
        logging.info("Duplicate Twilio webhook ignored (MessageSid=%s)", message_sid)
        return ("", 204)

    # Hard firewall: if this message was addressed to an SMB tenant number it must
    # be handled exclusively by /smb/twilio/webhook — never by Zarna's brain.
    _to_number = form_data.get("To", "")
    try:
        from app.smb.tenants import get_registry as _smb_registry
        if _smb_registry().is_smb_number(_to_number):
            logging.warning(
                "Zarna webhook received message addressed to SMB number ...%s — dropping. "
                "Check Twilio webhook config for that number.",
                _to_number[-4:] if _to_number else "?",
            )
            return ("", 204)
    except Exception:
        logging.exception("SMB firewall check failed — continuing with Zarna handler")

    raw_from, raw_body = twilio.peek_inbound(form_data)
    signup_res = LiveShowSignupResult()
    if raw_from and raw_body:
        _tw_ch = "twilio_whatsapp" if raw_from.lower().startswith("whatsapp:") else "twilio"
        signup_res = _safe_try_live_show_signup(raw_from, raw_body, _tw_ch)

    # Keyword-only joins skip the AI/welcome path that normally carries the A2P
    # disclosure, so detect first-contact before the early return and ride the
    # disclosure on the join confirmation (and send the contact card) for new fans.
    ls_new_fan = False
    if raw_from and (signup_res.join_confirmation_sms or signup_res.suppress_ai):
        ls_new_fan = _live_show_is_new_fan(raw_from)

    if signup_res.join_confirmation_sms and signup_res.confirmation_phone:
        _send_join_confirmation_async(
            signup_res.confirmation_phone,
            signup_res.confirmation_channel or "twilio",
            signup_res.join_confirmation_sms,
            append_compliance=ls_new_fan,
        )
    elif signup_res.suppress_ai and ls_new_fan and raw_from:
        # Keyword join with no themed confirmation copy (e.g. "other" category) —
        # still deliver a minimal disclosure-bearing opt-in so no signup is silent.
        _send_join_confirmation_async(
            raw_from, "twilio", MINIMAL_OPT_IN_CONFIRMATION, append_compliance=True,
        )

    # Contact card (vCard MMS) for a brand-new keyword joiner — card only, no
    # welcome text (the join confirmation is the welcome). No-op unless the
    # creator enabled send_contact_card, so Zarna (card off) is unaffected.
    if signup_res.suppress_ai and ls_new_fan and raw_from:
        _send_live_show_contact_card_async(raw_from)

    # Track opt-outs: persist phone to broadcast_optouts and increment blast counter.
    _TW_OPT_OUT_KEYWORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}
    if raw_from and raw_body and raw_body.strip().lower() in _TW_OPT_OUT_KEYWORDS:
        threading.Thread(target=_record_blast_optout, args=(raw_from,), daemon=True).start()

    # Photos/MMS arrive with an empty Body — substitute a placeholder so a
    # caption-less photo still gets an in-character reply instead of being
    # dropped as "unparseable". (Signup + opt-out checks above use raw_body so
    # a photo never accidentally counts as a keyword.)
    ai_body = twilio.normalize_inbound_body(form_data, raw_body)
    phone_number, message_text = twilio.filter_inbound_for_ai(raw_from, ai_body)

    if signup_res.suppress_ai:
        logging.info(
            "Twilio webhook: live show keyword-only join — no AI reply (...%s)",
            raw_from[-4:] if raw_from else "?",
        )
        return ("", 204)

    if not phone_number or not message_text:
        logging.info("Twilio webhook: message filtered or unparseable.")
        return ("", 204)

    if _is_rate_limited(phone_number):
        logging.warning("Rate limit hit for Twilio ...%s — dropping message", phone_number[-4:] if phone_number else "?")
        return ("", 204)

    # Resolve which creator owns the number this fan texted, and pick that
    # creator's brain (multi-tenant routing). Falls back to the global brain
    # for dedicated deployments.
    slug_param = request.args.get("slug", "")
    target_brain, target_slug, reply_from = _resolve_twilio_tenant(_to_number, slug_param)
    if target_brain is None:
        logging.warning(
            "Twilio webhook: number ...%s maps to no creator — dropping (multi-tenant strict mode)",
            _to_number[-4:] if _to_number else "?",
        )
        return ("", 204)

    # Check for an active pop quiz for this fan — inject context so AI can react in character.
    quiz_ctx = None
    try:
        quiz_session = get_active_quiz_for_fan(phone_number)
        if quiz_session:
            record_quiz_response(quiz_session["id"], phone_number, message_text)
            quiz_ctx = build_quiz_context(
                quiz_session["question_text"],
                quiz_session["correct_answer"],
                message_text,
            )
            logging.info("Quiz intercept: quiz_id=%s fan=...%s", quiz_session["id"], phone_number[-4:] if phone_number else "?")
    except Exception:
        logging.exception("Quiz intercept failed — continuing with normal AI reply")

    # Check for active blast context — soft background framing if no quiz is active.
    # (See /webhook/slicktext above for why we always use the Zarna slug here.)
    blast_ctx = None
    if not quiz_ctx:
        try:
            blast_slug = target_slug or os.getenv("ZARNA_CREATOR_SLUG", "zarna")
            context_note = get_active_blast_context(creator_slug=blast_slug)
            if context_note:
                blast_ctx = build_blast_context_prompt(context_note)
                logging.info(
                    "Blast context injected for ...%s (slug=%s)",
                    phone_number[-4:] if phone_number else "?", blast_slug,
                )
        except Exception:
            logging.exception("Blast context lookup failed — continuing with normal AI reply")

    threading.Thread(
        target=_process_twilio_message,
        args=(phone_number, message_text, quiz_ctx),
        kwargs={
            "blast_context": blast_ctx,
            "brain_obj": target_brain,
            "from_number": reply_from,
        },
        daemon=True,
    ).start()

    return ("", 204)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
