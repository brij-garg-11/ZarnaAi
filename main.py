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
from app.admin import admin_bp
from app.analytics.blueprint import analytics_bp
from app.live_shows.blueprint import live_shows_bp
from app.smb.blueprint import smb_bp
from app.smb.portal import portal_bp
from app.smb.portal_interactive import portal_interactive_bp
from app.live_shows.signup import LiveShowSignupResult, try_live_show_signup
from app.live_shows.quiz import get_active_quiz_for_fan, record_quiz_response, build_quiz_context
from app.live_shows.blast_context import get_active_blast_context, build_blast_context_prompt
from app.ops_metrics import ai_reply_enter, ai_reply_leave, bump as ops_bump

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


def _record_blast_optout() -> None:
    """Increment opt_out_count on the most recent sent blast (within 7 days)."""
    try:
        from app.admin_auth import get_db_connection
        conn = get_db_connection()
        if not conn:
            return
        with conn:
            with conn.cursor() as cur:
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
    """Never let live-show DB logic break inbound webhooks."""
    try:
        return try_live_show_signup(phone_number, message_text, channel)
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


def _send_join_confirmation_async(phone: str, channel: str, body: str) -> None:
    """Queue a confirmation SMS through the bounded pool so webhooks stay fast."""

    def run():
        try:
            ch = (channel or "").lower()
            if ch == "slicktext":
                slicktext.send_reply(phone, body)
            else:
                twilio.send_reply(phone, body)
        except Exception as e:
            logging.error("Join confirmation SMS failed (...%s): %s", phone[-4:] if phone else "?", e)

    _confirm_pool.submit(run)


app = Flask(__name__)
app.register_blueprint(admin_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(live_shows_bp)
app.register_blueprint(smb_bp)
app.register_blueprint(portal_bp)
app.register_blueprint(portal_interactive_bp)

brain     = create_brain()
slicktext = create_slicktext_adapter()
twilio    = create_twilio_adapter()

if running_in_production() and not slicktext_webhook_secret_configured():
    logging.warning(
        "Production: SLICKTEXT_WEBHOOK_SECRET is not set — anyone who can POST /slicktext/webhook "
        "may trigger your bot. Generate a long random secret, set it in Railway, and add header "
        "X-Zarna-Webhook-Secret on SlickText's webhook (if their UI supports custom headers)."
    )

# ---------------------------------------------------------------------------
# Deduplication: last 200 message IDs (SlickText + Twilio)
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
        try:
            reply = brain.handle_incoming_message(phone_number, message_text, quiz_context=quiz_context, blast_context=blast_context)
        except Exception as e:
            ops_bump("ai_reply_error")
            logging.error("Error processing SlickText message from %s: %s", phone_number, e)
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

    try:
        raw_data = _json.loads(payload.get("data", "{}")) if isinstance(payload.get("data"), str) else payload
        message_id = str(raw_data.get("ChatMessage", {}).get("ChatMessageId", ""))
    except Exception:
        message_id = ""

    if _already_processed(message_id):
        logging.info("Duplicate SlickText webhook ignored (ChatMessageId=%s)", message_id)
        return jsonify({"status": "duplicate"}), 200

    raw_phone, raw_body = slicktext.peek_inbound(payload)

    # Persist iOS/Android reactions — counts toward engagement metrics, no AI reply.
    if raw_phone and raw_body and _slick_is_reaction(raw_body):
        threading.Thread(target=_record_reaction, args=(raw_phone, raw_body), daemon=True).start()

    signup_res = LiveShowSignupResult()
    if raw_phone and raw_body:
        signup_res = _safe_try_live_show_signup(raw_phone, raw_body, "slicktext")

    if signup_res.join_confirmation_sms and signup_res.confirmation_phone:
        _send_join_confirmation_async(
            signup_res.confirmation_phone,
            signup_res.confirmation_channel or "slicktext",
            signup_res.join_confirmation_sms,
        )

    # Track opt-outs on the most recent blast (within 7 days)
    _OPT_OUT_KEYWORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}
    if raw_phone and raw_body and raw_body.strip().lower() in _OPT_OUT_KEYWORDS:
        threading.Thread(target=_record_blast_optout, daemon=True).start()

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
    blast_ctx = None
    if not quiz_ctx:
        try:
            context_note = get_active_blast_context()
            if context_note:
                blast_ctx = build_blast_context_prompt(context_note)
                logging.info("Blast context injected for ...%s", phone_number[-4:] if phone_number else "?")
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


def _process_twilio_message(phone_number: str, message_text: str, quiz_context: str = None, blast_context: str = None) -> None:
    if not ai_reply_enter():
        ops_bump("ai_reply_capacity_reject")
        logging.warning("AI at capacity — Twilio message dropped (...%s)", phone_number[-4:])
        return
    try:
        try:
            reply = brain.handle_incoming_message(phone_number, message_text, quiz_context=quiz_context, blast_context=blast_context)
        except Exception as e:
            ops_bump("ai_reply_error")
            logging.error("Error processing Twilio message from %s: %s", phone_number, e)
            return
        if not (reply or "").strip():
            logging.info("No Twilio reply for ...%s (conversation ender or empty)", phone_number[-4:])
            return
        twilio.send_reply(phone_number, reply)
        _consume_message_credits(reply, message_text, source=f"twilio:{phone_number[-4:]}")
    finally:
        ai_reply_leave()


def _consume_message_credits(outbound_text: str, inbound_text: str, *, source: str) -> None:
    """Charge the brain's creator_slug for 1 inbound + N outbound segments.

    Writes directly to operator_credit_usage + credit_events so the main app
    doesn't need to import the operator package (they share a top-level
    'app/' directory name — a direct sys.path insert would shadow imports).

    Fail-open: never blocks message processing — billing is secondary to replies.
    """
    slug = getattr(brain, "slug", None) or "zarna"
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
                # Resolve user_id for this slug (owner preferred)
                cur.execute(
                    """
                    SELECT u.id, u.plan_tier, u.trial_credits_remaining
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
                user_id, plan_tier, _trial_left = row
                total_credits = 1 + outbound_credits  # 1 inbound + N outbound

                if plan_tier == "trial":
                    cur.execute(
                        """UPDATE operator_users
                           SET trial_credits_remaining = GREATEST(0, trial_credits_remaining - %s)
                           WHERE id=%s""",
                        (total_credits, user_id),
                    )

                cur.execute(
                    """
                    INSERT INTO operator_credit_usage
                        (operator_user_id, creator_slug, period_start, credits_included, credits_used)
                    VALUES (%s, %s, CURRENT_DATE, 0, %s)
                    ON CONFLICT (operator_user_id, period_start)
                    DO UPDATE SET credits_used = operator_credit_usage.credits_used + EXCLUDED.credits_used,
                                  updated_at = NOW()
                    """,
                    (user_id, slug, total_credits),
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

    if signup_res.join_confirmation_sms and signup_res.confirmation_phone:
        _send_join_confirmation_async(
            signup_res.confirmation_phone,
            signup_res.confirmation_channel or "twilio",
            signup_res.join_confirmation_sms,
        )

    phone_number, message_text = twilio.filter_inbound_for_ai(raw_from, raw_body)

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
    blast_ctx = None
    if not quiz_ctx:
        try:
            context_note = get_active_blast_context()
            if context_note:
                blast_ctx = build_blast_context_prompt(context_note)
                logging.info("Blast context injected for ...%s", phone_number[-4:] if phone_number else "?")
        except Exception:
            logging.exception("Blast context lookup failed — continuing with normal AI reply")

    threading.Thread(
        target=_process_twilio_message,
        args=(phone_number, message_text, quiz_ctx),
        kwargs={"blast_context": blast_ctx},
        daemon=True,
    ).start()

    return ("", 204)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
