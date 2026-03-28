import json as _json
import logging
import os
import threading
import time
from collections import OrderedDict
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify

from app.brain.handler import create_brain
from app.messaging.slicktext_adapter import create_slicktext_adapter
from app.messaging.twilio_adapter import create_twilio_adapter
from app.admin import admin_bp

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.register_blueprint(admin_bp)

brain     = create_brain()
slicktext = create_slicktext_adapter()
twilio    = create_twilio_adapter()

# ---------------------------------------------------------------------------
# Deduplication: last 200 message IDs (SlickText + Twilio)
# Prevents double-replies when either platform retries a webhook we already handled
# ---------------------------------------------------------------------------

_seen_message_ids: OrderedDict = OrderedDict()
_seen_lock = threading.Lock()
_MAX_SEEN = 200


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
# Per-phone rate limiting — max 3 messages per 60 seconds per number
# Protects against runaway loops and abuse during high-volume events
# ---------------------------------------------------------------------------

_rate_data: dict = {}   # phone -> [timestamp, ...]
_rate_lock = threading.Lock()
_RATE_WINDOW = 60       # seconds
_RATE_MAX    = 3        # messages per window


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


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "zarna-ai"})


# ---------------------------------------------------------------------------
# Provider-agnostic endpoint (useful for testing without SlickText)
# POST JSON: { "phone_number": "...", "message": "..." }
# ---------------------------------------------------------------------------

_API_SECRET = os.getenv("API_SECRET_KEY", "")


@app.route("/message", methods=["POST"])
def message():
    # API key check — requires X-Api-Key header matching API_SECRET_KEY env var
    if _API_SECRET and request.headers.get("X-Api-Key") != _API_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    # IP-based rate limiting — max 3 requests per 60 seconds
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

    reply = brain.handle_incoming_message(phone_number, message_text)
    return jsonify({"reply": reply})


# ---------------------------------------------------------------------------
# SlickText webhook
#
# Configure this URL in SlickText:
#   Dashboard → API & Webhooks → Webhook URL → https://yourdomain.com/slicktext/webhook
#   Event to subscribe: "Inbox Chat Message Received"
# ---------------------------------------------------------------------------

def _process_slicktext_message(phone_number: str, message_text: str) -> None:
    """Run brain + reply in a background thread so the webhook returns instantly."""
    try:
        reply = brain.handle_incoming_message(phone_number, message_text)
        slicktext.send_reply(phone_number, reply)
    except Exception as e:
        logging.error("Error processing SlickText message from %s: %s", phone_number, e)


@app.route("/slicktext/webhook", methods=["POST"])
def slicktext_webhook():
    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict() or {}

    logging.info("SlickText webhook raw payload: %s", payload)

    # Deduplicate using ChatMessageId (guards against SlickText retries)
    try:
        raw_data = _json.loads(payload.get("data", "{}")) if isinstance(payload.get("data"), str) else payload
        message_id = str(raw_data.get("ChatMessage", {}).get("ChatMessageId", ""))
    except Exception:
        message_id = ""

    if _already_processed(message_id):
        logging.info("Duplicate SlickText webhook ignored (ChatMessageId=%s)", message_id)
        return jsonify({"status": "duplicate"}), 200

    phone_number, message_text = slicktext.parse_inbound(payload)

    if not phone_number or not message_text:
        logging.info("SlickText webhook: message filtered or unparseable. Payload: %s", payload)
        return jsonify({"status": "ignored"}), 200

    if _is_rate_limited(phone_number):
        logging.warning("Rate limit hit for %s — dropping message", phone_number)
        return jsonify({"status": "rate_limited"}), 200

    threading.Thread(
        target=_process_slicktext_message,
        args=(phone_number, message_text),
        daemon=True,
    ).start()

    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Twilio webhook
#
# Configure in Twilio Console:
#   Phone Numbers → Active Numbers → your number
#   Messaging → A message comes in → Webhook → HTTP POST
#   URL: https://web-production-ec3da.up.railway.app/twilio/webhook
# ---------------------------------------------------------------------------

def _process_twilio_message(phone_number: str, message_text: str) -> None:
    try:
        reply = brain.handle_incoming_message(phone_number, message_text)
        twilio.send_reply(phone_number, reply)
    except Exception as e:
        logging.error("Error processing Twilio message from %s: %s", phone_number, e)


@app.route("/twilio/webhook", methods=["POST"])
def twilio_webhook():
    form_data = request.form.to_dict()
    logging.info(
        "Twilio webhook received: From=%s Body=%s",
        form_data.get("From"),
        form_data.get("Body"),
    )

    # Signature validation — rejects spoofed requests in production.
    # Behind Railway's proxy, request.url is http:// but Twilio signs the https:// URL,
    # so we reconstruct the correct URL using the X-Forwarded-Proto header.
    if os.getenv("TWILIO_VALIDATE_SIGNATURE", "true").lower() == "true":
        sig = request.headers.get("X-Twilio-Signature", "")
        forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
        url = request.url
        if forwarded_proto == "https" and url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        if not twilio.validate_signature(url, form_data, sig):
            logging.warning("Invalid Twilio signature from %s", form_data.get("From"))
            return ("Forbidden", 403)

    # Deduplicate using MessageSid
    message_sid = form_data.get("MessageSid", "")
    if _already_processed(message_sid):
        logging.info("Duplicate Twilio webhook ignored (MessageSid=%s)", message_sid)
        return ("", 204)

    phone_number, message_text = twilio.parse_inbound(form_data)

    if not phone_number or not message_text:
        logging.info("Twilio webhook: message filtered or unparseable.")
        return ("", 204)

    if _is_rate_limited(phone_number):
        logging.warning("Rate limit hit for Twilio %s — dropping message", phone_number)
        return ("", 204)

    threading.Thread(
        target=_process_twilio_message,
        args=(phone_number, message_text),
        daemon=True,
    ).start()

    return ("", 204)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
