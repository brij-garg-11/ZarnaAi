import json as _json
import logging
import threading
from collections import OrderedDict
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify

from app.brain.handler import create_brain
from app.messaging.slicktext_adapter import create_slicktext_adapter

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

brain = create_brain()
slicktext = create_slicktext_adapter()

# Deduplication: remember the last 200 processed message IDs
# Prevents double-replies when SlickText retries a webhook we already handled
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
# Health check
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "zarna-ai"})


# ---------------------------------------------------------------------------
# Provider-agnostic endpoint (useful for testing without SlickText)
# POST JSON: { "phone_number": "...", "message": "..." }
# ---------------------------------------------------------------------------

@app.route("/message", methods=["POST"])
def message():
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
#
# SlickText POSTs JSON with event type "ChatMessageRecieved" (their spelling).
# We parse the sender + message, run the brain, then send the reply back
# through the SlickText API.
# ---------------------------------------------------------------------------

def _process_message(phone_number: str, message_text: str) -> None:
    """Run brain + reply in a background thread so the webhook returns instantly."""
    try:
        reply = brain.handle_incoming_message(phone_number, message_text)
        slicktext.send_reply(phone_number, reply)
    except Exception as e:
        logging.error(f"Error processing message from {phone_number}: {e}")


@app.route("/slicktext/webhook", methods=["POST"])
def slicktext_webhook():
    # Try JSON first, fall back to form-encoded (SlickText v1 sends form data)
    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict() or {}

    logging.info(f"SlickText webhook raw payload: {payload}")

    # Deduplicate using ChatMessageId (guards against SlickText retries)
    try:
        raw_data = _json.loads(payload.get("data", "{}")) if isinstance(payload.get("data"), str) else payload
        message_id = str(raw_data.get("ChatMessage", {}).get("ChatMessageId", ""))
    except Exception:
        message_id = ""

    if _already_processed(message_id):
        logging.info(f"Duplicate webhook ignored (ChatMessageId={message_id})")
        return jsonify({"status": "duplicate"}), 200

    phone_number, message_text = slicktext.parse_inbound(payload)

    if not phone_number or not message_text:
        logging.info(f"SlickText webhook: message filtered or unparseable. Payload: {payload}")
        return jsonify({"status": "ignored"}), 200

    # Return 200 to SlickText immediately — stops retries before they start.
    # Process the message in a background thread.
    threading.Thread(
        target=_process_message,
        args=(phone_number, message_text),
        daemon=True,
    ).start()

    return jsonify({"status": "ok"})


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
