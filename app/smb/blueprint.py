"""
SMB Flask blueprint.

Registers all routes for the SMB vertical under /smb/.
Kept separate from the Zarna routes in main.py so the two streams
never interfere with each other.

Routes:
  POST /smb/twilio/webhook  — inbound SMS from any SMB Twilio number
  GET  /smb/health          — quick liveness check for this blueprint

Twilio signature validation reuses the same logic as the Zarna webhook.
The To field in the Twilio payload identifies which business the message
belongs to — SMBBrain.handle_message() resolves the tenant from there.
"""

import logging
import os
import threading

from flask import Blueprint, jsonify, request

from app.messaging.twilio_adapter import create_twilio_adapter
from app.smb.brain import create_smb_brain

logger = logging.getLogger(__name__)

smb_bp = Blueprint("smb", __name__, url_prefix="/smb")

# Instantiated once at import time — shared across all requests.
_brain = create_smb_brain()
_twilio = create_twilio_adapter()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@smb_bp.route("/health", methods=["GET"])
def smb_health():
    return jsonify({"status": "ok", "service": "smb"})


# ---------------------------------------------------------------------------
# Twilio inbound webhook
# ---------------------------------------------------------------------------

@smb_bp.route("/twilio/webhook", methods=["POST"])
def smb_twilio_webhook():
    form_data = request.form.to_dict()
    from_number = form_data.get("From", "")
    to_number = form_data.get("To", "")
    body = form_data.get("Body", "")

    logger.info(
        "SMB webhook received: From=...%s To=...%s body_chars=%d",
        from_number[-4:] if from_number else "?",
        to_number[-4:] if to_number else "?",
        len(body),
    )

    # Twilio signature validation — same flag as the Zarna webhook
    if os.getenv("TWILIO_VALIDATE_SIGNATURE", "true").lower() == "true":
        sig = request.headers.get("X-Twilio-Signature", "")
        forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
        url = request.url
        if forwarded_proto == "https" and url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        if not _twilio.validate_signature(url, form_data, sig):
            logger.warning(
                "SMB webhook: invalid Twilio signature from ...%s",
                from_number[-4:] if from_number else "?",
            )
            return ("Forbidden", 403)

    if not from_number or not body.strip():
        logger.info("SMB webhook: missing From or Body — ignored")
        return ("", 204)

    # Fire-and-forget so Twilio gets its 204 within the 15-second window
    threading.Thread(
        target=_process_smb_message,
        args=(from_number, to_number, body.strip()),
        daemon=True,
    ).start()

    return ("", 204)


# ---------------------------------------------------------------------------
# Async message processor
# ---------------------------------------------------------------------------

def _process_smb_message(from_number: str, to_number: str, message_text: str) -> None:
    try:
        reply = _brain.handle_message(from_number, to_number, message_text)
    except Exception:
        logger.exception(
            "SMB brain error: From=...%s To=...%s",
            from_number[-4:] if from_number else "?",
            to_number[-4:] if to_number else "?",
        )
        return

    if not reply or not reply.strip():
        logger.info(
            "SMB brain: no reply for ...%s (dropped or silent)",
            from_number[-4:] if from_number else "?",
        )
        return

    try:
        _twilio.send_reply(from_number, reply)
    except Exception:
        logger.exception(
            "SMB twilio send failed: to=...%s",
            from_number[-4:] if from_number else "?",
        )


def register_smb_routes(app):
    """Register the SMB blueprint with the Flask app. Call from main.py."""
    app.register_blueprint(smb_bp)
    logger.info("SMB blueprint registered at /smb/")
