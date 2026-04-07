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

from flask import Blueprint, Response, jsonify, request

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
# vCard endpoint — used for MMS contact-save on subscriber signup
# ---------------------------------------------------------------------------

# Simple in-process cache: slug → (mime_type, base64_data) or None
_logo_cache: dict = {}


def _fetch_logo_b64(logo_url: str):
    """Download logo and return (mime_type, base64_string), or None on failure."""
    try:
        import base64
        import urllib.request
        with urllib.request.urlopen(logo_url, timeout=5) as resp:
            data = resp.read()
            content_type = resp.headers.get_content_type() or "image/jpeg"
            return content_type, base64.b64encode(data).decode("ascii")
    except Exception:
        logger.warning("SMB vcard: failed to fetch logo from %s", logo_url, exc_info=True)
        return None


@smb_bp.route("/vcard/<slug>.vcf", methods=["GET"])
def smb_vcard(slug: str):
    """
    Serve a vCard for a tenant so subscribers can save the business
    as a contact with one tap. Linked in an MMS sent on first signup.
    Logo is fetched once, cached in memory, and embedded as base64 for
    reliable iOS display (external URL references are often ignored).
    """
    from app.smb.tenants import get_registry
    tenant = get_registry().get_by_slug(slug)
    if tenant is None:
        return ("Not found", 404)

    lines = [
        "BEGIN:VCARD",
        "VERSION:3.0",
        f"FN:{tenant.display_name}",
        "N:;;;;",                          # empty structured name → iOS treats as company contact
        f"ORG:{tenant.display_name}",
    ]
    if tenant.sms_number:
        lines.append(f"TEL;TYPE=CELL:{tenant.sms_number}")

    if tenant.logo_url:
        if slug not in _logo_cache:
            _logo_cache[slug] = _fetch_logo_b64(tenant.logo_url)
        logo = _logo_cache[slug]
        if logo:
            mime, b64 = logo
            img_type = mime.split("/")[-1].upper()  # e.g. "JPEG" or "PNG"
            lines.append(f"PHOTO;TYPE={img_type};ENCODING=BASE64:{b64}")

    lines.append("END:VCARD")

    vcf = "\r\n".join(lines) + "\r\n"
    return Response(vcf, mimetype="text/vcard",
                    headers={"Content-Disposition": f'attachment; filename="{slug}.vcf"'})


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

    # Hard firewall: only process messages addressed to a known SMB number.
    # Anything addressed to Zarna's number or an unknown number is dropped here.
    from app.smb.tenants import get_registry as _smb_registry
    if not _smb_registry().is_smb_number(to_number):
        logger.warning(
            "SMB webhook received message addressed to non-SMB number ...%s — dropping. "
            "Check Twilio webhook config for that number.",
            to_number[-4:] if to_number else "?",
        )
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
        # Reply FROM the tenant's SMS number, not from Zarna's default number.
        _twilio.send_reply(from_number, reply, from_number=to_number)
    except Exception:
        logger.exception(
            "SMB twilio send failed: to=...%s",
            from_number[-4:] if from_number else "?",
        )


def register_smb_routes(app):
    """Register the SMB blueprint with the Flask app. Call from main.py."""
    app.register_blueprint(smb_bp)
    logger.info("SMB blueprint registered at /smb/")
