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
from pathlib import Path

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


def _load_logo_b64(slug: str, logo_url: str):
    """
    Return (mime_type, base64_string) for the tenant logo, or None on failure.

    Preference order:
      1. Local file: creator_config/<slug>_logo.png  (no network, always works)
      2. Remote URL from config (fallback if no local file)

    The image is resized to a 300×300 square — centred, preserving aspect ratio —
    so it fits the iOS contact photo circle without clipping or appearing tiny.
    """
    import base64
    import io
    from pathlib import Path
    from PIL import Image

    def _process(img_bytes: bytes) -> tuple[str, str]:
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        # Centre-crop to square so the circular badge fills the whole contact photo
        w, h = img.size
        side = min(w, h)
        img = img.crop(((w - side) // 2, (h - side) // 2,
                         (w + side) // 2, (h + side) // 2))
        img = img.resize((300, 300), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return "image/jpeg", base64.b64encode(buf.getvalue()).decode("ascii")

    # 1. Local file
    local_path = Path(__file__).parent.parent.parent / "creator_config" / f"{slug}_logo.png"
    if not local_path.exists():
        local_path = local_path.with_suffix(".jpg")
    if local_path.exists():
        try:
            return _process(local_path.read_bytes())
        except Exception:
            logger.warning("SMB vcard: failed to process local logo %s", local_path, exc_info=True)

    # 2. Remote URL
    if logo_url:
        try:
            import urllib.request
            req = urllib.request.Request(
                logo_url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SMBVCard/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return _process(resp.read())
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

    if tenant.logo_url or (Path(__file__).parent.parent.parent / "creator_config" / f"{slug}_logo.png").exists():
        if slug not in _logo_cache:
            _logo_cache[slug] = _load_logo_b64(slug, tenant.logo_url)
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

    # Meter credits for this conversation turn (1 inbound + N outbound segments).
    # Mirrors the same logic in main.py _consume_message_credits; uses the
    # operator billing stack directly since the SMB app runs in the same process
    # as the operator API (unlike the Zarna performer app in main.py).
    # Fail-open: billing must never stop a reply from being sent.
    try:
        import math as _math
        from app.smb.tenants import get_registry as _smb_registry  # type: ignore

        tenant = _smb_registry().get_by_to(to_number) if to_number else None
        slug = tenant.slug if tenant else None
        if slug:
            def _segs(text: str) -> int:
                if not text:
                    return 1
                n = len(text)
                if any(ord(c) > 127 for c in text):
                    return 1 if n <= 70 else max(1, _math.ceil(n / 67))
                return 1 if n <= 160 else max(1, _math.ceil(n / 153))

            outbound_segs = _segs(reply)
            from operator.app.billing.credits import consume_credit, KIND_SMS_INBOUND, KIND_SMS_OUTBOUND  # type: ignore
            source = f"smb:{from_number[-4:] if from_number else '?'}"
            consume_credit(slug=slug, kind=KIND_SMS_INBOUND, credits=1, source_id=source)
            consume_credit(slug=slug, kind=KIND_SMS_OUTBOUND, credits=outbound_segs, source_id=source)
    except Exception:
        logger.warning("SMB credit metering failed for to_number=...%s",
                       to_number[-4:] if to_number else "?")


# ---------------------------------------------------------------------------
# Link click tracking  GET /smb/r/<slug>/<link_key>
# ---------------------------------------------------------------------------

@smb_bp.route("/r/<slug>/<link_key>", methods=["GET"])
def smb_link_redirect(slug: str, link_key: str):
    """
    Tracked redirect for links sent to subscribers.
    Links are loaded from each tenant's creator_config JSON (tracked_links key).
    Logs the click then issues a 302 to the real URL.
    """
    from app.smb.tenants import get_registry as _smb_registry
    tenant = _smb_registry().get_by_slug(slug)
    target = tenant.tracked_links.get(link_key) if tenant else None

    if not target:
        from flask import abort
        abort(404)

    # Log asynchronously so redirect is instant
    phone = request.args.get("p", "")  # optional subscriber phone hint
    threading.Thread(
        target=_log_link_click,
        args=(slug, link_key, phone or None),
        daemon=True,
    ).start()

    from flask import redirect
    return redirect(target, code=302)


def _log_link_click(slug: str, link_key: str, phone: str | None) -> None:
    try:
        from app.admin_auth import get_db_connection
        conn = get_db_connection()
        if not conn:
            return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO smb_link_clicks (tenant_slug, link_key, phone_number)
                        VALUES (%s, %s, %s)
                        """,
                        (slug, link_key, phone),
                    )
        finally:
            conn.close()
    except Exception:
        logger.exception("SMB link click log failed slug=%s key=%s", slug, link_key)


def register_smb_routes(app):
    """Register the SMB blueprint with the Flask app. Call from main.py."""
    app.register_blueprint(smb_bp)
    logger.info("SMB blueprint registered at /smb/")
