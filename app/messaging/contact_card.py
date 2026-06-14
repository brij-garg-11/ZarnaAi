"""
Performer vCard + first-message sequence (Item 2).

When a brand-new fan texts a performer for the first time, we optionally send:
  1. A vCard MMS (so the fan can one-tap save the performer as a real contact,
     with name + photo) — only when ``send_contact_card`` is enabled.
  2. The performer's ``first_message`` welcome text, with the compliance footer
     appended — only when ``first_message`` is set.
Then the normal AI reply follows (handled by the caller).

Everything here is opt-in and OFF by default: a creator config with none of the
SMS-profile fields set sends nothing extra, so existing behaviour is unchanged.

The vCard itself is served by the ``/vcard/performer/<slug>.vcf`` route (see
main.py) because Twilio MMS needs a publicly fetchable media URL. The photo is
fetched once and embedded as base64 — iOS frequently ignores external PHOTO URIs.
"""
from __future__ import annotations

import base64
import io
import logging
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# Mirrors operator/app/branding.PERFORMER_COMPLIANCE_FOOTER so the opt-in text a
# fan receives carries the required A2P disclosures.
COMPLIANCE_FOOTER = os.getenv(
    "PERFORMER_COMPLIANCE_FOOTER",
    "Msg & data rates may apply. Reply STOP to opt out, HELP for help.",
)

# Cache embedded photos per slug so we don't re-fetch on every vCard request.
_photo_cache: dict[str, Optional[tuple[str, str]]] = {}
_photo_cache_lock = threading.Lock()

# Repo root, so a creator can ship a fixed headshot as a bundled asset
# (e.g. "app/assets/creator_photos/zarna.png") instead of relying on an
# external image host staying reachable at send time.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _read_photo_bytes(slug: str, photo_url: str) -> Optional[bytes]:
    """Load raw image bytes from an http(s) URL or a bundled local asset path.

    Local paths are resolved relative to the project root (unless absolute), so
    ``profile_photo_url`` can point at a file committed alongside the code.
    """
    if photo_url.startswith(("http://", "https://")):
        try:
            import urllib.request

            req = urllib.request.Request(
                photo_url, headers={"User-Agent": "Mozilla/5.0 (compatible; ZarnaVCard/1.0)"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.read()
        except Exception:
            logger.warning("vcard: failed to fetch photo for slug=%s url=%s", slug, photo_url,
                           exc_info=True)
            return None

    path = photo_url if os.path.isabs(photo_url) else os.path.join(_PROJECT_ROOT, photo_url)
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except Exception:
        logger.warning("vcard: failed to read local photo for slug=%s path=%s", slug, path,
                       exc_info=True)
        return None


def _load_photo_b64(slug: str, photo_url: str) -> Optional[tuple[str, str]]:
    """Load the profile photo, centre-crop to square, return (mime, base64) or None."""
    if not photo_url:
        return None
    raw = _read_photo_bytes(slug, photo_url)
    if raw is None:
        return None
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(raw)).convert("RGB")
        w, h = img.size
        side = min(w, h)
        img = img.crop(((w - side) // 2, (h - side) // 2,
                        (w + side) // 2, (h + side) // 2)).resize((300, 300), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return "image/jpeg", base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        logger.warning("vcard: failed to process photo for slug=%s", slug, exc_info=True)
        return None


def build_performer_vcard(creator_config, tel: str = "") -> str:
    """Build the vCard text for a performer from their CreatorConfig.

    ``tel`` (the creator's SMS number) is included as the contact phone so iOS
    links the saved contact to the conversation. The photo is embedded base64.
    """
    display_name = (
        getattr(creator_config, "sms_display_name", "")
        or getattr(creator_config, "name", "")
        or "Contact"
    )
    slug = getattr(creator_config, "slug", "") or "creator"
    lines = [
        "BEGIN:VCARD",
        "VERSION:3.0",
        f"FN:{display_name}",
        "N:;;;;",
        f"ORG:{display_name}",
    ]
    tel = (tel or "").strip()
    if tel:
        lines.append(f"TEL;TYPE=CELL:{tel}")

    photo_url = getattr(creator_config, "profile_photo_url", "") or ""
    if photo_url:
        with _photo_cache_lock:
            if slug not in _photo_cache:
                _photo_cache[slug] = _load_photo_b64(slug, photo_url)
            cached = _photo_cache[slug]
        if cached:
            mime, b64 = cached
            lines.append(f"PHOTO;TYPE={mime.split('/')[-1].upper()};ENCODING=BASE64:{b64}")

    lines.append("END:VCARD")
    return "\r\n".join(lines) + "\r\n"


def clear_photo_cache() -> None:
    """Test/admin helper."""
    with _photo_cache_lock:
        _photo_cache.clear()


def _vcard_base_url() -> str:
    domain = (
        os.getenv("OPERATOR_API_BASE_URL")
        or os.getenv("RAILWAY_PUBLIC_DOMAIN")
        or os.getenv("PUBLIC_BASE_URL")
        or ""
    ).strip()
    if domain and not domain.startswith("http"):
        domain = f"https://{domain}"
    return domain.rstrip("/")


def first_message_with_footer(first_message: str) -> str:
    """Append the compliance footer to the creator's welcome text (once)."""
    body = (first_message or "").strip()
    if not body:
        return ""
    if COMPLIANCE_FOOTER and COMPLIANCE_FOOTER.lower() not in body.lower():
        return f"{body}\n\n{COMPLIANCE_FOOTER}"
    return body


def maybe_send_first_contact(adapter, to_number: str, creator_config, from_number: str = "") -> bool:
    """
    Send the vCard MMS and/or first-message welcome to a brand-new fan.

    Returns True if anything was sent. Safe no-op (returns False) when the
    creator has neither send_contact_card nor first_message configured, or when
    inputs are missing. Never raises — failures are logged and swallowed so the
    normal AI reply is never blocked.
    """
    if creator_config is None or not to_number:
        return False

    send_card = bool(getattr(creator_config, "send_contact_card", False))
    first_msg = getattr(creator_config, "first_message", "") or ""
    if not send_card and not first_msg.strip():
        return False

    slug = getattr(creator_config, "slug", "") or "creator"
    sent_any = False

    # 1. vCard MMS first, so the saved contact name/photo is in place.
    if send_card:
        base = _vcard_base_url()
        if base:
            from urllib.parse import quote
            tel_q = quote((from_number or "").strip(), safe="")
            vcard_url = f"{base}/vcard/performer/{slug}.vcf"
            if tel_q:
                vcard_url += f"?tel={tel_q}"
            try:
                ok = adapter.send_reply(
                    to_number,
                    body=f"Tap to save {getattr(creator_config, 'sms_display_name', '') or getattr(creator_config, 'name', 'me')} to your contacts.",
                    from_number=from_number or None,
                    media_url=vcard_url,
                )
                sent_any = sent_any or bool(ok)
            except Exception:
                logger.warning("vcard: send failed for slug=%s to=...%s", slug,
                               to_number[-4:] if to_number else "?", exc_info=True)
        else:
            logger.warning("vcard: no public base URL configured — skipping vCard for slug=%s", slug)

    # 2. The welcome text with the compliance footer.
    welcome = first_message_with_footer(first_msg)
    if welcome:
        try:
            ok = adapter.send_reply(to_number, body=welcome, from_number=from_number or None)
            sent_any = sent_any or bool(ok)
        except Exception:
            logger.warning("first_message: send failed for slug=%s to=...%s", slug,
                           to_number[-4:] if to_number else "?", exc_info=True)

    return sent_any
