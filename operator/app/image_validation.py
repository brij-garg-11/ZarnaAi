"""
Shared image-upload validation (Item 2).

Both upload endpoints (blast image + SMS profile photo) run uploads through
``validate_image_bytes`` so the rules are enforced server-side and the user gets
a clear, specific error they can act on:

  - Max 5 MB (Twilio MMS hard limit for media).
  - Must be a real, decodable image.
  - Optionally square (1:1) within a small tolerance — OFF by default. Non-square
    images are allowed everywhere; the frontend warns that a non-square photo may
    crop oddly inside the circular contact-card/avatar frame. Callers can opt in
    to hard square enforcement via require_square=True if they ever need it.

Returns an error string (safe to show the user) or None when the image is valid.
"""
from __future__ import annotations

import io
import logging

logger = logging.getLogger(__name__)

MAX_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB
SQUARE_TOLERANCE = 0.02  # allow ~2% off-square (e.g. 640x638) before rejecting


def _human_mb(n: int) -> str:
    return f"{n / (1024 * 1024):.1f} MB"


def validate_image_bytes(data: bytes, *, require_square: bool = False) -> str | None:
    """Validate raw image bytes. Return a user-facing error string, or None if OK."""
    if not data:
        return "Uploaded file is empty — please try again."

    if len(data) > MAX_UPLOAD_BYTES:
        return (
            f"Image is {_human_mb(len(data))} — the maximum is 5 MB. "
            "Please resize or compress it and try again."
        )

    try:
        from PIL import Image
    except Exception:
        # Pillow should always be installed; if not, fail open on dimensions so a
        # missing dep can't block uploads — size was already enforced above.
        logger.warning("image_validation: Pillow unavailable — skipping dimension check")
        return None

    try:
        with Image.open(io.BytesIO(data)) as img:
            width, height = img.size
    except Exception:
        return "That file isn't a valid image. Please upload a JPG or PNG."

    if width <= 0 or height <= 0:
        return "That image has invalid dimensions. Please upload a JPG or PNG."

    if require_square:
        longest = max(width, height)
        if longest and abs(width - height) / longest > SQUARE_TOLERANCE:
            return (
                f"Image must be square (e.g. 640×640). Yours was {width}×{height}. "
                "Please crop it to a square and re-upload."
            )

    return None
