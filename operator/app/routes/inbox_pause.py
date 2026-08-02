"""
Inbox AI pause / resume JSON API — human takeover controls for the dashboard.

POST /api/inbox/<ident>/pause-ai   → stop the bot from auto-replying to this fan
POST /api/inbox/<ident>/resume-ai  → let the bot auto-reply again

`ident` is the fan's last-4 digits or full number (same resolver the rest of the
inbox uses). Pausing only happens through these explicit endpoints — sending a
manual message from the inbox does NOT auto-pause the bot.

Registered via register_inbox_pause_api_routes(api_bp) from
operator/app/routes/api.py.
"""

import logging

from flask import jsonify

from ..routes.auth import login_required, current_user
from ..db import get_conn
from ..ai_pause import pause_ai, resume_ai

logger = logging.getLogger(__name__)


def register_inbox_pause_api_routes(api_bp, slug_or_abort, require_performer_account):
    """Attach AI pause/resume routes to the shared api blueprint."""

    def _resolve(ident, slug):
        # Imported lazily: api.py registers this module at import-time bottom,
        # after _resolve_fan_phone is defined, so importing at call-time is safe.
        from .api import _resolve_fan_phone
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                return _resolve_fan_phone(cur, ident, slug)
        finally:
            conn.close()

    @api_bp.route("/api/inbox/<ident>/pause-ai", methods=["POST"])
    @login_required
    def inbox_pause_ai(ident):
        require_performer_account()
        slug = slug_or_abort()
        phone = _resolve(ident, slug)
        if not phone:
            return jsonify(success=False, error=f"No fan found for '{ident}'."), 404
        by = (current_user() or {}).get("email", "")
        try:
            pause_ai(phone, slug, paused_by=by)
        except Exception:
            logger.exception("inbox_pause_ai failed for slug=%s", slug)
            return jsonify(success=False, error="Failed to pause AI."), 500
        return jsonify(success=True, ai_paused=True)

    @api_bp.route("/api/inbox/<ident>/resume-ai", methods=["POST"])
    @login_required
    def inbox_resume_ai(ident):
        require_performer_account()
        slug = slug_or_abort()
        phone = _resolve(ident, slug)
        if not phone:
            return jsonify(success=False, error=f"No fan found for '{ident}'."), 404
        try:
            resume_ai(phone, slug)
        except Exception:
            logger.exception("inbox_resume_ai failed for slug=%s", slug)
            return jsonify(success=False, error="Failed to resume AI."), 500
        return jsonify(success=True, ai_paused=False)
