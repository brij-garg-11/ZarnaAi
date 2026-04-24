"""
Universal Bot Provisioning Pipeline.

Orchestrates the full "new creator → live bot" flow. Called as a background
thread from POST /api/onboarding/submit.

Contract with callers:
  provision_new_creator(user_id, slug, config) -> None
  - Never raises. All failures are caught, logged, and stored on
    bot_configs.error_message so the frontend can surface them.
  - Updates bot_configs.provisioning_status as it progresses:
      NULL → in_progress → live | failed
    (bot_configs.status keeps its original meaning — 'submitted' — so
     existing code that reads it is unaffected.)
  - Idempotent: safe to retry a failed provisioning; each submodule checks
    whether its work is already done and skips cleanly.

Submodules:
  phone         — buy + wire Twilio number (stubbed until A2P approved)
  config_writer — generate personality JSON via Gemini, store in creator_configs
  ingestion     — scrape, chunk, embed content, store in creator_embeddings
  notifications — welcome email via Resend
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Dict, Optional

from ..db import get_conn
from . import config_writer, ingestion, notifications, phone

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def _set_status(slug: str, status: str, error_message: Optional[str] = None) -> None:
    """
    Update bot_configs.provisioning_status (and optionally .error_message).
    Allowed values: 'in_progress', 'live', 'failed', or NULL (not started).
    When status != 'failed' we also clear any stale error_message so the
    next successful run leaves a clean record.
    """
    conn = get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            if error_message is None:
                cur.execute(
                    """
                    UPDATE bot_configs
                    SET provisioning_status=%s,
                        error_message=NULL,
                        updated_at=NOW()
                    WHERE creator_slug=%s
                    """,
                    (status, slug),
                )
            else:
                cur.execute(
                    """
                    UPDATE bot_configs
                    SET provisioning_status=%s,
                        error_message=%s,
                        updated_at=NOW()
                    WHERE creator_slug=%s
                    """,
                    (status, error_message, slug),
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def provision_new_creator(
    user_id: int,
    slug: str,
    config: Dict[str, Any],
) -> None:
    """
    Run every provisioning step for a new creator.

    Steps (in order):
      1. Mark provisioning_status = 'in_progress'
      2. Phone     — buy + wire a Twilio number (stubbed today)
      3. Config    — generate personality JSON with Gemini, write to creator_configs
      4. Ingestion — scrape + chunk + embed content, write to creator_embeddings
      5. Email     — welcome email via Resend
      6. Mark provisioning_status = 'live'

    Any exception → provisioning_status = 'failed' with the traceback stored.
    """
    _log.info("provisioning[%s]: starting (user_id=%s)", slug, user_id)
    try:
        _set_status(slug, "in_progress")

        _log.info("provisioning[%s]: step 1/4 phone", slug)
        phone_number = phone.buy_and_configure(slug)

        _log.info("provisioning[%s]: step 2/4 config_writer", slug)
        config_writer.generate_and_write(slug, config)

        _log.info("provisioning[%s]: step 3/4 ingestion", slug)
        chunks_inserted = ingestion.run(slug, config)

        _log.info("provisioning[%s]: step 4/4 notifications", slug)
        notifications.send_welcome(user_id, phone_number)

        _set_status(slug, "live")
        _log.info(
            "provisioning[%s]: LIVE (phone=%s chunks=%d)",
            slug, phone_number, chunks_inserted,
        )

    except Exception as exc:
        tb = traceback.format_exc()
        _log.exception("provisioning[%s]: FAILED — %s", slug, exc)
        try:
            _set_status(slug, "failed", error_message=tb[:4000])
        except Exception:
            _log.exception("provisioning[%s]: could not persist error_message", slug)


__all__ = ["provision_new_creator"]
