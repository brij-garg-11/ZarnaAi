"""Shared helpers for inbound HTTP security (webhooks, test API)."""

from __future__ import annotations

import hmac
import json
import logging
import os
from typing import Any, Mapping

logger = logging.getLogger(__name__)


def running_in_production() -> bool:
    """True when we should enforce stricter inbound rules (Railway prod, explicit env, etc.)."""
    if (os.getenv("RAILWAY_ENVIRONMENT") or "").strip().lower() == "production":
        return True
    env = (os.getenv("ENVIRONMENT") or os.getenv("FLASK_ENV") or "").strip().lower()
    if env in ("production", "prod"):
        return True
    if (os.getenv("PRODUCTION") or "").strip().lower() in ("1", "true", "yes"):
        return True
    return False


def timing_safe_equal(expected: str, received: str) -> bool:
    """Constant-time string compare for secrets (UTF-8)."""
    if not expected or not received:
        return False
    try:
        return hmac.compare_digest(
            expected.encode("utf-8"),
            received.encode("utf-8"),
        )
    except Exception:
        return False


def slicktext_webhook_secret_configured() -> bool:
    return bool((os.getenv("SLICKTEXT_WEBHOOK_SECRET") or "").strip())


def verify_slicktext_webhook_secret() -> bool:
    """
    If SLICKTEXT_WEBHOOK_SECRET is set, caller must send matching X-Zarna-Webhook-Secret.
    If unset, returns True (backward compatible).
    """
    from flask import request

    secret = (os.getenv("SLICKTEXT_WEBHOOK_SECRET") or "").strip()
    if not secret:
        return True
    got = (request.headers.get("X-Zarna-Webhook-Secret") or "").strip()
    return timing_safe_equal(secret, got)


def log_sensitive_webhook_data() -> bool:
    return (os.getenv("LOG_SENSITIVE_WEBHOOK_DATA") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def slicktext_webhook_log_line(payload: Mapping[str, Any] | dict) -> None:
    """Log either a redacted summary or full payload (opt-in)."""
    if log_sensitive_webhook_data():
        logger.info("SlickText webhook payload (LOG_SENSITIVE_WEBHOOK_DATA): %s", payload)
        return
    try:
        raw = payload.get("data")
        if isinstance(raw, str):
            d = json.loads(raw)
        else:
            d = dict(payload) if payload else {}
        chat = d.get("ChatMessage") or {}
        mid = str(chat.get("ChatMessageId") or "")
        fn = str(chat.get("FromNumber") or "")
        body = str(chat.get("Body") or "")
        last4 = fn[-4:] if fn else "?"
        logger.info(
            "SlickText webhook: ChatMessageId=%s from_last4=...%s body_chars=%s",
            mid or "?",
            last4,
            len(body),
        )
    except Exception:
        logger.info("SlickText webhook: received (could not parse summary fields)")


def slicktext_ignored_log(payload: Mapping[str, Any] | dict) -> None:
    if log_sensitive_webhook_data():
        logger.info("SlickText webhook ignored, payload=%s", payload)
    else:
        slicktext_webhook_log_line(payload)
