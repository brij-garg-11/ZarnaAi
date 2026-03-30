"""Attach inbound texters to the active live show when rules match."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from app.live_shows import repository as repo

logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _body_matches_keyword(body: str, keyword: str) -> bool:
    body = (body or "").strip().lower()
    kw = (keyword or "").strip().lower()
    if not kw:
        return True
    if body == kw:
        return True
    parts = body.split()
    return bool(parts) and parts[0] == kw


def _as_utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _in_time_window(show: dict, now: datetime) -> bool:
    ws = _as_utc_aware(show.get("window_start"))
    we = _as_utc_aware(show.get("window_end"))
    now_u = now.astimezone(timezone.utc)
    if ws is None and we is None:
        return True
    if ws is not None and now_u < ws:
        return False
    if we is not None and now_u > we:
        return False
    return True


def try_live_show_signup(phone_number: str, message_text: str, channel: str) -> None:
    """
    If a show is live and the message matches its rules, record a signup.
    Safe to call on every inbound; logs and swallows errors (DB optional in dev).
    """
    if not phone_number or not message_text:
        return
    try:
        try:
            shows = repo.active_live_shows()
        except Exception as e:
            logger.warning("live show signup: could not load active shows: %s", e)
            return
        if not shows:
            return

        now = _now_utc()
        for show in shows:
            use_kw = bool(show.get("use_keyword_only", True))

            if use_kw:
                show_kw = (show.get("keyword") or "").strip()
                if not show_kw:
                    continue
                if not _body_matches_keyword(message_text, show_kw):
                    continue
                if not _in_time_window(show, now):
                    continue
            else:
                if show.get("window_start") is None or show.get("window_end") is None:
                    continue
                if not _in_time_window(show, now):
                    continue

            sid = show["id"]
            if repo.add_signup(sid, phone_number, channel):
                logger.info(
                    "Live show signup: show_id=%s phone=...%s channel=%s",
                    sid,
                    phone_number[-4:],
                    channel,
                )
            else:
                logger.debug(
                    "live show signup: no row inserted (duplicate or bad phone) show_id=%s",
                    sid,
                )
    except Exception:
        logger.exception("live show signup failed")
