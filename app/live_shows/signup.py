"""Attach inbound texters to the active live show when rules match."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from app.live_shows import repository as repo
from app.live_shows.join_confirmations import (
    random_comedy_confirmation_new,
    random_comedy_confirmation_repeat,
    random_live_stream_confirmation_new,
    random_live_stream_confirmation_repeat,
)
from app.live_shows.keyword_match import body_matches_keyword, is_keyword_only_join

logger = logging.getLogger(__name__)


@dataclass
class LiveShowSignupResult:
    """Result of try_live_show_signup for webhook routing."""

    suppress_ai: bool = False
    """If True, skip Gemini reply (keyword-only path)."""
    join_confirmation_sms: Optional[str] = None
    """If set, send this one SMS to the fan (comedy show join copy)."""
    confirmation_phone: Optional[str] = None
    confirmation_channel: Optional[str] = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def _event_category(show: dict) -> str:
    raw = (show.get("event_category") or "other") or "other"
    v = str(raw).strip().lower()
    if v == "livestream":
        return "live_stream"
    return v


def try_live_show_signup(phone_number: str, message_text: str, channel: str) -> LiveShowSignupResult:
    """
    Record signup when a live show's rules match.

    Comedy or live stream + keyword-only: random confirmation SMS (new vs repeat);
    other categories: keyword-only suppresses AI, no SMS.
    """
    out = LiveShowSignupResult()
    if not phone_number or not message_text:
        return out
    try:
        try:
            shows = repo.active_live_shows()
        except Exception as e:
            logger.warning("live show signup: could not load active shows: %s", e)
            return out
        if not shows:
            return out

        now = _now_utc()
        for show in shows:
            use_kw = bool(show.get("use_keyword_only", True))

            if use_kw:
                show_kw = (show.get("keyword") or "").strip()
                if not show_kw:
                    continue
                if not body_matches_keyword(message_text, show_kw):
                    continue
                if not _in_time_window(show, now):
                    continue
                if is_keyword_only_join(message_text, show_kw):
                    out.suppress_ai = True
            else:
                if show.get("window_start") is None or show.get("window_end") is None:
                    continue
                if not _in_time_window(show, now):
                    continue

            sid = show["id"]
            inserted = repo.add_signup(sid, phone_number, channel)
            if inserted:
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

            cat = _event_category(show)
            if use_kw and is_keyword_only_join(message_text, show_kw) and cat in ("comedy", "live_stream"):
                if cat == "comedy":
                    if inserted:
                        out.join_confirmation_sms = random_comedy_confirmation_new()
                    else:
                        out.join_confirmation_sms = random_comedy_confirmation_repeat()
                else:
                    if inserted:
                        out.join_confirmation_sms = random_live_stream_confirmation_new()
                    else:
                        out.join_confirmation_sms = random_live_stream_confirmation_repeat()
                out.confirmation_phone = phone_number
                out.confirmation_channel = channel

            break
    except Exception:
        logger.exception("live show signup failed")
        return LiveShowSignupResult()
    return out
