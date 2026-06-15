"""Postgres access for live_shows, signups, broadcast jobs."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
try:
    # Real psycopg2 ships psycopg2.errors. Tests sometimes stub psycopg2
    # with MagicMock (which isn't a real package), so the submodule import
    # fails — guard it so test imports succeed and the runtime DB code
    # can fall back to a generic exception type.
    from psycopg2 import errors as _pg_errors
    _UndefinedColumn = _pg_errors.UndefinedColumn
except (ImportError, AttributeError):
    _UndefinedColumn = type("UndefinedColumnSentinel", (Exception,), {})

from app.admin_auth import get_db_connection
from app.area_codes import area_code_from_phone
from app.live_shows.event_time import normalize_event_timezone
from app.messaging.broadcast import normalize_e164

logger = logging.getLogger(__name__)


def _conn():
    return get_db_connection()


def save_broadcast_message(phone: str, body: str, creator_slug: str | None = None) -> None:
    """Persist a live-show broadcast into the shared conversation history.

    Mirrors the operator blast pattern (operator/app/blast_sender.py):
    writes an assistant row tagged msg_source='blast' so the broadcast shows
    up in the inbox thread — giving a fan's later reply preceding context —
    while being excluded from the LLM conversation history
    (get_conversation_history filters msg_source='blast').

    Also upserts a contact so the recipient appears in the inbox list with a
    card; the card then auto-populates as the fan replies (handler._update_memory).

    Best-effort: any failure here must never break the send loop, so we log
    and swallow exceptions.
    """
    norm = normalize_e164(phone)
    if not norm:
        return
    slug = (creator_slug or os.getenv("CREATOR_SLUG") or "zarna").strip().lower()
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                _npa = area_code_from_phone(norm)
                cur.execute(
                    """
                    INSERT INTO contacts (phone_number, source, creator_slug, area_codes)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (phone_number) DO UPDATE
                      SET area_codes = EXCLUDED.area_codes
                      WHERE contacts.area_codes = '{}'
                        AND EXCLUDED.area_codes <> '{}'
                    """,
                    (norm, "live_show_broadcast", slug, [_npa] if _npa else []),
                )
                cur.execute(
                    """
                    INSERT INTO messages (phone_number, role, text, msg_source, creator_slug)
                    VALUES (%s, 'assistant', %s, 'blast', %s)
                    """,
                    (norm, body, slug),
                )
    except Exception:
        logger.warning(
            "save_broadcast_message failed for ...%s", norm[-4:], exc_info=True
        )
    finally:
        c.close()


def list_shows() -> List[Dict[str, Any]]:
    c = _conn()
    if not c:
        return []
    try:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT s.*,
                  (SELECT COUNT(*) FROM live_show_signups x WHERE x.show_id = s.id) AS signup_count
                FROM live_shows s
                ORDER BY s.created_at DESC
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        c.close()


def get_show(show_id: int) -> Optional[Dict[str, Any]]:
    c = _conn()
    if not c:
        return None
    try:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT s.*,
                  (SELECT COUNT(*) FROM live_show_signups x WHERE x.show_id = s.id) AS signup_count
                FROM live_shows s WHERE s.id = %s
            """, (show_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        c.close()


def _normalize_event_category(raw: Optional[str]) -> str:
    v = (raw or "other").strip().lower()
    if v == "livestream":
        v = "live_stream"
    return v if v in ("comedy", "live_stream", "other") else "other"


def create_show(
    name: str,
    keyword: str,
    use_keyword_only: bool,
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    deliver_as: str,
    event_category: str = "other",
    event_timezone: Optional[str] = None,
) -> int:
    c = _conn()
    if not c:
        raise RuntimeError("No database")
    ec = _normalize_event_category(event_category)
    etz = normalize_event_timezone(event_timezone or "America/New_York")
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO live_shows
                      (name, keyword, use_keyword_only, window_start, window_end, deliver_as, status, event_category, event_timezone)
                    VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s)
                    RETURNING id
                    """,
                    (
                        name.strip(),
                        keyword.strip().lower() if keyword else "",
                        use_keyword_only,
                        window_start,
                        window_end,
                        deliver_as,
                        ec,
                        etz,
                    ),
                )
                return cur.fetchone()[0]
    finally:
        c.close()


def update_show_name(show_id: int, name: str) -> bool:
    """Trim and persist show name. Returns False if name is empty or no DB."""
    n = (name or "").strip()
    if not n:
        return False
    n = n[:500]
    c = _conn()
    if not c:
        return False
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE live_shows SET name = %s, updated_at = NOW() WHERE id = %s
                    """,
                    (n, show_id),
                )
                return cur.rowcount > 0
    finally:
        c.close()


def update_show_schedule(
    show_id: int,
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    event_timezone: str,
) -> None:
    c = _conn()
    if not c:
        return
    etz = normalize_event_timezone(event_timezone)
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE live_shows
                    SET window_start = %s, window_end = %s, event_timezone = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (window_start, window_end, etz, show_id),
                )
    finally:
        c.close()


def update_show_status(show_id: int, status: str) -> None:
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                # Only one show may be "live" — otherwise inbound signup matches the
                # lowest id first and fans texting the new show's keyword never join it.
                if status == "live":
                    cur.execute(
                        """
                        UPDATE live_shows SET status = 'ended', updated_at = NOW()
                        WHERE status = 'live' AND id <> %s
                        """,
                        (show_id,),
                    )
                cur.execute(
                    """
                    UPDATE live_shows SET status = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (status, show_id),
                )
    finally:
        c.close()


def delete_show(show_id: int) -> None:
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM live_shows WHERE id = %s", (show_id,))
    finally:
        c.close()


def signups_for_show(show_id: int) -> List[Dict[str, Any]]:
    c = _conn()
    if not c:
        return []
    try:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT phone_number, channel, signed_up_at
                FROM live_show_signups
                WHERE show_id = %s
                ORDER BY signed_up_at DESC
                """,
                (show_id,),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        c.close()


def signup_phones_for_show(show_id: int) -> List[str]:
    rows = signups_for_show(show_id)
    return [r["phone_number"] for r in rows]


def add_signup(show_id: int, phone: str, channel: str, creator_slug: str = None) -> bool:
    """Insert signup (normalized E.164 without whatsapp:). Returns True if inserted.

    creator_slug: which tenant's bot received this signup. Falls back to env
    CREATOR_SLUG (or 'zarna') for backward compatibility. Without this, multi-
    tenant deployments would silently tag every new contact as 'zarna'.
    """
    norm = normalize_e164(phone)
    if not norm:
        return False
    c = _conn()
    if not c:
        return False
    slug = (creator_slug or os.getenv("CREATOR_SLUG") or "zarna").strip().lower()
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO live_show_signups (show_id, phone_number, channel)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (show_id, phone_number) DO NOTHING
                    """,
                    (show_id, norm, channel[:32]),
                )
                inserted = cur.rowcount > 0
                # Ensure the fan exists in contacts so analytics can segment
                # show-acquired fans from organic fans (source preserved if already set).
                _npa = area_code_from_phone(norm)
                cur.execute(
                    """
                    INSERT INTO contacts (phone_number, source, creator_slug, area_codes)
                    VALUES (%s, 'live_show', %s, %s)
                    ON CONFLICT (phone_number) DO UPDATE
                      SET area_codes = EXCLUDED.area_codes
                      WHERE contacts.area_codes = '{}'
                        AND EXCLUDED.area_codes <> '{}'
                    """,
                    (norm, slug, [_npa] if _npa else []),
                )
                return inserted
    finally:
        c.close()


def active_live_shows(creator_slug: Optional[str] = None) -> List[Dict[str, Any]]:
    """Shows with status = live, optionally scoped to a single tenant.

    On a single-DB-per-creator deployment this is a no-op (every row already
    belongs to the same tenant). It matters on shared-DB deployments — without
    the filter, a different tenant's live show could match an inbound keyword.
    Falls back to env CREATOR_SLUG when caller doesn't pass one.
    """
    c = _conn()
    if not c:
        return []
    slug = (creator_slug or os.getenv("CREATOR_SLUG") or "").strip().lower()
    try:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # creator_slug column may not exist on older schemas; tolerate that
            # by falling back to the unscoped query if the filter raises.
            try:
                if slug:
                    cur.execute(
                        "SELECT * FROM live_shows WHERE status = 'live' "
                        "AND (creator_slug = %s OR creator_slug IS NULL) "
                        "ORDER BY id ASC",
                        (slug,),
                    )
                else:
                    cur.execute("SELECT * FROM live_shows WHERE status = 'live' ORDER BY id ASC")
                return [dict(r) for r in cur.fetchall()]
            except _UndefinedColumn:
                c.rollback()
                cur.execute("SELECT * FROM live_shows WHERE status = 'live' ORDER BY id ASC")
                return [dict(r) for r in cur.fetchall()]
    finally:
        c.close()


def create_broadcast_job(show_id: int, body: str, provider: str) -> int:
    c = _conn()
    if not c:
        raise RuntimeError("No database")
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO live_broadcast_jobs (show_id, body, provider, status)
                    VALUES (%s, %s, %s, 'queued')
                    RETURNING id
                    """,
                    (show_id, body, provider),
                )
                return cur.fetchone()[0]
    finally:
        c.close()


def update_job_running(job_id: int, total: int) -> None:
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE live_broadcast_jobs
                    SET status = 'running', total_recipients = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (total, job_id),
                )
    finally:
        c.close()


def update_job_progress(job_id: int, sent: int, failed: int) -> None:
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE live_broadcast_jobs
                    SET sent_count = %s, failed_count = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (sent, failed, job_id),
                )
    finally:
        c.close()


def complete_job(job_id: int, sent: int, failed: int, err: Optional[str]) -> None:
    c = _conn()
    if not c:
        return
    if failed == 0:
        status = "completed"
    elif sent > 0:
        status = "partial"
    else:
        status = "failed"
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE live_broadcast_jobs
                    SET status = %s, sent_count = %s, failed_count = %s,
                        last_error = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (status, sent, failed, (err or "")[:2000], job_id),
                )
    finally:
        c.close()


def latest_job_for_show(show_id: int) -> Optional[Dict[str, Any]]:
    c = _conn()
    if not c:
        return None
    try:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM live_broadcast_jobs
                WHERE show_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (show_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        c.close()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log_audit(action: str, detail: str = "", show_id: Optional[int] = None) -> None:
    c = _conn()
    if not c:
        return
    try:
        with c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO admin_audit_log (action, detail, show_id)
                    VALUES (%s, %s, %s)
                    """,
                    (action[:120], (detail or "")[:2000], show_id),
                )
    finally:
        c.close()


def recent_audit_for_show(show_id: int, limit: int = 15) -> List[Dict[str, Any]]:
    c = _conn()
    if not c:
        return []
    try:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT created_at, action, detail
                FROM admin_audit_log
                WHERE show_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (show_id, limit),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        c.close()
