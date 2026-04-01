"""
Safe aggregate queries — never returns raw phone numbers.
All phone numbers are masked or only counted.
"""

from __future__ import annotations
from collections import Counter
from .db import get_conn
import psycopg2.extras


# ── Stats ──────────────────────────────────────────────────────────────────

def get_overview_stats() -> dict:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
            total_subscribers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user'")
            total_messages = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '24 hours'")
            messages_today = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '7 days'")
            messages_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE role='user' AND created_at >= NOW()-INTERVAL '14 days'
                  AND created_at < NOW()-INTERVAL '7 days'
            """)
            messages_prev_week = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE created_at >= NOW()-INTERVAL '7 days'")
            new_subs_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(DISTINCT phone_number) FROM contacts
                WHERE created_at >= NOW()-INTERVAL '14 days'
                  AND created_at < NOW()-INTERVAL '7 days'
            """)
            new_subs_prev_week = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '1 hour'")
            messages_last_hour = cur.fetchone()[0]

            cur.execute("""
                SELECT DATE(created_at AT TIME ZONE 'America/New_York') as day, COUNT(*) as cnt
                FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '30 days'
                GROUP BY day ORDER BY day
            """)
            messages_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]

            cur.execute("""
                SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int as hr,
                       COUNT(*) as cnt
                FROM messages WHERE role='user' AND created_at >= NOW()-INTERVAL '30 days'
                GROUP BY hr ORDER BY hr
            """)
            hour_map = {r["hr"]: r["cnt"] for r in cur.fetchall()}
            messages_by_hour = [hour_map.get(h, 0) for h in range(24)]

            cur.execute("""
                SELECT UNNEST(fan_tags) as tag, COUNT(*) as cnt
                FROM contacts WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                GROUP BY tag ORDER BY cnt DESC LIMIT 20
            """)
            tag_breakdown = [(r["tag"], r["cnt"]) for r in cur.fetchall()]

            cur.execute("SELECT phone_number FROM contacts")
            all_phones = [r[0] for r in cur.fetchall()]
            area_codes = Counter()
            for p in all_phones:
                digits = "".join(c for c in p if c.isdigit())
                if len(digits) == 11 and digits[0] == "1":
                    area_codes[digits[1:4]] += 1
                elif len(digits) == 10:
                    area_codes[digits[:3]] += 1
            top_area_codes = area_codes.most_common(10)

            cur.execute("SELECT COUNT(*) FROM contacts WHERE fan_memory IS NOT NULL AND fan_memory != ''")
            profiled_fans = cur.fetchone()[0]

        return {
            "total_subscribers": total_subscribers,
            "total_messages": total_messages,
            "messages_today": messages_today,
            "messages_week": messages_week,
            "messages_prev_week": messages_prev_week,
            "new_subs_week": new_subs_week,
            "new_subs_prev_week": new_subs_prev_week,
            "messages_last_hour": messages_last_hour,
            "messages_by_day": messages_by_day,
            "messages_by_hour": messages_by_hour,
            "tag_breakdown": tag_breakdown,
            "top_area_codes": top_area_codes,
            "profiled_fans": profiled_fans,
        }
    finally:
        conn.close()


# ── Audience count (no PII) ────────────────────────────────────────────────

def count_audience(audience_type: str, audience_filter: str, sample_pct: int = 100) -> int:
    """Count how many subscribers match the given filters — no phones returned."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if audience_type == "tag" and audience_filter:
                cur.execute(
                    "SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE %s = ANY(fan_tags)",
                    (audience_filter.lower(),),
                )
            elif audience_type == "location" and audience_filter:
                cur.execute(
                    "SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE LOWER(fan_location) LIKE %s",
                    (f"%{audience_filter.lower()}%",),
                )
            else:
                cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
            total = cur.fetchone()[0]

        if audience_type == "random" and 0 < sample_pct < 100:
            return max(1, round(total * sample_pct / 100))
        return total
    finally:
        conn.close()


def get_audience_phones(audience_type: str, audience_filter: str, sample_pct: int = 100) -> list[str]:
    """Fetch phone numbers for blast — internal use only, never sent to frontend."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            optout_set = _get_optouts(cur)

            if audience_type == "tag" and audience_filter:
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts WHERE %s = ANY(fan_tags)",
                    (audience_filter.lower(),),
                )
            elif audience_type == "location" and audience_filter:
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts WHERE LOWER(fan_location) LIKE %s",
                    (f"%{audience_filter.lower()}%",),
                )
            else:
                cur.execute("SELECT DISTINCT phone_number FROM contacts")

            phones = [r[0] for r in cur.fetchall() if r[0] not in optout_set]

        if audience_type == "random" and 0 < sample_pct < 100:
            import random
            k = max(1, round(len(phones) * sample_pct / 100))
            return random.sample(phones, min(k, len(phones)))
        return phones
    finally:
        conn.close()


def _get_optouts(cur) -> set:
    cur.execute("SELECT phone_number FROM broadcast_optouts")
    return {r[0] for r in cur.fetchall()}


# ── Audience tags list ──────────────────────────────────────────────────────

def get_all_tags() -> list[str]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT UNNEST(fan_tags) as tag FROM contacts
                WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                ORDER BY tag
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


# ── Blast drafts ───────────────────────────────────────────────────────────

def list_blast_drafts() -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, name, body, channel, audience_type, audience_filter,
                       audience_sample_pct, status, scheduled_at, sent_at,
                       sent_count, failed_count, total_recipients, created_by, created_at
                FROM blast_drafts ORDER BY updated_at DESC LIMIT 100
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_blast_draft(draft_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM blast_drafts WHERE id = %s", (draft_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def save_blast_draft(*, name: str, body: str, channel: str, audience_type: str,
                     audience_filter: str, sample_pct: int, created_by: str,
                     draft_id: int | None = None) -> int:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if draft_id:
                    cur.execute("""
                        UPDATE blast_drafts
                        SET name=%s, body=%s, channel=%s, audience_type=%s,
                            audience_filter=%s, audience_sample_pct=%s,
                            status='draft', updated_at=NOW()
                        WHERE id=%s
                    """, (name, body, channel, audience_type, audience_filter, sample_pct, draft_id))
                    return draft_id
                else:
                    cur.execute("""
                        INSERT INTO blast_drafts
                          (name, body, channel, audience_type, audience_filter,
                           audience_sample_pct, status, created_by)
                        VALUES (%s,%s,%s,%s,%s,%s,'draft',%s) RETURNING id
                    """, (name, body, channel, audience_type, audience_filter, sample_pct, created_by))
                    return cur.fetchone()[0]
    finally:
        conn.close()


def schedule_blast(draft_id: int, send_at) -> None:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE blast_drafts
                    SET status='scheduled', scheduled_at=%s, updated_at=NOW()
                    WHERE id=%s
                """, (send_at, draft_id))
    finally:
        conn.close()


def mark_blast_sent(draft_id: int, sent: int, failed: int, total: int) -> None:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE blast_drafts
                    SET status='sent', sent_at=NOW(), sent_count=%s,
                        failed_count=%s, total_recipients=%s, updated_at=NOW()
                    WHERE id=%s
                """, (sent, failed, total, draft_id))
    finally:
        conn.close()


def mark_blast_cancelled(draft_id: int) -> None:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET status='cancelled', updated_at=NOW() WHERE id=%s",
                    (draft_id,),
                )
    finally:
        conn.close()


def get_pending_scheduled_blasts():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT * FROM blast_drafts
                WHERE status='scheduled' AND scheduled_at <= NOW()
                ORDER BY scheduled_at ASC
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Live shows (read-only, PII-free) ───────────────────────────────────────

def list_shows() -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT ls.id, ls.name, ls.status, ls.keyword, ls.deliver_as,
                       ls.event_category, ls.window_start, ls.window_end,
                       ls.event_timezone, ls.created_at,
                       COUNT(lss.id) AS signup_count
                FROM live_shows ls
                LEFT JOIN live_show_signups lss ON lss.show_id = ls.id
                GROUP BY ls.id ORDER BY ls.created_at DESC
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def get_show(show_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT ls.*, COUNT(lss.id) AS signup_count
                FROM live_shows ls
                LEFT JOIN live_show_signups lss ON lss.show_id = ls.id
                WHERE ls.id = %s GROUP BY ls.id
            """, (show_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception:
        return None
    finally:
        conn.close()
