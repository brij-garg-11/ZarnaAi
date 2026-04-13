"""
SMB database operations.

All functions accept an open psycopg2 connection. The caller is
responsible for acquiring/releasing the connection and managing the
transaction boundary (commit or rollback).

Use get_db_connection() from app.admin_auth to acquire a connection.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subscribers
# ---------------------------------------------------------------------------

def get_subscriber(conn, phone_number: str, tenant_slug: str) -> Optional[dict]:
    """Return subscriber row as dict, or None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, phone_number, tenant_slug, status, onboarding_step, created_at
            FROM smb_subscribers
            WHERE phone_number = %s AND tenant_slug = %s
            """,
            (phone_number, tenant_slug),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "phone_number": row[1],
        "tenant_slug": row[2],
        "status": row[3],
        "onboarding_step": row[4],
        "created_at": row[5],
    }


def create_subscriber(conn, phone_number: str, tenant_slug: str) -> dict:
    """
    Insert a new subscriber in 'onboarding' status at step 0.
    If already exists (race condition), returns the existing row unchanged.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO smb_subscribers (phone_number, tenant_slug, status, onboarding_step)
            VALUES (%s, %s, 'active', 0)
            ON CONFLICT (phone_number, tenant_slug) DO NOTHING
            """,
            (phone_number, tenant_slug),
        )
    return get_subscriber(conn, phone_number, tenant_slug)


def advance_onboarding(conn, subscriber_id: int, new_step: int, new_status: str) -> None:
    """Move a subscriber to the next onboarding step or mark them active."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE smb_subscribers
            SET onboarding_step = %s, status = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (new_step, new_status, subscriber_id),
        )


def get_active_subscribers(conn, tenant_slug: str) -> list:
    """Return all active subscribers for a tenant (includes step=0 preference-pending)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, phone_number
            FROM smb_subscribers
            WHERE tenant_slug = %s AND status = 'active'
            ORDER BY created_at
            """,
            (tenant_slug,),
        )
        return [{"id": r[0], "phone_number": r[1]} for r in cur.fetchall()]


def get_subscribers_by_preference(
    conn, tenant_slug: str, question_key: str, answer: str
) -> list:
    """
    Return active subscribers who gave a specific answer to a preference question.
    Used by blast.py to target the right audience.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.phone_number
            FROM smb_subscribers s
            JOIN smb_preferences p ON p.subscriber_id = s.id
            WHERE s.tenant_slug = %s
              AND s.status = 'active'
              AND p.question_key = %s
              AND LOWER(p.answer) = LOWER(%s)
            ORDER BY s.created_at
            """,
            (tenant_slug, question_key, answer),
        )
        return [{"id": r[0], "phone_number": r[1]} for r in cur.fetchall()]


def get_subscribers_by_segment(
    conn, tenant_slug: str, question_key: str, answers: list
) -> list:
    """
    Return active subscribers who gave ANY of the provided answers to a question.

    Used for segmented blasts — e.g. a STANDUP blast targets subscribers who
    answered "STANDUP" OR "BOTH" (since BOTH means they like everything).
    Deduplicates automatically via DISTINCT.
    """
    if not answers:
        return []
    lower_answers = [a.strip().lower() for a in answers]
    placeholders = ",".join(["%s"] * len(lower_answers))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT s.id, s.phone_number
            FROM smb_subscribers s
            JOIN smb_preferences p ON p.subscriber_id = s.id
            WHERE s.tenant_slug = %s
              AND s.status = 'active'
              AND p.question_key = %s
              AND LOWER(p.answer) IN ({placeholders})
            ORDER BY s.id
            """,
            (tenant_slug, question_key, *lower_answers),
        )
        return [{"id": r[0], "phone_number": r[1]} for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Pending blast confirmations (DB-backed so all gunicorn workers share state)
# ---------------------------------------------------------------------------

_PENDING_TTL_SECONDS = 600  # 10 minutes


def set_pending_blast(conn, owner_phone: str, tenant_slug: str, message_text: str) -> None:
    """Upsert a pending blast awaiting audience confirmation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO smb_pending_blasts (owner_phone, tenant_slug, message_text, created_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (owner_phone)
            DO UPDATE SET tenant_slug = EXCLUDED.tenant_slug,
                          message_text = EXCLUDED.message_text,
                          created_at = NOW()
            """,
            (owner_phone, tenant_slug, message_text),
        )


def get_pending_blast(conn, owner_phone: str) -> Optional[dict]:
    """Return the pending blast for this owner if it exists and hasn't expired."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tenant_slug, message_text, created_at
            FROM smb_pending_blasts
            WHERE owner_phone = %s
              AND created_at > NOW() - INTERVAL '%s seconds'
            """,
            (owner_phone, _PENDING_TTL_SECONDS),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"tenant_slug": row[0], "message_text": row[1], "created_at": row[2]}


def clear_pending_blast(conn, owner_phone: str) -> None:
    """Delete the pending blast for this owner."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM smb_pending_blasts WHERE owner_phone = %s",
            (owner_phone,),
        )


# ---------------------------------------------------------------------------
# Preferences
# ---------------------------------------------------------------------------

def save_preference(conn, subscriber_id: int, question_key: str, answer: str) -> None:
    """Upsert a subscriber's answer to a preference question."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO smb_preferences (subscriber_id, question_key, answer)
            VALUES (%s, %s, %s)
            ON CONFLICT (subscriber_id, question_key)
            DO UPDATE SET answer = EXCLUDED.answer, answered_at = NOW()
            """,
            (subscriber_id, question_key, answer),
        )


def get_preferences(conn, subscriber_id: int) -> dict:
    """Return all preference answers for a subscriber as {question_key: answer}."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT question_key, answer FROM smb_preferences WHERE subscriber_id = %s",
            (subscriber_id,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Shows and check-ins
# ---------------------------------------------------------------------------

def create_show(
    conn,
    tenant_slug: str,
    name: str,
    show_date: str,
    checkin_keyword: str,
) -> Optional[dict]:
    """
    Create a new show with a check-in keyword fans text at the door.
    Returns the created show, or None if the keyword is already taken.
    """
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO smb_shows (tenant_slug, name, show_date, checkin_keyword)
                VALUES (%s, %s, %s, %s)
                RETURNING id, tenant_slug, name, show_date, checkin_keyword, status, created_at
                """,
                (tenant_slug, name, show_date, checkin_keyword.upper().strip()),
            )
            row = cur.fetchone()
        except Exception:
            return None
    if not row:
        return None
    return {
        "id": row[0], "tenant_slug": row[1], "name": row[2],
        "show_date": row[3], "checkin_keyword": row[4],
        "status": row[5], "created_at": row[6],
    }


def list_shows(conn, tenant_slug: str, limit: int = 30) -> list:
    """Return the most recent shows for a tenant, newest first."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.name, s.show_date, s.checkin_keyword, s.status, s.created_at,
                   COUNT(c.id) AS checkin_count
            FROM smb_shows s
            LEFT JOIN smb_show_checkins c ON c.show_id = s.id
            WHERE s.tenant_slug = %s
            GROUP BY s.id
            ORDER BY s.show_date DESC, s.created_at DESC
            LIMIT %s
            """,
            (tenant_slug, limit),
        )
        return [
            {
                "id": r[0], "name": r[1], "show_date": r[2],
                "checkin_keyword": r[3], "status": r[4],
                "created_at": r[5], "checkin_count": r[6],
            }
            for r in cur.fetchall()
        ]


def get_show_by_keyword(conn, tenant_slug: str, keyword: str) -> Optional[dict]:
    """Return a show row matching the given keyword (case-insensitive), or None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, show_date, checkin_keyword, status, created_at
            FROM smb_shows
            WHERE tenant_slug = %s AND UPPER(checkin_keyword) = UPPER(%s) AND status = 'active'
            """,
            (tenant_slug, keyword.strip()),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "name": row[1], "show_date": row[2],
        "checkin_keyword": row[3], "status": row[4], "created_at": row[5],
    }


def get_show_by_id(conn, show_id: int) -> Optional[dict]:
    """Return a show row by primary key."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, tenant_slug, name, show_date, checkin_keyword, status, created_at
            FROM smb_shows WHERE id = %s
            """,
            (show_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "tenant_slug": row[1], "name": row[2],
        "show_date": row[3], "checkin_keyword": row[4],
        "status": row[5], "created_at": row[6],
    }


def record_checkin(conn, show_id: int, phone_number: str, tenant_slug: str) -> bool:
    """
    Record a fan checking in to a show. Returns True if this is a new check-in,
    False if they already checked in (idempotent — no error, no duplicate row).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO smb_show_checkins (show_id, phone_number, tenant_slug)
            VALUES (%s, %s, %s)
            ON CONFLICT (show_id, phone_number) DO NOTHING
            """,
            (show_id, phone_number, tenant_slug),
        )
        return cur.rowcount > 0


def get_show_attendees(conn, show_id: int) -> list:
    """Return all phone numbers that checked in to a show."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT phone_number, checked_in_at
            FROM smb_show_checkins
            WHERE show_id = %s
            ORDER BY checked_in_at
            """,
            (show_id,),
        )
        return [{"phone_number": r[0], "checked_in_at": r[1]} for r in cur.fetchall()]


def get_recent_shows_for_blast(conn, tenant_slug: str, limit: int = 10) -> list:
    """Return recent active shows with attendee counts — used to populate blast AI prompt."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.name, s.show_date, s.checkin_keyword, COUNT(c.id) AS checkin_count
            FROM smb_shows s
            LEFT JOIN smb_show_checkins c ON c.show_id = s.id
            WHERE s.tenant_slug = %s AND s.status = 'active'
            GROUP BY s.id
            ORDER BY s.show_date DESC
            LIMIT %s
            """,
            (tenant_slug, limit),
        )
        return [
            {
                "id": r[0], "name": r[1], "show_date": r[2],
                "checkin_keyword": r[3], "checkin_count": r[4],
            }
            for r in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Conversation history
# ---------------------------------------------------------------------------

def save_message(conn, tenant_slug: str, phone_number: str, role: str, body: str) -> None:
    """Persist one turn of a subscriber conversation (role = 'user' or 'assistant')."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO smb_messages (tenant_slug, phone_number, role, body)
            VALUES (%s, %s, %s, %s)
            """,
            (tenant_slug, phone_number, role, body),
        )


def get_history(conn, tenant_slug: str, phone_number: str, limit: int = 8) -> list:
    """
    Return the last `limit` messages for this subscriber, oldest-first,
    as a list of {"role": ..., "body": ...} dicts.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT role, body FROM (
                SELECT role, body, created_at
                FROM smb_messages
                WHERE tenant_slug = %s AND phone_number = %s
                ORDER BY created_at DESC
                LIMIT %s
            ) sub
            ORDER BY created_at ASC
            """,
            (tenant_slug, phone_number, limit),
        )
        return [{"role": row[0], "body": row[1]} for row in cur.fetchall()]
