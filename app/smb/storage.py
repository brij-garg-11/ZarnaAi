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
