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
# Schema migrations — called from the quality digest script on each run
# ---------------------------------------------------------------------------

def ensure_smb_engagement_schema(conn) -> None:
    """
    Idempotently add engagement-tracking columns to smb_messages and create
    the smb_winning_examples table. Safe to run multiple times.
    """
    with conn:
        with conn.cursor() as cur:
            # Engagement columns on smb_messages
            for col, typedef in [
                ("did_subscriber_reply",  "BOOLEAN"),
                ("went_silent_after",     "BOOLEAN"),
                ("reply_delay_seconds",   "INTEGER"),
                ("body_length_chars",     "INTEGER"),
            ]:
                cur.execute(f"""
                    ALTER TABLE smb_messages
                    ADD COLUMN IF NOT EXISTS {col} {typedef}
                """)

            # Backfill body_length_chars where it's null
            cur.execute("""
                UPDATE smb_messages
                SET body_length_chars = LENGTH(body)
                WHERE body_length_chars IS NULL
            """)

            # Per-tenant winning examples table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS smb_winning_examples (
                    id            SERIAL PRIMARY KEY,
                    tenant_slug   TEXT        NOT NULL,
                    example_text  TEXT        NOT NULL,
                    snapshot_tag  TEXT,
                    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
                    source_msg_id INTEGER,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (tenant_slug, source_msg_id)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_smb_winning_active
                ON smb_winning_examples (tenant_slug, is_active)
            """)

            # Quality reports table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS smb_quality_reports (
                    id           SERIAL PRIMARY KEY,
                    tenant_slug  TEXT        NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    week_start   DATE        NOT NULL,
                    headline_json TEXT       NOT NULL DEFAULT '{}',
                    findings_json TEXT       NOT NULL DEFAULT '[]',
                    notion_page_id TEXT
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_smb_quality_reports_week
                ON smb_quality_reports (tenant_slug, week_start DESC)
            """)


# ---------------------------------------------------------------------------
# Engagement scoring
# ---------------------------------------------------------------------------

def score_smb_messages(conn, tenant_slug: str, silence_cutoff_hours: int = 4) -> int:
    """
    For each unscored assistant message in smb_messages:
      - Look for the next user message from the same subscriber after the bot reply.
      - If found within 24h  → did_subscriber_reply=TRUE, reply_delay_seconds=<seconds>
      - If none and >silence_cutoff_hours old → went_silent_after=TRUE

    Returns the number of rows updated.
    """
    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(hours=silence_cutoff_hours)
    updated = 0

    with conn.cursor() as cur:
        # Fetch unscored assistant messages older than silence_cutoff_hours
        cur.execute(
            """
            SELECT id, phone_number, created_at
            FROM smb_messages
            WHERE tenant_slug = %s
              AND role = 'assistant'
              AND did_subscriber_reply IS NULL
              AND went_silent_after IS NULL
              AND created_at < %s
            ORDER BY created_at
            LIMIT 2000
            """,
            (tenant_slug, cutoff),
        )
        rows = cur.fetchall()

    for msg_id, phone, sent_at in rows:
        # Look for next user message from same phone/tenant within 24h
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at
                FROM smb_messages
                WHERE tenant_slug = %s
                  AND phone_number = %s
                  AND role = 'user'
                  AND created_at > %s
                  AND created_at <= %s + INTERVAL '24 hours'
                ORDER BY created_at
                LIMIT 1
                """,
                (tenant_slug, phone, sent_at, sent_at),
            )
            reply_row = cur.fetchone()

        if reply_row:
            delay = int((reply_row[1] - sent_at).total_seconds())
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE smb_messages
                    SET did_subscriber_reply = TRUE,
                        went_silent_after    = FALSE,
                        reply_delay_seconds  = %s,
                        body_length_chars    = COALESCE(body_length_chars, LENGTH(body))
                    WHERE id = %s
                    """,
                    (max(0, delay), msg_id),
                )
                updated += cur.rowcount
        else:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE smb_messages
                    SET did_subscriber_reply = FALSE,
                        went_silent_after    = TRUE,
                        body_length_chars    = COALESCE(body_length_chars, LENGTH(body))
                    WHERE id = %s
                    """,
                    (msg_id,),
                )
                updated += cur.rowcount

    conn.commit()
    return updated


# ---------------------------------------------------------------------------
# SMB analytics queries
# ---------------------------------------------------------------------------

def fetch_smb_analytics(conn, tenant_slug: str, this_start, this_end, baseline_start) -> dict:
    """
    Fetch per-tenant engagement metrics from smb_messages.
    Returns headline stats, recent silenced replies, best performers, and opt-outs.
    """
    with conn.cursor() as cur:
        # Headline
        cur.execute(
            """
            SELECT
              COUNT(*)                                                  AS scored,
              ROUND(AVG(did_subscriber_reply::int) * 100, 1)           AS reply_rate,
              ROUND(
                100.0 * COUNT(*) FILTER (WHERE went_silent_after = TRUE)::numeric
                / NULLIF(COUNT(*), 0), 1
              )                                                         AS silence_rate,
              ROUND(AVG(body_length_chars))                            AS avg_len
            FROM smb_messages
            WHERE tenant_slug = %s
              AND role = 'assistant'
              AND did_subscriber_reply IS NOT NULL
              AND created_at >= %s AND created_at < %s
            """,
            (tenant_slug, this_start, this_end),
        )
        headline = dict(zip(
            ["scored", "reply_rate", "silence_rate", "avg_len"],
            cur.fetchone() or (0, None, None, None),
        ))

        # Baseline reply rate
        cur.execute(
            """
            SELECT ROUND(AVG(did_subscriber_reply::int) * 100, 1) AS baseline_reply_rate
            FROM smb_messages
            WHERE tenant_slug = %s
              AND role = 'assistant'
              AND did_subscriber_reply IS NOT NULL
              AND created_at >= %s AND created_at < %s
            """,
            (tenant_slug, baseline_start, this_start),
        )
        row = cur.fetchone()
        headline["baseline_reply_rate"] = row[0] if row else None

        # Top silenced replies
        cur.execute(
            """
            SELECT
              LEFT(body, 220)        AS preview,
              body_length_chars      AS chars,
              created_at
            FROM smb_messages
            WHERE tenant_slug = %s
              AND role = 'assistant'
              AND went_silent_after = TRUE
              AND created_at >= %s AND created_at < %s
            ORDER BY created_at DESC
            LIMIT 8
            """,
            (tenant_slug, this_start, this_end),
        )
        cols = ["preview", "chars", "created_at"]
        silenced = [dict(zip(cols, r)) for r in cur.fetchall()]

        # Best performers (fast replies)
        cur.execute(
            """
            SELECT
              LEFT(body, 180)         AS preview,
              reply_delay_seconds     AS reply_s,
              body_length_chars       AS chars
            FROM smb_messages
            WHERE tenant_slug = %s
              AND role = 'assistant'
              AND did_subscriber_reply = TRUE
              AND reply_delay_seconds IS NOT NULL
              AND reply_delay_seconds BETWEEN 5 AND 600
              AND created_at >= %s AND created_at < %s
            ORDER BY reply_delay_seconds
            LIMIT 5
            """,
            (tenant_slug, this_start, this_end),
        )
        cols = ["preview", "reply_s", "chars"]
        winners = [dict(zip(cols, r)) for r in cur.fetchall()]

        # Recent opt-outs (stopped subscribers with their last conversation)
        cur.execute(
            """
            SELECT phone_number, updated_at
            FROM smb_subscribers
            WHERE tenant_slug = %s
              AND status = 'stopped'
              AND updated_at >= %s
            ORDER BY updated_at DESC
            LIMIT 5
            """,
            (tenant_slug, this_start),
        )
        opt_outs_raw = cur.fetchall()
        opt_outs = []
        for phone, stopped_at in opt_outs_raw:
            # Fetch last 3 messages before they stopped
            cur.execute(
                """
                SELECT role, LEFT(body, 120) AS body
                FROM smb_messages
                WHERE tenant_slug = %s AND phone_number = %s
                ORDER BY created_at DESC
                LIMIT 3
                """,
                (tenant_slug, phone),
            )
            last_msgs = [{"role": r[0], "body": r[1]} for r in cur.fetchall()]
            opt_outs.append({
                "phone_suffix": phone[-4:] if phone else "?",
                "stopped_at": stopped_at.isoformat() if stopped_at else None,
                "last_messages": list(reversed(last_msgs)),
            })

    return dict(
        headline=headline,
        silenced=silenced,
        winners=winners,
        opt_outs=opt_outs,
    )


# ---------------------------------------------------------------------------
# Winning examples — corpus for few-shot injection
# ---------------------------------------------------------------------------

_AUTO_SNAPSHOTS_TO_KEEP = 3
_MAX_REPLY_SECONDS = 300


def fetch_smb_winners(conn, tenant_slug: str, this_start, this_end) -> list:
    """
    Return top-performing bot replies from smb_messages that are candidates
    for the winning examples corpus.

    Criteria: fan replied within _MAX_REPLY_SECONDS, bot reply 30-200 chars,
    not already in smb_winning_examples.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    id                          AS source_msg_id,
                    body                        AS text,
                    reply_delay_seconds,
                    body_length_chars,
                    RANK() OVER (
                        ORDER BY reply_delay_seconds ASC
                    )                           AS rk
                FROM smb_messages
                WHERE tenant_slug           = %s
                  AND role                  = 'assistant'
                  AND did_subscriber_reply  = TRUE
                  AND went_silent_after     IS DISTINCT FROM TRUE
                  AND reply_delay_seconds   BETWEEN 5 AND %s
                  AND body_length_chars     BETWEEN 30 AND 200
                  AND created_at            >= %s
                  AND created_at            <  %s
                  AND id NOT IN (
                      SELECT source_msg_id
                      FROM smb_winning_examples
                      WHERE tenant_slug = %s
                        AND source_msg_id IS NOT NULL
                  )
            )
            SELECT source_msg_id, text, reply_delay_seconds, body_length_chars
            FROM ranked
            WHERE rk <= 5
            ORDER BY rk
            """,
            (tenant_slug, _MAX_REPLY_SECONDS, this_start, this_end, tenant_slug),
        )
        rows = cur.fetchall()
    return [
        {"source_msg_id": r[0], "text": r[1],
         "reply_delay_seconds": r[2], "body_length_chars": r[3]}
        for r in rows
    ]


def save_smb_winners(conn, tenant_slug: str, week_start, winners: list, dry_run: bool = False) -> int:
    """
    Insert winners into smb_winning_examples under a dated snapshot tag,
    then deactivate old auto-snapshot batches beyond the rolling window.
    Returns the number of rows inserted.
    """
    if not winners:
        return 0

    snapshot_tag = f"auto-{week_start}"
    if dry_run:
        for w in winners:
            logger.info(
                "[DRY RUN] SMB winner: tenant=%s delay=%ss  \"%s\"",
                tenant_slug, w["reply_delay_seconds"], w["text"][:80],
            )
        return 0

    inserted = 0
    with conn:
        with conn.cursor() as cur:
            for w in winners:
                cur.execute(
                    """
                    INSERT INTO smb_winning_examples
                        (tenant_slug, example_text, snapshot_tag, is_active, source_msg_id)
                    VALUES (%s, %s, %s, TRUE, %s)
                    ON CONFLICT (tenant_slug, source_msg_id) DO NOTHING
                    """,
                    (tenant_slug, w["text"], snapshot_tag, w["source_msg_id"]),
                )
                inserted += cur.rowcount

            # Deactivate old auto-snapshots beyond rolling window
            cur.execute(
                """
                UPDATE smb_winning_examples
                SET is_active = FALSE
                WHERE tenant_slug = %s
                  AND snapshot_tag LIKE 'auto-%%'
                  AND snapshot_tag NOT IN (
                      SELECT DISTINCT snapshot_tag
                      FROM smb_winning_examples
                      WHERE tenant_slug = %s
                        AND snapshot_tag LIKE 'auto-%%'
                      ORDER BY snapshot_tag DESC
                      LIMIT %s
                  )
                """,
                (tenant_slug, tenant_slug, _AUTO_SNAPSHOTS_TO_KEEP),
            )

    logger.info("SMB winners: inserted=%d tenant=%s snapshot=%s", inserted, tenant_slug, snapshot_tag)
    return inserted


def load_winning_examples(conn, tenant_slug: str, limit: int = 4) -> list[str]:
    """
    Return active winning example texts for a tenant.
    Used by brain.py to inject few-shot examples into the conversational prompt.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT example_text
            FROM smb_winning_examples
            WHERE tenant_slug = %s AND is_active = TRUE
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (tenant_slug, limit),
        )
        return [r[0] for r in cur.fetchall()]


def save_smb_quality_report(conn, tenant_slug: str, week_start, headline: dict, findings: dict) -> None:
    """Save the quality digest report to smb_quality_reports."""
    import json
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO smb_quality_reports (tenant_slug, week_start, headline_json, findings_json)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    tenant_slug,
                    week_start,
                    json.dumps(headline, default=str),
                    json.dumps(findings, default=str),
                ),
            )


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


# ---------------------------------------------------------------------------
# Outreach invites (free-ticket / timed offer tracking)
# ---------------------------------------------------------------------------

def record_outreach_invite(
    conn,
    tenant_slug: str,
    phone_number: str,
    offer: str = "free_ticket",
    batch_name: str | None = None,
) -> None:
    """
    Record that an outbound invite was sent to this number.

    - If no prior unclaimed record exists: insert fresh.
    - If an unclaimed record exists: reset sent_at so the 24h clock restarts.
    - If the most recent record is already claimed: insert a new row so the
      offer can be re-extended (e.g. a different campaign batch).

    Note: the DB-level unique constraint on (tenant_slug, phone_number) was
    dropped in a later migration to support multi-campaign outreach logging.
    Dedup is now handled here at the application level.
    """
    with conn.cursor() as cur:
        # Check for an existing unclaimed invite for this number
        cur.execute(
            """SELECT id FROM smb_outreach_invites
               WHERE tenant_slug=%s AND phone_number=%s AND claimed_at IS NULL
               ORDER BY sent_at DESC LIMIT 1""",
            (tenant_slug, phone_number),
        )
        existing = cur.fetchone()
        if existing:
            # Refresh the clock on the unclaimed invite (same behaviour as before)
            cur.execute(
                """UPDATE smb_outreach_invites
                   SET offer=%s, sent_at=NOW(),
                       batch_name=COALESCE(%s, batch_name)
                   WHERE id=%s""",
                (offer, batch_name, existing[0]),
            )
        else:
            # Insert fresh — either first invite ever or re-invite after claiming
            cur.execute(
                """INSERT INTO smb_outreach_invites
                       (tenant_slug, phone_number, offer, sent_at, claimed_at, batch_name)
                   VALUES (%s, %s, %s, NOW(), NULL, %s)""",
                (tenant_slug, phone_number, offer, batch_name),
            )


def get_active_invite(conn, tenant_slug: str, phone_number: str, window_hours: int = 24) -> dict | None:
    """
    Return the invite row if one exists, was sent within window_hours, and hasn't been claimed.
    Returns None otherwise.
    """
    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, offer, sent_at
            FROM smb_outreach_invites
            WHERE tenant_slug = %s
              AND phone_number = %s
              AND sent_at > %s
              AND claimed_at IS NULL
            """,
            (tenant_slug, phone_number, cutoff),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {"id": row[0], "offer": row[1], "sent_at": row[2]}


def claim_invite(conn, invite_id: int, tenant_slug: str) -> int:
    """
    Mark an invite as claimed, assign the next sequential ticket number for this
    tenant (starting at 100), and return that number.

    The ticket number is assigned atomically in a single UPDATE so concurrent
    claims cannot collide.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE smb_outreach_invites
            SET claimed_at    = NOW(),
                ticket_number = (
                    SELECT COALESCE(MAX(ticket_number), 99) + 1
                    FROM smb_outreach_invites
                    WHERE tenant_slug = %s
                )
            WHERE id = %s
            RETURNING ticket_number
            """,
            (tenant_slug, invite_id),
        )
        row = cur.fetchone()
        return row[0] if row else 100
