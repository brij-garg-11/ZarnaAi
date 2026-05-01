"""
Business (SMB) blast helpers: tier classification, Smart Send cadence,
audience resolution, AI cleanup and the actual Twilio send loop.

Mirrors the performer Smart Send behaviour (see operator/app/routes/api.py
api_smart_send_preview) but reads from the smb_* tables and uses
smb_blast_recipients to compute per-fan suppression windows.

Tiers are computed on-the-fly from smb_messages activity rather than stored
on smb_subscribers — this keeps the migration footprint small and lets the
classifier evolve without backfills.

  regular  — 5+ inbound messages in the last 60 days (your repeat customers)
  engaged  — replied at least once in the last 30 days, but not regular
  new      — joined in the last 14 days, not already engaged/regular
  lapsed   — opted in but no reply in 60+ days

Cadence is intentionally generous for "lapsed" so re-engagement blasts don't
torch goodwill.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

# Tier order matters: list it from most-engaged to least-engaged so the UI
# renders cards in the right sequence and the classifier picks the highest
# tier a fan qualifies for.
TIER_ORDER: tuple[str, ...] = ("regular", "engaged", "new", "lapsed")

TIER_LABELS: dict[str, str] = {
    "regular": "Regular ⭐",
    "engaged": "Engaged ✅",
    "new":     "New 👋",
    "lapsed":  "Lapsed 💤",
}

TIER_DESCRIPTIONS: dict[str, str] = {
    "regular": "5+ inbound messages in the last 60 days",
    "engaged": "Replied at least once in the last 30 days",
    "new":     "Joined in the last 14 days",
    "lapsed":  "No activity in 60+ days",
}

# Smart Send cadence: skip a fan if they got a blast within this many days.
# Tighter for engaged customers (they tolerate frequency), generous for
# lapsed/new so we don't over-message people who haven't bought in.
CADENCE_DAYS: dict[str, int] = {
    "regular": 5,
    "engaged": 7,
    "new":     14,
    "lapsed":  30,
}


# ---------------------------------------------------------------------------
# Tier classification
# ---------------------------------------------------------------------------

def _tier_predicates() -> dict[str, str]:
    """
    Returns SQL boolean expressions (one per tier) that classify a row from
    the activity CTE below into exactly one tier. The CTE supplies columns
    `inbound_60d`, `inbound_30d`, and `created_at`.
    """
    return {
        "regular": "inbound_60d >= 5",
        "engaged": "inbound_60d < 5 AND inbound_30d >= 1",
        "new":     "inbound_30d = 0 AND created_at > NOW() - INTERVAL '14 days'",
        "lapsed":  "inbound_30d = 0 AND created_at <= NOW() - INTERVAL '14 days' "
                   "AND (last_inbound_at IS NULL OR last_inbound_at < NOW() - INTERVAL '60 days')",
    }


def _tier_case_sql() -> str:
    """SQL CASE expression returning the tier name as a string column."""
    parts = []
    for tier, predicate in _tier_predicates().items():
        parts.append(f"WHEN {predicate} THEN '{tier}'")
    return "CASE " + " ".join(parts) + " ELSE NULL END"


def _activity_cte(slug_param: str = "%s") -> str:
    """
    CTE that surfaces, for every active subscriber of a tenant, their
    inbound message counts and last inbound timestamp. Used as the basis for
    tier classification and audience selection.
    """
    return f"""
    WITH subs AS (
      SELECT id, phone_number, created_at
      FROM smb_subscribers
      WHERE tenant_slug = {slug_param} AND status = 'active'
    ),
    activity AS (
      SELECT
        s.id,
        s.phone_number,
        s.created_at,
        COUNT(m.*) FILTER (
          WHERE m.role = 'user'
            AND m.created_at >= NOW() - INTERVAL '60 days'
        ) AS inbound_60d,
        COUNT(m.*) FILTER (
          WHERE m.role = 'user'
            AND m.created_at >= NOW() - INTERVAL '30 days'
        ) AS inbound_30d,
        MAX(m.created_at) FILTER (WHERE m.role = 'user') AS last_inbound_at
      FROM subs s
      LEFT JOIN smb_messages m
        ON m.tenant_slug = {slug_param}
       AND m.phone_number = s.phone_number
      GROUP BY s.id, s.phone_number, s.created_at
    )
    """


def compute_tier_counts(slug: str, conn) -> list[dict]:
    """
    Returns per-tier subscriber counts for a tenant. Always returns one row
    per tier in TIER_ORDER, with zero counts for tiers nobody qualifies for.
    """
    counts: dict[str, int] = {tier: 0 for tier in TIER_ORDER}
    sql = (
        _activity_cte() +
        f"""
        SELECT {_tier_case_sql()} AS tier, COUNT(*) AS cnt
        FROM activity
        GROUP BY tier
        """
    )
    with conn.cursor() as cur:
        cur.execute(sql, (slug, slug))
        for row in cur.fetchall():
            tier = row[0]
            if tier in counts:
                counts[tier] = int(row[1] or 0)

    return [
        {
            "tier": tier,
            "label": TIER_LABELS[tier],
            "description": TIER_DESCRIPTIONS[tier],
            "count": counts[tier],
            "cadence_days": CADENCE_DAYS[tier],
        }
        for tier in TIER_ORDER
    ]


def compute_smart_send_preview(slug: str, conn) -> dict:
    """
    For each tier, return how many fans would actually be sent to vs
    suppressed by the cadence rule.

    Result shape mirrors the performer api_smart_send_preview so the React
    component can be reused with minimal branching.
    """
    tiers: dict[str, dict] = {}
    total_sending = 0
    total_suppressed = 0

    sql = (
        _activity_cte() +
        f"""
        , classified AS (
          SELECT phone_number, {_tier_case_sql()} AS tier
          FROM activity
        )
        SELECT
          c.tier,
          COUNT(*) AS total,
          COUNT(*) FILTER (
            WHERE NOT EXISTS (
              SELECT 1 FROM smb_blast_recipients r
              WHERE r.tenant_slug = %s
                AND r.phone_number = c.phone_number
                AND r.sent_at >= NOW() - (
                  CASE c.tier
                    WHEN 'regular' THEN INTERVAL '5 days'
                    WHEN 'engaged' THEN INTERVAL '7 days'
                    WHEN 'new'     THEN INTERVAL '14 days'
                    WHEN 'lapsed'  THEN INTERVAL '30 days'
                    ELSE INTERVAL '0 days'
                  END
                )
            )
          ) AS sending
        FROM classified c
        WHERE c.tier IS NOT NULL
        GROUP BY c.tier
        """
    )

    with conn.cursor() as cur:
        cur.execute(sql, (slug, slug, slug))
        rows = cur.fetchall()

    seen = {row[0] for row in rows}
    for row in rows:
        tier, total, sending = row[0], int(row[1] or 0), int(row[2] or 0)
        suppressed = max(total - sending, 0)
        tiers[tier] = {
            "total": total,
            "sending": sending,
            "suppressed": suppressed,
            "cadence_days": CADENCE_DAYS.get(tier, 0),
        }
        total_sending += sending
        total_suppressed += suppressed

    for tier in TIER_ORDER:
        if tier not in seen:
            tiers[tier] = {
                "total": 0,
                "sending": 0,
                "suppressed": 0,
                "cadence_days": CADENCE_DAYS[tier],
            }

    return {
        "tiers": tiers,
        "total_sending": total_sending,
        "total_suppressed": total_suppressed,
    }


# ---------------------------------------------------------------------------
# Audience resolution
# ---------------------------------------------------------------------------

class UnknownAudience(ValueError):
    """Raised when an audience string can't be resolved to a phone list."""


def resolve_audience(
    slug: str,
    audience: str,
    conn,
    segments_def: list[dict],
) -> list[str]:
    """
    Resolve an audience string into a deduped list of E.164 phone numbers.

    Supported audience formats:
      "all"                           → every active subscriber
      "tier:regular|engaged|new|lapsed"
      "smart-send"                    → every active subscriber, minus those
                                        suppressed by their tier's cadence
      "segment:<NAME>"                → matches a segment defined in the
                                        tenant's business_configs json
      "customer_of_the_week"          → every phone ever featured as
                                        Customer of the Week
    """
    aud = audience.strip().lower()

    with conn.cursor() as cur:
        if aud == "all":
            cur.execute(
                "SELECT phone_number FROM smb_subscribers "
                "WHERE tenant_slug=%s AND status='active'",
                (slug,),
            )
            return [r[0] for r in cur.fetchall()]

        if aud.startswith("tier:"):
            tier = aud[5:].strip()
            if tier not in TIER_ORDER:
                raise UnknownAudience(f"Unknown tier: {tier}")
            sql = (
                _activity_cte() +
                f"""
                SELECT phone_number FROM activity
                WHERE {_tier_predicates()[tier]}
                """
            )
            cur.execute(sql, (slug, slug))
            return [r[0] for r in cur.fetchall()]

        if aud == "smart-send":
            sql = (
                _activity_cte() +
                f"""
                , classified AS (
                  SELECT phone_number, {_tier_case_sql()} AS tier
                  FROM activity
                )
                SELECT phone_number
                FROM classified c
                WHERE c.tier IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM smb_blast_recipients r
                    WHERE r.tenant_slug = %s
                      AND r.phone_number = c.phone_number
                      AND r.sent_at >= NOW() - (
                        CASE c.tier
                          WHEN 'regular' THEN INTERVAL '5 days'
                          WHEN 'engaged' THEN INTERVAL '7 days'
                          WHEN 'new'     THEN INTERVAL '14 days'
                          WHEN 'lapsed'  THEN INTERVAL '30 days'
                          ELSE INTERVAL '0 days'
                        END
                      )
                  )
                """
            )
            cur.execute(sql, (slug, slug, slug))
            return [r[0] for r in cur.fetchall()]

        if aud == "customer_of_the_week":
            cur.execute(
                """
                SELECT DISTINCT s.phone_number
                FROM smb_customer_of_the_week cotw
                JOIN smb_subscribers s
                  ON s.tenant_slug = cotw.tenant_slug
                 AND s.phone_number = cotw.phone_number
                WHERE cotw.tenant_slug = %s
                  AND s.status = 'active'
                """,
                (slug,),
            )
            return [r[0] for r in cur.fetchall()]

        if aud.startswith("segment:"):
            seg_name = audience[8:].strip().upper()
            seg = next(
                (s for s in segments_def if s.get("name", "").upper() == seg_name),
                None,
            )
            if not seg:
                raise UnknownAudience(f"Unknown segment: {seg_name}")
            answers = seg.get("answers", [])
            question_key = seg.get("question_key", "")
            if not answers or not question_key:
                raise UnknownAudience(f"Misconfigured segment: {seg_name}")
            placeholders = ",".join(["%s"] * len(answers))
            cur.execute(
                f"""SELECT DISTINCT s.phone_number
                    FROM smb_subscribers s
                    JOIN smb_preferences p ON p.subscriber_id = s.id
                    WHERE s.tenant_slug=%s
                      AND s.status='active'
                      AND p.question_key=%s
                      AND p.answer IN ({placeholders})""",
                (slug, question_key, *answers),
            )
            return [r[0] for r in cur.fetchall()]

    raise UnknownAudience(f"Invalid audience format: {audience}")


def audience_label(audience: str) -> str:
    """Human-readable audience label for status messages and DB segment col."""
    aud = audience.strip().lower()
    if aud == "all":
        return "all subscribers"
    if aud == "smart-send":
        return "Smart Send"
    if aud.startswith("tier:"):
        tier = aud[5:].strip()
        return f"{TIER_LABELS.get(tier, tier).split(' ')[0]} customers"
    if aud == "customer_of_the_week":
        return "past Customers of the Week"
    if aud.startswith("segment:"):
        return audience[8:].strip().lower()
    return audience


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

def _ai_cleanup(message: str, slug: str, display_name: str) -> str:
    """
    Lightly clean up the owner's raw message using the same prompt Felicia's
    SMS-driven blast flow uses. Falls back to the raw message (with a tenant
    prefix) if every AI provider is unavailable.
    """
    try:
        from app.smb.tenants import get_registry
        from app.smb.blast import _ai_enhance_blast

        tenant = get_registry().get(slug)
        if tenant is not None:
            return _ai_enhance_blast(message, tenant)
    except Exception:
        logger.exception("AI cleanup unavailable for tenant=%s — using raw", slug)

    msg = message.strip()
    if display_name and display_name.lower() not in msg.lower():
        msg = f"{display_name}: {msg}"
    return msg


def _twilio_send(client, body: str, from_number: str, to: str) -> bool:
    try:
        client.messages.create(body=body, from_=from_number, to=to)
        return True
    except Exception as e:
        logger.warning("business_blast: send to %s failed: %s", to[-4:], e)
        return False


def send_blast(
    slug: str,
    raw_message: str,
    audience: str,
    *,
    ai_cleanup: bool = True,
    display_name: Optional[str] = None,
    business_configs_dir: Optional[Path] = None,
    get_conn,
) -> dict:
    """
    Resolve the audience synchronously (so we can echo a recipient count back
    to the UI), then fan-out to Twilio in a daemon thread. Returns a dict
    suitable for jsonify-ing immediately.

    Recording the blast row before sending means the per-recipient inserts
    have a foreign key to point at; the row's attempted/succeeded counters
    are updated when the worker finishes.
    """
    business_configs_dir = business_configs_dir or (
        Path(__file__).parent / "business_configs"
    )

    try:
        cfg_path = business_configs_dir / f"{slug}.json"
        cfg = json.loads(cfg_path.read_text())
    except (OSError, ValueError):
        cfg = {}
    segments_def = cfg.get("segments", []) if isinstance(cfg, dict) else []
    display_name = display_name or (
        cfg.get("display_name") if isinstance(cfg, dict) else slug
    ) or slug

    conn = get_conn()
    try:
        phones = resolve_audience(slug, audience, conn, segments_def)
    finally:
        conn.close()

    phones = list(dict.fromkeys(p for p in phones if p))  # dedupe, preserve order

    if not phones:
        return {
            "success": False,
            "error": f"No subscribers match {audience_label(audience)}.",
            "recipient_count": 0,
        }

    body = _ai_cleanup(raw_message, slug, display_name) if ai_cleanup else raw_message.strip()
    seg_label = audience_label(audience) if audience.lower() != "all" else None

    # Pre-record the blast row so the worker can attribute recipients.
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO smb_blasts
                           (tenant_slug, owner_message, body, attempted, succeeded, segment)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (slug, raw_message[:500], body[:500], 0, 0, seg_label),
                )
                blast_id = cur.fetchone()[0]
    finally:
        conn.close()

    from_number = os.getenv(f"SMB_{slug.upper()}_SMS_NUMBER")
    if not from_number:
        return {
            "success": False,
            "error": "SMS number not configured for this account.",
            "recipient_count": len(phones),
        }

    threading.Thread(
        target=_dispatch,
        args=(slug, blast_id, body, from_number, phones, get_conn),
        daemon=True,
    ).start()

    return {
        "success": True,
        "blast_id": blast_id,
        "recipient_count": len(phones),
        "audience_label": audience_label(audience),
        "ai_cleaned": ai_cleanup,
        "body_preview": body,
    }


def _dispatch(
    slug: str,
    blast_id: int,
    body: str,
    from_number: str,
    phones: Iterable[str],
    get_conn,
) -> None:
    """Background worker — sends each SMS and logs per-recipient outcomes."""
    try:
        from twilio.rest import Client as TwilioClient
        client = TwilioClient(
            os.getenv("TWILIO_ACCOUNT_SID"),
            os.getenv("TWILIO_AUTH_TOKEN"),
        )
    except Exception:
        logger.exception("business_blast: failed to init Twilio for tenant=%s", slug)
        return

    phones = list(phones)
    attempted = 0
    succeeded = 0

    for i, phone in enumerate(phones):
        attempted += 1
        ok = _twilio_send(client, body, from_number, phone)
        if ok:
            succeeded += 1
        # Log every attempt — Smart Send cadence is computed from this table
        # regardless of whether Twilio accepted the message, so we don't
        # repeatedly retry a failing fan and over-message the rest.
        try:
            conn = get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO smb_blast_recipients
                                   (blast_id, tenant_slug, phone_number, status)
                               VALUES (%s, %s, %s, %s)""",
                            (blast_id, slug, phone, "sent" if ok else "failed"),
                        )
            finally:
                conn.close()
        except Exception:
            logger.exception("business_blast: could not record recipient")
        if len(phones) > 1 and i < len(phones) - 1:
            time.sleep(0.35)

    try:
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE smb_blasts SET attempted=%s, succeeded=%s WHERE id=%s",
                        (attempted, succeeded, blast_id),
                    )
        finally:
            conn.close()
    except Exception:
        logger.exception("business_blast: could not finalize counts for blast=%s", blast_id)

    logger.info(
        "business_blast done: tenant=%s blast=%s attempted=%d succeeded=%d",
        slug, blast_id, attempted, succeeded,
    )


def preview_count(
    slug: str,
    audience: str,
    *,
    business_configs_dir: Optional[Path] = None,
    get_conn,
) -> int:
    """Count subscribers without actually sending anything."""
    business_configs_dir = business_configs_dir or (
        Path(__file__).parent / "business_configs"
    )
    try:
        cfg = json.loads((business_configs_dir / f"{slug}.json").read_text())
    except (OSError, ValueError):
        cfg = {}
    segments_def = cfg.get("segments", []) if isinstance(cfg, dict) else []

    conn = get_conn()
    try:
        return len(resolve_audience(slug, audience, conn, segments_def))
    finally:
        conn.close()
