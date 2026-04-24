"""
Credit accounting primitives.

Rules:
- 1 credit = 1 SMS segment (inbound or outbound) or an MMS send billed at 3.
- `trial` accounts draw from operator_users.trial_credits_remaining — hard stop at 0.
- Paid accounts draw from operator_credit_usage.credits_used for the current period,
  bounded by credits_included + boosters_purchased + overage up to SOFT_GRACE_MULTIPLIER.
- Every consumption (and grant) also writes a credit_events row for audit.

This module is intentionally tolerant of missing slugs / missing users — SMS
processing MUST NOT fail because billing isn't set up yet. We log a warning
and continue, so a misconfigured tenant never drops messages.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Optional

from ..db import get_conn
from .plans import SOFT_GRACE_MULTIPLIER, TRIAL_CREDITS, get_plan_credits

logger = logging.getLogger(__name__)


# ── Segment counting ──────────────────────────────────────────────────────
# Mirrors app/utils/sms_segments.py so the operator doesn't need a cross-package
# import. Kept intentionally simple — GSM-7 detection is skipped because the
# only meaningful bucket is "≤160 chars == 1 segment, else ceil(chars/153)".

def count_segments(text: str, has_media: bool = False) -> int:
    """Approximate SMS billing segments. Returns >= 1."""
    if has_media:
        return 3
    if not text:
        return 1
    length = len(text)
    # If the message contains any non-ASCII char, Twilio switches to UCS-2
    # (70-char single / 67 per multi-part). Use the tighter bucket.
    if any(ord(c) > 127 for c in text):
        if length <= 70:
            return 1
        return max(1, math.ceil(length / 67))
    if length <= 160:
        return 1
    return max(1, math.ceil(length / 153))


# ── Event kinds (audit log) ───────────────────────────────────────────────

KIND_SMS_INBOUND = "sms_inbound"
KIND_SMS_OUTBOUND = "sms_outbound"
KIND_BLAST_SENT = "blast_sent"
KIND_BOOSTER_PURCHASED = "booster_purchased"
KIND_PLAN_RESET = "plan_reset"
KIND_PLAN_CHANGED = "plan_changed"
KIND_ADJUSTMENT = "adjustment"
KIND_TRIAL_GRANT = "trial_grant"


# ── Helpers ────────────────────────────────────────────────────────────────

def _find_user_by_slug(slug: str) -> Optional[dict]:
    """Resolve a creator_slug to the owning operator_user row.

    Picks the row with team_members.role='owner' when available, else falls
    back to the first operator_users row carrying that slug.
    """
    if not slug:
        return None
    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.plan_tier, u.trial_credits_remaining, u.creator_slug
                FROM   operator_users u
                LEFT JOIN team_members tm
                       ON tm.user_id = u.id AND tm.tenant_slug = %s
                WHERE  u.creator_slug = %s
                ORDER BY CASE WHEN tm.role = 'owner' THEN 0 ELSE 1 END, u.id
                LIMIT 1
                """,
                (slug, slug),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _current_period(user_id: int) -> tuple[date, Optional[date]]:
    """Return (period_start, period_end) for the active billing window.

    For trial users without a billing_cycle_anchor, period_start = trial_started_at
    (or today if never set) and period_end is NULL (trial doesn't roll).
    For paid users, period_start = billing_cycle_anchor (date) and period_end
    is anchor + 1 month.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT plan_tier, billing_cycle_anchor, trial_started_at
                FROM   operator_users WHERE id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return date.today(), None
            plan_tier, anchor, trial_started = row
    finally:
        conn.close()

    if plan_tier == "trial":
        start = (trial_started.date() if trial_started else date.today())
        return start, None

    if anchor:
        start = anchor.date()
        # Monthly cycle: approximate period_end as start + 30 days.
        # Stripe's invoice.paid webhook resets this to the real anchor.
        return start, start + timedelta(days=30)

    # Paid plan with no anchor yet (shouldn't happen post-checkout) — use today.
    return date.today(), date.today() + timedelta(days=30)


# ── Seeding ────────────────────────────────────────────────────────────────

def seed_trial_credits(user_id: int, slug: str, *, credits: int = TRIAL_CREDITS) -> None:
    """Seed a newly-onboarded user with trial credits.

    Idempotent: if the user already has trial_started_at set, does nothing.
    Called at the end of api_onboarding_submit once the slug is claimed.
    """
    if not user_id or not slug:
        return
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE operator_users
                    SET    plan_tier = COALESCE(plan_tier, 'trial'),
                           trial_credits_remaining = COALESCE(trial_credits_remaining, %s),
                           trial_started_at = COALESCE(trial_started_at, NOW())
                    WHERE  id = %s
                    RETURNING trial_started_at
                    """,
                    (credits, user_id),
                )
                started = cur.fetchone()

                # First-time seeding → ensure matching operator_credit_usage row for reporting
                cur.execute(
                    """
                    INSERT INTO operator_credit_usage
                        (operator_user_id, creator_slug, period_start,
                         credits_used, credits_included)
                    VALUES (%s, %s, CURRENT_DATE, 0, %s)
                    ON CONFLICT (operator_user_id, period_start) DO NOTHING
                    """,
                    (user_id, slug, credits),
                )

                cur.execute(
                    """
                    INSERT INTO credit_events
                        (operator_user_id, creator_slug, kind, credits, source_id)
                    VALUES (%s, %s, %s, %s, 'onboarding')
                    """,
                    (user_id, slug, KIND_TRIAL_GRANT, credits),
                )
        logger.info("seed_trial_credits: user_id=%s slug=%s credits=%s started=%s",
                    user_id, slug, credits, started)
    except Exception:
        logger.exception("seed_trial_credits failed for user_id=%s slug=%s", user_id, slug)
    finally:
        conn.close()


# ── Core consumption ───────────────────────────────────────────────────────

def consume_credit(
    *,
    slug: Optional[str] = None,
    user_id: Optional[int] = None,
    kind: str,
    credits: int = 1,
    source_id: Optional[str] = None,
) -> dict:
    """Deduct `credits` from the right bucket for (user_id OR slug).

    Returns the post-deduction status dict:
        { used, included, remaining, overage, plan_tier, is_trial,
          exhausted, hard_blocked }

    Silently no-ops on missing user/slug — billing MUST NEVER block inbound SMS.
    """
    if credits <= 0:
        credits = 1

    # Resolve to a concrete user row
    user_row: Optional[dict] = None
    if user_id:
        conn = get_conn()
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """SELECT id, plan_tier, trial_credits_remaining, creator_slug
                       FROM operator_users WHERE id = %s""",
                    (user_id,),
                )
                r = cur.fetchone()
                user_row = dict(r) if r else None
        finally:
            conn.close()
    elif slug:
        user_row = _find_user_by_slug(slug)

    if not user_row:
        logger.info("consume_credit: no user for slug=%s user_id=%s — skipping", slug, user_id)
        return _empty_status()

    resolved_slug = user_row.get("creator_slug") or slug or ""
    plan_tier = user_row.get("plan_tier") or "trial"
    uid = user_row["id"]

    period_start, period_end = _current_period(uid)

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Ensure a usage row exists for this period
                cur.execute(
                    """
                    INSERT INTO operator_credit_usage
                        (operator_user_id, creator_slug, period_start, period_end,
                         credits_used, credits_included)
                    VALUES (%s, %s, %s, %s, 0, %s)
                    ON CONFLICT (operator_user_id, period_start) DO NOTHING
                    """,
                    (uid, resolved_slug, period_start, period_end,
                     get_plan_credits(plan_tier)),
                )

                if plan_tier == "trial":
                    # Draw from trial_credits_remaining; log consumption on usage row too
                    cur.execute(
                        """
                        UPDATE operator_users
                        SET    trial_credits_remaining = GREATEST(0, trial_credits_remaining - %s)
                        WHERE  id = %s
                        RETURNING trial_credits_remaining
                        """,
                        (credits, uid),
                    )
                    remaining_row = cur.fetchone()
                    trial_left = int(remaining_row["trial_credits_remaining"] or 0)

                    cur.execute(
                        """
                        UPDATE operator_credit_usage
                        SET    credits_used = credits_used + %s,
                               updated_at = NOW()
                        WHERE  operator_user_id = %s AND period_start = %s
                        """,
                        (credits, uid, period_start),
                    )
                else:
                    # Paid plan: draw from period usage row, compute overage
                    cur.execute(
                        """
                        UPDATE operator_credit_usage
                        SET    credits_used = credits_used + %s,
                               updated_at = NOW()
                        WHERE  operator_user_id = %s AND period_start = %s
                        RETURNING credits_used, credits_included, boosters_purchased
                        """,
                        (credits, uid, period_start),
                    )
                    usage = cur.fetchone() or {"credits_used": credits, "credits_included": 0, "boosters_purchased": 0}
                    total_available = int(usage["credits_included"]) + int(usage["boosters_purchased"])
                    used = int(usage["credits_used"])
                    new_overage = max(0, used - total_available)
                    cur.execute(
                        """
                        UPDATE operator_credit_usage
                        SET    overage_credits = %s
                        WHERE  operator_user_id = %s AND period_start = %s
                        """,
                        (new_overage, uid, period_start),
                    )

                # Audit event (negative credits means consumption)
                cur.execute(
                    """
                    INSERT INTO credit_events
                        (operator_user_id, creator_slug, kind, credits, source_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (uid, resolved_slug, kind, -credits, source_id),
                )

        return get_credit_status(user_id=uid)
    except Exception:
        logger.exception("consume_credit failed for user_id=%s slug=%s", uid, resolved_slug)
        return _empty_status()
    finally:
        conn.close()


# ── Read / status ──────────────────────────────────────────────────────────

def _empty_status() -> dict:
    return {
        "plan_tier": None,
        "is_trial": False,
        "used": 0,
        "included": 0,
        "boosters_purchased": 0,
        "overage": 0,
        "remaining": 0,
        "exhausted": False,
        "hard_blocked": False,
        "warning": None,
        "period_start": None,
        "period_end": None,
    }


def get_credit_status(
    *,
    user_id: Optional[int] = None,
    slug: Optional[str] = None,
) -> dict:
    """Snapshot of the user's credit state. Safe to call on every request."""
    user_row = None
    if user_id:
        conn = get_conn()
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """SELECT id, plan_tier, trial_credits_remaining, creator_slug,
                              billing_cycle_anchor, billing_cycle, trial_started_at
                       FROM operator_users WHERE id = %s""",
                    (user_id,),
                )
                r = cur.fetchone()
                user_row = dict(r) if r else None
        finally:
            conn.close()
    elif slug:
        user_row = _find_user_by_slug(slug)
        if user_row:
            # Need anchor + billing_cycle too
            conn = get_conn()
            try:
                import psycopg2.extras
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(
                        """SELECT billing_cycle_anchor, billing_cycle, trial_started_at
                           FROM operator_users WHERE id=%s""",
                        (user_row["id"],),
                    )
                    extra = cur.fetchone()
                    if extra:
                        user_row.update(dict(extra))
            finally:
                conn.close()

    if not user_row:
        return _empty_status()

    uid = user_row["id"]
    plan_tier = user_row.get("plan_tier") or "trial"
    period_start, period_end = _current_period(uid)

    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT credits_used, credits_included,
                          boosters_purchased, overage_credits,
                          period_start, period_end
                   FROM   operator_credit_usage
                   WHERE  operator_user_id = %s AND period_start = %s""",
                (uid, period_start),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    is_trial = plan_tier == "trial"
    if is_trial:
        included = int(user_row.get("trial_credits_remaining") or 0)
        # For trial, credits_used = TRIAL_CREDITS - remaining
        used = max(0, TRIAL_CREDITS - included)
        total = TRIAL_CREDITS
        boosters_purchased = 0
        overage = 0
        remaining = included
        exhausted = included <= 0
        hard_blocked = exhausted  # trial has no grace
    elif row:
        used = int(row["credits_used"] or 0)
        included = int(row["credits_included"] or get_plan_credits(plan_tier))
        boosters_purchased = int(row["boosters_purchased"] or 0)
        overage = int(row["overage_credits"] or 0)
        total = included + boosters_purchased
        remaining = max(0, total - used)
        exhausted = used >= total
        # Soft grace: only blocked once past SOFT_GRACE_MULTIPLIER
        hard_blocked = used >= int(total * SOFT_GRACE_MULTIPLIER) if total > 0 else False
    else:
        used = 0
        included = get_plan_credits(plan_tier)
        boosters_purchased = 0
        overage = 0
        total = included
        remaining = total
        exhausted = False
        hard_blocked = False

    warning: Optional[str] = None
    if total > 0:
        pct = used / total
        if pct >= 1.0:
            warning = "critical"
        elif pct >= 0.80:
            warning = "low"

    return {
        "user_id": uid,
        "plan_tier": plan_tier,
        "is_trial": is_trial,
        "used": used,
        "included": included,
        "boosters_purchased": boosters_purchased,
        "total": total,
        "overage": overage,
        "remaining": remaining,
        "exhausted": exhausted,
        "hard_blocked": hard_blocked,
        "warning": warning,
        "period_start": period_start.isoformat() if period_start else None,
        "period_end": period_end.isoformat() if period_end else None,
    }


# ── Send-quota gate (used by blast send + inbox send) ──────────────────────

def check_send_quota(
    *,
    user_id: Optional[int] = None,
    slug: Optional[str] = None,
    requested: int = 1,
) -> tuple[bool, dict]:
    """Gate an outbound send attempt.

    Returns (allowed, status). `requested` is the number of credits this send
    will consume if allowed. Callers should call consume_credit() after a
    successful send.

    Rules:
    - Trial plan: hard block at 0 remaining.
    - Paid plan: hard block only past soft-grace (total * SOFT_GRACE_MULTIPLIER).
    """
    status = get_credit_status(user_id=user_id, slug=slug)
    if not status.get("plan_tier"):
        # Unknown user (e.g. local dev without billing init) — allow, don't block.
        return True, status

    if status["is_trial"]:
        allowed = status["remaining"] >= requested
        if not allowed:
            status["hard_blocked"] = True
        return allowed, status

    # Paid: allow until we cross the soft-grace ceiling
    total = status["total"]
    projected = status["used"] + requested
    ceiling = int(total * SOFT_GRACE_MULTIPLIER) if total > 0 else 0
    allowed = total == 0 or projected <= ceiling
    if not allowed:
        status["hard_blocked"] = True
    return allowed, status


# ── Grants (plan change, booster purchase, manual adjustment) ──────────────

def grant_booster_credits(
    *,
    user_id: int,
    slug: str,
    credits: int,
    stripe_invoice_id: Optional[str] = None,
) -> None:
    """Called by Stripe webhook when a booster Checkout completes.

    Adds `credits` to the current period's boosters_purchased. Never reduces.
    """
    if credits <= 0:
        return
    period_start, _ = _current_period(user_id)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO operator_credit_usage
                        (operator_user_id, creator_slug, period_start,
                         credits_included, boosters_purchased)
                    VALUES (%s, %s, %s, 0, %s)
                    ON CONFLICT (operator_user_id, period_start)
                    DO UPDATE SET boosters_purchased = operator_credit_usage.boosters_purchased + EXCLUDED.boosters_purchased,
                                  updated_at = NOW()
                    """,
                    (user_id, slug, period_start, credits),
                )
                cur.execute(
                    """
                    INSERT INTO credit_events
                        (operator_user_id, creator_slug, kind, credits, source_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (user_id, slug, KIND_BOOSTER_PURCHASED, credits, stripe_invoice_id),
                )
        logger.info("grant_booster_credits: user_id=%s slug=%s +%s credits", user_id, slug, credits)
    finally:
        conn.close()


def set_plan_tier(
    *,
    user_id: int,
    slug: str,
    plan_tier: str,
    billing_cycle: str,
    stripe_customer_id: Optional[str],
    stripe_subscription_id: Optional[str],
    billing_cycle_anchor,  # datetime
    included_credits: int,
) -> None:
    """Apply a plan change (from Stripe webhook).

    - Updates operator_users with the new tier/cycle + Stripe ids.
    - Seeds a new operator_credit_usage row for this period with credits_included.
    - Logs credit_events (plan_reset kind on first period, plan_changed on tier swap).
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE operator_users
                    SET    plan_tier = %s,
                           billing_cycle = %s,
                           stripe_customer_id = COALESCE(%s, stripe_customer_id),
                           stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                           billing_cycle_anchor = %s
                    WHERE  id = %s
                    """,
                    (plan_tier, billing_cycle,
                     stripe_customer_id, stripe_subscription_id,
                     billing_cycle_anchor, user_id),
                )

                # Reset usage for the new period
                period_start = billing_cycle_anchor.date()
                period_end = period_start + timedelta(days=30)
                cur.execute(
                    """
                    INSERT INTO operator_credit_usage
                        (operator_user_id, creator_slug, period_start, period_end,
                         credits_used, credits_included, boosters_purchased, overage_credits)
                    VALUES (%s, %s, %s, %s, 0, %s, 0, 0)
                    ON CONFLICT (operator_user_id, period_start)
                    DO UPDATE SET credits_included = EXCLUDED.credits_included,
                                  period_end = EXCLUDED.period_end,
                                  updated_at = NOW()
                    """,
                    (user_id, slug, period_start, period_end, included_credits),
                )

                cur.execute(
                    """
                    INSERT INTO credit_events
                        (operator_user_id, creator_slug, kind, credits, source_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (user_id, slug, KIND_PLAN_RESET, included_credits, stripe_subscription_id),
                )
        logger.info("set_plan_tier: user_id=%s slug=%s tier=%s cycle=%s credits=%s",
                    user_id, slug, plan_tier, billing_cycle, included_credits)
    finally:
        conn.close()


def clear_subscription(*, user_id: int) -> None:
    """Called by customer.subscription.deleted webhook."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE operator_users
                       SET plan_tier = 'cancelled',
                           stripe_subscription_id = NULL
                       WHERE id = %s""",
                    (user_id,),
                )
        logger.info("clear_subscription: user_id=%s", user_id)
    finally:
        conn.close()
