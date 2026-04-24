"""
Background scheduler for processing timed blast sends.
Uses APScheduler — runs in-process. On deploy restarts, pending items
are re-read from the DB automatically (they stay in status='scheduled').
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)
_scheduler: BackgroundScheduler | None = None


def init_scheduler(app):
    global _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(
        _process_scheduled_blasts,
        trigger="interval",
        minutes=1,
        id="process_scheduled_blasts",
        replace_existing=True,
    )
    _scheduler.add_job(
        _recompute_engagement,
        trigger="cron",
        hour=7,
        minute=0,
        id="recompute_engagement",
        replace_existing=True,
    )
    _scheduler.add_job(
        _check_trial_alerts,
        trigger="cron",
        hour=9,
        minute=0,
        id="trial_alerts",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Operator scheduler started")


def _check_trial_alerts():
    """Daily at 09:00 UTC — send one-time alert emails for trial milestones.

    Sends a 'running low' email when trial_credits_remaining drops below 200
    and a 'trial exhausted' email at 0. Uses a sent_trial_low_alert /
    sent_trial_exhausted_alert flag column to ensure each message fires exactly once.
    Falls back gracefully if columns don't exist yet.
    """
    try:
        from .db import get_conn
        from .routes.billing import _send_billing_email

        conn = get_conn()
        rows_low = rows_exhausted = []
        try:
            with conn.cursor() as cur:
                # Low-credits alert (< 200 remaining, not yet notified)
                cur.execute("""
                    SELECT id, email
                    FROM   operator_users
                    WHERE  plan_tier = 'trial'
                      AND  trial_credits_remaining IS NOT NULL
                      AND  trial_credits_remaining > 0
                      AND  trial_credits_remaining < 200
                      AND  (sent_trial_low_alert IS NULL OR sent_trial_low_alert = FALSE)
                """)
                rows_low = cur.fetchall()

                # Exhausted alert (0 remaining, not yet notified)
                cur.execute("""
                    SELECT id, email
                    FROM   operator_users
                    WHERE  plan_tier = 'trial'
                      AND  trial_credits_remaining IS NOT NULL
                      AND  trial_credits_remaining <= 0
                      AND  (sent_trial_exhausted_alert IS NULL OR sent_trial_exhausted_alert = FALSE)
                """)
                rows_exhausted = cur.fetchall()
        finally:
            conn.close()

        for uid, _email in rows_low:
            _send_billing_email(
                user_id=uid,
                subject="You're running low on ZarBot trial credits",
                html=(
                    "<p>You have fewer than 200 trial credits remaining. "
                    "Upgrade now to keep your bot running without interruption.</p>"
                    "<p><a href='https://zar.bot/plans'>See plans →</a></p>"
                ),
                text=(
                    "You have fewer than 200 trial credits remaining. "
                    "Upgrade now to keep your bot running.\n\nhttps://zar.bot/plans"
                ),
            )
            try:
                _conn = get_conn()
                with _conn:
                    with _conn.cursor() as _cur:
                        _cur.execute(
                            "UPDATE operator_users SET sent_trial_low_alert=TRUE WHERE id=%s",
                            (uid,),
                        )
                _conn.close()
            except Exception:
                logger.exception("trial_alerts: could not mark low alert for user_id=%s", uid)

        for uid, _email in rows_exhausted:
            _send_billing_email(
                user_id=uid,
                subject="Your ZarBot trial has ended",
                html=(
                    "<p>Your 1,000 trial credits are used up. Your bot will no longer "
                    "respond to fans until you upgrade.</p>"
                    "<p><a href='https://zar.bot/plans'>Upgrade now →</a></p>"
                ),
                text=(
                    "Your 1,000 trial credits are used up. Your bot will no longer "
                    "respond to fans until you upgrade.\n\nhttps://zar.bot/plans"
                ),
            )
            try:
                _conn = get_conn()
                with _conn:
                    with _conn.cursor() as _cur:
                        _cur.execute(
                            "UPDATE operator_users SET sent_trial_exhausted_alert=TRUE WHERE id=%s",
                            (uid,),
                        )
                _conn.close()
            except Exception:
                logger.exception("trial_alerts: could not mark exhausted alert for user_id=%s", uid)

        if rows_low or rows_exhausted:
            logger.info(
                "trial_alerts: sent low=%d exhausted=%d",
                len(rows_low), len(rows_exhausted),
            )

    except Exception:
        logger.exception("trial_alerts: job failed")


def _recompute_engagement():
    try:
        from .engagement import recompute_all
        count = recompute_all()
        logger.info("Nightly engagement recompute complete: %s contacts updated", count)
    except Exception as e:
        logger.exception("Engagement recompute failed: %s", e)


def _process_scheduled_blasts():
    try:
        from .queries import claim_pending_scheduled_blasts
        from .blast_sender import execute_blast
        # claim_pending_scheduled_blasts atomically marks each blast 'sending'
        # using FOR UPDATE SKIP LOCKED, so concurrent workers never double-fire
        claimed = claim_pending_scheduled_blasts()
        for draft in claimed:
            logger.info("Scheduler claimed blast id=%s name=%s — starting send",
                        draft["id"], draft["name"])
            execute_blast(draft["id"])
    except Exception as e:
        logger.exception("Scheduler error: %s", e)
