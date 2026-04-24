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
    _scheduler.start()
    logger.info("Operator scheduler started")


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
