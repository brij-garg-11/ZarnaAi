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
    _scheduler.start()
    logger.info("Operator scheduler started")


def _process_scheduled_blasts():
    try:
        from .queries import get_pending_scheduled_blasts
        from .blast_sender import execute_blast
        pending = get_pending_scheduled_blasts()
        for draft in pending:
            logger.info("Processing scheduled blast id=%s name=%s", draft["id"], draft["name"])
            execute_blast(draft["id"])
    except Exception as e:
        logger.exception("Scheduler error: %s", e)
