"""
Cross-worker inbound message deduplication.

Gunicorn runs multiple workers (8 in production), each with its own memory.
The original dedup was a per-worker LRU, so the same webhook delivered twice
(SlickText/Twilio retries, double-POSTs) could hit two different workers and
both would generate + send an AI reply — fans got two different answers to one
text.

This module keeps the in-memory LRU as a fast path and adds a shared Postgres
claim table: the first worker to INSERT a message_id wins; every other worker
sees the conflict and drops the duplicate. Rows expire after a few days — the
table only needs to cover the webhook retry window.

Fail-open by design: if Postgres is unreachable the LRU still provides the
same per-worker protection we had before, and the message is processed (a rare
double reply is better than a fan getting no reply).

Standalone module (own connection, like app/analytics/session_manager.py) so
app/storage/postgres.py stays migration-only.
"""
from __future__ import annotations

import logging
import os
import random
import threading
from collections import OrderedDict

_logger = logging.getLogger(__name__)

_MAX_SEEN = 1000
_seen_message_ids: OrderedDict = OrderedDict()
_seen_lock = threading.Lock()

# Claim rows only need to outlive the providers' webhook retry window.
_RETENTION_DAYS = 7
# Opportunistic cleanup: roughly one delete sweep per N claims.
_CLEANUP_PROBABILITY = 1 / 500

_table_ready = False
_table_lock = threading.Lock()

_DDL = """
CREATE TABLE IF NOT EXISTS processed_messages (
    message_id   TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def _db_dedup_enabled() -> bool:
    """Kill-switch (INBOUND_DEDUP_DB=off) — used by tests so synthetic webhook
    ids never claim rows in a real database, and as an ops escape hatch."""
    return os.getenv("INBOUND_DEDUP_DB", "on").strip().lower() not in ("0", "false", "off")


def _get_conn():
    if not _db_dedup_enabled():
        return None
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    import psycopg2
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def _ensure_table(conn) -> None:
    global _table_ready
    if _table_ready:
        return
    with _table_lock:
        if _table_ready:
            return
        with conn:
            with conn.cursor() as cur:
                cur.execute(_DDL)
        _table_ready = True


def _seen_in_memory(message_id: str) -> bool:
    """Per-worker LRU check-and-record. True if this worker saw the id before."""
    with _seen_lock:
        if message_id in _seen_message_ids:
            return True
        _seen_message_ids[message_id] = True
        if len(_seen_message_ids) > _MAX_SEEN:
            _seen_message_ids.popitem(last=False)
    return False


def _claimed_in_db(message_id: str) -> bool:
    """Atomically claim the id in Postgres. True if ANOTHER worker already had it.

    Fail-open: any DB problem returns False so the message is still processed.
    """
    conn = None
    try:
        conn = _get_conn()
        if conn is None:
            return False
        _ensure_table(conn)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO processed_messages (message_id) VALUES (%s) "
                    "ON CONFLICT (message_id) DO NOTHING",
                    (message_id,),
                )
                claimed = cur.rowcount == 1
                if claimed and random.random() < _CLEANUP_PROBABILITY:
                    cur.execute(
                        "DELETE FROM processed_messages "
                        "WHERE processed_at < NOW() - INTERVAL '%s days'",
                        (_RETENTION_DAYS,),
                    )
        return not claimed
    except Exception:
        _logger.exception("[ZARNA] inbound dedup DB check failed — falling back to in-memory only")
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def already_processed(message_id: str) -> bool:
    """True if this inbound message id was already handled (any worker)."""
    if not message_id:
        return False
    if _seen_in_memory(message_id):
        return True
    return _claimed_in_db(message_id)


def reset_for_tests() -> None:
    """Clear per-process state (LRU + table-ready flag)."""
    global _table_ready
    with _seen_lock:
        _seen_message_ids.clear()
    _table_ready = False
