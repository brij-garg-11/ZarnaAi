"""
PostgreSQL storage — used in production when DATABASE_URL is set.

Railway adds a Postgres database via:
  Dashboard → your project → + New → Database → Add PostgreSQL
  The DATABASE_URL environment variable is injected automatically.

Tables are created on first startup — no migration step needed.
"""

import logging
from typing import List, Optional

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from .base import BaseStorage
from .models import Contact, Message

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS contacts (
    phone_number TEXT PRIMARY KEY,
    source       TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS messages (
    id           BIGSERIAL PRIMARY KEY,
    phone_number TEXT        NOT NULL,
    role         TEXT        NOT NULL,
    text         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS messages_phone_created
    ON messages (phone_number, created_at);
"""

# Additive migrations — safe to run on every startup (idempotent)
_MIGRATIONS = """
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_memory   TEXT    DEFAULT '';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_tags     TEXT[]  DEFAULT '{}';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS fan_location TEXT    DEFAULT '';
"""


class PostgresStorage(BaseStorage):
    """Thread-safe Postgres storage using a connection pool."""

    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 10):
        self._pool = ThreadedConnectionPool(minconn, maxconn, dsn)
        self._ensure_tables()
        logger.info("PostgresStorage initialised")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _acquire(self):
        return self._pool.getconn()

    def _release(self, conn):
        self._pool.putconn(conn)

    def _ensure_tables(self):
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(_DDL)
                    cur.execute(_MIGRATIONS)
        except psycopg2.errors.UniqueViolation:
            # Race condition: two workers started simultaneously and both tried
            # to CREATE TABLE at the same moment. The other worker already
            # created the tables — safe to continue.
            conn.rollback()
        finally:
            self._release(conn)

    # ------------------------------------------------------------------
    # BaseStorage interface
    # ------------------------------------------------------------------

    def save_contact(self, phone_number: str, source: Optional[str] = None) -> Contact:
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO contacts (phone_number, source)
                        VALUES (%s, %s)
                        ON CONFLICT (phone_number) DO NOTHING
                        """,
                        (phone_number, source),
                    )
        finally:
            self._release(conn)
        return Contact(phone_number=phone_number, source=source)

    def get_contact(self, phone_number: str) -> Optional[Contact]:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT phone_number, source FROM contacts WHERE phone_number = %s",
                    (phone_number,),
                )
                row = cur.fetchone()
                if row:
                    return Contact(phone_number=row["phone_number"], source=row["source"])
        finally:
            self._release(conn)
        return None

    def save_message(self, phone_number: str, role: str, text: str) -> Message:
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO messages (phone_number, role, text) VALUES (%s, %s, %s)",
                        (phone_number, role, text),
                    )
        finally:
            self._release(conn)
        return Message(phone_number=phone_number, role=role, text=text)

    def get_conversation_history(self, phone_number: str, limit: int = 10) -> List[Message]:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT role, text FROM (
                        SELECT role, text, created_at
                        FROM messages
                        WHERE phone_number = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    ) sub
                    ORDER BY created_at ASC
                    """,
                    (phone_number, limit),
                )
                return [
                    Message(phone_number=phone_number, role=r["role"], text=r["text"])
                    for r in cur.fetchall()
                ]
        finally:
            self._release(conn)
        return []

    def is_first_message(self, phone_number: str) -> bool:
        """Efficient single-row check — no need to fetch the full history."""
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM messages WHERE phone_number = %s LIMIT 1",
                    (phone_number,),
                )
                return cur.fetchone() is None
        finally:
            self._release(conn)

    def get_memory(self, phone_number: str) -> str:
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fan_memory FROM contacts WHERE phone_number = %s",
                    (phone_number,),
                )
                row = cur.fetchone()
                return (row[0] or "") if row else ""
        finally:
            self._release(conn)

    def update_memory(self, phone_number: str, memory: str, tags: list, location: str = "") -> None:
        conn = self._acquire()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE contacts
                        SET fan_memory = %s, fan_tags = %s, fan_location = COALESCE(NULLIF(%s, ''), fan_location)
                        WHERE phone_number = %s
                        """,
                        (memory[:400], tags, location[:100], phone_number),
                    )
        finally:
            self._release(conn)

    def get_fans_by_tag(self, tag: str) -> list:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE %s = ANY(fan_tags)
                    ORDER BY created_at DESC
                    """,
                    (tag.lower(),),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)

    def get_fans_by_location(self, location: str) -> list:
        conn = self._acquire()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT phone_number, fan_memory, fan_tags, fan_location, created_at
                    FROM contacts
                    WHERE LOWER(fan_location) LIKE %s
                    ORDER BY created_at DESC
                    """,
                    (f"%{location.lower()}%",),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)
