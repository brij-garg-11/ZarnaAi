"""
Performer phone-number → creator routing registry.

The multi-tenant ("apartment building") main app serves many self-serve
creators from one process. When a fan texts, Twilio hits
`/twilio/webhook?slug=<slug>` — but we never trust that query param blindly.
We validate it against this registry, which maps the *destination* number
(the `To` field) to the creator that owns it.

Source of truth: the `creator_numbers` table (written by the operator's
provisioning phone step). Falls back to the legacy
`operator_users.phone_number → bot_configs.creator_slug` join for any number
provisioned before the registry existed.

Numbers are bought at runtime (not known at startup), so unlike the SMB
`TenantRegistry` (which loads JSON files once at boot) this registry caches
DB lookups with a short TTL and self-refreshes — a freshly provisioned number
becomes routable within one TTL window.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Short TTL: a number bought seconds ago must become routable quickly, and the
# lookup is a single indexed PK query so re-validating is cheap.
_CACHE_TTL = float(os.getenv("CREATOR_NUMBER_CACHE_TTL", "60"))


class PerformerRegistry:
    def __init__(self, ttl: float = _CACHE_TTL):
        self._ttl = ttl
        # phone_number -> (slug_or_None, expires_at)
        self._cache: dict[str, tuple[Optional[str], float]] = {}
        self._lock = threading.Lock()

    def get_slug_by_to_number(self, to_number: Optional[str]) -> Optional[str]:
        """
        Return the creator slug that owns this destination number, or None if
        the number isn't ours. Cached for `ttl` seconds (including misses, so a
        flood of texts to an unknown number doesn't hammer the DB).
        """
        if not to_number:
            return None
        key = to_number.strip()
        if not key:
            return None

        now = time.time()
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None and cached[1] > now:
                return cached[0]

        slug = self._query_db(key)

        with self._lock:
            self._cache[key] = (slug, time.time() + self._ttl)
        return slug

    def invalidate(self, to_number: Optional[str] = None) -> None:
        """Drop a single number (or the whole cache) — call after (de)provisioning."""
        with self._lock:
            if to_number is None:
                self._cache.clear()
            else:
                self._cache.pop(to_number.strip(), None)

    # -- DB access (isolated so tests can monkeypatch without a live DB) --------
    def _query_db(self, to_number: str) -> Optional[str]:
        dsn = os.getenv("DATABASE_URL", "")
        if not dsn:
            return None
        try:
            import psycopg2
        except ImportError:
            return None
        try:
            conn = psycopg2.connect(dsn.replace("postgres://", "postgresql://", 1))
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    # Primary: the routing registry.
                    cur.execute(
                        "SELECT creator_slug FROM creator_numbers "
                        "WHERE phone_number = %s AND status = 'active' LIMIT 1",
                        (to_number,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        return row[0]
                    # Fallback: legacy operator_users.phone_number mapping.
                    cur.execute(
                        "SELECT bc.creator_slug "
                        "FROM operator_users ou "
                        "JOIN bot_configs bc ON bc.operator_user_id = ou.id "
                        "WHERE ou.phone_number = %s LIMIT 1",
                        (to_number,),
                    )
                    row = cur.fetchone()
                    return row[0] if row and row[0] else None
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(
                "PerformerRegistry: DB lookup failed for ...%s (%s)",
                to_number[-4:] if len(to_number) >= 4 else "?", exc,
            )
            return None


_registry: Optional[PerformerRegistry] = None


def get_registry() -> PerformerRegistry:
    global _registry
    if _registry is None:
        _registry = PerformerRegistry()
    return _registry
