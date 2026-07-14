"""
Throughput pacing for operator blasts.

Twilio accounts with a registered A2P/toll-free sender support high-rate
concurrent sending (25 MPS on this account). The blast sender uses these
primitives to run a paced thread pool instead of the old sequential
one-message-at-a-time loop.

Mirrors the rate limiter in the main app's app/messaging/broadcast.py —
duplicated here because the operator container ships without the app/ package.
"""

import os
import threading
import time

# Default matches the account's registered throughput. Capped so a
# misconfigured env var can't hammer the Twilio REST API into 429s.
DEFAULT_BROADCAST_MPS = 25
MAX_BROADCAST_MPS = 100


def broadcast_mps() -> int:
    """Messages-per-second cap for Twilio blasts (TWILIO_BROADCAST_MPS env)."""
    try:
        v = int(os.getenv("TWILIO_BROADCAST_MPS", str(DEFAULT_BROADCAST_MPS)))
    except ValueError:
        v = DEFAULT_BROADCAST_MPS
    return max(1, min(v, MAX_BROADCAST_MPS))


class RateLimiter:
    """Thread-safe pacer that caps acquisitions to ``rate_per_sec`` across threads.

    Each worker calls ``acquire()`` immediately before a send; the limiter
    spaces grants out by ``1/rate`` seconds so the combined throughput of all
    worker threads never exceeds the target messages-per-second.
    """

    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            scheduled = max(now, self._next_at)
            self._next_at = scheduled + self._interval
        wait = scheduled - now
        if wait > 0:
            time.sleep(wait)
