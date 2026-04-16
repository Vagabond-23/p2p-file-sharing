"""
Token bucket rate limiter.

A token bucket refills at `rate` bytes/sec. Callers call consume(n)
before sending/receiving n bytes — it sleeps just long enough to
stay within the configured rate. Thread-safe.

Usage:
    limiter = TokenBucket(rate=50 * 1024 * 1024)  # 50 MB/s
    limiter.consume(len(data))
    sock.sendall(data)

Pass rate=0 (or None) to disable throttling entirely.
"""

import threading
import time


class TokenBucket:
    def __init__(self, rate: float = 0):
        """
        rate: bytes per second. 0 = unlimited.
        Burst capacity is capped at 2× one chunk (1 MB) to keep
        latency smooth — prevents a stalled connection from saving
        up tokens and then blasting everything at once.
        """
        self.rate = rate
        self._lock = threading.Lock()
        self._tokens = float(rate) if rate > 0 else 0.0
        self._last = time.monotonic()
        self._max_tokens = rate * 2 if rate > 0 else 0.0  # 2-second burst cap

    def set_rate(self, rate: float) -> None:
        """Change rate at runtime (bytes/sec). 0 = unlimited."""
        with self._lock:
            self.rate = rate
            self._max_tokens = rate * 2 if rate > 0 else 0.0
            self._tokens = min(self._tokens, self._max_tokens)

    def consume(self, n: int) -> None:
        """Block until n bytes worth of tokens are available."""
        if self.rate <= 0:
            return  # unlimited

        with self._lock:
            # Refill tokens based on elapsed time
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._max_tokens, self._tokens + elapsed * self.rate)

            if self._tokens >= n:
                self._tokens -= n
                return

            # Not enough tokens — calculate sleep needed
            deficit = n - self._tokens
            sleep_time = deficit / self.rate
            self._tokens = 0

        time.sleep(sleep_time)

    @staticmethod
    def parse_limit(s: str) -> float:
        """
        Parse a human-readable bandwidth string to bytes/sec.
        Examples: '50MB', '10mb', '100KB', '1GB', '0' (unlimited)
        """
        s = s.strip().upper().replace(" ", "")
        if s in ("0", "UNLIMITED", ""):
            return 0.0
        units = {"KB": 1024, "MB": 1024**2, "GB": 1024**3,
                 "K": 1024,  "M": 1024**2,  "G": 1024**3}
        for suffix, mult in units.items():
            if s.endswith(suffix):
                return float(s[:-len(suffix)]) * mult
        return float(s)  # bare number = bytes/sec