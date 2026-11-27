"""Per-credential rolling request limit for the single-worker broker."""

from collections import defaultdict, deque
from math import ceil
from threading import Lock
from time import monotonic


class RateLimiter:
    def __init__(self, limit=60, window=60, cooldown=120):
        self.limit = limit
        self.window = window
        self.cooldown = cooldown
        self.lock = Lock()
        self.requests = defaultdict(deque)
        self.blocked_until = {}

    def retry_after(self, identity):
        with self.lock:
            now = monotonic()
            deadline = self.blocked_until.get(identity)
            if deadline is not None:
                if now < deadline:
                    return ceil(deadline - now)
                del self.blocked_until[identity]

            timestamps = self.requests[identity]
            while timestamps and timestamps[0] <= now - self.window:
                timestamps.popleft()
            if len(timestamps) >= self.limit:
                timestamps.clear()
                self.blocked_until[identity] = now + self.cooldown
                return self.cooldown

            timestamps.append(now)
            return None
