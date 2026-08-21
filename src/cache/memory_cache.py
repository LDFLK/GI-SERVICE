"""In-process cache with TTL — for unit tests and single-worker experiments."""

import time
from typing import Any


class InMemoryCache:
    def __init__(self) -> None:
        # key -> (value, expires_at_monotonic)
        self._store: dict[str, tuple[Any, float]] = {}

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self._store.clear()

    async def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if time.monotonic() >= expires_at:
            del self._store[key]
            return None
        return value

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            self._store.pop(key, None)
            return
        self._store[key] = (value, time.monotonic() + ttl_seconds)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)
