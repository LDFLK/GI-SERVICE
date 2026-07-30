"""No-op cache — always misses. Used when CACHE_ENABLED=false."""

from typing import Any


class NullCache:
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get(self, key: str) -> Any | None:
        return None

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        return None

    async def delete(self, key: str) -> None:
        return None
