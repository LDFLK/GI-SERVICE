"""Cache backend contract — callers depend on this, not Redis directly."""

from typing import Any, Protocol


class CacheBackend(Protocol):
    async def connect(self) -> None:
        """Open connections / warm resources. No-op for null/memory."""

    async def close(self) -> None:
        """Release resources on app shutdown."""

    async def get(self, key: str) -> Any | None:
        """Return cached value or None on miss / expiry."""

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Store a JSON-serializable value with a TTL in seconds."""

    async def delete(self, key: str) -> None:
        """Remove a key if present."""
