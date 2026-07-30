"""Redis-backed cache using redis.asyncio (connect/close wired via app lifespan)."""

from typing import Any


class RedisCache:
    """Redis implementation of CacheBackend. Call connect() before get/set/delete."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client = None

    async def connect(self) -> None:
        raise NotImplementedError(
            "RedisCache.connect is not implemented yet; wire redis.asyncio in lifespan"
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, key: str) -> Any | None:
        raise NotImplementedError("RedisCache.get requires connect() first")

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        raise NotImplementedError("RedisCache.set requires connect() first")

    async def delete(self, key: str) -> None:
        raise NotImplementedError("RedisCache.delete requires connect() first")
