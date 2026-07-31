"""Redis-backed cache using redis.asyncio (connect/close wired via app lifespan)."""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RedisCache:
    """Redis implementation of CacheBackend. Call connect() before get/set/delete."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        """Expose the async client for SingleFlight Redis locks."""
        if self._client is None:
            raise RuntimeError("Redis cache not initialized; call connect() in lifespan")
        return self._client

    async def connect(self) -> None:
        if self._client is None:
            # decode_responses=True → str keys/values; we JSON-encode payloads ourselves
            self._client = Redis.from_url(self._redis_url, decode_responses=True)
            await self._client.ping()
            logger.info("Redis cache connected")

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("Redis cache closed")

    async def get(self, key: str) -> Any | None:
        raw = await self.client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            await self.delete(key)
            return
        await self.client.set(key, json.dumps(value), ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self.client.delete(key)
