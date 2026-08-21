"""Redis-backed cache using redis.asyncio (connect/close wired via app lifespan).

Redis is best-effort: get/set/delete failures are logged and swallowed so the
API can keep serving from OpenGIN (fail-open).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.connection import BlockingConnectionPool
from redis.exceptions import RedisError
from src.core import settings

logger = logging.getLogger(__name__)

# Connection / protocol failures we treat as "cache unavailable"
_REDIS_SOFT_ERRORS = (RedisError, ConnectionError, OSError, TimeoutError)


class RedisCache:
    """Redis implementation of CacheBackend. Call connect() before get/set/delete."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        """Expose the async client for SingleFlight Redis locks."""
        if self._client is None:
            raise RuntimeError(
                "Redis cache not initialized; call connect() in lifespan"
            )
        return self._client

    async def connect(self) -> None:
        if self._client is None:
            # Use a blocking pool so short Redis bursts wait briefly for a free
            # connection instead of immediately fail-opening on pool exhaustion.
            pool = BlockingConnectionPool.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                timeout=settings.REDIS_POOL_TIMEOUT_SECONDS,
            )
            self._client = Redis(connection_pool=pool)
            await self._client.ping()
            logger.info("Redis cache connected")

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except _REDIS_SOFT_ERRORS as exc:
                logger.warning("Redis close failed: %s", exc)
            self._client = None
            logger.info("Redis cache closed")

    async def get(self, key: str) -> Any | None:
        if self._client is None:
            return None
        try:
            raw = await self._client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("Redis get bad payload key=%s: %s", key, exc)
            return None
        except _REDIS_SOFT_ERRORS as exc:
            logger.warning("Redis get failed key=%s: %s", key, exc)
            return None

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if self._client is None:
            return
        try:
            if ttl_seconds <= 0:
                await self.delete(key)
                return
            await self._client.set(key, json.dumps(value), ex=ttl_seconds)
        except (TypeError, ValueError) as exc:
            logger.warning("Redis set encode failed key=%s: %s", key, exc)
        except _REDIS_SOFT_ERRORS as exc:
            logger.warning("Redis set failed key=%s: %s", key, exc)

    async def delete(self, key: str) -> None:
        if self._client is None:
            return
        try:
            await self._client.delete(key)
        except _REDIS_SOFT_ERRORS as exc:
            logger.warning("Redis delete failed key=%s: %s", key, exc)
