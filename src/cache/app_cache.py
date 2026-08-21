"""
App-wide cache + single-flight singletons — same lifecycle idea as http_client.

Built once per uvicorn worker; connect()/close() run in main lifespan.
Do not construct a new cache/SingleFlight inside per-request Depends factories.
"""

from __future__ import annotations

import logging

from src.cache.null_cache import NullCache
from src.cache.protocol import CacheBackend
from src.cache.redis_cache import RedisCache
from src.cache.singleflight import SingleFlight
from src.core import settings

logger = logging.getLogger(__name__)


def build_cache() -> CacheBackend:
    """Pick Redis when enabled + URL set; otherwise NullCache (always-miss)."""
    if settings.CACHE_ENABLED and settings.REDIS_URL:
        logger.info("Cache backend: Redis (%s)", settings.REDIS_URL)
        return RedisCache(settings.REDIS_URL)
    logger.info("Cache backend: NullCache (CACHE_ENABLED=%s)", settings.CACHE_ENABLED)
    return NullCache()


# Module-level singletons — one per uvicorn worker process
cache: CacheBackend = build_cache()
# Shared so concurrent requests in this worker coalesce on the same Future map
singleflight = SingleFlight()


async def connect_cache() -> None:
    """Open Redis (if any) and attach client to SingleFlight for cross-worker locks.

    When CACHE_ENABLED selects Redis, connect failures propagate and abort startup.
    Use CACHE_ENABLED=false if you intentionally want to run without Redis.
    Runtime get/set still fail-open if Redis dies after a successful connect.
    """
    await cache.connect()
    if isinstance(cache, RedisCache):
        singleflight.bind_redis(cache.client)


async def close_cache() -> None:
    if isinstance(cache, RedisCache):
        singleflight.bind_redis(None)
    await cache.close()
