"""
App-wide cache singleton — same lifecycle idea as src.utils.http_client.

Built once from Settings at import time; connect()/close() run in main lifespan.
Do not construct a new cache inside per-request Depends factories.
"""

from __future__ import annotations

import logging

from src.cache.null_cache import NullCache
from src.cache.protocol import CacheBackend
from src.cache.redis_cache import RedisCache
from src.core import settings

logger = logging.getLogger(__name__)


def build_cache() -> CacheBackend:
    """Pick Redis when enabled + URL set; otherwise NullCache (always-miss)."""
    if settings.CACHE_ENABLED and settings.REDIS_URL:
        logger.info("Cache backend: Redis (%s)", settings.REDIS_URL)
        return RedisCache(settings.REDIS_URL)
    logger.info("Cache backend: NullCache (CACHE_ENABLED=%s)", settings.CACHE_ENABLED)
    return NullCache()


# Module-level singleton — one per uvicorn worker process
cache: CacheBackend = build_cache()
