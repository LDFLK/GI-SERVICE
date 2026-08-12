"""Coalesce concurrent fetches for the same cache key (stampede protection)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from src.cache.protocol import CacheBackend

logger = logging.getLogger(__name__)

T = TypeVar("T")
FetchFn = Callable[[], Awaitable[T]]

# Empty token: we proceeded without a Redis-owned lock (no redis / fail-open).
_NO_REDIS_LOCK = ""

# Delete lock only if the value still matches our token (atomic compare-and-del).
_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
"""


class SingleFlight:
    """
    One in-flight fetch per key within a process (shared asyncio.Future).

    If a redis.asyncio-compatible client is provided, also uses SET NX EX
    so only one worker across the fleet fetches; other workers poll the cache.
    Each acquisition stores a unique token; release deletes only when that
    token still owns the lock (Lua compare-and-del).
    """

    def __init__(
        self,
        *,
        redis_client: Any | None = None,
        lock_prefix: str = "gi:lock:",
        lock_ttl_seconds: int = 30,
        poll_interval_seconds: float = 0.05,
        # Shorter than lock TTL so losers fail over before the lock expires
        poll_timeout_seconds: float = 10.0,
    ) -> None:
        self._inflight: dict[str, asyncio.Future[Any]] = {}
        self._redis = redis_client
        self._lock_prefix = lock_prefix
        self._lock_ttl = lock_ttl_seconds
        self._poll_interval = poll_interval_seconds
        self._poll_timeout = poll_timeout_seconds

    def _lock_key(self, key: str) -> str:
        return f"{self._lock_prefix}{key}"

    def bind_redis(self, redis_client: Any | None) -> None:
        """Attach or detach the shared Redis client used for distributed locks."""
        self._redis = redis_client

    async def _try_acquire_lock(self, key: str) -> str | None:
        """Return owner token if acquired, None if another holder has the lock.

        ``_NO_REDIS_LOCK`` (empty str) means proceed without a Redis lock to release.
        """
        if self._redis is None:
            return _NO_REDIS_LOCK
        token = uuid.uuid4().hex
        try:
            # SET lock NX EX — only the first caller wins; value is our owner token
            acquired = await self._redis.set(
                self._lock_key(key),
                token,
                nx=True,
                ex=self._lock_ttl,
            )
            return token if acquired else None
        except Exception as exc:
            # Fail-open: skip distributed lock; local single-flight still applies
            logger.warning("Redis lock acquire failed key=%s: %s", key, exc)
            return _NO_REDIS_LOCK

    async def _release_lock(self, key: str, token: str) -> None:
        if self._redis is None or not token:
            return
        try:
            await self._redis.eval(
                _RELEASE_LOCK_LUA,
                1,
                self._lock_key(key),
                token,
            )
        except Exception as exc:
            logger.warning("Redis lock release failed key=%s: %s", key, exc)

    # Repeatedly poll the cache until we get a value or time out.
    async def _wait_for_cache(self, cache: CacheBackend, key: str) -> Any | None:
        """Poll until another worker populates the cache or we time out."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._poll_timeout
        while loop.time() < deadline:
            value = await cache.get(key)
            if value is not None:
                return value
            await asyncio.sleep(self._poll_interval)
        return None

    # This handles same-process deduplication of fetch calls.
    async def get_or_fetch(
        self,
        key: str,
        *,
        cache: CacheBackend,
        fetch: FetchFn[T],
        ttl_seconds: int,
    ) -> T:
        """
        Return cached value, or run fetch once while peers wait.

        On upstream failure the exception propagates and nothing is cached.
        """
        cached = await cache.get(key)
        if cached is not None:
            return cached

        existing = self._inflight.get(key)
        if existing is not None:
            # Shield so cancelling this caller does not cancel the shared Future
            return await asyncio.shield(existing)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._inflight[key] = fut

        try:
            result = await self._leader_fetch(
                key, cache=cache, fetch=fetch, ttl_seconds=ttl_seconds
            )
            if not fut.done():
                fut.set_result(result)
            return result
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
            raise
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
                # Avoid "Future exception was never retrieved" when no waiters joined
                fut.exception()
            raise
        finally:
            if self._inflight.get(key) is fut:
                del self._inflight[key]

    # This does the actual work of fetching the data.. Only one coroutine runs this (the leader who won the "local" race)
    # This is needed because there may be another worker process that is also trying to fetch the same key.
    async def _leader_fetch(
        self,
        key: str,
        *,
        cache: CacheBackend,
        fetch: FetchFn[T],
        ttl_seconds: int,
    ) -> T:
        token = await self._try_acquire_lock(key)

        # If we didn't get the lock, wait for someone else to fill the cache.
        # Keep polling/retrying until we acquire — do not fetch without a lock
        # while another worker still holds it (token is None). Fail-open returns
        # _NO_REDIS_LOCK ("") which is not None, so that path does not loop.
        while token is None:
            waited = await self._wait_for_cache(cache, key)
            if waited is not None:
                return waited
            # Retry until the current lock expires or its owner fills the cache.
            token = await self._try_acquire_lock(key)

        try:
            # Double-check: another waiter may have filled the cache
            cached = await cache.get(key)
            if cached is not None:
                return cached

            # If we still could not get a value from the cache, fetch the data ourselves.
            value = await fetch()
            if ttl_seconds > 0:
                await cache.set(key, value, ttl_seconds)
            return value
        finally:
            if token is not None:
                await self._release_lock(key, token)
