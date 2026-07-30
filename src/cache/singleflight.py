"""Coalesce concurrent fetches for the same cache key (stampede protection)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from src.cache.protocol import CacheBackend

logger = logging.getLogger(__name__)

T = TypeVar("T")
FetchFn = Callable[[], Awaitable[T]]


class SingleFlight:
    """
    One in-flight fetch per key within a process (shared asyncio.Future).

    If a redis.asyncio-compatible client is provided, also uses SET NX EX
    so only one worker across the fleet fetches; other workers poll the cache.
    """

    def __init__(
        self,
        *,
        redis_client: Any | None = None,
        lock_prefix: str = "gi:lock:",
        lock_ttl_seconds: int = 30,
        poll_interval_seconds: float = 0.05,
        poll_timeout_seconds: float = 30.0,
    ) -> None:
        self._inflight: dict[str, asyncio.Future[Any]] = {}
        self._redis = redis_client
        self._lock_prefix = lock_prefix
        self._lock_ttl = lock_ttl_seconds
        self._poll_interval = poll_interval_seconds
        self._poll_timeout = poll_timeout_seconds

    def _lock_key(self, key: str) -> str:
        return f"{self._lock_prefix}{key}"

    async def _try_acquire_lock(self, key: str) -> bool:
        if self._redis is None:
            return True
        # SET lock NX EX — only the first caller wins
        return bool(
            await self._redis.set(
                self._lock_key(key),
                "1",
                nx=True,
                ex=self._lock_ttl,
            )
        )

    async def _release_lock(self, key: str) -> None:
        if self._redis is None:
            return
        await self._redis.delete(self._lock_key(key))

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
            return await existing

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
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
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
        acquired = await self._try_acquire_lock(key)

        # If we didn't get the lock, wait for someone else to fill the cache.
        if not acquired:
            waited = await self._wait_for_cache(cache, key)
            if waited is not None:
                return waited
            # Timed out waiting for the other worker — try to take over
            acquired = await self._try_acquire_lock(key)

        try:
            # Double-check: another waiter may have filled the cache
            cached = await cache.get(key)
            if cached is not None:
                return cached

            # If we still could not get the lock or get a value from the cache, fetch the data ourselves.
            value = await fetch()
            if ttl_seconds > 0:
                await cache.set(key, value, ttl_seconds)
            return value
        finally:
            if acquired:
                await self._release_lock(key)
