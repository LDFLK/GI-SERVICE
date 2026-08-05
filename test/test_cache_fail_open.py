"""Fail-open behaviour: Redis errors must not break get_or_fetch."""

from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from src.cache import InMemoryCache, RedisCache, SingleFlight
from src.cache import app_cache as app_cache_mod


@pytest.mark.asyncio
async def test_redis_cache_disconnected_get_set_are_noops():
    cache = RedisCache("redis://localhost:6379/0")
    assert await cache.get("k") is None
    await cache.set("k", {"a": 1}, ttl_seconds=60)
    await cache.delete("k")


@pytest.mark.asyncio
async def test_redis_cache_get_swallows_redis_errors():
    cache = RedisCache("redis://localhost:6379/0")
    cache._client = AsyncMock()
    cache._client.get = AsyncMock(side_effect=RedisConnectionError("down"))

    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_redis_cache_set_swallows_redis_errors():
    cache = RedisCache("redis://localhost:6379/0")
    cache._client = AsyncMock()
    cache._client.set = AsyncMock(side_effect=RedisConnectionError("down"))

    await cache.set("k", {"a": 1}, ttl_seconds=60)


@pytest.mark.asyncio
async def test_redis_cache_get_bad_json_is_miss():
    cache = RedisCache("redis://localhost:6379/0")
    cache._client = AsyncMock()
    cache._client.get = AsyncMock(return_value="not-json{")

    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_lock_acquire_fail_open_still_fetches():
    cache = InMemoryCache()
    redis = AsyncMock()
    redis.set = AsyncMock(side_effect=RedisConnectionError("lock down"))
    redis.eval = AsyncMock(side_effect=RedisConnectionError("lock down"))

    sf = SingleFlight(redis_client=redis)
    fetch = AsyncMock(return_value={"ok": True})

    result = await sf.get_or_fetch("k", cache=cache, fetch=fetch, ttl_seconds=60)

    assert result == {"ok": True}
    assert fetch.await_count == 1
    assert await cache.get("k") == {"ok": True}
    # Fail-open acquire never set a Redis lock, so release must not eval/delete
    redis.eval.assert_not_called()


@pytest.mark.asyncio
async def test_get_or_fetch_with_soft_redis_still_returns_value():
    """RedisCache that fails get/set still allows OpenGIN fetch to succeed."""
    redis_cache = RedisCache("redis://unused")
    redis_cache._client = AsyncMock()
    redis_cache._client.get = AsyncMock(side_effect=RedisConnectionError("down"))
    redis_cache._client.set = AsyncMock(side_effect=RedisConnectionError("down"))

    sf = SingleFlight()
    fetch = AsyncMock(return_value={"id": "1"})

    result = await sf.get_or_fetch("k", cache=redis_cache, fetch=fetch, ttl_seconds=60)

    assert result == {"id": "1"}
    assert fetch.await_count == 1


@pytest.mark.asyncio
async def test_connect_cache_fails_startup_when_redis_unreachable():
    redis = RedisCache("redis://localhost:9")  # nothing listening
    with patch.object(app_cache_mod, "cache", redis):
        with patch.object(app_cache_mod.singleflight, "_redis", None):
            with pytest.raises(Exception):
                await app_cache_mod.connect_cache()

    assert app_cache_mod.singleflight._redis is None
