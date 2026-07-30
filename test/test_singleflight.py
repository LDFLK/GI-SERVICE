import asyncio
from unittest.mock import AsyncMock

import pytest

from src.cache import InMemoryCache, SingleFlight


@pytest.mark.asyncio
async def test_concurrent_misses_call_fetch_once():
    cache = InMemoryCache()
    sf = SingleFlight()
    fetch = AsyncMock(return_value={"id": "1"})

    async def one():
        return await sf.get_or_fetch(
            "k",
            cache=cache,
            fetch=fetch,
            ttl_seconds=60,
        )

    results = await asyncio.gather(*[one() for _ in range(20)])

    assert fetch.await_count == 1
    assert all(r == {"id": "1"} for r in results)
    assert await cache.get("k") == {"id": "1"}


@pytest.mark.asyncio
async def test_cache_hit_skips_fetch():
    cache = InMemoryCache()
    await cache.set("k", {"cached": True}, ttl_seconds=60)
    sf = SingleFlight()
    fetch = AsyncMock(return_value={"fresh": True})

    result = await sf.get_or_fetch("k", cache=cache, fetch=fetch, ttl_seconds=60)

    assert result == {"cached": True}
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_failure_is_not_cached_and_retries():
    cache = InMemoryCache()
    sf = SingleFlight()
    fetch = AsyncMock(side_effect=[RuntimeError("boom"), {"ok": True}])

    with pytest.raises(RuntimeError, match="boom"):
        await sf.get_or_fetch("k", cache=cache, fetch=fetch, ttl_seconds=60)

    assert await cache.get("k") is None

    result = await sf.get_or_fetch("k", cache=cache, fetch=fetch, ttl_seconds=60)
    assert result == {"ok": True}
    assert fetch.await_count == 2


@pytest.mark.asyncio
async def test_redis_lock_loser_waits_for_cache():
    """Simulate another worker holding the lock; loser polls until cache fills."""
    cache = InMemoryCache()
    redis = AsyncMock()
    # First caller (leader) gets the lock; loser does not
    redis.set = AsyncMock(side_effect=[True, False])
    redis.delete = AsyncMock()

    sf_leader = SingleFlight(redis_client=redis, poll_interval_seconds=0.01)
    sf_loser = SingleFlight(redis_client=redis, poll_interval_seconds=0.01)

    started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def slow_fetch():
        started.set()
        await release_fetch.wait()
        return {"from": "leader"}

    async def leader():
        return await sf_leader.get_or_fetch(
            "shared",
            cache=cache,
            fetch=slow_fetch,
            ttl_seconds=60,
        )

    async def loser():
        await started.wait()
        return await sf_loser.get_or_fetch(
            "shared",
            cache=cache,
            fetch=AsyncMock(return_value={"from": "loser"}),
            ttl_seconds=60,
        )

    leader_task = asyncio.create_task(leader())
    loser_task = asyncio.create_task(loser())

    await started.wait()
    # Give loser time to miss lock and start polling
    await asyncio.sleep(0.05)
    release_fetch.set()

    leader_result, loser_result = await asyncio.gather(leader_task, loser_task)

    assert leader_result == {"from": "leader"}
    assert loser_result == {"from": "leader"}
    # Loser's fetch must not have won
    assert await cache.get("shared") == {"from": "leader"}
