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
    # First caller (leader) gets the lock; subsequent acquires fail until lock released
    acquire_results = iter([True, False, False, False])
    redis.set = AsyncMock(side_effect=lambda *a, **k: next(acquire_results, False))
    redis.eval = AsyncMock(return_value=1)

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
    # Leader released via compare-and-del Lua, not unconditional DELETE
    redis.eval.assert_awaited()
    redis.delete.assert_not_called()


@pytest.mark.asyncio
async def test_release_lock_uses_owner_token():
    """Release passes the acquisition token into the Lua script."""
    cache = InMemoryCache()
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(return_value=1)

    sf = SingleFlight(redis_client=redis)
    fetch = AsyncMock(return_value={"ok": True})

    await sf.get_or_fetch("k", cache=cache, fetch=fetch, ttl_seconds=60)

    assert redis.set.await_count == 1
    # redis.set(lock_key, token, nx=True, ex=...)
    lock_key, stored_token = redis.set.await_args.args[0], redis.set.await_args.args[1]
    assert stored_token != "1"
    assert len(stored_token) == 32  # uuid4.hex

    redis.eval.assert_awaited_once()
    eval_args = redis.eval.await_args.args
    assert eval_args[1] == 1  # numkeys
    assert eval_args[2] == lock_key
    assert eval_args[3] == stored_token


@pytest.mark.asyncio
async def test_canceled_follower_does_not_cancel_shared_future():
    """Cancelling a waiter must not cancel the shared Future or the leader."""
    cache = InMemoryCache()
    sf = SingleFlight()

    started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def slow_fetch():
        started.set()
        await release_fetch.wait()
        return {"ok": True}

    async def call():
        return await sf.get_or_fetch(
            "k", cache=cache, fetch=slow_fetch, ttl_seconds=60
        )

    leader_task = asyncio.create_task(call())
    await started.wait()

    follower_task = asyncio.create_task(call())
    # Let follower attach to the shared in-flight Future
    await asyncio.sleep(0)
    follower_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await follower_task

    release_fetch.set()
    assert await leader_task == {"ok": True}
    assert await cache.get("k") == {"ok": True}


@pytest.mark.asyncio
async def test_canceled_leader_cancels_waiters():
    """Leader cancellation completes the shared Future so waiters do not hang."""
    cache = InMemoryCache()
    sf = SingleFlight()

    started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def slow_fetch():
        started.set()
        await release_fetch.wait()
        return {"ok": True}

    async def call():
        return await sf.get_or_fetch(
            "k", cache=cache, fetch=slow_fetch, ttl_seconds=60
        )

    leader_task = asyncio.create_task(call())
    await started.wait()

    follower_task = asyncio.create_task(call())
    await asyncio.sleep(0)

    leader_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader_task

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(follower_task, timeout=1.0)

    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_lock_loser_does_not_fetch_without_lock():
    """After poll timeout, keep retrying — do not fetch while Redis lock is held."""
    cache = InMemoryCache()
    redis = AsyncMock()
    # Never grant the lock to this worker
    redis.set = AsyncMock(return_value=False)
    redis.eval = AsyncMock(return_value=1)

    sf = SingleFlight(
        redis_client=redis,
        poll_interval_seconds=0.01,
        poll_timeout_seconds=0.03,
    )
    fetch = AsyncMock(return_value={"should": "not-run"})

    async def fill_cache_later():
        await asyncio.sleep(0.08)
        await cache.set("k", {"from": "other"}, ttl_seconds=60)

    filler = asyncio.create_task(fill_cache_later())
    result = await sf.get_or_fetch("k", cache=cache, fetch=fetch, ttl_seconds=60)
    await filler

    assert result == {"from": "other"}
    fetch.assert_not_awaited()
    assert redis.set.await_count >= 2  # initial miss + at least one retry
