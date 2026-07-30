import asyncio
from datetime import datetime, timezone, timedelta

import pytest

from src.cache import (
    CACHE_TTL_ENTITY_SECONDS,
    CACHE_TTL_HISTORICAL_SECONDS,
    CACHE_TTL_RECENT_SECONDS,
    InMemoryCache,
    NullCache,
    apply_jitter,
    attributes_key,
    choose_ttl,
    entity_key,
    relation_key,
)


def test_entity_key_format():
    assert entity_key("abc") == "gi:v1:entity:abc"


def test_relation_key_stable_regardless_of_dict_order():
    a = {"name": "HAS_CHILD", "direction": "OUT", "activeAt": "2020-01-01T00:00:00Z"}
    b = {"activeAt": "2020-01-01T00:00:00Z", "direction": "OUT", "name": "HAS_CHILD"}
    assert relation_key("org-1", a) == relation_key("org-1", b)


def test_attributes_key_normalizes_dates_and_sorts_fields():
    k1 = attributes_key(
        "cat",
        "budget",
        start_time="2022-05-05",
        fields=["b", "a"],
        filters={"x": 1},
    )
    k2 = attributes_key(
        "cat",
        "budget",
        start_time="2022-05-05T00:00:00Z",
        fields=["a", "b"],
        filters={"x": 1},
    )
    assert k1 == k2


def test_choose_ttl_historical():
    old = (datetime.now(timezone.utc).date() - timedelta(days=30)).isoformat()
    assert choose_ttl(old) == CACHE_TTL_HISTORICAL_SECONDS


def test_choose_ttl_recent():
    today = datetime.now(timezone.utc).date().isoformat()
    assert choose_ttl(today) == CACHE_TTL_RECENT_SECONDS


def test_choose_ttl_missing_uses_entity_default():
    assert choose_ttl(None) == CACHE_TTL_ENTITY_SECONDS


def test_apply_jitter_stays_within_10_percent():
    ttl = 1000
    for _ in range(50):
        jittered = apply_jitter(ttl, fraction=0.10)
        assert 900 <= jittered <= 1100


def test_apply_jitter_zero():
    assert apply_jitter(0) == 0


@pytest.mark.asyncio
async def test_null_cache_always_misses():
    cache = NullCache()
    await cache.connect()
    await cache.set("k", {"a": 1}, ttl_seconds=60)
    assert await cache.get("k") is None
    await cache.close()


@pytest.mark.asyncio
async def test_memory_cache_hit_and_delete():
    cache = InMemoryCache()
    await cache.set("k", {"a": 1}, ttl_seconds=60)
    assert await cache.get("k") == {"a": 1}
    await cache.delete("k")
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_memory_cache_expires():
    cache = InMemoryCache()
    await cache.set("k", "v", ttl_seconds=1)
    assert await cache.get("k") == "v"
    await asyncio.sleep(1.05)
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_memory_cache_zero_ttl_is_miss():
    cache = InMemoryCache()
    await cache.set("k", "v", ttl_seconds=0)
    assert await cache.get("k") is None
