"""Cache method decorators: cache hits skip the wrapped fetch."""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from src.cache import InMemoryCache, SingleFlight, cache_list, cache_value
from src.exception import NotFoundError


class Item(BaseModel):
    id: str


class DummyService:
    def __init__(self, cache, singleflight):
        self._cache = cache
        self._sf = singleflight
        self.fetch_list = AsyncMock(return_value=[Item(id="1")])
        self.fetch_value = AsyncMock(return_value={"ok": True})

    @cache_list(key_builder=lambda _self, item_id: f"list:{item_id}", model=Item)
    async def get_items(self, item_id: str) -> list[Item]:
        return await self.fetch_list(item_id)

    @cache_value(key_builder=lambda _self, item_id: f"val:{item_id}")
    async def get_payload(self, item_id: str) -> dict:
        return await self.fetch_value(item_id)


@pytest.mark.asyncio
async def test_cache_list_second_call_is_cache_hit():
    service = DummyService(InMemoryCache(), SingleFlight())

    first = await service.get_items("a")
    second = await service.get_items("a")

    assert first == second == [Item(id="1")]
    assert service.fetch_list.await_count == 1


@pytest.mark.asyncio
async def test_cache_list_does_not_cache_errors():
    service = DummyService(InMemoryCache(), SingleFlight())
    service.fetch_list = AsyncMock(side_effect=NotFoundError("missing"))

    with pytest.raises(NotFoundError):
        await service.get_items("a")

    service.fetch_list = AsyncMock(return_value=[Item(id="recovered")])
    result = await service.get_items("a")

    assert result == [Item(id="recovered")]


@pytest.mark.asyncio
async def test_cache_value_second_call_is_cache_hit():
    service = DummyService(InMemoryCache(), SingleFlight())

    first = await service.get_payload("a")
    second = await service.get_payload("a")

    assert first == second == {"ok": True}
    assert service.fetch_value.await_count == 1
