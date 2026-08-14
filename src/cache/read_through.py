"""Read-through cache decorators for OpenGIN service methods.

``@read_through_list`` / ``@read_through_value`` must wrap the retry decorator
(outer cache, inner retry) so cache hits skip HTTP and retries apply only on miss.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from pydantic import BaseModel

from src.cache.ttl import apply_jitter
from src.core import settings

logger = logging.getLogger(__name__)

TModel = TypeVar("TModel", bound=BaseModel)
TValue = TypeVar("TValue")
KeyBuilder = Callable[..., str]


def read_through_list(
    *,
    key_builder: KeyBuilder,
    model: type[TModel],
) -> Callable[
    [Callable[..., Awaitable[list[TModel]]]],
    Callable[..., Awaitable[list[TModel]]],
]:
    """Cache a method that returns ``list[model]``. Redis stores JSON dicts."""

    def decorator(
        fn: Callable[..., Awaitable[list[TModel]]],
    ) -> Callable[..., Awaitable[list[TModel]]]:
        @wraps(fn)
        async def wrapper(self, *args: Any, **kwargs: Any) -> list[TModel]:
            key = key_builder(self, *args, **kwargs)
            ttl = apply_jitter(settings.CACHE_TTL_SECONDS)
            logger.debug("%s key=%s", fn.__name__, key)

            async def fetch() -> list[dict[str, Any]]:
                models = await fn(self, *args, **kwargs)
                return [item.model_dump(mode="json") for item in models]

            raw = await self._sf.get_or_fetch(
                key, cache=self._cache, fetch=fetch, ttl_seconds=ttl
            )
            return [model.model_validate(item) for item in raw]

        return wrapper

    return decorator


def read_through_value(
    *,
    key_builder: KeyBuilder,
) -> Callable[[Callable[..., Awaitable[TValue]]], Callable[..., Awaitable[TValue]]]:
    """Cache a method that already returns JSON-serializable data."""

    def decorator(
        fn: Callable[..., Awaitable[TValue]],
    ) -> Callable[..., Awaitable[TValue]]:
        @wraps(fn)
        async def wrapper(self, *args: Any, **kwargs: Any) -> TValue:
            key = key_builder(self, *args, **kwargs)
            ttl = apply_jitter(settings.CACHE_TTL_SECONDS)
            logger.debug("%s key=%s", fn.__name__, key)

            async def fetch() -> TValue:
                return await fn(self, *args, **kwargs)

            return await self._sf.get_or_fetch(
                key, cache=self._cache, fetch=fetch, ttl_seconds=ttl
            )

        return wrapper

    return decorator
