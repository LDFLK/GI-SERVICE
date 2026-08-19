"""Method decorator that caches OpenGIN fetch results via SingleFlight + Redis.

``@cached`` must wrap the retry decorator (outer cache, inner retry) so cache
hits skip HTTP and retries apply only on cache miss.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from pydantic import TypeAdapter

from src.cache.ttl import apply_jitter
from src.core import settings

logger = logging.getLogger(__name__)

T = TypeVar("T")
KeyBuilder = Callable[..., str]


def cached(
    *,
    key_builder: KeyBuilder,
    return_type: type[T],
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Cache a method that returns any JSON-serializable or Pydantic type."""
    adapter = TypeAdapter(return_type)

    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(fn)
        async def wrapper(self, *args: Any, **kwargs: Any) -> T:
            key = key_builder(self, *args, **kwargs)
            ttl = apply_jitter(settings.CACHE_TTL_SECONDS)
            logger.debug("%s key=%s", fn.__name__, key)

            async def fetch() -> Any:
                result = await fn(self, *args, **kwargs)
                return adapter.dump_python(result, mode="json")

            raw = await self._sf.get_or_fetch(
                key, cache=self._cache, fetch=fetch, ttl_seconds=ttl
            )
            return adapter.validate_python(raw)

        return wrapper

    return decorator
