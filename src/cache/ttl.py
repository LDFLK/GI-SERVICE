"""TTL selection + jitter so hot keys do not all expire at once."""

from __future__ import annotations

import random
from datetime import datetime, timezone, timedelta

# Defaults until Settings / env wiring is added
CACHE_TTL_HISTORICAL_SECONDS = 21_600  # 6h
CACHE_TTL_RECENT_SECONDS = 120  # 2m
CACHE_TTL_ENTITY_SECONDS = 3_600  # 1h
CACHE_TTL_NEGATIVE_SECONDS = 45


def _parse_active_at(active_at: str | None) -> datetime | None:
    if not active_at:
        return None
    ts = active_at.strip()
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        if "T" not in ts:
            try:
                return datetime.fromisoformat(f"{ts}T00:00:00+00:00").astimezone(timezone.utc)
            except ValueError:
                return None
        return None


def choose_ttl(
    active_at: str | None = None,
    *,
    historical_seconds: int = CACHE_TTL_HISTORICAL_SECONDS,
    recent_seconds: int = CACHE_TTL_RECENT_SECONDS,
    entity_seconds: int = CACHE_TTL_ENTITY_SECONDS,
) -> int:
    """
    Historical dates (before yesterday UTC) get a long TTL.
    Recent / today / missing date get a short or entity default TTL.
    """
    dt = _parse_active_at(active_at)
    if dt is None:
        return entity_seconds

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if dt.date() < yesterday:
        return historical_seconds
    return recent_seconds


def apply_jitter(ttl_seconds: int, *, fraction: float = 0.10) -> int:
    """Return ttl ± fraction (default 10%), never below 1 when ttl >= 1."""
    if ttl_seconds <= 0:
        return 0
    delta = ttl_seconds * fraction
    jittered = int(round(ttl_seconds + random.uniform(-delta, delta)))
    return max(1, jittered)
