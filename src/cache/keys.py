"""Stable cache key builders. Prefix bump (v1 -> v2) invalidates the whole cache."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from src.utils import Util

DEFAULT_KEY_PREFIX = "gi:v1"


def _stable_hash(payload: Any) -> str:
    """Hash JSON with sorted keys so dict key order cannot fork cache keys."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def entity_key(entity_id: str, *, prefix: str = DEFAULT_KEY_PREFIX) -> str:
    return f"{prefix}:entity:{entity_id}"


def relation_key(
    entity_id: str,
    relation: dict[str, Any],
    *,
    prefix: str = DEFAULT_KEY_PREFIX,
) -> str:
    """relation should already be model_dump(mode='json') (or equivalent dict)."""
    digest = _stable_hash(relation)
    return f"{prefix}:relation:{entity_id}:{digest}"


def attributes_key(
    category_id: str,
    dataset_name: str,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    fields: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    prefix: str = DEFAULT_KEY_PREFIX,
) -> str:
    payload = {
        "startTime": Util.normalize_timestamp(start_time),
        "endTime": Util.normalize_timestamp(end_time),
        "fields": sorted(fields) if fields else None,
        "filters": filters or {},
    }
    digest = _stable_hash(payload)
    return f"{prefix}:attr:{category_id}:{dataset_name}:{digest}"


def metadata_key(entity_id: str, *, prefix: str = DEFAULT_KEY_PREFIX) -> str:
    return f"{prefix}:metadata:{entity_id}"
