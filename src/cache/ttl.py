import random

"""TTL jitter so hot keys do not all expire at once."""
def apply_jitter(ttl_seconds: int, *, fraction: float = 0.10) -> int:
    """Return ttl ± fraction (default 10%), never below 1 when ttl >= 1."""
    if ttl_seconds <= 0:
        return 0
    delta = ttl_seconds * fraction
    jittered = int(round(ttl_seconds + random.uniform(-delta, delta)))
    return max(1, jittered)
