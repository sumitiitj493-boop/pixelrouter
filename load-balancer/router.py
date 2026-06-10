# PixelRouter - Routing Logic
# This module contains the CPU-aware routing algorithm.
# Called by load-balancer/app.py to decide which processor gets a job.
#
# Routing strategy:
#   1. Read metrics for all processors from Redis
#   2. Sort by pending_jobs (ascending)
#   3. Use CPU% as tiebreaker
#
# Race condition protection:
#   - threading.Lock() on the pending job counter
#   - Redis INCRBY for atomic counter updates

import threading
from urllib.parse import urlparse

METRICS_TTL_SECONDS = 10

_lock = threading.Lock()


def processor_id_from_url(processor_url: str) -> str:
    """
    Extract the Redis metrics processor id from a processor URL.
    Example: http://processor-1:8002 -> processor-1
    """
    parsed = urlparse(processor_url)
    return parsed.hostname or processor_url


def _parse_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def select_processor(processor_urls: list, redis_client) -> str:
    """
    Returns the URL of the best processor to handle the next job.
    This function has no scaling side effects; app.py owns endpoint flow,
    overload handling, and fallback decisions.
    Routing order:
      1. Skip processors without live CPU metrics
      2. Pick the processor with the fewest pending jobs
      3. Use lower CPU usage as the tiebreaker
    """
    candidates = []

    for processor_url in processor_urls:
        processor_id = processor_id_from_url(processor_url)
        cpu_percent = _parse_float(
            redis_client.get(f"metrics:{processor_id}:cpu")
        )

        if cpu_percent is None:
            continue

        pending_jobs = _parse_int(
            redis_client.get(f"metrics:{processor_id}:pending")
        )

        candidates.append({
            "url": processor_url,
            "processor_id": processor_id,
            "pending_jobs": max(0, pending_jobs),
            "cpu_percent": cpu_percent,
        })

    if not candidates:
        raise ValueError("No live processors available for routing")

    selected = min(
        candidates,
        key=lambda candidate: (
            candidate["pending_jobs"],
            candidate["cpu_percent"],
            candidate["processor_id"],
        )
    )
    return selected["url"]


def update_pending_count(processor_id: str, delta: int, redis_client):
    """
    Thread-safe update of pending job count for a processor.
    delta = +1 when job assigned, -1 when job completes.
    """
    with _lock:
        pending_key = f"metrics:{processor_id}:pending"
        new_count = redis_client.incrby(pending_key, delta)

        if new_count < 0:
            new_count = 0
            redis_client.set(
                pending_key,
                new_count,
                ex=METRICS_TTL_SECONDS
            )
        else:
            redis_client.expire(pending_key, METRICS_TTL_SECONDS)

        return new_count
