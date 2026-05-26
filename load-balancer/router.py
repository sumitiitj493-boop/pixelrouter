# PixelRouter — Routing Logic
# This module contains the CPU-aware routing algorithm.
# Called by load-balancer/app.py to decide which processor gets a job.
#
# Routing strategy:
#   1. Read metrics for all processors from Redis
#   2. Sort by pending_jobs (ascending)
#   3. Use CPU% as tiebreaker
#   4. If all processors above MAX_CPU_THRESHOLD → trigger autoscale
#
# Race condition protection:
#   - threading.Lock() on the pending job counter
#   - SET NX EX on job claim (distributed lock)
#
# TODO : Implement select_processor()
# TODO : Implement update_pending_count()
# TODO : Implement trigger_autoscale()

import threading

_lock = threading.Lock()

def select_processor(processor_urls: list, redis_client) -> str:
    """
    Returns the URL of the best processor to handle the next job.
    Placeholder — implement on Upcoming Days.
    """
    # TODO: Read metrics:processor-N:cpu and metrics:processor-N:pending
    # TODO: Sort by pending_jobs, CPU% as tiebreaker
    # TODO: Return URL of winner
    raise NotImplementedError("Implement on Upcoming Days.")


def update_pending_count(processor_id: str, delta: int, redis_client):
    """
    Thread-safe update of pending job count for a processor.
    delta = +1 when job assigned, -1 when job completes.
    """
    with _lock:
        # TODO: INCRBY metrics:processor-id:pending delta
        raise NotImplementedError("Implement on Upcoming Days.")


def trigger_autoscale(redis_client):
    """
    Spawn a new processor container via Docker SDK
    when all processors exceed MAX_CPU_THRESHOLD.
    TODO: Implement on Upcoming Days using docker.from_env()
    """
    raise NotImplementedError("Implement on Upcoming Days")
