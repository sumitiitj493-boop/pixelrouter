# PixelRouter — Processor Metrics
# Responsibility: Collect and report CPU, RAM, and job count metrics.
#                 Runs as a background task every 2 seconds.
#                 Writes to Redis with short TTL so dead processors
#                 are automatically detected by the load balancer.
#
# TODO : Implement start_metrics_heartbeat()

import psutil
import asyncio


async def start_metrics_heartbeat(processor_id: str, redis_client,
                                   interval: int = 2):
    """
    Background task that writes CPU and pending job metrics
    to Redis every 'interval' seconds.
    Each key has a 10s TTL — if processor dies, keys expire automatically.
    Load balancer treats expired keys as offline processors.
    TODO: Implement on Upcoming Days
    """
    while True:
        cpu = psutil.cpu_percent(interval=0.5)
        # TODO: Write metrics:{processor_id}:cpu to Redis with ex=10
        # TODO: Write metrics:{processor_id}:pending to Redis with ex=10
        await asyncio.sleep(interval)
