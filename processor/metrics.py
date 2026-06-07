# PixelRouter — Processor Metrics Heartbeat

import psutil
import asyncio
import logging

logger = logging.getLogger("metrics")


async def start_metrics_heartbeat(
    processor_id: str,
    redis_client,
    interval: int = 2
) -> None:
    """
    Writes CPU/RAM/pending metrics to Redis every interval seconds.
    Keys have a 10s TTL — if this processor dies, the load balancer
    sees them disappear and stops routing traffic here. No explicit
    "I'm dead" signal needed; the absence of data is the signal.
    """
    logger.info(f"Metrics heartbeat started for {processor_id}, interval={interval}s")

    while True:
        try:
            cpu = psutil.cpu_percent(interval=0.5)  # 0.5s sample — interval=0 returns 0.0
            ram = psutil.virtual_memory().percent

            pending_raw = redis_client.get(f"metrics:{processor_id}:pending")
            pending = int(pending_raw) if pending_raw else 0

            # Write all three metrics with 10s TTL
            redis_client.set(f"metrics:{processor_id}:cpu", cpu, ex=10)
            redis_client.set(f"metrics:{processor_id}:ram", ram, ex=10)
            redis_client.set(f"metrics:{processor_id}:pending", pending, ex=10)

            logger.debug(
                f"[{processor_id}] CPU:{cpu}% RAM:{ram}% Pending:{pending}"
            )

        except Exception as e:
            # Don't let a Redis blip kill the loop
            logger.error(f"Heartbeat error: {e}")

        await asyncio.sleep(interval)
