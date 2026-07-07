import logging
import redis
from router import select_processor, processor_id_from_url
from registry import get_processor_urls

logger = logging.getLogger("load-balancer.requeue")

async def requeue_orphaned_jobs(redis_url: str, dead_processor_id: str):
    """
    Safely migrates pending jobs from a dead processor's queue to active processors.
    Pops jobs individually so the queue naturally empties without explicit deletion.
    """
    r = redis.from_url(redis_url, decode_responses=True)
    dead_queue_key = f"queue:{dead_processor_id}"

    while True:
        job_id = r.rpop(dead_queue_key)
        if not job_id:
            break

        job_key = f"job:{job_id}"
        job_data = r.hgetall(job_key)

        if not job_data or job_data.get("status") != "pending":
            continue

        active_urls = get_processor_urls(
            r,
            processor_type="local",
            statuses={"active"}
        )

        try:
            new_processor_url = select_processor(active_urls, r)
        except ValueError as exc:
            logger.error(f"[{job_id}] Requeue failed: {exc}. Returning to dead queue.")
            # Return job to queue and abort requeue process if no routing targets exist.
            r.lpush(dead_queue_key, job_id)
            break

        new_processor_id = processor_id_from_url(new_processor_url)
        new_queue_key = f"queue:{new_processor_id}"

        pipeline = r.pipeline(transaction=True)
        pipeline.hset(job_key, mapping={
            "processor_id": new_processor_id,
            "processor_url": new_processor_url,
            "route_reason": "requeued_from_dead_processor"
        })
        pipeline.lpush(new_queue_key, job_id)
        
        try:
            pipeline.execute()
            logger.info(f"[{job_id}] Requeued from {dead_processor_id} to {new_processor_id}")
        except Exception as exc:
            logger.error(f"[{job_id}] Pipeline failure during requeue: {exc}")
            r.lpush(dead_queue_key, job_id)
