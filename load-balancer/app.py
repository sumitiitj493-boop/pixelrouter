# PixelRouter — Load Balancer Service
# Responsibility: Route jobs to least-loaded processor.
#                 Poll processor metrics every 2 seconds.
#                 Trigger autoscaling when all processors are overloaded.

from fastapi import FastAPI
import httpx
import redis
import os
import asyncio

app = FastAPI(
    title="PixelRouter — Load Balancer",
    description="CPU-aware job routing across processor instances",
    version="0.1.0"
)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PROCESSOR_URLS = os.getenv(
    "PROCESSOR_URLS",
    "http://processor-1:8002,http://processor-2:8003"
).split(",")
MAX_CPU_THRESHOLD = int(os.getenv("MAX_CPU_THRESHOLD", "80"))

r = redis.from_url(REDIS_URL, decode_responses=True)


@app.get("/")
async def root():
    return {
        "service": "load-balancer",
        "version": "0.1.0",
        "processors_registered": len(PROCESSOR_URLS)
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "load-balancer"}


@app.get("/route")
async def get_best_processor():
    """
    Returns the URL of the least-loaded processor.
    Routing logic: lowest pending_jobs first, CPU% as tiebreaker.
    TODO: Implement CPU-aware routing logic in router.py
    TODO: Trigger autoscaling if all processors are above MAX_CPU_THRESHOLD
    """
    # Placeholder — router.py will implement the real logic
    return {
        "processor_url": PROCESSOR_URLS[0],
        "reason": "placeholder — routing logic coming in router.py"
    }


@app.get("/processors/status")
async def processors_status():
    """
    Returns current metrics for all registered processors.
    Reads from Redis metrics keys written by each processor.
    """
    statuses = []
    for url in PROCESSOR_URLS:
        processor_id = url.split("//")[1].split(":")[0]
        cpu = r.get(f"metrics:{processor_id}:cpu") or "unknown"
        pending = r.get(f"metrics:{processor_id}:pending") or "0"
        statuses.append({
            "processor_id": processor_id,
            "url": url,
            "cpu_percent": cpu,
            "pending_jobs": pending
        })
    return {"processors": statuses}
