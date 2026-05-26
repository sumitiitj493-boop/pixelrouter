# PixelRouter — Processor Service
# Responsibility: Pick jobs from Redis queue, run AI pipeline,
#                 stream progress via WebSocket, write results to GCS,
#                 expose /metrics for load balancer polling.

from fastapi import FastAPI, WebSocket
import redis
import os
import asyncio
import psutil

app = FastAPI(
    title="PixelRouter — Processor",
    description="AI image processing worker",
    version="0.1.0"
)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PROCESSOR_ID = os.getenv("PROCESSOR_ID", "processor-1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "pixelrouter-images")

r = redis.from_url(REDIS_URL, decode_responses=True)


@app.get("/")
async def root():
    return {
        "service": "processor",
        "processor_id": PROCESSOR_ID,
        "version": "0.1.0"
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "processor_id": PROCESSOR_ID
    }


@app.get("/metrics")
async def get_metrics():
    """
    Exposes live CPU and memory metrics.
    Called by load balancer every 2 seconds.
    Also writes metrics to Redis for persistence.
    """
    cpu_percent = psutil.cpu_percent(interval=0.5)
    ram_percent = psutil.virtual_memory().percent
    pending_jobs = int(r.get(f"metrics:{PROCESSOR_ID}:pending") or 0)

    # Write to Redis with 10s TTL
    # If this processor dies, keys expire and load balancer knows
    r.set(f"metrics:{PROCESSOR_ID}:cpu", cpu_percent, ex=10)
    r.set(f"metrics:{PROCESSOR_ID}:pending", pending_jobs, ex=10)

    return {
        "processor_id": PROCESSOR_ID,
        "cpu_percent": cpu_percent,
        "ram_percent": ram_percent,
        "pending_jobs": pending_jobs
    }


@app.websocket("/ws/{job_id}")
async def websocket_progress(websocket: WebSocket, job_id: str):
    """
    Streams processing progress to client in real time.
    Emits stage updates: uploaded → removing_bg → captioning → done
    TODO Upcoming Days: Implement real progress streaming
    """
    await websocket.accept()
    await websocket.send_json({
        "job_id": job_id,
        "stage": "connected",
        "message": "WebSocket connected — processing not yet implemented"
    })
    await websocket.close()
