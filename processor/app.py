# PixelRouter — Processor Service
# Responsibility: Pick jobs from Redis queue, run AI pipeline,
#                 stream progress via WebSocket, write results to GCS,
#                 expose /metrics for load balancer polling.

from fastapi import FastAPI, WebSocket
import redis
import os
import asyncio
import psutil
from contextlib import asynccontextmanager
from transformers import BlipProcessor, BlipForConditionalGeneration
from pipeline import init_models   # our new pipeline.py function
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup/shutdown hook.
    Models load here so a failure stops the container from accepting traffic
    and triggers a Docker restart instead of silently running broken.
    """
    # ── STARTUP ──────────────────────────────────────────────────────────
    logger = logging.getLogger("processor.startup")
    logger.info("Loading BLIP-base model... (this takes 15–30 seconds)")

    # from_pretrained is synchronous and slow — offload to a thread
    # so we don't freeze the event loop during the 15–30s load time.
    loop = asyncio.get_event_loop()
    from concurrent.futures import ThreadPoolExecutor
    startup_executor = ThreadPoolExecutor(max_workers=1)

    blip_processor = await loop.run_in_executor(
        startup_executor,
        BlipProcessor.from_pretrained,
        "Salesforce/blip-image-captioning-base"
    )

    blip_model = await loop.run_in_executor(
        startup_executor,
        BlipForConditionalGeneration.from_pretrained,
        "Salesforce/blip-image-captioning-base"
    )

    # Wire the loaded models into pipeline.py
    init_models(blip_processor, blip_model)
    logger.info("BLIP-base loaded and ready. Processor accepting jobs.")

    # ADDED: Start periodic metrics heartbeat to Redis
    from metrics import start_metrics_heartbeat
    heartbeat_task = asyncio.create_task(
        start_metrics_heartbeat(PROCESSOR_ID, r)
    )

    yield  # app is live from here

    # ── SHUTDOWN ─────────────────────────────────────────────────────────
    logger.info("Processor shutting down")
    heartbeat_task.cancel()
    startup_executor.shutdown(wait=False)


app = FastAPI(
    title="PixelRouter — Processor",
    description="AI image processing worker",
    version="0.1.0",
    lifespan=lifespan
)

import logging


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
