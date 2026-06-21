# PixelRouter — Processor Service
# Responsibility: Pick jobs from Redis queue, run AI pipeline,
#                 stream progress via WebSocket, write results to GCS,
#                 expose /metrics for load balancer polling.
#
# A3 CHANGES:
#   - Added ConnectionManager: routes progress to connected browsers
#   - Added worker_loop(): BRPOP image_queue → run_pipeline() → update Redis
#   - Added minimal GCS download helper (full GCS in A4)
#   - Rewrote WebSocket endpoint: real connect/disconnect + state_sync
#   - Fixed import/definition order (logging, config, r now load first)

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Dict

import psutil
import redis
from fastapi import FastAPI, WebSocket
from fastapi.websockets import WebSocketDisconnect
from google.cloud import storage
from transformers import BlipProcessor, BlipForConditionalGeneration

from pipeline import init_models, run_pipeline
from metrics import start_metrics_heartbeat

logger = logging.getLogger("processor")
logging.basicConfig(level=logging.INFO)


# ── Config + clients — defined FIRST, before anything uses them ────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PROCESSOR_ID = os.getenv("PROCESSOR_ID", "processor-1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "pixelrouter-images")
QUEUE_KEY = f"queue:{PROCESSOR_ID}"
BRPOP_TIMEOUT = 5  # seconds — how often the loop wakes if queue is empty

r = redis.from_url(REDIS_URL, decode_responses=True)

# Thread pool for blocking calls in app.py:
# - BRPOP (blocking Redis call)
# - GCS download (blocking network I/O)
# - BLIP/rembg startup loading
# Separate from pipeline.py's own executor (max_workers=2) so
# queue-waiting never competes with active AI inference for a thread.
io_executor = ThreadPoolExecutor(max_workers=2)

_gcs_client = None


def _get_gcs_client():
    """Lazy singleton — created on first use, reused after."""
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client()
    return _gcs_client


def _download_blob_sync(gcs_path: str) -> bytes:
    """
    Downloads bytes from a gs:// path. Synchronous — call via run_in_executor.
    gcs_path example: gs://pixelrouter-images/uploads/job_abc123.jpg
    """
    bucket_name, blob_path = gcs_path.replace("gs://", "").split("/", 1)
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()


async def download_image(gcs_path: str) -> bytes:
    """Async wrapper — same run_in_executor pattern as pipeline.py (A1)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(io_executor, _download_blob_sync, gcs_path)


# ── Connection Manager ───────────────────────────────────────────────────────
class ConnectionManager:
    """
    Tracks active WebSocket connections, keyed by job_id.
    One browser tab per job_id (typical case) — but the dict naturally
    handles "no one watching" (key absent) without special-casing.
    """

    def __init__(self):
        self.active: Dict[str, WebSocket] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active[job_id] = websocket
        logger.info(f"[WS] connected job={job_id} (active={len(self.active)})")

    def disconnect(self, job_id: str):
        self.active.pop(job_id, None)
        logger.info(f"[WS] disconnected job={job_id} (active={len(self.active)})")

    async def send(self, job_id: str, data: dict):
        """
        Send to job_id's socket IF one is connected. Silent no-op otherwise —
        processing must never depend on someone watching.
        """
        ws = self.active.get(job_id)
        if ws is None:
            return
        try:
            await ws.send_json(data)
        except Exception:
            # socket died without a clean disconnect event — clean up
            self.disconnect(job_id)


manager = ConnectionManager()


# ── Worker Loop ──────────────────────────────────────────────────────────────
def _brpop_sync(timeout: int):
    """
    BRPOP is a blocking Redis call — synchronous redis-py client blocks
    the calling THREAD for up to `timeout` seconds.
    Run via run_in_executor so it doesn't block the event loop.
    Returns (queue_name, job_id) tuple, or None if timeout with no job.
    """
    return r.brpop(QUEUE_KEY, timeout=timeout)


async def worker_loop():
    """
    Runs forever as a background task (started in lifespan).
    Each iteration:
      1. BRPOP image_queue (blocks up to BRPOP_TIMEOUT seconds)
      2. If a job_id arrived, process it fully
      3. If timeout (no job), loop again — lets us check for cancellation
    """
    logger.info(f"[{PROCESSOR_ID}] Worker loop started, watching {QUEUE_KEY}")
    loop = asyncio.get_event_loop()

    while True:
        try:
            result = await loop.run_in_executor(io_executor, _brpop_sync, BRPOP_TIMEOUT)
        except asyncio.CancelledError:
            logger.info(f"[{PROCESSOR_ID}] Worker loop cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[{PROCESSOR_ID}] BRPOP error: {e}")
            await asyncio.sleep(1)  # brief backoff before retry
            continue

        if result is None:
            # BRPOP timed out — queue empty. Normal. Loop again.
            continue

        _queue_name, job_id = result
        logger.info(f"[{PROCESSOR_ID}] Picked up job: {job_id}")
        await process_job(job_id)


async def process_job(job_id: str):
    """
    Full lifecycle for one job:
      1. Mark as processing, claim with this processor's ID
      2. Read image_url from job hash, download bytes from GCS
      3. Run AI pipeline (A1) with a real progress_callback
      4. Mark as done (result upload to GCS is A4 — for now we
         store the caption + flag; result_bytes handling completes in A4)
    Errors at any stage mark the job as "failed" with an error message
    rather than crashing the worker loop — one bad job must not take
    down the whole processor.
    """
    # Step 1 — claim the job
    r.hset(f"job:{job_id}", mapping={
        "status": "processing",
        "processor": PROCESSOR_ID,
    })
    # Increment this processor's pending counter — read by /metrics and
    # by load-balancer's routing decision (B1/B2)
    r.incr(f"metrics:{PROCESSOR_ID}:pending")

    try:
        # Step 2 — get the image
        job_data = r.hgetall(f"job:{job_id}")
        image_url = job_data.get("image_url")
        if not image_url:
            raise ValueError("job hash missing image_url")

        image_bytes = await download_image(image_url)
        logger.info(f"[{job_id}] Downloaded {len(image_bytes)} bytes from {image_url}")

        # Step 3 — progress callback: Redis (durable) + WebSocket (live)
        async def on_progress(stage: str, progress: int):
            r.hset(f"job:{job_id}", mapping={"stage": stage, "progress": progress})
            await manager.send(job_id, {
                "type": "progress",
                "job_id": job_id,
                "stage": stage,
                "progress": progress,
            })

        result = await run_pipeline(
            job_id=job_id,
            image_bytes=image_bytes,
            progress_callback=on_progress,
        )

        # Step 4 — done (GCS upload of result_bytes is A4's job)
        # For now: record caption + status so the rest of the system
        # (dashboard, /job/{id}) already has something meaningful to show.
        r.hset(f"job:{job_id}", mapping={
            "status": "done",
            "caption": result["caption"],
        })
        await manager.send(job_id, {
            "type": "complete",
            "job_id": job_id,
            "caption": result["caption"],
        })
        logger.info(f"[{job_id}] Done. Caption: {result['caption']}")

    except Exception as e:
        # One bad job (corrupt image, missing GCS object, model error, etc.)
        # must not crash the worker loop — mark failed and move on.
        # B4 (fault tolerance) builds retry/DLQ logic on top of this status.
        logger.error(f"[{job_id}] Pipeline failed: {e}")
        r.hset(f"job:{job_id}", mapping={"status": "failed", "error": str(e)})
        await manager.send(job_id, {
            "type": "error",
            "job_id": job_id,
            "error": str(e),
        })

    finally:
        # Always decrement pending — success or failure, the slot frees up
        r.decr(f"metrics:{PROCESSOR_ID}:pending")


# ── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    STARTUP order:
      1. Load BLIP (A1, unchanged)
      2. init_models() (A1, unchanged)
      3. Start metrics heartbeat (A2, unchanged)
      4. Start worker loop (NEW — A3)
    SHUTDOWN: cancel both background tasks cleanly.
    """
    logger.info("Loading BLIP-base model... (this takes 15-30 seconds)")

    loop = asyncio.get_event_loop()
    startup_executor = ThreadPoolExecutor(max_workers=1)

    blip_processor = await loop.run_in_executor(
        startup_executor,
        BlipProcessor.from_pretrained,
        "Salesforce/blip-image-captioning-base",
    )
    blip_model = await loop.run_in_executor(
        startup_executor,
        BlipForConditionalGeneration.from_pretrained,
        "Salesforce/blip-image-captioning-base",
    )
    init_models(blip_processor, blip_model)
    logger.info("BLIP-base loaded and ready.")

    heartbeat_task = asyncio.create_task(start_metrics_heartbeat(PROCESSOR_ID, r))
    worker_task = asyncio.create_task(worker_loop())
    logger.info(f"[{PROCESSOR_ID}] Processor fully started — accepting jobs.")

    yield

    logger.info("Processor shutting down")
    worker_task.cancel()
    heartbeat_task.cancel()
    startup_executor.shutdown(wait=False)
    io_executor.shutdown(wait=False)


app = FastAPI(
    title="PixelRouter — Processor",
    description="AI image processing worker",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Basic endpoints (unchanged from A1/A2) ──────────────────────────────────
@app.get("/")
async def root():
    return {"service": "processor", "processor_id": PROCESSOR_ID, "version": "0.1.0"}


@app.get("/health")
async def health():
    return {"status": "ok", "processor_id": PROCESSOR_ID}


@app.get("/metrics")
async def get_metrics():
    cpu_percent = psutil.cpu_percent(interval=0.5)
    ram_percent = psutil.virtual_memory().percent
    pending_jobs = int(r.get(f"metrics:{PROCESSOR_ID}:pending") or 0)

    r.set(f"metrics:{PROCESSOR_ID}:cpu", cpu_percent, ex=10)
    r.set(f"metrics:{PROCESSOR_ID}:pending", pending_jobs, ex=10)

    return {
        "processor_id": PROCESSOR_ID,
        "cpu_percent": cpu_percent,
        "ram_percent": ram_percent,
        "pending_jobs": pending_jobs,
    }


# ── WebSocket — NEW real implementation ─────────────────────────────────────
@app.websocket("/ws/{job_id}")
async def websocket_progress(websocket: WebSocket, job_id: str):
    """
    Browser connects here to watch a specific job's progress live.

    On connect: send current state immediately (state_sync) — handles
    the case where the job is already partway done (e.g. browser
    refreshed mid-processing).

    Then: just wait. All actual messages are PUSHED by process_job()
    via manager.send(). We don't need a loop here — we just need to
    stay connected and detect disconnection.
    """
    await manager.connect(job_id, websocket)

    try:
        # Send current state immediately — covers reconnect / already-done cases
        job_data = r.hgetall(f"job:{job_id}")
        if job_data:
            await websocket.send_json({"type": "state_sync", "job": job_data})

        # Keep the connection open until the client disconnects.
        # We don't expect incoming messages, but receive_text() is how
        # FastAPI detects the disconnect (raises WebSocketDisconnect).
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        pass  # normal — browser closed tab / navigated away
    finally:
        manager.disconnect(job_id)
