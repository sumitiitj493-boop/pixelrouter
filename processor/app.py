# PixelRouter — Processor Service
# Responsibility: Pick jobs from Redis queue, run AI pipeline,
#                 stream progress via WebSocket, write results to GCS,
#                 expose /metrics for load balancer polling.

from fastapi import FastAPI, WebSocket
import redis
import os
import asyncio
import psutil
import logging  # MOVED UP to fix A1 import order bug
from contextlib import asynccontextmanager
from transformers import BlipProcessor, BlipForConditionalGeneration
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

# A3 New Imports for Worker Loop & WebSocket
from fastapi.websockets import WebSocketDisconnect
from google.cloud import storage
from pipeline import init_models, run_pipeline
from metrics import start_metrics_heartbeat

logger = logging.getLogger("processor")
logging.basicConfig(level=logging.INFO)

# ── A3: Connection Manager ─────────────────────────────────────────────────
class ConnectionManager:
    """Routes WebSocket progress to the correct browser by job_id."""
    def __init__(self):
        self.active: Dict[str, WebSocket] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active[job_id] = websocket
        logger.info(f"[WS] connected job={job_id}")

    def disconnect(self, job_id: str):
        self.active.pop(job_id, None)

    async def send(self, job_id: str, data: dict):
        ws = self.active.get(job_id)
        if ws:
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(job_id)

manager = ConnectionManager()

# ── A3: Worker Loop & GCS Helpers ──────────────────────────────────────────
io_executor = ThreadPoolExecutor(max_workers=2)  # Separate from startup executor
gcs_client = None

def _get_gcs_client():
    global gcs_client
    if gcs_client is None:
        gcs_client = storage.Client()
    return gcs_client

def _download_blob_sync(gcs_path: str) -> bytes:
    bucket_name, blob_path = gcs_path.replace("gs://", "").split("/", 1)
    return _get_gcs_client().bucket(bucket_name).blob(blob_path).download_as_bytes()

async def download_image(gcs_path: str) -> bytes:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(io_executor, _download_blob_sync, gcs_path)

def _brpop_sync(timeout: int):
    return r.brpop("image_queue", timeout=timeout)

async def worker_loop():
    logger.info(f"[{PROCESSOR_ID}] Worker loop started")
    loop = asyncio.get_event_loop()
    while True:
        try:
            result = await loop.run_in_executor(io_executor, _brpop_sync, 5)
        except asyncio.CancelledError:
            logger.info(f"[{PROCESSOR_ID}] Worker loop cancelled")
            raise
        except Exception as e:
            logger.error(f"[{PROCESSOR_ID}] BRPOP error: {e}")
            await asyncio.sleep(1)
            continue
            
        if result is None: continue
            
        _queue_name, job_id = result
        logger.info(f"[{PROCESSOR_ID}] Picked up job: {job_id}")
        await process_job(job_id)

async def process_job(job_id: str):
    r.hset(f"job:{job_id}", mapping={"status": "processing", "processor": PROCESSOR_ID})
    r.incr(f"metrics:{PROCESSOR_ID}:pending")
    
    try:
        job_data = r.hgetall(f"job:{job_id}")
        image_url = job_data.get("image_url")
        if not image_url: raise ValueError("job hash missing image_url")
            
        image_bytes = await download_image(image_url)
        logger.info(f"[{job_id}] Downloaded {len(image_bytes)} bytes")
        
        async def on_progress(stage: str, progress: int):
            r.hset(f"job:{job_id}", mapping={"stage": stage, "progress": progress})
            await manager.send(job_id, {"type": "progress", "stage": stage, "progress": progress})
            
        result = await run_pipeline(job_id, image_bytes, progress_callback=on_progress)
        
        r.hset(f"job:{job_id}", mapping={"status": "done", "caption": result["caption"]})
        await manager.send(job_id, {"type": "complete", "caption": result["caption"]})
        logger.info(f"[{job_id}] Done. Caption: {result['caption']}")
        
    except Exception as e:
        logger.error(f"[{job_id}] Pipeline failed: {e}")
        r.hset(f"job:{job_id}", mapping={"status": "failed", "error": str(e)})
    finally:
        r.decr(f"metrics:{PROCESSOR_ID}:pending")

# ── Lifespan ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown hook. Models load here so a failure stops the container."""
    startup_executor = ThreadPoolExecutor(max_workers=1)
    
    # ── STARTUP ──────────────────────────────────────────────────────────
    logger.info("Loading BLIP-base model...")
    loop = asyncio.get_event_loop()

    blip_processor = await loop.run_in_executor(
        startup_executor, BlipProcessor.from_pretrained, "Salesforce/blip-image-captioning-base"
    )
    blip_model = await loop.run_in_executor(
        startup_executor, BlipForConditionalGeneration.from_pretrained, "Salesforce/blip-image-captioning-base"
    )

    # Wire the loaded models into pipeline.py
    init_models(blip_processor, blip_model)
    logger.info("BLIP-base loaded and ready. Processor accepting jobs.")

    # Start periodic metrics heartbeat to Redis (A2)
    heartbeat_task = asyncio.create_task(
        start_metrics_heartbeat(PROCESSOR_ID, r)
    )
    
    # Start worker loop (A3)
    worker_task = asyncio.create_task(worker_loop())

    yield  # app is live from here

    # ── SHUTDOWN ─────────────────────────────────────────────────────────
    logger.info("Processor shutting down")
    heartbeat_task.cancel()
    worker_task.cancel()
    startup_executor.shutdown(wait=False)
    io_executor.shutdown(wait=False)

# ── App Config ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="PixelRouter — Processor",
    description="AI image processing worker",
    version="0.1.0",
    lifespan=lifespan
)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PROCESSOR_ID = os.getenv("PROCESSOR_ID", "processor-1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "pixelrouter-images")

r = redis.from_url(REDIS_URL, decode_responses=True)

# ── Endpoints ──────────────────────────────────────────────────────────────
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
    """Exposes live CPU and memory metrics."""
    cpu_percent = psutil.cpu_percent(interval=0.5)
    ram_percent = psutil.virtual_memory().percent
    pending_jobs = int(r.get(f"metrics:{PROCESSOR_ID}:pending") or 0)

    r.set(f"metrics:{PROCESSOR_ID}:cpu", cpu_percent, ex=10)
    r.set(f"metrics:{PROCESSOR_ID}:pending", pending_jobs, ex=10)

    return {
        "processor_id": PROCESSOR_ID,
        "cpu_percent": cpu_percent,
        "ram_percent": ram_percent,
        "pending_jobs": pending_jobs
    }

# ── WebSocket (Replaced placeholder with real A3 implementation) ───────────
@app.websocket("/ws/{job_id}")
async def websocket_progress(websocket: WebSocket, job_id: str):
    """Streams processing progress to client in real time."""
    await manager.connect(job_id, websocket)
    try:
        # Send current state immediately (state_sync)
        job_data = r.hgetall(f"job:{job_id}")
        if job_data:
            await websocket.send_json({"type": "state_sync", "job": job_data})
            
        # Keep connection open until client disconnects
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(job_id)