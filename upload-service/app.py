# PixelRouter — Upload Service
# Responsibility: Accept image uploads, validate, store in GCS,
#                 ask load balancer which processor to use,
#                 track job state in Redis

from contextlib import asynccontextmanager
from dataclasses import dataclass
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
import httpx
import redis
import os
import uuid
import time


@dataclass(frozen=True)
class UploadServiceSettings:
    """Configuration loaded once at startup from environment."""
    redis_url: str
    load_balancer_url: str
    gcs_bucket_name: str
    gcs_object_prefix: str

    @staticmethod
    def from_env() -> "UploadServiceSettings":
        return UploadServiceSettings(
            redis_url=os.getenv("REDIS_URL", "redis://redis:6379"),
            load_balancer_url=os.getenv(
                "LOAD_BALANCER_URL",
                "http://load-balancer:8001"
            ),
            gcs_bucket_name=os.getenv(
                "GCS_BUCKET_NAME",
                "pixelrouter-images"
            ),
            gcs_object_prefix=os.getenv("GCS_OBJECT_PREFIX", "jobs"),
        )


# Clients initialized in lifespan startup
settings = UploadServiceSettings.from_env()
r = redis.from_url(settings.redis_url, decode_responses=True)
gcs_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize GCS client and Redis connection at startup."""
    global gcs_client
    gcs_client = storage.Client()
    yield
    # Cleanup handled by context manager; GCS client does not require explicit close


app = FastAPI(
    title="PixelRouter — Upload Service",
    description="Handles image uploads and job creation",
    version="0.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "upload-service",
        "version": "0.1.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    try:
        r.ping()
        redis_status = "connected"
    except Exception:
        redis_status = "disconnected"
    
    gcs_status = "initialized" if gcs_client is not None else "not_initialized"
    
    return {
        "status": "ok",
        "service": "upload-service",
        "redis": redis_status,
        "gcs": gcs_status
    }


@app.post("/upload")
async def upload_image(file: UploadFile = File(...)):
    """
    Accept an image upload and create a job.
    Returns job_id for tracking.
    TODO: Store image in GCS
    TODO: Ask load balancer which processor to route to
    TODO: Push job_id to Redis queue
    """
    # Validate file type
    if file.content_type not in ["image/jpeg", "image/png", "image/webp"]:
        raise HTTPException(status_code=400, detail="Only JPEG, PNG, WebP allowed")

    # Generate unique job ID
    job_id = f"job_{uuid.uuid4().hex[:8]}"

    # Create job record in Redis
    r.hset(f"job:{job_id}", mapping={
        "status": "pending",
        "filename": file.filename,
        "created_at": str(int(time.time())),
        "processor": "",
        "result_url": ""
    })

    # Set TTL — auto-delete after 24 hours
    r.expire(f"job:{job_id}", 86400)

    # Push to processing queue
    r.lpush("image_queue", job_id)

    return {
        "job_id": job_id,
        "status": "pending",
        "message": "Job created successfully"
    }


@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """
    Get current status of a job.
    """
    job = r.hgetall(f"job:{job_id}")
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, **job}


@app.get("/queue/length")
async def queue_length():
    """
    How many jobs are waiting to be processed.
    """
    length = r.llen("image_queue")
    return {"queue_length": length}
