# PixelRouter — Upload Service
# Responsibility: Accept image uploads, validate, store in GCS,
#                 ask load balancer which processor to use,
#                 track job state in Redis

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import redis
import os
import uuid
import time

app = FastAPI(
    title="PixelRouter — Upload Service",
    description="Handles image uploads and job creation",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Config from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
LOAD_BALANCER_URL = os.getenv("LOAD_BALANCER_URL", "http://load-balancer:8001")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "pixelrouter-images")

# Redis client
r = redis.from_url(REDIS_URL, decode_responses=True)


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
    return {
        "status": "ok",
        "service": "upload-service",
        "redis": redis_status
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

    # Call load balancer to get the assigned processor
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{LOAD_BALANCER_URL}/route")
        response.raise_for_status()
        route_decision = response.json()
    processor_id = route_decision["processor_id"]

    # Create job record in Redis
    r.hset(f"job:{job_id}", mapping={
        "status": "pending",
        "filename": file.filename,
        "created_at": str(int(time.time())),
        "processor_id": processor_id,
        "result_url": ""
    })

    # Set TTL — auto-delete after 24 hours
    r.expire(f"job:{job_id}", 86400)

    # Push to processing queue for the SPECIFIC processor
    r.lpush(f"queue:{processor_id}", job_id)

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
