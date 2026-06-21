# PixelRouter — Upload Service
# Responsibility: Accept image uploads, validate, store in GCS,
#                 ask load balancer which processor to use,
#                 track job state in Redis

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
import httpx
import redis
import os
import uuid
import time
import logging


logger = logging.getLogger("upload-service")

CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": {"jpg", "jpeg"},
    "image/png": {"png"},
    "image/webp": {"webp"},
}
CANONICAL_EXTENSIONS = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
}
GCS_URL_STRATEGIES = {"gcs_uri", "signed", "public"}


@dataclass(frozen=True)
class UploadServiceSettings:
    """Configuration loaded once at startup from environment."""
    redis_url: str
    load_balancer_url: str
    gcs_bucket_name: str
    gcs_object_prefix: str
    gcs_url_strategy: str
    gcs_signed_url_ttl_seconds: int

    @staticmethod
    def from_env() -> "UploadServiceSettings":
        url_strategy = os.getenv("GCS_URL_STRATEGY", "gcs_uri").lower()
        if url_strategy not in GCS_URL_STRATEGIES:
            raise ValueError(
                "GCS_URL_STRATEGY must be one of: gcs_uri, signed, public"
            )
        signed_url_ttl = int(os.getenv("GCS_SIGNED_URL_TTL_SECONDS", "3600"))
        if signed_url_ttl <= 0:
            raise ValueError("GCS_SIGNED_URL_TTL_SECONDS must be positive")

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
            gcs_url_strategy=url_strategy,
            gcs_signed_url_ttl_seconds=signed_url_ttl,
        )


# Process-wide clients; lifespan owns GCS initialization.
settings = UploadServiceSettings.from_env()
r = redis.from_url(settings.redis_url, decode_responses=True)
gcs_client = None


def _get_file_extension(filename: str, content_type: str) -> str:
    """Return a safe extension consistent with the validated media type."""
    extension = ""
    if filename and "." in filename:
        extension = filename.rsplit(".", 1)[-1].lower()

    if extension in CONTENT_TYPE_EXTENSIONS[content_type]:
        return extension
    return CANONICAL_EXTENSIONS[content_type]


def _build_object_name(job_id: str, filename: str, content_type: str) -> str:
    prefix = settings.gcs_object_prefix.strip("/")
    extension = _get_file_extension(filename, content_type)
    object_name = f"{job_id}.{extension}"
    return f"{prefix}/{object_name}" if prefix else object_name


def _build_access_metadata(blob, gcs_uri: str) -> dict:
    strategy = settings.gcs_url_strategy

    if strategy == "signed":
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=settings.gcs_signed_url_ttl_seconds
        )
        access_url = blob.generate_signed_url(
            version="v4",
            expiration=expires_at,
            method="GET",
        )
        return {
            "access_url": access_url,
            "url_strategy": strategy,
            "url_expires_at": expires_at.isoformat(),
        }

    if strategy == "public":
        return {
            "access_url": blob.public_url,
            "url_strategy": strategy,
            "url_expires_at": "",
        }

    return {
        "access_url": gcs_uri,
        "url_strategy": strategy,
        "url_expires_at": "",
    }


def _queue_key(processor_id: str) -> str:
    return f"queue:{processor_id}"


async def _upload_to_gcs(
    file_bytes: bytes,
    job_id: str,
    filename: str,
    content_type: str
) -> dict:
    """
    Upload validated image bytes and return the storage access contract.
    """
    if gcs_client is None:
        raise RuntimeError("GCS client not initialized")
    
    object_name = _build_object_name(job_id, filename, content_type)
    
    bucket = gcs_client.bucket(settings.gcs_bucket_name)
    blob = bucket.blob(object_name)
    uploaded = False
    
    try:
        blob.upload_from_string(
            file_bytes,
            content_type=content_type
        )
        uploaded = True
        gcs_uri = f"gs://{settings.gcs_bucket_name}/{object_name}"
        access_metadata = _build_access_metadata(blob, gcs_uri)
        logger.info(
            f"[{job_id}] Uploaded to GCS: {object_name} ({len(file_bytes)} bytes)"
        )
    except Exception as exc:
        if uploaded:
            try:
                blob.delete()
            except Exception:
                logger.exception(f"[{job_id}] Failed to roll back GCS object")
        logger.error(f"[{job_id}] GCS upload failed: {exc}")
        raise HTTPException(
            status_code=500,
            detail="GCS upload failed"
        ) from exc
    
    return {
        "object_name": object_name,
        "bucket_name": settings.gcs_bucket_name,
        "gcs_path": gcs_uri,
        "content_type": content_type,
        "size_bytes": len(file_bytes),
        **access_metadata,
    }


async def _get_route_decision(job_id: str) -> dict:
    """Fetch the current routing decision from the load balancer."""
    route_url = f"{settings.load_balancer_url.rstrip('/')}/route"

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=2.0)) as client:
            response = await client.get(route_url)
            response.raise_for_status()
            route_data = response.json()
    except httpx.HTTPStatusError as exc:
        logger.error(f"[{job_id}] Load balancer returned {exc.response.status_code}")
        raise HTTPException(
            status_code=502,
            detail="Load balancer route request failed"
        ) from exc
    except httpx.RequestError as exc:
        logger.error(f"[{job_id}] Load balancer request failed: {exc}")
        raise HTTPException(
            status_code=502,
            detail="Load balancer is unavailable"
        ) from exc
    except ValueError as exc:
        logger.error(f"[{job_id}] Invalid route response: {exc}")
        raise HTTPException(
            status_code=502,
            detail="Invalid load balancer response"
        ) from exc

    processor_url = route_data.get("processor_url")
    processor_id = route_data.get("processor_id")
    tier = route_data.get("tier")

    if not processor_url or not processor_id or not tier:
        raise HTTPException(
            status_code=502,
            detail="Incomplete routing metadata from load balancer"
        )

    return {
        "processor_url": processor_url,
        "processor_id": processor_id,
        "tier": tier,
        "fallback_used": bool(route_data.get("fallback_used", False)),
        "scaled": bool(route_data.get("scaled", False)),
        "scaling_action": route_data.get("scaling_action", ""),
        "reason": route_data.get("reason", ""),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize GCS client and Redis connection at startup."""
    global gcs_client
    gcs_client = storage.Client()
    yield


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
    Accept an image upload, store in GCS, create a job, and enqueue.
    Returns job_id for tracking.
    """
    if file.content_type not in ["image/jpeg", "image/png", "image/webp"]:
        raise HTTPException(status_code=400, detail="Only JPEG, PNG, WebP allowed")

    job_id = f"job_{uuid.uuid4().hex[:8]}"
    
    try:
        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Empty file")
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[{job_id}] File read failed: {exc}")
        raise HTTPException(
            status_code=400,
            detail="Failed to read file"
        ) from exc

    gcs_metadata = await _upload_to_gcs(
        file_bytes,
        job_id,
        file.filename or "upload.jpg",
        file.content_type
    )

    route_decision = await _get_route_decision(job_id)

    r.hset(f"job:{job_id}", mapping={
        "status": "pending",
        "filename": file.filename,
        "created_at": str(int(time.time())),
        "gcs_bucket": gcs_metadata["bucket_name"],
        "gcs_object": gcs_metadata["object_name"],
        "gcs_path": gcs_metadata["gcs_path"],
        "gcs_access_url": gcs_metadata["access_url"],
        "gcs_url_strategy": gcs_metadata["url_strategy"],
        "gcs_url_expires_at": gcs_metadata["url_expires_at"],
        "gcs_content_type": gcs_metadata["content_type"],
        "gcs_size_bytes": str(gcs_metadata["size_bytes"]),
        "processor_id": route_decision["processor_id"],
        "processor_url": route_decision["processor_url"],
        "processor_tier": route_decision["tier"],
        "fallback_used": str(route_decision["fallback_used"]).lower(),
        "scaled": str(route_decision["scaled"]).lower(),
        "scaling_action": route_decision["scaling_action"],
        "route_reason": route_decision["reason"],
        "result_url": ""
    })

    # Job metadata is transient; source objects follow the bucket lifecycle.
    r.expire(f"job:{job_id}", 86400)

    queue_key = _queue_key(route_decision["processor_id"])

    # Queue publication happens only after storage and routing succeed.
    r.lpush(queue_key, job_id)
    logger.info(f"[{job_id}] Queued for processing on {queue_key}")

    return {
        "job_id": job_id,
        "status": "pending",
        "gcs_path": gcs_metadata["gcs_path"],
        "gcs_access_url": gcs_metadata["access_url"],
        "gcs_url_strategy": gcs_metadata["url_strategy"],
        "processor_id": route_decision["processor_id"],
        "processor_url": route_decision["processor_url"],
        "tier": route_decision["tier"],
        "fallback_used": route_decision["fallback_used"],
        "queue_key": queue_key,
        "message": "Job created and queued for processing"
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
    How many jobs are waiting across all processor queues.
    """
    queue_lengths = {
        key: r.llen(key)
        for key in r.scan_iter(match="queue:*")
    }
    return {
        "queue_length": sum(queue_lengths.values()),
        "queue_lengths": queue_lengths,
    }
