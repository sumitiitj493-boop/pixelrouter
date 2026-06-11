# PixelRouter - Load Balancer Service
# Responsibility: Route jobs to least-loaded processor.
#                 Poll processor metrics before routing.
#                 Request autoscaling when all processors are overloaded.

from fastapi import FastAPI, HTTPException
import httpx
import redis

from autoscaler import scale_local_processors
from config import get_settings
from registry import (
    bootstrap_processor_registry,
    get_processor_urls,
    get_processors,
    mark_processor_metrics_seen,
    mark_processor_stale,
)

from router import (
    processor_id_from_url,
    select_processor,
)

app = FastAPI(
    title="PixelRouter - Load Balancer",
    description="CPU-aware job routing across processor instances",
    version="0.1.0"
)

settings = get_settings()

r = redis.from_url(settings.redis_url, decode_responses=True)


def ensure_processor_registry():
    bootstrap_processor_registry(
        r,
        local_processor_urls=settings.processor_urls,
        cloud_processor_url=settings.cloud_run_processor_url,
    )


@app.on_event("startup")
async def startup():
    ensure_processor_registry()


@app.get("/")
async def root():
    ensure_processor_registry()
    processors = get_processors(r)

    return {
        "service": "load-balancer",
        "version": "0.1.0",
        "processors_registered": len(processors)
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "load-balancer"}


async def refresh_processor_metrics(processor_urls: list[str]):
    """
    Ask each processor for fresh metrics before routing.
    Processor /metrics also writes those values to Redis with a short TTL.
    """
    # The metrics request doubles as a liveness check; failed calls mark the
    # processor stale so routing stops considering it for active traffic.
    async with httpx.AsyncClient(
        timeout=settings.metrics_refresh_timeout_seconds
    ) as client:
        for processor_url in processor_urls:
            processor_id = processor_id_from_url(processor_url)
            try:
                response = await client.get(f"{processor_url}/metrics")
                response.raise_for_status()
                mark_processor_metrics_seen(r, processor_id)
            except httpx.HTTPError:
                mark_processor_stale(r, processor_id)
                continue


def _parse_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_live_processor_metrics(processor_urls: list[str], redis_client):
    """
    Read the live processor snapshot app.py needs for scaling decisions.
    Routing itself remains delegated to router.py.
    """
    live_processors = []

    for processor_url in processor_urls:
        processor_id = processor_id_from_url(processor_url)
        cpu_percent = _parse_float(
            redis_client.get(f"metrics:{processor_id}:cpu")
        )

        if cpu_percent is None:
            continue

        pending_jobs = _parse_int(
            redis_client.get(f"metrics:{processor_id}:pending")
        )
        live_processors.append({
            "processor_id": processor_id,
            "url": processor_url,
            "cpu_percent": cpu_percent,
            "pending_jobs": max(0, pending_jobs)
        })

    return live_processors


def all_live_processors_overloaded(live_processors: list[dict]) -> bool:
    if not live_processors:
        return False

    return all(
        processor["cpu_percent"] >= settings.max_cpu_threshold
        for processor in live_processors
    )


def has_healthy_local_capacity(live_processors: list[dict]) -> bool:
    return any(
        processor["cpu_percent"] < settings.max_cpu_threshold
        for processor in live_processors
    )


def get_local_capacity_state(live_processors: list[dict]) -> str:
    if not live_processors:
        return "no_live_local_processors"

    if all_live_processors_overloaded(live_processors):
        return "overloaded"

    return "healthy"


def decide_scaling_action(live_processors: list[dict], redis_client) -> str:
    if get_local_capacity_state(live_processors) != "overloaded":
        return "none"

    # Prefer extending the local pool first; cloud fallback is reserved for
    # cases where local autoscaling is unavailable or fully capped.
    if settings.local_autoscale_enabled:
        result = scale_local_processors(redis_client, settings)
        if result.scaled:
            return f"local_scaled:{result.processor_id}"
        return result.reason

    if settings.cloud_fallback_enabled:
        return "cloud_fallback_available"

    return "overloaded_no_fallback_configured"


def get_cloud_fallback_processor(redis_client):
    cloud_processors = get_processors(
        redis_client,
        processor_type="cloud",
        statuses={"active"},
    )
    if not cloud_processors:
        return None
    return cloud_processors[0]


def build_route_response(
    processor_url: str,
    processor_id: str,
    tier: str,
    scaled: bool,
    fallback_used: bool,
    reason: str,
    scaling_action: str,
):
    return {
        "processor_url": processor_url,
        "processor_id": processor_id,
        "tier": tier,
        "processor_type": tier,
        "scaled": scaled,
        "fallback_used": fallback_used,
        "scaling_action": scaling_action,
        "reason": reason,
    }


def maybe_scale_or_fallback(
    live_processors: list[dict],
    redis_client,
    scale_fn=scale_local_processors,
):
    local_capacity_state = get_local_capacity_state(live_processors)

    if local_capacity_state != "overloaded":
        return {
            "scaled": False,
            "fallback_processor": None,
            "scaling_action": "none",
            "reason": "selected by lowest pending_jobs, then lowest CPU usage",
        }

    # Fallback only matters once the local pool is already saturated.
    if not settings.local_autoscale_enabled:
        return {
            "scaled": False,
            "fallback_processor": None,
            "scaling_action": "local_autoscale_disabled",
            "reason": "local processors overloaded but autoscaling is disabled",
        }

    scale_result = scale_fn(redis_client, settings)

    if scale_result.scaled:
        return {
            "scaled": True,
            "fallback_processor": None,
            "scaling_action": f"local_scaled:{scale_result.processor_id}",
            "reason": "local_overload_scaled_new_processor",
        }

    if scale_result.reason == "max_processors_reached":
        cloud_processor = get_cloud_fallback_processor(redis_client)
        if cloud_processor:
            return {
                "scaled": False,
                "fallback_processor": cloud_processor,
                "scaling_action": "max_processors_reached",
                "reason": "cloud_fallback_max_local_capacity",
            }

    return {
        "scaled": False,
        "fallback_processor": None,
        "scaling_action": scale_result.reason,
        "reason": scale_result.reason,
    }


def build_scaling_status(redis_client):
    local_processors = get_processors(redis_client, processor_type="local")
    active_local_urls = get_processor_urls(
        redis_client,
        processor_type="local",
        statuses={"active"},
    )
    live_processors = get_live_processor_metrics(
        active_local_urls,
        redis_client,
    )
    cloud_processor = get_cloud_fallback_processor(redis_client)

    return {
        "local_count": len(local_processors),
        "max_processors": settings.max_processors,
        "local_capacity_state": get_local_capacity_state(live_processors),
        "local_autoscale_enabled": settings.local_autoscale_enabled,
        "cloud_fallback_enabled": settings.cloud_fallback_enabled,
        "cloud_fallback_configured": cloud_processor is not None,
        "cloud_processor_url": (
            cloud_processor.get("url") if cloud_processor else ""
        ),
        "max_cpu_threshold": settings.max_cpu_threshold,
    }


@app.get("/route")
async def get_best_processor():
    """
    Returns the URL of the least-loaded live processor.
    Routing logic: lowest pending_jobs first, CPU% as tiebreaker.
    Pending count is not incremented here; it should be updated only
    after the selected processor actually accepts/claims the job.
    """
    ensure_processor_registry()
    processor_urls = get_processor_urls(
        r,
        processor_type="local",
        statuses={"active", "stale"},
    )
    await refresh_processor_metrics(processor_urls)
    processor_urls = get_processor_urls(
        r,
        processor_type="local",
        statuses={"active"},
    )
    live_processors = get_live_processor_metrics(processor_urls, r)
    route_decision = maybe_scale_or_fallback(live_processors, r)

    fallback_processor = route_decision["fallback_processor"]
    if fallback_processor:
        return build_route_response(
            processor_url=fallback_processor["url"],
            processor_id=fallback_processor["processor_id"],
            tier="cloud",
            scaled=route_decision["scaled"],
            fallback_used=True,
            reason=route_decision["reason"],
            scaling_action=route_decision["scaling_action"],
        )

    try:
        processor_url = select_processor(processor_urls, r)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    processor_id = processor_id_from_url(processor_url)

    return build_route_response(
        processor_url=processor_url,
        processor_id=processor_id,
        tier="local",
        scaled=route_decision["scaled"],
        fallback_used=False,
        reason=route_decision["reason"],
        scaling_action=route_decision["scaling_action"],
    )


@app.get("/processors/status")
async def processors_status():
    """
    Returns current metrics for all registered processors.
    Reads from Redis metrics keys written by each processor.
    """
    ensure_processor_registry()
    statuses = []
    for processor in get_processors(r):
        url = processor.get("url")
        processor_id = processor_id_from_url(url)
        cpu = r.get(f"metrics:{processor_id}:cpu") or "unknown"
        pending = r.get(f"metrics:{processor_id}:pending") or "0"
        statuses.append({
            "processor_id": processor_id,
            "url": url,
            "type": processor.get("type"),
            "status": processor.get("status"),
            "created_at": processor.get("created_at"),
            "last_metrics_at": processor.get("last_metrics_at"),
            "cpu_percent": cpu,
            "pending_jobs": pending
        })
    return {
        "processors": statuses,
        "max_cpu_threshold": settings.max_cpu_threshold,
        "local_autoscale_enabled": settings.local_autoscale_enabled,
        "max_processors": settings.max_processors,
        "cloud_fallback_enabled": settings.cloud_fallback_enabled
    }


@app.get("/scaling/status")
async def scaling_status():
    """
    Exposes local scale limits and fallback readiness for dashboards.
    """
    ensure_processor_registry()
    return build_scaling_status(r)
