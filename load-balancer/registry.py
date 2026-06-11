# PixelRouter - Processor Registry

import time
from urllib.parse import urlparse

PROCESSOR_REGISTRY_KEY = "processors:registry"


def processor_key(processor_id: str) -> str:
    return f"processor:{processor_id}"


def processor_id_from_url(processor_url: str) -> str:
    parsed = urlparse(processor_url)
    return parsed.hostname or processor_url


def register_processor(
    redis_client,
    processor_id: str,
    url: str,
    processor_type: str,
    status: str = "active",
):
    existing = redis_client.hgetall(processor_key(processor_id))
    created_at = existing.get("created_at") or str(int(time.time()))
    current_status = existing.get("status") or status

    redis_client.sadd(PROCESSOR_REGISTRY_KEY, processor_id)
    redis_client.hset(processor_key(processor_id), mapping={
        "processor_id": processor_id,
        "url": url,
        "type": processor_type,
        "status": current_status,
        "created_at": created_at,
        "last_metrics_at": existing.get("last_metrics_at", ""),
    })


def bootstrap_processor_registry(
    redis_client,
    local_processor_urls: list[str],
    cloud_processor_url: str = "",
):
    """
    Seed Redis from static config so later autoscaled processors can join the
    same registry without changing the routing code path.
    """
    for processor_url in local_processor_urls:
        processor_id = processor_id_from_url(processor_url)
        register_processor(
            redis_client,
            processor_id=processor_id,
            url=processor_url,
            processor_type="local",
        )

    if cloud_processor_url:
        register_processor(
            redis_client,
            processor_id=processor_id_from_url(cloud_processor_url),
            url=cloud_processor_url,
            processor_type="cloud",
        )


def get_processors(
    redis_client,
    processor_type: str | None = None,
    statuses: set[str] | None = None,
):
    processor_ids = sorted(redis_client.smembers(PROCESSOR_REGISTRY_KEY))
    processors = []

    for processor_id in processor_ids:
        processor = redis_client.hgetall(processor_key(processor_id))
        if not processor:
            continue

        if processor_type and processor.get("type") != processor_type:
            continue

        if statuses is not None and processor.get("status") not in statuses:
            continue

        processors.append(processor)

    return processors


def get_processor_urls(
    redis_client,
    processor_type: str | None = None,
    statuses: set[str] | None = None,
) -> list[str]:
    return [
        processor["url"]
        for processor in get_processors(redis_client, processor_type, statuses)
        if processor.get("url")
    ]


def mark_processor_metrics_seen(redis_client, processor_id: str):
    redis_client.hset(processor_key(processor_id), mapping={
        "status": "active",
        "last_metrics_at": str(int(time.time())),
    })


def mark_processor_stale(redis_client, processor_id: str):
    redis_client.hset(processor_key(processor_id), mapping={
        "status": "stale",
    })
