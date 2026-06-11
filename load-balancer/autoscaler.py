# PixelRouter - Docker Autoscaling Manager
# Spawns new local processor containers when the registered pool reaches capacity.

import os
import re
from dataclasses import dataclass

from registry import get_processors, register_processor

PROCESSOR_ID_PATTERN = re.compile(r"^processor-(\d+)$")


@dataclass(frozen=True)
class AutoscaleResult:
    scaled: bool
    reason: str
    local_count: int
    max_processors: int
    processor_id: str = ""
    processor_url: str = ""


def _processor_number(processor_id: str) -> int | None:
    match = PROCESSOR_ID_PATTERN.match(processor_id)
    if not match:
        return None
    return int(match.group(1))


def _next_processor_number(local_processors: list[dict]) -> int:
    existing_numbers = [
        number
        for number in (
            _processor_number(processor.get("processor_id", ""))
            for processor in local_processors
        )
        if number is not None
    ]

    return max(existing_numbers, default=0) + 1


def _processor_url(processor_id: str, port: int) -> str:
    return f"http://{processor_id}:{port}"


def _container_environment(settings, processor_id: str, port: int) -> dict:
    # Mirror the runtime configuration the processor expects so the new
    # container behaves like the compose-managed instances.
    environment = {
        "REDIS_URL": settings.redis_url,
        "PROCESSOR_ID": processor_id,
        "PORT": str(port),
    }

    for optional_name in (
        "GCS_BUCKET_NAME",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "LOG_LEVEL",
    ):
        value = os.getenv(optional_name)
        if value:
            environment[optional_name] = value

    return environment


def scale_local_processors(redis_client, settings, docker_client=None):
    local_processors = get_processors(redis_client, processor_type="local")
    local_count = len(local_processors)

    if local_count >= settings.max_processors:
        return AutoscaleResult(
            scaled=False,
            reason="max_processors_reached",
            local_count=local_count,
            max_processors=settings.max_processors,
        )

    processor_number = _next_processor_number(local_processors)
    processor_id = f"processor-{processor_number}"
    port = settings.processor_base_port + processor_number - 1
    # Keep the container name, Redis identity, and host port aligned so the
    # rest of the load balancer can treat the new instance as a first-class peer.
    processor_url = _processor_url(processor_id, port)

    if docker_client is None:
        # Import Docker lazily so tests and non-autoscaling paths do not need
        # the SDK unless a new local processor is actually being created.
        import docker
        docker_client = docker.from_env()

    try:
        docker_client.containers.run(
            settings.processor_image,
            name=processor_id,
            detach=True,
            network=settings.processor_network,
            environment=_container_environment(settings, processor_id, port),
            ports={f"{port}/tcp": port},
            restart_policy={"Name": "unless-stopped"},
        )
    except Exception as exc:
        return AutoscaleResult(
            scaled=False,
            reason=f"docker_error:{exc.__class__.__name__}",
            local_count=local_count,
            max_processors=settings.max_processors,
            processor_id=processor_id,
            processor_url=processor_url,
        )

    # Register the container only after it starts successfully so routing does
    # not point traffic at an instance that never came up.
    register_processor(
        redis_client,
        processor_id=processor_id,
        url=processor_url,
        processor_type="local",
        status="active",
    )

    return AutoscaleResult(
        scaled=True,
        reason="processor_spawned",
        local_count=local_count + 1,
        max_processors=settings.max_processors,
        processor_id=processor_id,
        processor_url=processor_url,
    )
