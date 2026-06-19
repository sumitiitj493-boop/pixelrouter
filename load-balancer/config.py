# PixelRouter - Load Balancer Configuration

import os
from dataclasses import dataclass


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        # Keep startup resilient if an optional tuning value is misconfigured.
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        # Invalid thresholds should not prevent the load balancer from booting.
        return default


def _get_csv(name: str, default: str) -> list[str]:
    raw_value = os.getenv(name, default)
    return [
        item.strip()
        for item in raw_value.split(",")
        if item.strip()
    ]


@dataclass(frozen=True)
class LoadBalancerSettings:
    # Settings are loaded once at startup so routing decisions use a stable
    # configuration snapshot for the lifetime of the service process.
    redis_url: str
    processor_urls: list[str]
    max_cpu_threshold: float
    metrics_refresh_timeout_seconds: float
    local_autoscale_enabled: bool
    max_processors: int
    processor_base_port: int
    processor_image: str
    processor_network: str
    cloud_run_processor_url: str

    @property
    def cloud_fallback_enabled(self) -> bool:
        # Empty by default for local development; enabled only when a Cloud Run
        # processor endpoint is explicitly configured.
        return bool(self.cloud_run_processor_url)


def get_settings() -> LoadBalancerSettings:
    return LoadBalancerSettings(
        redis_url=os.getenv("REDIS_URL", "redis://redis:6379"),
        processor_urls=_get_csv(
            "PROCESSOR_URLS",
            "http://processor-1:8002,http://processor-2:8003"
        ),
        max_cpu_threshold=_get_float("MAX_CPU_THRESHOLD", 80.0),
        metrics_refresh_timeout_seconds=_get_float(
            "METRICS_REFRESH_TIMEOUT_SECONDS",
            2.0
        ),
        local_autoscale_enabled=_get_bool("LOCAL_AUTOSCALE_ENABLED", True),
        max_processors=_get_int("MAX_PROCESSORS", 5),
        processor_base_port=_get_int("PROCESSOR_BASE_PORT", 8002),
        # Future Docker SDK autoscaling uses these values to launch processors
        # that match the compose-managed local processor contract.
        processor_image=os.getenv(
            "PROCESSOR_IMAGE",
            "pixelrouter-processor:latest"
        ),
        processor_network=os.getenv(
            "PROCESSOR_NETWORK",
            "pixelrouter_pixelrouter-network"
        ),
        cloud_run_processor_url=os.getenv("CLOUD_RUN_PROCESSOR_URL", ""),
    )
