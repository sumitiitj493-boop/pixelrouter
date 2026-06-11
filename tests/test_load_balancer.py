# PixelRouter - Load Balancer Tests

import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../load-balancer"))

if "fastapi" not in sys.modules:
    fastapi_stub = types.ModuleType("fastapi")

    class FastAPI:  # pragma: no cover - test import shim
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            def decorator(handler):
                return handler

            return decorator

        def on_event(self, *args, **kwargs):
            def decorator(handler):
                return handler

            return decorator

    class HTTPException(Exception):  # pragma: no cover - test import shim
        def __init__(self, status_code, detail):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi_stub.FastAPI = FastAPI
    fastapi_stub.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")

    class HTTPError(Exception):  # pragma: no cover - test import shim
        pass

    class AsyncClient:  # pragma: no cover - test import shim
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            raise HTTPError("httpx is not installed in this test environment")

    httpx_stub.AsyncClient = AsyncClient
    httpx_stub.HTTPError = HTTPError
    sys.modules["httpx"] = httpx_stub

if "redis" not in sys.modules:
    redis_stub = types.ModuleType("redis")
    redis_stub.from_url = lambda *args, **kwargs: object()
    sys.modules["redis"] = redis_stub

import app as load_balancer_app
from app import (
    all_live_processors_overloaded,
    build_route_response,
    build_scaling_status,
    decide_scaling_action,
    get_cloud_fallback_processor,
    get_local_capacity_state,
    has_healthy_local_capacity,
    maybe_scale_or_fallback,
)
from autoscaler import AutoscaleResult, scale_local_processors
from registry import (
    bootstrap_processor_registry,
    get_processor_urls,
    get_processors,
    mark_processor_stale,
    register_processor,
)
from router import processor_id_from_url, select_processor, update_pending_count


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.expirations = {}
        self.hashes = {}
        self.sets = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values:
            return False

        self.values[key] = str(value)
        if ex is not None:
            self.expirations[key] = ex
        return True

    def incrby(self, key, delta):
        new_value = int(self.values.get(key, 0)) + delta
        self.values[key] = str(new_value)
        return new_value

    def expire(self, key, seconds):
        self.expirations[key] = seconds
        return True

    def hgetall(self, key):
        return self.hashes.get(key, {}).copy()

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {})
        self.hashes[key].update({
            field: str(value)
            for field, value in mapping.items()
        })
        return len(mapping)

    def sadd(self, key, *values):
        self.sets.setdefault(key, set())
        before_count = len(self.sets[key])
        self.sets[key].update(values)
        return len(self.sets[key]) - before_count

    def smembers(self, key):
        return self.sets.get(key, set()).copy()


PROCESSORS = [
    "http://processor-1:8002",
    "http://processor-2:8003",
]


class FakeSettings:
    redis_url = "redis://redis:6379"
    max_processors = 5
    processor_base_port = 8002
    processor_image = "pixelrouter-processor:latest"
    processor_network = "pixelrouter_pixelrouter-network"
    max_cpu_threshold = 80.0
    local_autoscale_enabled = True
    cloud_run_processor_url = ""


class FakeContainers:
    def __init__(self):
        self.run_calls = []

    def run(self, *args, **kwargs):
        self.run_calls.append({
            "args": args,
            "kwargs": kwargs,
        })
        return object()


class FakeDockerClient:
    def __init__(self):
        self.containers = FakeContainers()


def test_processor_id_from_url_returns_hostname():
    assert processor_id_from_url("http://processor-1:8002") == "processor-1"


def test_select_processor_prefers_lowest_pending_jobs():
    redis_client = FakeRedis({
        "metrics:processor-1:cpu": "15",
        "metrics:processor-1:pending": "4",
        "metrics:processor-2:cpu": "70",
        "metrics:processor-2:pending": "1",
    })

    selected = select_processor(PROCESSORS, redis_client)

    assert selected == "http://processor-2:8003"


def test_select_processor_uses_cpu_as_tiebreaker():
    redis_client = FakeRedis({
        "metrics:processor-1:cpu": "75",
        "metrics:processor-1:pending": "2",
        "metrics:processor-2:cpu": "25",
        "metrics:processor-2:pending": "2",
    })

    selected = select_processor(PROCESSORS, redis_client)

    assert selected == "http://processor-2:8003"


def test_select_processor_does_not_increment_pending_count():
    redis_client = FakeRedis({
        "metrics:processor-1:cpu": "15",
        "metrics:processor-1:pending": "0",
        "metrics:processor-2:cpu": "40",
        "metrics:processor-2:pending": "2",
    })

    selected = select_processor(PROCESSORS, redis_client)

    assert selected == "http://processor-1:8002"
    assert redis_client.values["metrics:processor-1:pending"] == "0"


def test_select_processor_ignores_processors_without_live_cpu_metrics():
    redis_client = FakeRedis({
        "metrics:processor-1:pending": "0",
        "metrics:processor-2:cpu": "20",
        "metrics:processor-2:pending": "3",
    })

    selected = select_processor(PROCESSORS, redis_client)

    assert selected == "http://processor-2:8003"


def test_select_processor_has_no_autoscale_side_effect():
    redis_client = FakeRedis({
        "metrics:processor-1:cpu": "90",
        "metrics:processor-1:pending": "1",
        "metrics:processor-2:cpu": "95",
        "metrics:processor-2:pending": "2",
    })

    selected = select_processor(PROCESSORS, redis_client)

    assert selected == "http://processor-1:8002"
    assert "autoscale:requested" not in redis_client.values


def test_bootstrap_processor_registry_registers_local_and_cloud_processors():
    redis_client = FakeRedis()

    bootstrap_processor_registry(
        redis_client,
        local_processor_urls=PROCESSORS,
        cloud_processor_url="https://pixelrouter-cloud.run.app",
    )

    processors = get_processors(redis_client)

    assert len(processors) == 3
    assert get_processor_urls(redis_client, processor_type="local") == PROCESSORS
    assert get_processor_urls(redis_client, processor_type="cloud") == [
        "https://pixelrouter-cloud.run.app"
    ]


def test_register_processor_preserves_stale_status_across_bootstrap():
    redis_client = FakeRedis()

    register_processor(
        redis_client,
        processor_id="processor-1",
        url="http://processor-1:8002",
        processor_type="local",
    )
    created_at = redis_client.hashes["processor:processor-1"]["created_at"]
    mark_processor_stale(redis_client, "processor-1")

    register_processor(
        redis_client,
        processor_id="processor-1",
        url="http://processor-1:8002",
        processor_type="local",
    )

    processor = redis_client.hashes["processor:processor-1"]
    assert processor["created_at"] == created_at
    assert processor["status"] == "stale"


def test_get_processor_urls_filters_by_type_and_status():
    redis_client = FakeRedis()
    bootstrap_processor_registry(redis_client, PROCESSORS)
    mark_processor_stale(redis_client, "processor-2")

    active_local_urls = get_processor_urls(
        redis_client,
        processor_type="local",
        statuses={"active"},
    )

    assert active_local_urls == ["http://processor-1:8002"]


def test_all_live_processors_overloaded_counts_only_live_metrics():
    live_processors = [
        {"processor_id": "processor-1", "cpu_percent": 85.0},
        {"processor_id": "processor-2", "cpu_percent": 92.0},
    ]

    assert all_live_processors_overloaded(live_processors) is True
    assert has_healthy_local_capacity(live_processors) is False
    assert get_local_capacity_state(live_processors) == "overloaded"


def test_one_healthy_local_processor_prevents_scaling():
    redis_client = FakeRedis()
    live_processors = [
        {"processor_id": "processor-1", "cpu_percent": 95.0},
        {"processor_id": "processor-2", "cpu_percent": 45.0},
    ]

    assert all_live_processors_overloaded(live_processors) is False
    assert has_healthy_local_capacity(live_processors) is True
    assert get_local_capacity_state(live_processors) == "healthy"
    assert decide_scaling_action(live_processors, redis_client) == "none"
    assert "autoscale:requested" not in redis_client.values


def test_normal_routing_stays_on_local_processors_without_scaling():
    original_settings = load_balancer_app.settings
    load_balancer_app.settings = FakeSettings
    try:
        redis_client = FakeRedis({
            "metrics:processor-1:cpu": "30",
            "metrics:processor-1:pending": "4",
            "metrics:processor-2:cpu": "20",
            "metrics:processor-2:pending": "1",
        })
        live_processors = [
            {"processor_id": "processor-1", "cpu_percent": 30.0},
            {"processor_id": "processor-2", "cpu_percent": 20.0},
        ]

        decision = maybe_scale_or_fallback(live_processors, redis_client)
        selected = select_processor(PROCESSORS, redis_client)

        assert decision["scaled"] is False
        assert decision["fallback_processor"] is None
        assert decision["scaling_action"] == "none"
        assert selected == "http://processor-2:8003"
    finally:
        load_balancer_app.settings = original_settings


def test_local_scaling_triggers_when_all_live_processors_are_overloaded():
    original_settings = load_balancer_app.settings
    load_balancer_app.settings = FakeSettings
    try:
        redis_client = FakeRedis()
        live_processors = [
            {"processor_id": "processor-1", "cpu_percent": 95.0},
            {"processor_id": "processor-2", "cpu_percent": 91.0},
        ]

        def fake_scale_fn(redis_client, settings):
            return AutoscaleResult(
                scaled=True,
                reason="processor_spawned",
                local_count=3,
                max_processors=5,
                processor_id="processor-3",
                processor_url="http://processor-3:8004",
            )

        decision = maybe_scale_or_fallback(
            live_processors,
            redis_client,
            scale_fn=fake_scale_fn,
        )

        assert decision["scaled"] is True
        assert decision["fallback_processor"] is None
        assert decision["scaling_action"] == "local_scaled:processor-3"
        assert decision["reason"] == "local_overload_scaled_new_processor"
    finally:
        load_balancer_app.settings = original_settings


def test_no_scaling_occurs_when_local_capacity_is_already_at_max():
    original_settings = load_balancer_app.settings
    load_balancer_app.settings = FakeSettings
    try:
        redis_client = FakeRedis()
        live_processors = [
            {"processor_id": "processor-1", "cpu_percent": 95.0},
            {"processor_id": "processor-2", "cpu_percent": 91.0},
        ]

        def fake_scale_fn(redis_client, settings):
            return AutoscaleResult(
                scaled=False,
                reason="max_processors_reached",
                local_count=5,
                max_processors=5,
            )

        decision = maybe_scale_or_fallback(
            live_processors,
            redis_client,
            scale_fn=fake_scale_fn,
        )

        assert decision["scaled"] is False
        assert decision["fallback_processor"] is None
        assert decision["scaling_action"] == "max_processors_reached"
        assert decision["reason"] == "max_processors_reached"
    finally:
        load_balancer_app.settings = original_settings


def test_no_live_processors_are_not_treated_as_overloaded():
    redis_client = FakeRedis()

    assert all_live_processors_overloaded([]) is False
    assert has_healthy_local_capacity([]) is False
    assert get_local_capacity_state([]) == "no_live_local_processors"
    assert decide_scaling_action([], redis_client) == "none"


def test_maybe_scale_or_fallback_uses_cloud_when_max_local_capacity_reached():
    original_settings = load_balancer_app.settings
    load_balancer_app.settings = FakeSettings
    try:
        redis_client = FakeRedis()
        bootstrap_processor_registry(
            redis_client,
            local_processor_urls=PROCESSORS,
            cloud_processor_url="https://pixelrouter-cloud.run.app",
        )
        live_processors = [
            {"processor_id": "processor-1", "cpu_percent": 95.0},
            {"processor_id": "processor-2", "cpu_percent": 91.0},
        ]

        def fake_scale_fn(redis_client, settings):
            return AutoscaleResult(
                scaled=False,
                reason="max_processors_reached",
                local_count=5,
                max_processors=5,
            )

        decision = maybe_scale_or_fallback(
            live_processors,
            redis_client,
            scale_fn=fake_scale_fn,
        )

        assert decision["scaled"] is False
        assert decision["fallback_processor"]["url"] == (
            "https://pixelrouter-cloud.run.app"
        )
        assert decision["fallback_processor"]["type"] == "cloud"
        assert decision["scaling_action"] == "max_processors_reached"
        assert decision["reason"] == "cloud_fallback_max_local_capacity"
    finally:
        load_balancer_app.settings = original_settings


def test_maybe_scale_or_fallback_reports_local_scale_success():
    original_settings = load_balancer_app.settings
    load_balancer_app.settings = FakeSettings
    try:
        redis_client = FakeRedis()
        live_processors = [
            {"processor_id": "processor-1", "cpu_percent": 95.0},
            {"processor_id": "processor-2", "cpu_percent": 91.0},
        ]

        def fake_scale_fn(redis_client, settings):
            return AutoscaleResult(
                scaled=True,
                reason="processor_spawned",
                local_count=3,
                max_processors=5,
                processor_id="processor-3",
                processor_url="http://processor-3:8004",
            )

        decision = maybe_scale_or_fallback(
            live_processors,
            redis_client,
            scale_fn=fake_scale_fn,
        )

        assert decision["scaled"] is True
        assert decision["fallback_processor"] is None
        assert decision["scaling_action"] == "local_scaled:processor-3"
        assert decision["reason"] == "local_overload_scaled_new_processor"
    finally:
        load_balancer_app.settings = original_settings


def test_stale_processor_is_ignored_by_routing():
    redis_client = FakeRedis()
    bootstrap_processor_registry(redis_client, PROCESSORS)
    mark_processor_stale(redis_client, "processor-1")

    redis_client.set("metrics:processor-1:cpu", "5")
    redis_client.set("metrics:processor-1:pending", "0")
    redis_client.set("metrics:processor-2:cpu", "55")
    redis_client.set("metrics:processor-2:pending", "3")

    active_urls = get_processor_urls(
        redis_client,
        processor_type="local",
        statuses={"active"},
    )
    selected = select_processor(active_urls, redis_client)

    assert active_urls == ["http://processor-2:8003"]
    assert selected == "http://processor-2:8003"


def test_build_route_response_includes_routing_metadata():
    response = build_route_response(
        processor_url="https://pixelrouter-cloud.run.app",
        processor_id="pixelrouter-cloud.run.app",
        tier="cloud",
        scaled=False,
        fallback_used=True,
        reason="cloud_fallback_max_local_capacity",
        scaling_action="max_processors_reached",
    )

    assert response == {
        "processor_url": "https://pixelrouter-cloud.run.app",
        "processor_id": "pixelrouter-cloud.run.app",
        "tier": "cloud",
        "processor_type": "cloud",
        "scaled": False,
        "fallback_used": True,
        "scaling_action": "max_processors_reached",
        "reason": "cloud_fallback_max_local_capacity",
    }


def test_build_scaling_status_reports_local_limit_and_cloud_config():
    redis_client = FakeRedis({
        "metrics:processor-1:cpu": "35",
        "metrics:processor-1:pending": "1",
    })
    bootstrap_processor_registry(
        redis_client,
        local_processor_urls=PROCESSORS,
        cloud_processor_url="https://pixelrouter-cloud.run.app",
    )
    mark_processor_stale(redis_client, "processor-2")

    status = build_scaling_status(redis_client)

    assert status["local_count"] == 2
    assert status["max_processors"] == 5
    assert status["local_capacity_state"] == "healthy"
    assert status["cloud_fallback_configured"] is True
    assert status["cloud_processor_url"] == "https://pixelrouter-cloud.run.app"


def test_get_cloud_fallback_processor_returns_registered_cloud_processor():
    redis_client = FakeRedis()
    bootstrap_processor_registry(
        redis_client,
        local_processor_urls=PROCESSORS,
        cloud_processor_url="https://pixelrouter-cloud.run.app",
    )

    processor = get_cloud_fallback_processor(redis_client)

    assert processor["processor_id"] == "pixelrouter-cloud.run.app"
    assert processor["url"] == "https://pixelrouter-cloud.run.app"


def test_scale_local_processors_spawns_next_processor_and_registers_it():
    redis_client = FakeRedis()
    docker_client = FakeDockerClient()
    bootstrap_processor_registry(redis_client, PROCESSORS)

    result = scale_local_processors(
        redis_client,
        FakeSettings,
        docker_client=docker_client,
    )

    run_call = docker_client.containers.run_calls[0]

    assert result.scaled is True
    assert result.processor_id == "processor-3"
    assert result.processor_url == "http://processor-3:8004"
    assert result.local_count == 3
    assert run_call["args"] == ("pixelrouter-processor:latest",)
    assert run_call["kwargs"]["name"] == "processor-3"
    assert run_call["kwargs"]["network"] == "pixelrouter_pixelrouter-network"
    assert run_call["kwargs"]["ports"] == {"8004/tcp": 8004}
    assert run_call["kwargs"]["environment"]["PROCESSOR_ID"] == "processor-3"
    assert run_call["kwargs"]["environment"]["PORT"] == "8004"
    assert get_processor_urls(redis_client, processor_type="local") == [
        "http://processor-1:8002",
        "http://processor-2:8003",
        "http://processor-3:8004",
    ]


def test_scale_local_processors_enforces_max_processor_limit():
    redis_client = FakeRedis()
    docker_client = FakeDockerClient()
    bootstrap_processor_registry(redis_client, PROCESSORS)
    register_processor(
        redis_client,
        processor_id="processor-3",
        url="http://processor-3:8004",
        processor_type="local",
    )
    register_processor(
        redis_client,
        processor_id="processor-4",
        url="http://processor-4:8005",
        processor_type="local",
    )
    register_processor(
        redis_client,
        processor_id="processor-5",
        url="http://processor-5:8006",
        processor_type="local",
    )

    result = scale_local_processors(
        redis_client,
        FakeSettings,
        docker_client=docker_client,
    )

    assert result.scaled is False
    assert result.reason == "max_processors_reached"
    assert result.local_count == 5
    assert docker_client.containers.run_calls == []


def test_update_pending_count_increments_and_clamps_to_zero():
    redis_client = FakeRedis({"metrics:processor-1:pending": "1"})

    incremented = update_pending_count("processor-1", 2, redis_client)
    decremented = update_pending_count("processor-1", -5, redis_client)

    assert incremented == 3
    assert decremented == 0
    assert redis_client.values["metrics:processor-1:pending"] == "0"
