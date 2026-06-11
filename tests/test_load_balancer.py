# PixelRouter - Load Balancer Tests

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../load-balancer"))

from app import (
    all_live_processors_overloaded,
    decide_scaling_action,
    get_local_capacity_state,
    has_healthy_local_capacity,
)
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


def test_no_live_processors_are_not_treated_as_overloaded():
    redis_client = FakeRedis()

    assert all_live_processors_overloaded([]) is False
    assert has_healthy_local_capacity([]) is False
    assert get_local_capacity_state([]) == "no_live_local_processors"
    assert decide_scaling_action([], redis_client) == "none"


def test_update_pending_count_increments_and_clamps_to_zero():
    redis_client = FakeRedis({"metrics:processor-1:pending": "1"})

    incremented = update_pending_count("processor-1", 2, redis_client)
    decremented = update_pending_count("processor-1", -5, redis_client)

    assert incremented == 3
    assert decremented == 0
    assert redis_client.values["metrics:processor-1:pending"] == "0"
