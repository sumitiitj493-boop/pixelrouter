# PixelRouter - Load Balancer Tests

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../load-balancer"))

from router import processor_id_from_url, select_processor, update_pending_count


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.expirations = {}

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


def test_update_pending_count_increments_and_clamps_to_zero():
    redis_client = FakeRedis({"metrics:processor-1:pending": "1"})

    incremented = update_pending_count("processor-1", 2, redis_client)
    decremented = update_pending_count("processor-1", -5, redis_client)

    assert incremented == 3
    assert decremented == 0
    assert redis_client.values["metrics:processor-1:pending"] == "0"
