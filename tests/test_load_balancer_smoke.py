"""Docker Compose smoke path for the load balancer.

This file stays out of the default unit-test flow. Run it only after the
compose stack is up and the local processor containers are healthy.
"""

from __future__ import annotations

import json
import os
from urllib.request import urlopen

import pytest


RUN_SMOKE_TESTS = os.getenv("RUN_DOCKER_SMOKE_TESTS", "0") == "1"
LOAD_BALANCER_URL = os.getenv("LOAD_BALANCER_URL", "http://localhost:8001")


pytestmark = pytest.mark.skipif(
    not RUN_SMOKE_TESTS,
    reason="Reserved for Docker Compose smoke validation",
)


def _get_json(url: str) -> dict:
    with urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def test_compose_stack_registers_processors_and_routes_requests():
    base_url = LOAD_BALANCER_URL.rstrip("/")

    health = _get_json(f"{base_url}/health")
    status = _get_json(f"{base_url}/processors/status")
    route = _get_json(f"{base_url}/route")

    registered_processors = status["processors"]
    registered_urls = {
        processor["url"]
        for processor in registered_processors
        if processor.get("url")
    }

    assert health["status"] == "ok"
    assert len(registered_processors) >= 2
    assert route["processor_url"] in registered_urls
    assert route["processor_id"]
    assert route["tier"] in {"local", "cloud"}
    assert route["fallback_used"] in {True, False}