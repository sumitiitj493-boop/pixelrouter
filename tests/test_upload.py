import importlib.util
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient


UPLOAD_APP_PATH = Path(__file__).resolve().parents[1] / "upload-service" / "app.py"


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.uploaded = None
        self.deleted = False
        self.public_url = f"https://storage.example/{name}"

    def upload_from_string(self, file_bytes, content_type):
        self.uploaded = {
            "bytes": file_bytes,
            "content_type": content_type,
        }

    def delete(self):
        self.deleted = True


class FakeBucket:
    def __init__(self):
        self.blobs = {}

    def blob(self, object_name):
        if object_name not in self.blobs:
            self.blobs[object_name] = FakeBlob(object_name)
        return self.blobs[object_name]


class FakeGCSClient:
    def __init__(self):
        self.buckets = {}

    def bucket(self, bucket_name):
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = FakeBucket()
        return self.buckets[bucket_name]


class FailingGCSClient(FakeGCSClient):
    def bucket(self, bucket_name):
        bucket = super().bucket(bucket_name)

        class FailingBucket:
            def blob(self, object_name):
                blob = bucket.blob(object_name)

                def fail_upload(*args, **kwargs):
                    raise RuntimeError("storage unavailable")

                blob.upload_from_string = fail_upload
                return blob

        return FailingBucket()


class FakePipeline:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.commands = []

    def hset(self, key, mapping):
        self.commands.append(("hset", key, mapping))
        return self

    def expire(self, key, ttl):
        self.commands.append(("expire", key, ttl))
        return self

    def lpush(self, key, value):
        self.commands.append(("lpush", key, value))
        return self

    def delete(self, key):
        self.commands.append(("delete", key))
        return self

    def lrem(self, key, count, value):
        self.commands.append(("lrem", key, count, value))
        return self

    def execute(self):
        if self.redis_client.fail_pipeline:
            raise RuntimeError("redis unavailable")

        for command in self.commands:
            name = command[0]
            if name == "hset":
                _, key, mapping = command
                self.redis_client.hashes[key] = dict(mapping)
            elif name == "expire":
                _, key, ttl = command
                self.redis_client.ttls[key] = ttl
            elif name == "lpush":
                _, key, value = command
                self.redis_client.lists.setdefault(key, []).insert(0, value)
            elif name == "delete":
                _, key = command
                self.redis_client.hashes.pop(key, None)
            elif name == "lrem":
                _, key, _count, value = command
                self.redis_client.lists[key] = [
                    item for item in self.redis_client.lists.get(key, [])
                    if item != value
                ]
        return [True] * len(self.commands)


class FakeRedis:
    def __init__(self, fail_pipeline=False):
        self.fail_pipeline = fail_pipeline
        self.hashes = {}
        self.ttls = {}
        self.lists = {}

    def pipeline(self, transaction=True):
        self.last_transaction = transaction
        return FakePipeline(self)


@pytest.fixture()
def upload_app(monkeypatch):
    monkeypatch.setenv("GCS_BUCKET_NAME", "pixelrouter-test")
    monkeypatch.setenv("GCS_OBJECT_PREFIX", "jobs")
    monkeypatch.setenv("GCS_URL_STRATEGY", "gcs_uri")
    monkeypatch.setenv("GCS_SIGNED_URL_TTL_SECONDS", "3600")

    module_name = "upload_service_app_under_test"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, UPLOAD_APP_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def client(upload_app):
    return TestClient(upload_app.app)


def test_upload_success_persists_job_and_enqueues(client, upload_app, monkeypatch):
    fake_gcs = FakeGCSClient()
    fake_redis = FakeRedis()
    upload_app.gcs_client = fake_gcs
    upload_app.r = fake_redis

    async def route_decision(job_id):
        return {
            "processor_url": "http://processor-1:8002",
            "processor_id": "processor-1",
            "tier": "local",
            "fallback_used": False,
            "scaled": False,
            "scaling_action": "",
            "reason": "local_capacity_available",
        }

    monkeypatch.setattr(upload_app, "_get_route_decision", route_decision)

    response = client.post(
        "/upload",
        files={"file": ("sample.png", b"image-bytes", "image/png")},
    )

    assert response.status_code == 200
    body = response.json()
    job_id = body["job_id"]
    job_key = f"job:{job_id}"

    assert fake_redis.last_transaction is True
    assert fake_redis.hashes[job_key]["status"] == "pending"
    assert fake_redis.hashes[job_key]["processor_id"] == "processor-1"
    assert fake_redis.hashes[job_key]["gcs_object"] == f"jobs/{job_id}.png"
    assert fake_redis.hashes[job_key]["gcs_content_type"] == "image/png"
    assert fake_redis.ttls[job_key] == 86400
    assert fake_redis.lists["queue:processor-1"] == [job_id]

    uploaded_blob = fake_gcs.bucket("pixelrouter-test").blob(f"jobs/{job_id}.png")
    assert uploaded_blob.uploaded == {
        "bytes": b"image-bytes",
        "content_type": "image/png",
    }
    assert body["queue_key"] == "queue:processor-1"


def test_upload_rejects_invalid_file_type(client, upload_app):
    upload_app.gcs_client = FakeGCSClient()
    upload_app.r = FakeRedis()

    response = client.post(
        "/upload",
        files={"file": ("sample.txt", b"not-an-image", "text/plain")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Only JPEG, PNG, WebP allowed"


def test_upload_returns_error_when_gcs_upload_fails(client, upload_app):
    fake_redis = FakeRedis()
    upload_app.gcs_client = FailingGCSClient()
    upload_app.r = fake_redis

    response = client.post(
        "/upload",
        files={"file": ("sample.webp", b"image-bytes", "image/webp")},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "GCS upload failed"
    assert fake_redis.hashes == {}
    assert fake_redis.lists == {}


def test_upload_removes_gcs_object_when_route_fails(client, upload_app, monkeypatch):
    fake_gcs = FakeGCSClient()
    fake_redis = FakeRedis()
    upload_app.gcs_client = fake_gcs
    upload_app.r = fake_redis

    async def route_failure(job_id):
        raise HTTPException(status_code=502, detail="Load balancer is unavailable")

    monkeypatch.setattr(upload_app, "_get_route_decision", route_failure)

    response = client.post(
        "/upload",
        files={"file": ("sample.jpg", b"image-bytes", "image/jpeg")},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Load balancer is unavailable"
    assert fake_redis.hashes == {}
    assert fake_redis.lists == {}

    object_name = next(iter(fake_gcs.bucket("pixelrouter-test").blobs))
    assert fake_gcs.bucket("pixelrouter-test").blob(object_name).deleted is True


def test_upload_cleans_up_when_redis_publish_fails(client, upload_app, monkeypatch):
    fake_gcs = FakeGCSClient()
    fake_redis = FakeRedis(fail_pipeline=True)
    upload_app.gcs_client = fake_gcs
    upload_app.r = fake_redis

    async def route_decision(job_id):
        return {
            "processor_url": "http://processor-1:8002",
            "processor_id": "processor-1",
            "tier": "local",
            "fallback_used": False,
            "scaled": False,
            "scaling_action": "",
            "reason": "local_capacity_available",
        }

    monkeypatch.setattr(upload_app, "_get_route_decision", route_decision)

    response = client.post(
        "/upload",
        files={"file": ("sample.png", b"image-bytes", "image/png")},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to publish job"

    object_name = next(iter(fake_gcs.bucket("pixelrouter-test").blobs))
    assert fake_gcs.bucket("pixelrouter-test").blob(object_name).deleted is True
