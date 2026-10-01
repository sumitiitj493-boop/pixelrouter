import sys
import os
import pytest
import fakeredis
from unittest.mock import patch, MagicMock, AsyncMock
from contextlib import asynccontextmanager
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processor"))

@asynccontextmanager
async def fake_lifespan(app):
    yield

@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)

@pytest.fixture
async def client(fake_redis):
    with patch("app.lifespan", fake_lifespan), \
         patch("app.r", fake_redis), \
         patch("app.psutil.cpu_percent", return_value=25.0), \
         patch("app.psutil.virtual_memory") as mock_mem:

        mock_mem.return_value = MagicMock(percent=55.0)

        import app as processor_app
        processor_app.app.router.lifespan_context = fake_lifespan

        async with AsyncClient(
            transport=ASGITransport(app=processor_app.app),
            base_url="http://test"
        ) as c:
            yield c, fake_redis

async def test_root_returns_200(client):
    c, _ = client
    response = await c.get("/")
    assert response.status_code == 200

async def test_root_has_service_field(client):
    c, _ = client
    data = (await c.get("/")).json()
    assert data["service"] == "processor"

async def test_root_has_version(client):
    c, _ = client
    data = (await c.get("/")).json()
    assert "version" in data
    assert data["version"] == "0.1.0"

async def test_root_has_processor_id(client):
    c, _ = client
    data = (await c.get("/")).json()
    assert "processor_id" in data

async def test_health_returns_200(client):
    c, _ = client
    response = await c.get("/health")
    assert response.status_code == 200

async def test_health_status_ok(client):
    c, _ = client
    data = (await c.get("/health")).json()
    assert data["status"] == "ok"

async def test_health_has_processor_id(client):
    c, _ = client
    data = (await c.get("/health")).json()
    assert "processor_id" in data

async def test_metrics_returns_200(client):
    c, _ = client
    response = await c.get("/metrics")
    assert response.status_code == 200

async def test_metrics_has_required_fields(client):
    c, _ = client
    data = (await c.get("/metrics")).json()
    assert "cpu_percent" in data
    assert "ram_percent" in data
    assert "pending_jobs" in data
    assert "processor_id" in data

async def test_metrics_cpu_is_float(client):
    c, _ = client
    data = (await c.get("/metrics")).json()
    assert isinstance(data["cpu_percent"], (int, float))

async def test_metrics_pending_is_int(client):
    c, _ = client
    data = (await c.get("/metrics")).json()
    assert isinstance(data["pending_jobs"], int)

async def test_metrics_writes_to_redis(client):
    c, fake_r = client
    await c.get("/metrics")
    from app import PROCESSOR_ID
    cpu_val = fake_r.get(f"metrics:{PROCESSOR_ID}:cpu")
    assert cpu_val is not None
    ttl = fake_r.ttl(f"metrics:{PROCESSOR_ID}:cpu")
    assert ttl > 0

async def test_connection_manager_connect_disconnect():
    from app import ConnectionManager
    mgr = ConnectionManager()
    mock_ws = AsyncMock()
    mock_ws.send_json = AsyncMock()
    await mgr.connect("job_test", mock_ws)
    assert "job_test" in mgr.active
    mgr.disconnect("job_test")
    assert "job_test" not in mgr.active

async def test_connection_manager_send_noop_when_no_socket():
    from app import ConnectionManager
    mgr = ConnectionManager()
    await mgr.send("ghost_job", {"type": "progress", "progress": 50})

async def test_connection_manager_send_delivers_to_socket():
    from app import ConnectionManager
    mgr = ConnectionManager()
    mock_ws = AsyncMock()
    mock_ws.send_json = AsyncMock()
    await mgr.connect("job_abc", mock_ws)
    payload = {"type": "progress", "stage": "removing_background", "progress": 40}
    await mgr.send("job_abc", payload)
    mock_ws.send_json.assert_called_once_with(payload)

async def test_save_result_raises_on_empty_bytes():
    from app import save_result
    with patch("app.upload_bytes", new_callable=AsyncMock):
        with pytest.raises(ValueError, match="empty"):
            await save_result(
                job_id="job_test",
                result_bytes=b"", 
                caption="test caption"
            )
