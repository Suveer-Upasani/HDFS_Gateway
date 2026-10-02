"""Unit and integration tests for Camera API endpoints and CameraService."""

import threading
import time
from unittest.mock import patch

import httpx
import pytest

from app.main import app
from app.services.camera_service import CameraService, camera_service
from vision_client.config import VisionClientSettings


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def async_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.anyio
async def test_dashboard_endpoint(async_client: httpx.AsyncClient):
    """Verify /dashboard returns HTML page successfully."""
    response = await async_client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Vision Monitor" in response.text
    assert "btn-start" in response.text
    assert "btn-stop" in response.text
    assert "camera-stream" in response.text


@pytest.mark.anyio
async def test_camera_status_endpoint(async_client: httpx.AsyncClient):
    """Verify GET /api/camera/status returns camera state."""
    response = await async_client.get("/api/camera/status")
    assert response.status_code == 200
    data = response.json()
    assert "running" in data
    assert "camera_id" in data
    assert "camera_source" in data


@pytest.mark.anyio
async def test_camera_stats_endpoint(async_client: httpx.AsyncClient):
    """Verify GET /api/camera/stats returns statistics structure."""
    response = await async_client.get("/api/camera/stats")
    assert response.status_code == 200
    data = response.json()
    assert "fps" in data
    assert "frames" in data
    assert "total_detections" in data
    assert "confidence" in data
    assert "camera_id" in data


@pytest.mark.anyio
async def test_camera_stream_generator():
    """Verify camera_service.generate_mjpeg_stream yields multipart MJPEG boundary."""
    gen = camera_service.generate_mjpeg_stream()
    first_frame = await anext(gen)
    assert b"--frame" in first_frame
    assert b"Content-Type: image/jpeg" in first_frame


@pytest.mark.anyio
@patch("vision_client.runner.VisionRunner.run")
async def test_camera_start_and_stop_lifecycle(mock_run, async_client: httpx.AsyncClient):
    """Verify POST /api/camera/start and POST /api/camera/stop manage lifecycle."""
    camera_service.stop()

    stop_event = threading.Event()

    def fake_run(*args, **kwargs):
        while not stop_event.is_set():
            time.sleep(0.02)

    mock_run.side_effect = fake_run

    # 1. Start camera
    response = await async_client.post("/api/camera/start")
    assert response.status_code == 200
    data = response.json()
    assert data["running"] is True
    assert data["status"] in ("started", "already_running")

    # 2. Check status is running
    status_res = await async_client.get("/api/camera/status")
    assert status_res.status_code == 200
    assert status_res.json()["running"] is True

    # 3. Start again returns already_running
    dup_res = await async_client.post("/api/camera/start")
    assert dup_res.status_code == 200
    assert dup_res.json()["status"] == "already_running"

    # Signal fake_run to exit when stop is triggered
    stop_event.set()

    # 4. Stop camera
    stop_res = await async_client.post("/api/camera/stop")
    assert stop_res.status_code == 200
    assert stop_res.json()["running"] is False
    assert stop_res.json()["status"] == "stopped"

    # 5. Stop again returns already_stopped
    stop_dup_res = await async_client.post("/api/camera/stop")
    assert stop_dup_res.status_code == 200
    assert stop_dup_res.json()["status"] == "already_stopped"


def test_camera_service_standby_frame():
    """Verify CameraService generates valid JPEG standby frame."""
    settings = VisionClientSettings(_env_file=None)
    service = CameraService(settings=settings)
    frame = service._create_standby_frame()
    assert len(frame) > 0
    assert frame.startswith(b"\xff\xd8")  # JPEG magic bytes
