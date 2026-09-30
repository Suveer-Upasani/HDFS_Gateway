"""Unit tests for the FastAPI Vision Streaming router endpoints."""

import httpx
import pytest

from app.main import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def async_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.anyio
async def test_vision_status_endpoint(async_client: httpx.AsyncClient):
    """Verify /api/v1/vision/status returns correct pipeline architectural metadata."""
    response = await async_client.get("/api/v1/vision/status")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "operational"
    assert data["kafka_topic"] == "vision-events"
    assert data["stream_window"] == "10s_tumbling"
    assert data["watermark_delay_seconds"] == 2.0
    assert data["active_sink"] == "print (dev)"
    assert "kafka_bootstrap_servers" in data
    assert "flink_jobmanager_url" in data


@pytest.mark.anyio
async def test_vision_events_endpoint(async_client: httpx.AsyncClient):
    """Verify /api/v1/vision/events returns real system contract without mock data."""
    response = await async_client.get("/api/v1/vision/events")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "streaming_to_kafka_and_flink"
    assert data["sink_type"] == "flink_print_sink"
    assert data["events"] == []
    assert data["count"] == 0
    assert "vision-events" in data["message"]


@pytest.mark.anyio
async def test_vision_stats_endpoint(async_client: httpx.AsyncClient):
    """Verify /api/v1/vision/stats returns real aggregation parameters."""
    response = await async_client.get("/api/v1/vision/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "active"
    assert data["window_duration_seconds"] == 10
    assert data["watermark_delay_seconds"] == 2.0
    assert data["grouping_keys"] == ["object"]
    assert data["source_topic"] == "vision-events"


@pytest.mark.anyio
async def test_vision_analytics_endpoint(async_client: httpx.AsyncClient):
    """Verify /api/v1/vision/analytics returns window analytics state."""
    response = await async_client.get("/api/v1/vision/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert data["window_type"] == "tumbling"
    assert data["window_size_seconds"] == 10
    assert data["records"] == []
    assert "vision_analytics.sql" in data["message"]
