import io
from unittest.mock import AsyncMock, patch

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
async def test_dashboard_ui_render(async_client: httpx.AsyncClient):
    """Verify that dashboard root endpoint renders HTML template successfully."""
    response = await async_client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "HDFS Gateway" in response.text


@pytest.mark.anyio
async def test_health_check_endpoint(async_client: httpx.AsyncClient):
    """Verify health check endpoint returns 200 and live Hadoop connectivity status."""
    with patch("app.services.hdfs_service.HDFSService.check_health", new_callable=AsyncMock) as mock_health:
        mock_health.return_value = {
            "status": "connected",
            "namenode_url": "http://localhost:9870",
            "hdfs_user": "hadoop",
            "cluster_details": {"type": "DIRECTORY"},
        }

        response = await async_client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["gateway_status"] == "healthy"
        assert data["hdfs_connectivity"]["status"] == "connected"


@pytest.mark.anyio
async def test_list_files_endpoint(async_client: httpx.AsyncClient):
    """Verify list files API endpoint format."""
    mock_files = [
        {
            "name": "sample.txt",
            "path": "/sample.txt",
            "type": "FILE",
            "length": 1024,
            "owner": "hadoop",
            "group": "supergroup",
            "permission": "755",
            "modificationTime": 1700000000,
            "replication": 3,
            "blockSize": 134217728,
        }
    ]
    with patch("app.services.hdfs_service.HDFSService.list_directory", new_callable=AsyncMock) as mock_list:
        mock_list.return_value = mock_files

        response = await async_client.get("/api/v1/files/list?path=/")
        assert response.status_code == 200
        data = response.json()
        assert data["path"] == "/"
        assert data["count"] == 1
        assert data["items"][0]["name"] == "sample.txt"


@pytest.mark.anyio
async def test_path_traversal_protection(async_client: httpx.AsyncClient):
    """Verify that path traversal attempts are rejected with 400 Bad Request."""
    response = await async_client.get("/api/v1/files/list?path=/../../etc/passwd")
    assert response.status_code == 400
    assert "Invalid HDFS path" in response.json()["detail"]


@pytest.mark.anyio
async def test_upload_file_endpoint(async_client: httpx.AsyncClient):
    """Verify upload file endpoint passes file stream to HDFS service."""
    with patch("app.services.hdfs_service.HDFSService.upload_file_stream", new_callable=AsyncMock) as mock_upload:
        mock_upload.return_value = {
            "message": "File uploaded successfully",
            "path": "/data/test.txt",
        }

        files = {"file": ("test.txt", io.BytesIO(b"Hello HDFS!"), "text/plain")}
        data = {"destination_dir": "/data", "overwrite": "true"}

        response = await async_client.post("/api/v1/files/upload", files=files, data=data)
        assert response.status_code == 200
        assert response.json()["path"] == "/data/test.txt"


@pytest.mark.anyio
async def test_make_directory_endpoint(async_client: httpx.AsyncClient):
    """Verify directory creation endpoint."""
    with patch("app.services.hdfs_service.HDFSService.make_directory", new_callable=AsyncMock) as mock_mkdir:
        mock_mkdir.return_value = True

        response = await async_client.post("/api/v1/files/mkdir?path=/new_folder")
        assert response.status_code == 200
        assert response.json()["path"] == "/new_folder"
        assert response.json()["success"] is True


@pytest.mark.anyio
async def test_delete_endpoint(async_client: httpx.AsyncClient):
    """Verify delete file or directory endpoint."""
    with patch("app.services.hdfs_service.HDFSService.delete_path", new_callable=AsyncMock) as mock_delete:
        mock_delete.return_value = True

        response = await async_client.delete("/api/v1/files/delete?path=/data/test.txt")
        assert response.status_code == 200
        assert response.json()["path"] == "/data/test.txt"
