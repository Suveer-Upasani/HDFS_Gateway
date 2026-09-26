import asyncio
import io
import pytest
from unittest.mock import patch
import httpx

from app.main import app
from app.config import sanitize_filename, format_bytes
from app.hdfs import HDFSFileNotFoundError


class AsyncAppClient:
    """Synchronous test client adapter using httpx.AsyncClient with ASGITransport."""
    def __init__(self, asgi_app):
        self.app = asgi_app
        self.base_url = "http://testserver"

    def _call(self, method: str, url: str, **kwargs):
        async def _run():
            # If files are passed as tuple/bytes, httpx handles multipart upload
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url=self.base_url) as client:
                return await client.request(method, url, **kwargs)
        return asyncio.run(_run())

    def get(self, url: str, **kwargs):
        return self._call("GET", url, **kwargs)

    def post(self, url: str, **kwargs):
        return self._call("POST", url, **kwargs)

    def delete(self, url: str, **kwargs):
        return self._call("DELETE", url, **kwargs)


client = AsyncAppClient(app)


# ==========================================
# Filename Security & Utility Unit Tests
# ==========================================

def test_sanitize_filename_valid():
    assert sanitize_filename("test.txt") == "test.txt"
    assert sanitize_filename("dataset_2026.csv") == "dataset_2026.csv"
    assert sanitize_filename("my-file (1).tar.gz") == "my-file (1).tar.gz"


def test_sanitize_filename_traversal_rejections():
    # Directory traversal attempts
    with pytest.raises(ValueError):
        sanitize_filename("../../etc/passwd")

    with pytest.raises(ValueError):
        sanitize_filename("../secret.txt")

    with pytest.raises(ValueError):
        sanitize_filename("/absolute/path/file.txt")

    with pytest.raises(ValueError):
        sanitize_filename("folder/subfolder/file.txt")

    with pytest.raises(ValueError):
        sanitize_filename("")

    with pytest.raises(ValueError):
        sanitize_filename("   ")


def test_format_bytes():
    assert format_bytes(0) == "0 B"
    assert format_bytes(500) == "500 B"
    assert format_bytes(1024) == "1.0 KB"
    assert format_bytes(1024 * 1024 * 2.5) == "2.5 MB"
    assert format_bytes(1024 * 1024 * 1024 * 1.2) == "1.2 GB"


# ==========================================
# Web Page Route Tests
# ==========================================

def test_root_redirect():
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "/dashboard"


def test_get_dashboard():
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "HDFS Gateway" in response.text
    assert "Dashboard" in response.text
    assert "Total Files" in response.text


def test_get_files_page():
    response = client.get("/files")
    assert response.status_code == 200
    assert "Files Manager" in response.text


def test_get_upload_page():
    response = client.get("/upload")
    assert response.status_code == 200
    assert "Upload file to HDFS" in response.text


# ==========================================
# REST API Endpoint Tests
# ==========================================

def test_api_health_endpoint():
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["api"] == "Online"
    assert "hdfs_connected" in data
    assert "upload_path" in data


@patch("app.hdfs.hdfs_available", return_value=True)
@patch("app.hdfs.list_files")
def test_api_list_files(mock_list, mock_avail):
    mock_list.return_value = [
        {
            "name": "data.csv",
            "size": 2202009,
            "size_formatted": "2.1 MB",
            "path": "/user/suveer/hdfs-gateway/uploads/data.csv",
            "modified": "2026-09-26 12:00",
            "is_dir": False,
            "owner": "suveer",
        }
    ]

    response = client.get("/api/files")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["count"] == 1
    assert data["files"][0]["name"] == "data.csv"


@patch("app.hdfs.hdfs_available", return_value=False)
def test_api_files_hdfs_offline(mock_avail):
    response = client.get("/api/files")
    assert response.status_code == 503
    assert "unavailable" in response.json()["detail"].lower()


@patch("app.hdfs.hdfs_available", return_value=True)
@patch("app.hdfs.upload_file")
def test_api_upload_file_success(mock_upload, mock_avail):
    mock_upload.return_value = {
        "filename": "sample.txt",
        "path": "/user/suveer/hdfs-gateway/uploads/sample.txt",
        "size": 13,
        "size_formatted": "13 B",
        "status": "success",
    }

    files = {"file": ("sample.txt", io.BytesIO(b"Hello, Hadoop!"), "text/plain")}
    response = client.post("/api/upload", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["filename"] == "sample.txt"
    assert "sample.txt" in data["path"]


def test_api_upload_path_traversal_rejected():
    with patch("app.hdfs.hdfs_available", return_value=True):
        files = {"file": ("../../etc/passwd", io.BytesIO(b"malicious"), "text/plain")}
        response = client.post("/api/upload", files=files)
        assert response.status_code == 400


@patch("app.hdfs.hdfs_available", return_value=True)
@patch("app.hdfs.delete_file", return_value=True)
def test_api_delete_file_success(mock_delete, mock_avail):
    response = client.delete("/api/files/test.txt")
    assert response.status_code == 200
    assert response.json()["success"] is True


@patch("app.hdfs.hdfs_available", return_value=True)
@patch("app.hdfs.delete_file", side_effect=HDFSFileNotFoundError("File not found"))
def test_api_delete_file_not_found(mock_delete, mock_avail):
    response = client.delete("/api/files/missing.txt")
    assert response.status_code == 404
