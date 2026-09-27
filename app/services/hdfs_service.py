import logging
import posixpath
import re
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def sanitize_hdfs_path(path: str) -> str:
    """
    Sanitizes and normalizes user-provided HDFS paths.
    Prevents path traversal and validates against malicious path structures.
    """
    if not path or not path.strip():
        return "/"

    raw = path.strip().replace("\\", "/")

    # Check for path traversal segments
    parts = [p for p in raw.split("/") if p]
    if any(p == ".." for p in parts):
        raise ValueError("Invalid HDFS path: path traversal ('..') is not permitted")

    # Reject null bytes or dangerous control characters
    if "\x00" in raw or any(ord(c) < 32 for c in raw):
        raise ValueError("Invalid HDFS path: forbidden control characters detected")

    # Normalize path ensuring single leading slash
    normalized = posixpath.normpath("/" + "/".join(parts))
    while normalized.startswith("//"):
        normalized = normalized[1:]

    return normalized if normalized else "/"


def sanitize_filename(filename: str) -> str:
    """Validates and sanitizes an uploaded filename."""
    if not filename or not filename.strip():
        raise ValueError("Filename cannot be empty")

    base = posixpath.basename(filename.strip().replace("\\", "/"))
    if not base or base in (".", ".."):
        raise ValueError("Invalid filename")

    # Remove any dangerous characters
    clean = re.sub(r'[\x00/\\:*?"<>|]', "", base)
    if not clean:
        raise ValueError("Filename contains only invalid characters")

    return clean


class HDFSService:
    """
    Client service for Apache Hadoop WebHDFS v1 REST API.
    Interacts with Hadoop NameNode and DataNodes over HTTP.
    """

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.HDFS_NAMENODE_URL.rstrip("/")
        self.user = self.settings.HDFS_USER
        self.timeout = self.settings.HDFS_TIMEOUT_SECONDS

    def _get_webhdfs_url(
        self,
        path: str,
        op: str,
        extra_params: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        clean_path = sanitize_hdfs_path(path)
        # WebHDFS path format: /webhdfs/v1/<path>
        url_path = clean_path if clean_path != "/" else ""
        url = f"{self.base_url}/webhdfs/v1{url_path}"
        params: dict[str, Any] = {
            "op": op,
            "user.name": self.user,
        }
        if extra_params:
            params.update(extra_params)
        return url, params

    def _parse_error_response(self, response: httpx.Response) -> str:
        try:
            data = response.json()
            remote_exc = data.get("RemoteException", {})
            msg = remote_exc.get("message")
            exc_class = remote_exc.get("exception")
            if msg and exc_class:
                return f"{exc_class}: {msg}"
            if msg:
                return msg
        except (ValueError, KeyError, httpx.HTTPError):
            logger.debug("Failed to parse RemoteException from HDFS response body")
        return response.text or f"HTTP {response.status_code}"

    async def check_health(self) -> dict[str, Any]:
        """Verify connectivity to the Hadoop NameNode WebHDFS service."""
        url, params = self._get_webhdfs_url("/", op="GETFILESTATUS")
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, params=params)
                if response.status_code == 200:
                    data = response.json().get("FileStatus", {})
                    return {
                        "status": "connected",
                        "namenode_url": self.base_url,
                        "hdfs_user": self.user,
                        "cluster_details": {
                            "owner": data.get("owner"),
                            "group": data.get("group"),
                            "permission": data.get("permission"),
                            "type": data.get("type"),
                        },
                    }
                return {
                    "status": "degraded",
                    "namenode_url": self.base_url,
                    "status_code": response.status_code,
                    "error": self._parse_error_response(response),
                }
        except httpx.ConnectError:
            return {
                "status": "disconnected",
                "namenode_url": self.base_url,
                "error": f"Connection refused to Hadoop NameNode at {self.base_url}",
            }
        except httpx.TimeoutException:
            return {
                "status": "timeout",
                "namenode_url": self.base_url,
                "error": f"Timeout connecting to Hadoop NameNode at {self.base_url}",
            }
        except (httpx.RequestError, OSError) as e:
            return {
                "status": "error",
                "namenode_url": self.base_url,
                "error": str(e),
            }

    async def list_directory(self, path: str = "/") -> list[dict[str, Any]]:
        """List files and subdirectories at a given HDFS path."""
        clean_path = sanitize_hdfs_path(path)
        url, params = self._get_webhdfs_url(clean_path, op="LISTSTATUS")

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)
                if response.status_code != 200:
                    error_msg = self._parse_error_response(response)
                    raise RuntimeError(f"HDFS List failed ({response.status_code}): {error_msg}")

                data = response.json()
                file_statuses = data.get("FileStatuses", {}).get("FileStatus", [])

                results = []
                for item in file_statuses:
                    name = item.get("pathSuffix", "")
                    item_path = posixpath.join(clean_path, name)
                    results.append({
                        "name": name,
                        "path": item_path,
                        "type": item.get("type"),  # "FILE" or "DIRECTORY"
                        "length": item.get("length", 0),
                        "owner": item.get("owner", ""),
                        "group": item.get("group", ""),
                        "permission": item.get("permission", ""),
                        "modificationTime": item.get("modificationTime", 0),
                        "replication": item.get("replication", 0),
                        "blockSize": item.get("blockSize", 0),
                    })
                return results
        except httpx.ConnectError as ce:
            raise ConnectionError(f"Cannot connect to Hadoop NameNode at {self.base_url}") from ce

    async def upload_file_stream(
        self,
        stream: AsyncIterator[bytes],
        destination_path: str,
        overwrite: bool = True,
    ) -> dict[str, Any]:
        """
        Upload file stream to HDFS using WebHDFS 2-step write protocol:
        1. Step 1: PUT to NameNode with op=CREATE -> Receives 307 redirect with DataNode URL.
        2. Step 2: PUT streaming payload to the DataNode URL.
        """
        clean_path = sanitize_hdfs_path(destination_path)
        url, params = self._get_webhdfs_url(
            clean_path,
            op="CREATE",
            extra_params={"overwrite": "true" if overwrite else "false"},
        )

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Step 1: Request upload location (NameNode provides 307 redirect with DataNode URL)
                step1_response = await client.put(url, params=params, follow_redirects=False)

                if step1_response.status_code not in (307, 201, 200):
                    error_msg = self._parse_error_response(step1_response)
                    raise RuntimeError(f"HDFS upload initialization failed ({step1_response.status_code}): {error_msg}")

                datanode_url = step1_response.headers.get("Location")
                if not datanode_url:
                    raise RuntimeError("HDFS did not return DataNode redirect URL in Location header")

                # Step 2: Stream binary content directly to the DataNode
                step2_response = await client.put(
                    datanode_url,
                    content=stream,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=self.timeout,
                )

                if step2_response.status_code not in (200, 201):
                    error_msg = self._parse_error_response(step2_response)
                    raise RuntimeError(f"DataNode file transfer failed ({step2_response.status_code}): {error_msg}")

                return {
                    "message": "File uploaded successfully",
                    "path": clean_path,
                }
        except httpx.ConnectError as ce:
            raise ConnectionError(f"Cannot connect to Hadoop cluster at {self.base_url}") from ce

    async def download_file_stream(self, path: str) -> tuple[AsyncIterator[bytes], int | None]:
        """Stream file content directly from HDFS DataNode to avoid memory accumulation."""
        clean_path = sanitize_hdfs_path(path)
        url, params = self._get_webhdfs_url(clean_path, op="OPEN")

        client = httpx.AsyncClient(timeout=self.timeout, follow_redirects=True)
        try:
            req = client.build_request("GET", url, params=params)
            response = await client.send(req, stream=True)
            if response.status_code != 200:
                error_body = await response.aread()
                await client.aclose()
                raise RuntimeError(f"HDFS download failed ({response.status_code}): {error_body.decode('utf-8', errors='replace')}")

            content_length_hdr = response.headers.get("Content-Length")
            content_length = int(content_length_hdr) if content_length_hdr and content_length_hdr.isdigit() else None

            async def iterator() -> AsyncIterator[bytes]:
                try:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        yield chunk
                finally:
                    await response.aclose()
                    await client.aclose()

            return iterator(), content_length
        except httpx.ConnectError as ce:
            await client.aclose()
            raise ConnectionError(f"Cannot connect to Hadoop at {self.base_url}") from ce
        except Exception:
            await client.aclose()
            raise

    async def delete_path(self, path: str, recursive: bool = True) -> bool:
        """Delete a file or directory in HDFS."""
        clean_path = sanitize_hdfs_path(path)
        if clean_path == "/":
            raise ValueError("Root path '/' cannot be deleted")

        url, params = self._get_webhdfs_url(
            clean_path,
            op="DELETE",
            extra_params={"recursive": "true" if recursive else "false"},
        )
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.delete(url, params=params)
                if response.status_code != 200:
                    error_msg = self._parse_error_response(response)
                    raise RuntimeError(f"HDFS delete failed ({response.status_code}): {error_msg}")
                result = response.json().get("boolean", False)
                return bool(result)
        except httpx.ConnectError as ce:
            raise ConnectionError(f"Cannot connect to Hadoop at {self.base_url}") from ce

    async def make_directory(self, path: str) -> bool:
        """Create a new directory in HDFS."""
        clean_path = sanitize_hdfs_path(path)
        url, params = self._get_webhdfs_url(clean_path, op="MKDIRS")
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.put(url, params=params)
                if response.status_code != 200:
                    error_msg = self._parse_error_response(response)
                    raise RuntimeError(f"HDFS mkdir failed ({response.status_code}): {error_msg}")
                result = response.json().get("boolean", False)
                return bool(result)
        except httpx.ConnectError as ce:
            raise ConnectionError(f"Cannot connect to Hadoop at {self.base_url}") from ce


hdfs_service = HDFSService()
