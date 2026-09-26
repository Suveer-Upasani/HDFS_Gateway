import os
import shutil
import subprocess
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from app.config import (
    HDFS_BIN,
    HDFS_UPLOAD_PATH,
    TEMP_DIR,
    sanitize_filename,
    format_bytes,
)

logger = logging.getLogger(__name__)


class HDFSUnavailableError(Exception):
    """Raised when HDFS service or CLI tool is unreachable."""
    pass


class HDFSFileNotFoundError(Exception):
    """Raised when a specified file is not found in HDFS."""
    pass


class HDFSOperationError(Exception):
    """Raised when an HDFS command encounters an error."""
    pass


def _get_hdfs_binary() -> Optional[str]:
    """Locate the hdfs binary in system PATH or configured env."""
    bin_path = shutil.which(HDFS_BIN)
    if bin_path:
        return bin_path
    
    # Fallback checks in common Hadoop installation directories
    candidate_paths = [
        "/opt/hadoop/bin/hdfs",
        "/usr/local/hadoop/bin/hdfs",
        "/usr/bin/hdfs",
    ]
    for cand in candidate_paths:
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
            
    return None


def hdfs_available() -> bool:
    """
    Check if the HDFS binary exists and can reach the NameNode/filesystem.
    Returns False immediately if CLI is missing or unreachable.
    """
    hdfs_cmd = _get_hdfs_binary()
    if not hdfs_cmd:
        return False

    try:
        # Fast test probe to verify HDFS status
        result = subprocess.run(
            [hdfs_cmd, "dfs", "-test", "-d", "/"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return False


def ensure_upload_directory() -> None:
    """Ensure the target upload directory exists on HDFS."""
    hdfs_cmd = _get_hdfs_binary()
    if not hdfs_cmd:
        raise HDFSUnavailableError("HDFS binary is not found in system PATH.")

    try:
        result = subprocess.run(
            [hdfs_cmd, "dfs", "-mkdir", "-p", HDFS_UPLOAD_PATH],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0 and "File exists" not in result.stderr:
            logger.warning(f"Could not verify/create upload directory: {result.stderr}")
    except (subprocess.SubprocessError, FileNotFoundError, OSError) as e:
        raise HDFSUnavailableError(f"Failed to communicate with HDFS: {e}")


def list_files(hdfs_dir: str = HDFS_UPLOAD_PATH) -> List[Dict[str, Any]]:
    """
    List files in the specified HDFS directory.
    Parses 'hdfs dfs -ls' output safely.
    """
    if not hdfs_available():
        raise HDFSUnavailableError("HDFS service is currently unavailable.")

    hdfs_cmd = _get_hdfs_binary()
    try:
        # First ensure directory exists
        subprocess.run(
            [hdfs_cmd, "dfs", "-mkdir", "-p", hdfs_dir],
            capture_output=True,
            text=True,
            timeout=5,
        )

        result = subprocess.run(
            [hdfs_cmd, "dfs", "-ls", hdfs_dir],
            capture_output=True,
            text=True,
            timeout=8,
        )
        if result.returncode != 0:
            if "No such file or directory" in result.stderr:
                return []
            raise HDFSOperationError(f"Failed to list HDFS files: {result.stderr.strip()}")

        files = []
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("Found "):
                continue

            parts = line.split()
            # Standard hdfs dfs -ls line has 8 columns:
            # permissions replication owner group size date time path
            if len(parts) >= 8:
                permissions = parts[0]
                is_dir = permissions.startswith("d")
                try:
                    size_bytes = int(parts[4])
                except ValueError:
                    size_bytes = 0
                
                date_str = f"{parts[5]} {parts[6]}"
                full_path = " ".join(parts[7:])
                filename = Path(full_path).name

                # Filter out current directory itself if present
                if full_path == hdfs_dir or not filename:
                    continue

                files.append({
                    "name": filename,
                    "size": size_bytes,
                    "size_formatted": format_bytes(size_bytes) if not is_dir else "-",
                    "path": full_path,
                    "modified": date_str,
                    "is_dir": is_dir,
                    "permissions": permissions,
                    "owner": parts[2],
                })
        
        # Sort files by name or modification date
        return sorted(files, key=lambda x: x["name"].lower())

    except subprocess.TimeoutExpired:
        raise HDFSUnavailableError("HDFS list operation timed out.")
    except (subprocess.SubprocessError, OSError) as e:
        raise HDFSUnavailableError(f"HDFS communication error: {e}")


def upload_file(local_file_path: str, original_filename: str) -> Dict[str, Any]:
    """
    Upload a local file to HDFS upload directory safely.
    """
    if not hdfs_available():
        raise HDFSUnavailableError("HDFS service is currently unavailable.")

    clean_filename = sanitize_filename(original_filename)
    hdfs_cmd = _get_hdfs_binary()
    ensure_upload_directory()
    
    target_hdfs_path = f"{HDFS_UPLOAD_PATH}/{clean_filename}"

    try:
        result = subprocess.run(
            [hdfs_cmd, "dfs", "-put", "-f", local_file_path, target_hdfs_path],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise HDFSOperationError(f"HDFS upload error: {result.stderr.strip()}")

        # Fetch uploaded file metadata
        file_size = os.path.getsize(local_file_path) if os.path.exists(local_file_path) else 0

        return {
            "name": clean_filename,
            "path": target_hdfs_path,
            "size": file_size,
            "size_formatted": format_bytes(file_size),
            "status": "success",
            "message": f"Successfully uploaded {clean_filename} to HDFS",
        }

    except subprocess.TimeoutExpired:
        raise HDFSUnavailableError("HDFS upload operation timed out.")
    except (subprocess.SubprocessError, OSError) as e:
        raise HDFSOperationError(f"Failed to upload file to HDFS: {e}")


def download_file(filename: str) -> str:
    """
    Fetch a file from HDFS and save it to a local temporary location.
    Returns the path to the local temporary file.
    """
    if not hdfs_available():
        raise HDFSUnavailableError("HDFS service is currently unavailable.")

    clean_filename = sanitize_filename(filename)
    hdfs_cmd = _get_hdfs_binary()
    target_hdfs_path = f"{HDFS_UPLOAD_PATH}/{clean_filename}"
    local_temp_path = TEMP_DIR / f"dl_{clean_filename}"

    try:
        # Test if file exists on HDFS first
        test_res = subprocess.run(
            [hdfs_cmd, "dfs", "-test", "-e", target_hdfs_path],
            capture_output=True,
            timeout=5,
        )
        if test_res.returncode != 0:
            raise HDFSFileNotFoundError(f"File '{clean_filename}' was not found in HDFS.")

        # Download from HDFS
        result = subprocess.run(
            [hdfs_cmd, "dfs", "-get", "-f", target_hdfs_path, str(local_temp_path)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise HDFSOperationError(f"HDFS download error: {result.stderr.strip()}")

        return str(local_temp_path)

    except subprocess.TimeoutExpired:
        raise HDFSUnavailableError("HDFS download operation timed out.")
    except (subprocess.SubprocessError, OSError) as e:
        raise HDFSOperationError(f"Failed to download file from HDFS: {e}")


def delete_file(filename: str) -> bool:
    """
    Permanently delete a file from the HDFS upload directory.
    """
    if not hdfs_available():
        raise HDFSUnavailableError("HDFS service is currently unavailable.")

    clean_filename = sanitize_filename(filename)
    hdfs_cmd = _get_hdfs_binary()
    target_hdfs_path = f"{HDFS_UPLOAD_PATH}/{clean_filename}"

    try:
        # Check if file exists
        test_res = subprocess.run(
            [hdfs_cmd, "dfs", "-test", "-e", target_hdfs_path],
            capture_output=True,
            timeout=5,
        )
        if test_res.returncode != 0:
            raise HDFSFileNotFoundError(f"File '{clean_filename}' was not found in HDFS.")

        result = subprocess.run(
            [hdfs_cmd, "dfs", "-rm", "-skipTrash", target_hdfs_path],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            raise HDFSOperationError(f"HDFS deletion error: {result.stderr.strip()}")

        return True

    except subprocess.TimeoutExpired:
        raise HDFSUnavailableError("HDFS delete operation timed out.")
    except (subprocess.SubprocessError, OSError) as e:
        raise HDFSOperationError(f"Failed to delete file from HDFS: {e}")


def get_storage_stats() -> Dict[str, Any]:
    """
    Get overview statistics for the HDFS upload directory and system health.
    Returns clear 'Unavailable' states if HDFS cannot be reached.
    """
    is_online = hdfs_available()
    
    if not is_online:
        return {
            "hdfs_connected": False,
            "hdfs_status": "Unavailable",
            "api_status": "Online",
            "total_files": "Unavailable",
            "total_storage_bytes": None,
            "total_storage": "Unavailable",
            "upload_path": HDFS_UPLOAD_PATH,
        }

    try:
        files = list_files(HDFS_UPLOAD_PATH)
        file_items = [f for f in files if not f.get("is_dir", False)]
        total_size = sum(f.get("size", 0) for f in file_items)
        
        return {
            "hdfs_connected": True,
            "hdfs_status": "Connected",
            "api_status": "Online",
            "total_files": len(file_items),
            "total_storage_bytes": total_size,
            "total_storage": format_bytes(total_size),
            "upload_path": HDFS_UPLOAD_PATH,
        }
    except Exception as e:
        logger.error(f"Error computing HDFS storage stats: {e}")
        return {
            "hdfs_connected": False,
            "hdfs_status": "Unavailable",
            "api_status": "Online",
            "total_files": "Unavailable",
            "total_storage_bytes": None,
            "total_storage": "Unavailable",
            "upload_path": HDFS_UPLOAD_PATH,
        }
