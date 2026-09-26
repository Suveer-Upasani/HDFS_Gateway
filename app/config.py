import os
import re
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Base Configuration
HDFS_URI = os.getenv("HDFS_URI", "hdfs://localhost:9000")
HDFS_UPLOAD_PATH = os.getenv("HDFS_UPLOAD_PATH", "/user/suveer/hdfs-gateway/uploads").rstrip("/")
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", 50 * 1024 * 1024))  # 50 MB default
HDFS_BIN = os.getenv("HDFS_BIN", "hdfs")

# Temporary directory for file downloads/uploads buffering
TEMP_DIR = Path(os.getenv("TEMP_DIR", "/tmp/hdfs_gateway"))
TEMP_DIR.mkdir(parents=True, exist_ok=True)


def sanitize_filename(filename: str) -> str:
    """
    Sanitize and validate a filename to prevent directory traversal and invalid inputs.
    
    Raises:
        ValueError: If filename is empty, contains directory traversal components,
                    or contains forbidden characters.
    """
    if not filename or not filename.strip():
        raise ValueError("Filename cannot be empty")

    cleaned = filename.strip()
    
    # Reject explicit directory traversal attempts
    if ".." in cleaned or "/" in cleaned or "\\" in cleaned or "\0" in cleaned:
        raise ValueError("Invalid filename: Directory traversal or path separators are not allowed")

    # Keep only safe basename
    basename = Path(cleaned).name
    if not basename or basename in (".", ".."):
        raise ValueError("Invalid filename specified")

    # Match safe characters (alphanumeric, dots, hyphens, underscores, spaces)
    if not re.match(r"^[a-zA-Z0-9_.\-\s\(\)\[\]]+$", basename):
        raise ValueError("Filename contains unsafe special characters")

    return basename


def format_bytes(size_in_bytes: int) -> str:
    """Format byte size into human readable string (e.g. 24 B, 2.1 MB)."""
    if size_in_bytes < 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_in_bytes < 1024.0:
            if unit == "B":
                return f"{int(size_in_bytes)} {unit}"
            return f"{size_in_bytes:.1f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.1f} PB"
