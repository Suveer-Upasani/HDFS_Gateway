import os
import shutil
import logging
from pathlib import Path
from typing import Optional

from fastapi import (
    FastAPI,
    Request,
    UploadFile,
    File,
    HTTPException,
    status,
    BackgroundTasks,
)
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import (
    HDFS_UPLOAD_PATH,
    MAX_UPLOAD_SIZE,
    TEMP_DIR,
    sanitize_filename,
    format_bytes,
)
from app import hdfs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hdfs_gateway")

app = FastAPI(
    title="HDFS Gateway",
    description="A lightweight web-based file management gateway for Apache Hadoop HDFS",
    version="1.0.0",
)

# Static and Templates configuration
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "app" / "templates"

STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def cleanup_temp_file(file_path: str):
    """Background task to remove temporary files generated during downloads/uploads."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except OSError as e:
        logger.warning(f"Failed to delete temp file {file_path}: {e}")


# ==========================================
# Server-Rendered HTML Routes
# ==========================================

@app.get("/", response_class=HTMLResponse)
async def root():
    """Redirect root to dashboard."""
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """Render the main system overview and metrics dashboard."""
    stats = hdfs.get_storage_stats()
    recent_files = []
    if stats["hdfs_connected"]:
        try:
            all_files = hdfs.list_files()
            # Filter non-directories and get the 5 most recent
            recent_files = [f for f in all_files if not f.get("is_dir")][:5]
        except Exception as e:
            logger.warning(f"Could not load recent files for dashboard: {e}")

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "page": "dashboard",
            "stats": stats,
            "recent_files": recent_files,
            "max_upload_size_formatted": format_bytes(MAX_UPLOAD_SIZE),
        },
    )


@app.get("/files", response_class=HTMLResponse)
async def files_page(request: Request):
    """Render the full HDFS file manager view."""
    stats = hdfs.get_storage_stats()
    files = []
    if stats["hdfs_connected"]:
        try:
            files = hdfs.list_files()
        except Exception as e:
            logger.warning(f"Could not load files list: {e}")

    return templates.TemplateResponse(
        request=request,
        name="files.html",
        context={
            "page": "files",
            "stats": stats,
            "files": files,
        },
    )


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    """Render the dedicated drag-and-drop file upload view."""
    stats = hdfs.get_storage_stats()
    return templates.TemplateResponse(
        request=request,
        name="upload.html",
        context={
            "page": "upload",
            "stats": stats,
            "max_upload_size": MAX_UPLOAD_SIZE,
            "max_upload_size_formatted": format_bytes(MAX_UPLOAD_SIZE),
            "upload_path": HDFS_UPLOAD_PATH,
        },
    )


# ==========================================
# REST API Endpoints
# ==========================================

@app.get("/api/health")
async def api_health():
    """
    Health check endpoint returning API and HDFS connectivity status.
    """
    is_hdfs_up = hdfs.hdfs_available()
    return {
        "status": "ok",
        "api": "Online",
        "hdfs": "Connected" if is_hdfs_up else "Unavailable",
        "hdfs_connected": is_hdfs_up,
        "upload_path": HDFS_UPLOAD_PATH,
    }


@app.get("/api/stats")
async def api_stats():
    """Return storage and system statistics."""
    return hdfs.get_storage_stats()


@app.get("/api/files")
async def api_list_files():
    """
    List all files in the HDFS upload directory.
    """
    if not hdfs.hdfs_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Apache Hadoop HDFS is currently unavailable or unreachable.",
        )

    try:
        files = hdfs.list_files()
        return {
            "success": True,
            "count": len(files),
            "files": files,
            "upload_path": HDFS_UPLOAD_PATH,
        }
    except hdfs.HDFSUnavailableError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except hdfs.HDFSOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/api/upload")
async def api_upload_file(file: UploadFile = File(...)):
    """
    Upload a file into HDFS.
    Validates file presence, security, and maximum size.
    """
    if not hdfs.hdfs_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cannot upload file: Apache Hadoop HDFS is unavailable.",
        )

    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must have a valid filename.",
        )

    # Sanitize filename & protect against path traversal
    try:
        clean_name = sanitize_filename(file.filename)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    temp_file_path = TEMP_DIR / f"up_{clean_name}"
    uploaded_size = 0

    try:
        with open(temp_file_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB buffer
                if not chunk:
                    break
                uploaded_size += len(chunk)
                if uploaded_size > MAX_UPLOAD_SIZE:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"File exceeds maximum allowed upload size of {format_bytes(MAX_UPLOAD_SIZE)}.",
                    )
                buffer.write(chunk)

        if uploaded_size == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Empty files cannot be uploaded.",
            )

        result = hdfs.upload_file(str(temp_file_path), clean_name)
        return {
            "success": True,
            "filename": clean_name,
            "path": result["path"],
            "size": result["size"],
            "size_formatted": result["size_formatted"],
            "message": f"Successfully uploaded '{clean_name}' to HDFS.",
        }

    except hdfs.HDFSUnavailableError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except hdfs.HDFSOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        if temp_file_path.exists():
            try:
                os.remove(temp_file_path)
            except OSError:
                pass


@app.get("/api/files/{filename}")
async def api_download_file(filename: str, background_tasks: BackgroundTasks):
    """
    Download a file from HDFS.
    Returns the file stream and cleans up temporary local files.
    """
    if not hdfs.hdfs_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cannot download file: Apache Hadoop HDFS is unavailable.",
        )

    try:
        clean_name = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    try:
        local_path = hdfs.download_file(clean_name)
        # Schedule cleanup of the temp file after download finishes
        background_tasks.add_task(cleanup_temp_file, local_path)

        return FileResponse(
            path=local_path,
            filename=clean_name,
            media_type="application/octet-stream",
        )

    except hdfs.HDFSFileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except hdfs.HDFSUnavailableError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except hdfs.HDFSOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.delete("/api/files/{filename}")
async def api_delete_file(filename: str):
    """
    Delete a file from the HDFS upload directory.
    """
    if not hdfs.hdfs_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cannot delete file: Apache Hadoop HDFS is unavailable.",
        )

    try:
        clean_name = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    try:
        hdfs.delete_file(clean_name)
        return {
            "success": True,
            "filename": clean_name,
            "message": f"File '{clean_name}' was successfully deleted from HDFS.",
        }

    except hdfs.HDFSFileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except hdfs.HDFSUnavailableError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except hdfs.HDFSOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
