import logging
import posixpath
from typing import Annotated

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse

from app.api.vision import router as vision_router
from app.services.hdfs_service import (
    hdfs_service,
    sanitize_filename,
    sanitize_hdfs_path,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["HDFS Gateway Operations"])
router.include_router(vision_router)


@router.get("/health", summary="Hadoop WebHDFS Connectivity Check")
async def health_check():
    """Returns gateway status along with live Hadoop NameNode WebHDFS connectivity."""
    hdfs_health = await hdfs_service.check_health()
    return {
        "gateway_status": "healthy",
        "hdfs_connectivity": hdfs_health,
    }


@router.get("/files/list", summary="List HDFS Directory")
async def list_files(
    path: Annotated[str, Query(description="HDFS directory path to list")] = "/",
):
    """List files and subdirectories within a given HDFS path."""
    try:
        clean_path = sanitize_hdfs_path(path)
        items = await hdfs_service.list_directory(path=clean_path)
        return {
            "path": clean_path,
            "count": len(items),
            "items": items,
        }
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except ConnectionError as ce:
        logger.warning(f"Hadoop connection failure listing path '{path}': {ce!s}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(ce)) from ce
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(re)) from re
    except Exception as e:
        logger.error(f"Unexpected error listing path '{path}': {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list directory: {e!s}",
        ) from e


@router.post("/files/upload", summary="Upload File to HDFS")
async def upload_file(
    file: Annotated[UploadFile, File(description="Binary file payload")],
    destination_dir: Annotated[str, Form(description="Target HDFS directory")] = "/",
    overwrite: Annotated[bool, Form(description="Overwrite if destination file already exists")] = True,
):
    """
    Stream upload a file directly to Hadoop HDFS via WebHDFS protocol.
    Performs 2-step redirection without local filesystem staging.
    """
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No filename provided in upload request",
        )

    try:
        safe_filename = sanitize_filename(file.filename)
        clean_dir = sanitize_hdfs_path(destination_dir)
        target_path = posixpath.join(clean_dir, safe_filename)

        async def file_stream():
            while chunk := await file.read(65536):
                yield chunk

        return await hdfs_service.upload_file_stream(
            stream=file_stream(),
            destination_path=target_path,
            overwrite=overwrite,
        )
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except ConnectionError as ce:
        logger.warning(f"Hadoop connection failure uploading to '{destination_dir}': {ce!s}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(ce)) from ce
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(re)) from re
    except Exception as e:
        logger.error(f"Unexpected error uploading file: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Upload to HDFS failed: {e!s}",
        ) from e


@router.get("/files/download", summary="Download File from HDFS")
async def download_file(
    path: Annotated[str, Query(description="HDFS file path to download")],
):
    """Stream file content directly from HDFS to client."""
    try:
        clean_path = sanitize_hdfs_path(path)
        stream, content_length = await hdfs_service.download_file_stream(clean_path)
        filename = posixpath.basename(clean_path) or "download"

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        if content_length is not None:
            headers["Content-Length"] = str(content_length)

        return StreamingResponse(
            stream,
            media_type="application/octet-stream",
            headers=headers,
        )
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except ConnectionError as ce:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(ce)) from ce
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(re)) from re
    except Exception as e:
        logger.error(f"Unexpected error downloading '{path}': {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Download from HDFS failed: {e!s}",
        ) from e


@router.delete("/files/delete", summary="Delete File or Directory from HDFS")
async def delete_path(
    path: Annotated[str, Query(description="Path to delete in HDFS")],
    recursive: Annotated[bool, Query(description="Recursively delete if path is a directory")] = True,
):
    """Delete a file or directory from HDFS."""
    try:
        clean_path = sanitize_hdfs_path(path)
        success = await hdfs_service.delete_path(path=clean_path, recursive=recursive)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Path '{clean_path}' not found in HDFS or could not be deleted",
            )
        return {"message": f"Successfully deleted {clean_path}", "path": clean_path}
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except HTTPException:
        raise
    except ConnectionError as ce:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(ce)) from ce
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(re)) from re
    except Exception as e:
        logger.error(f"Unexpected error deleting '{path}': {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete {path}: {e!s}",
        ) from e


@router.post("/files/mkdir", summary="Create Directory in HDFS")
async def make_directory(
    path: Annotated[str, Query(description="HDFS directory path to create")],
):
    """Create a new directory in HDFS."""
    try:
        clean_path = sanitize_hdfs_path(path)
        success = await hdfs_service.make_directory(path=clean_path)
        return {"message": f"Directory '{clean_path}' created", "path": clean_path, "success": success}
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except ConnectionError as ce:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(ce)) from ce
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(re)) from re
    except Exception as e:
        logger.error(f"Unexpected error creating directory '{path}': {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create directory: {e!s}",
        ) from e
