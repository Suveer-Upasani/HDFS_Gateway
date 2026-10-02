"""Camera Control and Streaming API Endpoints.

Provides REST and MJPEG video streaming endpoints to start, stop, monitor,
and preview the local computer-vision capture pipeline.
"""

from typing import Any

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.services.camera_service import camera_service

router = APIRouter(prefix="/api/camera", tags=["Camera Control & Stream"])


class CameraStatusResponse(BaseModel):
    """Camera status response model."""

    running: bool = Field(..., description="Whether the camera stream is currently active")
    camera_id: str = Field(..., description="Configured camera identifier")
    camera_source: str = Field(..., description="Underlying camera video device index or source path")


class CameraStatsResponse(BaseModel):
    """Detection statistics response model."""

    fps: float = Field(0.0, description="Current processing frames per second")
    frames: int = Field(0, description="Total number of video frames processed")
    new_detections: int = Field(0, description="Number of new detections in the recent window")
    total_detections: int = Field(0, description="Cumulative count of all detected objects")
    confidence: float = Field(0.5, description="YOLO confidence threshold")
    running: bool = Field(False, description="Streaming pipeline running state")
    camera_id: str = Field("camera-01", description="Camera identifier")
    latest_object: str | None = Field(None, description="Most recently detected object class name")
    recent_objects: list[str] = Field(default_factory=list, description="List of objects detected in latest frame")


class CameraActionResponse(BaseModel):
    """Response model for camera start/stop actions."""

    message: str = Field(..., description="Action outcome description")
    running: bool = Field(..., description="New running state")
    camera_id: str | None = Field(None, description="Camera identifier")
    camera_source: str | None = Field(None, description="Camera source index")


@router.post(
    "/start",
    response_model=CameraActionResponse,
    summary="Start Camera Streaming Pipeline",
)
async def start_camera() -> Any:
    """Start the camera/YOLO pipeline in a non-blocking background thread.

    If the pipeline is already running, returns the active status safely.
    """
    result = camera_service.start()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )


@router.post(
    "/stop",
    response_model=CameraActionResponse,
    summary="Stop Camera Streaming Pipeline",
)
async def stop_camera() -> Any:
    """Gracefully stop the camera pipeline and release camera hardware."""
    result = camera_service.stop()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )


@router.get(
    "/status",
    response_model=CameraStatusResponse,
    summary="Get Camera Running Status",
)
async def get_camera_status() -> CameraStatusResponse:
    """Return the running status, camera ID, and camera source."""
    status_data = camera_service.get_status()
    return CameraStatusResponse(
        running=status_data["running"],
        camera_id=status_data["camera_id"],
        camera_source=status_data["camera_source"],
    )


@router.get(
    "/stats",
    response_model=CameraStatsResponse,
    summary="Get Live Detection Statistics",
)
async def get_camera_stats() -> CameraStatsResponse:
    """Return live FPS, frames processed, total detections, and latest object."""
    stats_data = camera_service.get_stats()
    return CameraStatsResponse(
        fps=stats_data.get("fps", 0.0),
        frames=stats_data.get("frames", 0),
        new_detections=stats_data.get("new_detections", 0),
        total_detections=stats_data.get("total_detections", 0),
        confidence=stats_data.get("confidence", 0.5),
        running=stats_data.get("running", False),
        camera_id=stats_data.get("camera_id", "camera-01"),
        latest_object=stats_data.get("latest_object"),
        recent_objects=stats_data.get("recent_objects", []),
    )


@router.get(
    "/stream",
    summary="Live MJPEG Camera Stream with YOLO Annotations",
)
async def get_camera_stream() -> StreamingResponse:
    """Expose processed camera frames with YOLO bounding boxes via MJPEG stream."""
    return StreamingResponse(
        camera_service.generate_mjpeg_stream(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate, pre-check=0, post-check=0, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "Connection": "keep-alive",
        },
    )
