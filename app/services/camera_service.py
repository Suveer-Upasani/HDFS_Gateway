"""Camera Service: Manages the lifecycle and streaming of the vision pipeline.

Encapsulates background thread execution of VisionRunner, thread-safe start/stop controls,
detection telemetry aggregation, and MJPEG video streaming for the FastAPI dashboard.
"""

import asyncio
import logging
import threading
import time
from typing import Any, AsyncGenerator

import cv2
import numpy as np

from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.runner import VisionRunner

logger = logging.getLogger("app.camera_service")


class CameraService:
    """Service class managing camera lifecycle, background processing thread, and stream delivery."""

    def __init__(self, settings: VisionClientSettings | None = None) -> None:
        self.settings = settings or get_vision_settings()
        self._runner: VisionRunner | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._is_running = False
        self._status_text = "stopped"
        self._last_error: str | None = None
        self._last_stats: dict[str, Any] = {
            "fps": 0.0,
            "frames": 0,
            "new_detections": 0,
            "total_detections": 0,
            "confidence": self.settings.YOLO_CONFIDENCE,
            "camera_id": self.settings.CAMERA_ID,
            "camera_source": str(self.settings.CAMERA_SOURCE),
            "latest_object": None,
            "recent_objects": [],
            "running": False,
        }
        self._standby_frame_bytes = self._create_standby_frame()

    def _create_standby_frame(self, message: str = "CAMERA OFFLINE") -> bytes:
        """Generate a dark placeholder JPEG frame for the camera stream when stopped."""
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        # Background gradient or grid
        img[:] = (15, 17, 23)  # Deep dark slate

        # Draw subtle border
        cv2.rectangle(img, (20, 20), (620, 460), (35, 40, 55), 2)
        cv2.circle(img, (320, 210), 45, (45, 52, 70), 2)
        cv2.circle(img, (320, 210), 16, (70, 80, 105), -1)

        # Draw offline text
        text_size, _ = cv2.getTextSize(message, cv2.FONT_HERSHEY_SIMPLEX, 0.75, 2)
        text_x = (640 - text_size[0]) // 2
        cv2.putText(
            img,
            message,
            (text_x, 300),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.75,
            (140, 150, 175),
            2,
            cv2.LINE_AA,
        )

        sub_msg = "Click 'Start Stream' to activate Mac Webcam & YOLO"
        sub_size, _ = cv2.getTextSize(sub_msg, cv2.FONT_HERSHEY_SIMPLEX, 0.45, 1)
        sub_x = (640 - sub_size[0]) // 2
        cv2.putText(
            img,
            sub_msg,
            (sub_x, 335),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.45,
            (90, 100, 125),
            1,
            cv2.LINE_AA,
        )

        ret, buffer = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return buffer.tobytes() if ret else b""

    @property
    def is_running(self) -> bool:
        """Check if camera streaming pipeline is active."""
        with self._lock:
            return self._is_running and (self._thread is not None and self._thread.is_alive())

    def start(self) -> dict[str, Any]:
        """Start the camera/YOLO pipeline in a non-blocking background thread."""
        with self._lock:
            if self._is_running and self._thread is not None and self._thread.is_alive():
                logger.info("Camera start requested, but camera is already running.")
                return {
                    "status": "already_running",
                    "message": "Camera is already running",
                    "running": True,
                    "camera_id": self.settings.CAMERA_ID,
                    "camera_source": str(self.settings.CAMERA_SOURCE),
                }

            self._last_error = None
            self._status_text = "starting"

            # Create a new runner instance with headless mode
            self._runner = VisionRunner(
                settings=self.settings,
                show_window=False,
            )

            # Start background thread
            startup_event = threading.Event()
            startup_error: list[Exception] = []

            def _worker() -> None:
                try:
                    logger.info("Background camera thread starting...")
                    startup_event.set()
                    if self._runner:
                        self._runner.run(show_window=False)
                except Exception as err:
                    logger.error("Camera worker thread encountered error: %s", err, exc_info=True)
                    self._last_error = str(err)
                    startup_error.append(err)
                finally:
                    with self._lock:
                        self._is_running = False
                        self._status_text = "stopped"
                        if self._runner:
                            self._last_stats = self._runner.get_stats()
                    logger.info("Background camera thread terminated.")

            self._thread = threading.Thread(target=_worker, name="CameraRunnerThread", daemon=True)
            self._thread.start()
            self._is_running = True
            self._status_text = "running"

            logger.info("Camera pipeline started in background thread for camera '%s'.", self.settings.CAMERA_ID)

            return {
                "status": "started",
                "message": "Camera pipeline started successfully",
                "running": True,
                "camera_id": self.settings.CAMERA_ID,
                "camera_source": str(self.settings.CAMERA_SOURCE),
            }

    def stop(self) -> dict[str, Any]:
        """Gracefully stop the camera pipeline and release webcam hardware."""
        with self._lock:
            if not self._is_running or self._thread is None:
                logger.info("Camera stop requested, but camera is already stopped.")
                return {
                    "status": "already_stopped",
                    "message": "Camera is already stopped",
                    "running": False,
                }

            logger.info("Stopping camera pipeline...")
            self._status_text = "stopping"

            if self._runner:
                self._runner.stop()

        # Wait for thread to exit without holding the lock
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=3.0)

        with self._lock:
            if self._runner:
                self._last_stats = self._runner.get_stats()
                self._runner = None
            self._is_running = False
            self._status_text = "stopped"
            self._thread = None

        logger.info("✓ Camera pipeline cleanly stopped and webcam released.")

        return {
            "status": "stopped",
            "message": "Camera pipeline stopped successfully",
            "running": False,
        }

    def get_status(self) -> dict[str, Any]:
        """Return the current running status and hardware identifiers."""
        running = self.is_running
        return {
            "running": running,
            "camera_id": self.settings.CAMERA_ID,
            "camera_source": str(self.settings.CAMERA_SOURCE),
            "status": "running" if running else ("error" if self._last_error else "stopped"),
            "error": self._last_error,
        }

    def get_stats(self) -> dict[str, Any]:
        """Return live detection statistics, FPS, and frame counts."""
        if self.is_running and self._runner:
            stats = self._runner.get_stats()
            stats["running"] = True
            return stats

        # If not running, return cached or default values
        return {
            "fps": 0.0,
            "frames": self._last_stats.get("frames", 0),
            "new_detections": 0,
            "total_detections": self._last_stats.get("total_detections", 0),
            "confidence": self.settings.YOLO_CONFIDENCE,
            "camera_id": self.settings.CAMERA_ID,
            "camera_source": str(self.settings.CAMERA_SOURCE),
            "latest_object": None,
            "recent_objects": [],
            "running": False,
        }

    async def generate_mjpeg_stream(self) -> AsyncGenerator[bytes, None]:
        """Yield multipart MJPEG stream frames continuously."""
        frame_boundary = b"--frame\r\nContent-Type: image/jpeg\r\n\r\n"

        while True:
            jpeg_bytes: bytes | None = None
            if self.is_running and self._runner:
                jpeg_bytes = self._runner.get_latest_jpeg()

            if jpeg_bytes is not None:
                yield frame_boundary + jpeg_bytes + b"\r\n"
                await asyncio.sleep(0.03)  # ~30 fps cap
            else:
                yield frame_boundary + self._standby_frame_bytes + b"\r\n"
                await asyncio.sleep(0.2)  # Low frequency standby frame


# Shared singleton instance
camera_service = CameraService()
