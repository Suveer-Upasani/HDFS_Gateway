"""Real-time Vision Client runner: Captures camera frames, executes YOLO inference, displays annotated video, and streams DetectionEvents to Kafka."""

import logging
import signal
import sys
import threading
import time
from types import FrameType
from typing import Any

import cv2
import numpy as np

from vision_client.camera import BaseCameraStream, OpenCVCameraStream
from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.detector import BaseDetector, YOLODetector
from vision_client.kafka_producer import BaseEventProducer, KafkaVisionProducer

logger = logging.getLogger("vision_client")


class VisionRunner:
    """Orchestrates video capture, YOLO object detection, preview, and Kafka streaming."""

    def __init__(
        self,
        settings: VisionClientSettings | None = None,
        camera: BaseCameraStream | None = None,
        detector: BaseDetector | None = None,
        producer: BaseEventProducer | None = None,
        show_window: bool = True,
    ) -> None:
        self.settings = settings or get_vision_settings()
        self.camera = camera or OpenCVCameraStream(source=self.settings.CAMERA_SOURCE)
        self.detector = detector or YOLODetector(
            model_path=self.settings.YOLO_MODEL,
            confidence=self.settings.YOLO_CONFIDENCE,
        )
        self.producer = producer or KafkaVisionProducer(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
            topic=self.settings.KAFKA_TOPIC,
        )
        self.show_window = show_window
        self._running = False
        self._kafka_connected = False
        self._lock = threading.Lock()
        self._latest_frame: Any | None = None
        self._latest_jpeg: bytes | None = None
        self._latest_events: list[Any] = []
        self._stats: dict[str, Any] = {
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

    def setup(self) -> None:
        """Initialize and verify camera, YOLO model, and Kafka producer."""

        logger.info("Initializing Vision Client components...")

        # 1. Connect to Kafka (resilient: warnings if broker is offline)
        logger.info(
            "Connecting to Kafka at %s (topic: %s)...",
            self.settings.KAFKA_BOOTSTRAP_SERVERS,
            self.settings.KAFKA_TOPIC,
        )

        try:
            self.producer.connect()
            self._kafka_connected = True
            logger.info("✓ Kafka producer connected successfully.")
        except Exception as err:
            self._kafka_connected = False
            logger.warning(
                "⚠️ Kafka broker unreachable at '%s' (%s). "
                "Local camera capture & YOLO inference will proceed without Kafka streaming.",
                self.settings.KAFKA_BOOTSTRAP_SERVERS,
                err,
            )

        # 2. Load YOLO Model
        logger.info(
            "Loading YOLO model '%s' (confidence: %.2f)...",
            self.settings.YOLO_MODEL,
            self.settings.YOLO_CONFIDENCE,
        )

        try:
            self.detector.load_model()
            logger.info(
                "✓ YOLO model '%s' loaded successfully.",
                self.settings.YOLO_MODEL,
            )
        except Exception as err:
            if self._kafka_connected:
                self.producer.close()
            raise RuntimeError(
                f"Failed to load YOLO model '{self.settings.YOLO_MODEL}': {err}."
            ) from err

        # 3. Open Camera
        logger.info(
            "Opening camera source '%s'...",
            self.settings.CAMERA_SOURCE,
        )

        opened = False

        try:
            opened = self.camera.open()
        except Exception as err:
            if self._kafka_connected:
                self.producer.close()
            raise RuntimeError(
                f"Error initializing camera source "
                f"'{self.settings.CAMERA_SOURCE}': {err}."
            ) from err

        if not opened:
            if self._kafka_connected:
                self.producer.close()
            raise RuntimeError(
                f"Could not open camera source '{self.settings.CAMERA_SOURCE}'. "
                "Verify camera connection, device permissions, or source index."
            )

        logger.info(
            "✓ Camera '%s' opened successfully.",
            self.settings.CAMERA_ID,
        )

    @staticmethod
    def annotate_frame(frame: Any, events: list[Any]) -> Any:
        """Draw detection bounding boxes and labels onto a video frame."""

        if frame is None or not isinstance(frame, np.ndarray):
            return frame

        annotated = frame.copy()

        for event in events:
            bbox = getattr(event, "bbox", None)
            if not bbox or len(bbox) != 4:
                continue

            x1, y1, x2, y2 = [int(value) for value in bbox]

            label = f"{event.object} {event.confidence:.2f}"

            # Bounding box
            cv2.rectangle(
                annotated,
                (x1, y1),
                (x2, y2),
                (0, 255, 0),
                2,
            )

            # Label background
            (text_width, text_height), baseline = cv2.getTextSize(
                label,
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                2,
            )

            label_y1 = max(0, y1 - text_height - baseline - 6)
            label_y2 = y1

            cv2.rectangle(
                annotated,
                (x1, label_y1),
                (x1 + text_width + 8, label_y2),
                (0, 255, 0),
                -1,
            )

            # Label text
            cv2.putText(
                annotated,
                label,
                (x1 + 4, max(text_height, y1 - 6)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (0, 0, 0),
                2,
                cv2.LINE_AA,
            )

        return annotated

    def stop(self) -> None:
        """Signal the processing loop to stop."""
        self._running = False

    def cleanup(self) -> None:
        """Release camera hardware and close Kafka producer connection."""

        logger.info("Releasing vision client resources...")

        try:
            self.camera.release()
            logger.info("✓ Camera released.")
        except Exception as err:
            logger.warning("Error releasing camera: %s", err)

        if self.show_window:
            try:
                cv2.destroyAllWindows()
            except Exception as err:
                logger.warning("Error closing OpenCV windows: %s", err)

        if getattr(self, "_kafka_connected", False):
            try:
                self.producer.close()
                logger.info("✓ Kafka producer closed.")
            except Exception as err:
                logger.warning("Error closing Kafka producer: %s", err)

    def get_latest_jpeg(self) -> bytes | None:
        """Return the latest encoded JPEG frame bytes in a thread-safe manner."""
        with self._lock:
            return self._latest_jpeg

    def get_stats(self) -> dict[str, Any]:
        """Return a copy of the current detection statistics in a thread-safe manner."""
        with self._lock:
            return dict(self._stats)

    @property
    def is_running(self) -> bool:
        """Check if runner loop is currently executing."""
        return self._running

    def run(self, show_window: bool | None = None) -> None:
        """Execute the real-time detection, preview, and streaming loop."""

        if show_window is not None:
            self.show_window = show_window

        self.setup()
        self._running = True

        window_name = f"Vision Client - {self.settings.CAMERA_ID}"

        logger.info(
            "🚀 Vision pipeline active. Streaming detection events from "
            "camera '%s' -> Kafka topic '%s'...",
            self.settings.CAMERA_ID,
            self.settings.KAFKA_TOPIC,
        )
        if self.show_window:
            logger.info("🎥 Camera preview active.")
            logger.info("Press 'q' in the camera window or Ctrl+C to terminate.")

        frame_count = 0
        total_detections = 0
        window_detections = 0
        last_log_time = time.time()
        fps_calc_time = time.time()
        fps_frame_count = 0
        current_fps = 0.0

        try:
            while self._running:
                success, frame = self.camera.read()

                if not success or frame is None:
                    time.sleep(0.01)
                    continue

                frame_count += 1
                fps_frame_count += 1

                # Dynamic rolling FPS calculation
                now = time.time()
                fps_elapsed = now - fps_calc_time
                if fps_elapsed >= 0.5:
                    current_fps = round(fps_frame_count / fps_elapsed, 1)
                    fps_frame_count = 0
                    fps_calc_time = now

                # YOLO inference
                events = self.detector.detect(
                    frame,
                    camera_id=self.settings.CAMERA_ID,
                )

                # Send detection events to Kafka if connected
                if getattr(self, "_kafka_connected", False):
                    for event in events:
                        try:
                            self.producer.send_event(event)
                        except Exception as k_err:
                            logger.debug("Kafka publish event warning: %s", k_err)

                total_detections += len(events)
                window_detections += len(events)

                # Draw detections for camera preview
                annotated_frame = self.annotate_frame(frame, events)

                # Overlay system information if frame is a valid image array
                jpeg_bytes: bytes | None = None
                if isinstance(annotated_frame, np.ndarray):
                    cv2.putText(
                        annotated_frame,
                        f"Camera: {self.settings.CAMERA_ID}",
                        (15, 30),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.7,
                        (0, 255, 0),
                        2,
                        cv2.LINE_AA,
                    )

                    cv2.putText(
                        annotated_frame,
                        f"Detections: {len(events)} | Total: {total_detections}",
                        (15, 60),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.65,
                        (0, 255, 0),
                        2,
                        cv2.LINE_AA,
                    )

                    ret, buffer = cv2.imencode(".jpg", annotated_frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                    if ret:
                        jpeg_bytes = buffer.tobytes()

                # Extract latest object info
                recent_obj_names = [e.object for e in events]
                latest_obj = recent_obj_names[0] if recent_obj_names else None

                # Update state thread-safely
                with self._lock:
                    self._latest_frame = annotated_frame
                    self._latest_jpeg = jpeg_bytes
                    self._latest_events = events
                    self._stats = {
                        "fps": current_fps,
                        "frames": frame_count,
                        "new_detections": window_detections,
                        "total_detections": total_detections,
                        "confidence": self.settings.YOLO_CONFIDENCE,
                        "camera_id": self.settings.CAMERA_ID,
                        "camera_source": str(self.settings.CAMERA_SOURCE),
                        "latest_object": latest_obj,
                        "recent_objects": recent_obj_names,
                        "running": True,
                    }

                # Display live camera feed if GUI window requested
                if self.show_window and isinstance(annotated_frame, np.ndarray):
                    cv2.imshow(window_name, annotated_frame)
                    key = cv2.waitKey(1) & 0xFF
                    if key == ord("q"):
                        logger.info("Quit requested from camera preview.")
                        self.stop()
                        break
                elif not self.show_window:
                    time.sleep(0.001)

                # Periodic log
                if now - last_log_time >= 5.0:
                    log_fps = (frame_count / (now - last_log_time)) if (now - last_log_time) > 0 else 0.0

                    logger.info(
                        "Stats: %.1f FPS | Frames: %d | New Detections: %d | "
                        "Total Detections: %d",
                        log_fps,
                        frame_count,
                        window_detections,
                        total_detections,
                    )

                    window_detections = 0
                    last_log_time = now

        except KeyboardInterrupt:
            logger.info(
                "\nReceived keyboard interrupt (Ctrl+C). "
                "Initiating graceful shutdown..."
            )

        finally:
            self.cleanup()
            with self._lock:
                self._running = False
                self._latest_jpeg = None
            logger.info("Vision Client shutdown complete.")


def main() -> None:
    """CLI entrypoint for running the vision client application."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    runner = VisionRunner()

    def handle_signal(
        sig: int,
        frame: FrameType | None,
    ) -> None:
        logger.info("Shutdown signal (%s) received. Exiting...", sig)
        runner.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        runner.run()
    except Exception as exc:
        logger.error("Vision Client failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
