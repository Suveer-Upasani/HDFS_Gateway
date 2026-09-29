"""Real-time Vision Client runner: Captures camera frames, executes YOLO inference, and streams DetectionEvents to Kafka."""

import logging
import signal
import sys
import time
from types import FrameType

from vision_client.camera import BaseCameraStream, OpenCVCameraStream
from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.detector import BaseDetector, YOLODetector
from vision_client.kafka_producer import BaseEventProducer, KafkaVisionProducer

logger = logging.getLogger("vision_client")


class VisionRunner:
    """Orchestrates video capture, YOLO object detection, and Kafka streaming."""

    def __init__(
        self,
        settings: VisionClientSettings | None = None,
        camera: BaseCameraStream | None = None,
        detector: BaseDetector | None = None,
        producer: BaseEventProducer | None = None,
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
        self._running = False

    def setup(self) -> None:
        """Initialize and verify camera, YOLO model, and Kafka producer.

        Raises:
            RuntimeError: If any subsystem fails to initialize.
        """
        logger.info("Initializing Vision Client components...")

        # 1. Connect to Kafka
        logger.info(
            "Connecting to Kafka at %s (topic: %s)...",
            self.settings.KAFKA_BOOTSTRAP_SERVERS,
            self.settings.KAFKA_TOPIC,
        )
        try:
            self.producer.connect()
            logger.info("✓ Kafka producer connected successfully.")
        except Exception as err:
            raise RuntimeError(
                f"Kafka connection failed for broker '{self.settings.KAFKA_BOOTSTRAP_SERVERS}': {err}. "
                "Ensure Kafka broker is running and reachable."
            ) from err

        # 2. Load YOLO Model
        logger.info(
            "Loading YOLO model '%s' (confidence: %.2f)...",
            self.settings.YOLO_MODEL,
            self.settings.YOLO_CONFIDENCE,
        )
        try:
            self.detector.load_model()
            logger.info("✓ YOLO model '%s' loaded successfully.", self.settings.YOLO_MODEL)
        except Exception as err:
            self.producer.close()
            raise RuntimeError(
                f"Failed to load YOLO model '{self.settings.YOLO_MODEL}': {err}."
            ) from err

        # 3. Open Camera
        logger.info("Opening camera source '%s'...", self.settings.CAMERA_SOURCE)
        opened = False
        try:
            opened = self.camera.open()
        except Exception as err:
            self.producer.close()
            raise RuntimeError(
                f"Error initializing camera source '{self.settings.CAMERA_SOURCE}': {err}."
            ) from err

        if not opened:
            self.producer.close()
            raise RuntimeError(
                f"Could not open camera source '{self.settings.CAMERA_SOURCE}'. "
                "Verify camera connection, device permissions, or source index."
            )
        logger.info("✓ Camera '%s' opened successfully.", self.settings.CAMERA_ID)

    def stop(self) -> None:
        """Signal the processing loop to stop."""
        self._running = False

    def cleanup(self) -> None:
        """Release camera hardware and close Kafka producer connection."""
        logger.info("Releasing vision client resources...")
        try:
            self.camera.release()
            logger.info("✓ Camera released.")
        except Exception as err:  # noqa: BLE001
            logger.warning("Error releasing camera: %s", err)

        try:
            self.producer.close()
            logger.info("✓ Kafka producer closed.")
        except Exception as err:  # noqa: BLE001
            logger.warning("Error closing Kafka producer: %s", err)

    def run(self) -> None:
        """Execute the real-time detection and streaming loop."""
        self.setup()
        self._running = True

        logger.info(
            "🚀 Vision pipeline active. Streaming detection events from camera '%s' -> Kafka topic '%s'...",
            self.settings.CAMERA_ID,
            self.settings.KAFKA_TOPIC,
        )
        logger.info("Press Ctrl+C to terminate.")

        frame_count = 0
        total_detections = 0
        window_detections = 0
        last_log_time = time.time()

        try:
            while self._running:
                success, frame = self.camera.read()
                if not success or frame is None:
                    time.sleep(0.01)
                    continue

                frame_count += 1
                events = self.detector.detect(frame, camera_id=self.settings.CAMERA_ID)

                for event in events:
                    self.producer.send_event(event)
                    total_detections += 1
                    window_detections += 1

                now = time.time()
                elapsed = now - last_log_time
                if elapsed >= 5.0:
                    fps = frame_count / elapsed if elapsed > 0 else 0.0
                    logger.info(
                        "Stats: %.1f FPS | Frames: %d | New Detections: %d | Total Detections: %d",
                        fps,
                        frame_count,
                        window_detections,
                        total_detections,
                    )
                    frame_count = 0
                    window_detections = 0
                    last_log_time = now

        except KeyboardInterrupt:
            logger.info("\nReceived keyboard interrupt (Ctrl+C). Initiating graceful shutdown...")
        finally:
            self.cleanup()
            logger.info("Vision Client shutdown complete.")


def main() -> None:
    """CLI entrypoint for running the vision client application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    runner = VisionRunner()

    def handle_signal(sig: int, frame: FrameType | None) -> None:
        logger.info("Shutdown signal (%s) received. Exiting...", sig)
        runner.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        runner.run()
    except Exception as exc:  # noqa: BLE001
        logger.error("Vision Client failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
