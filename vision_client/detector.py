"""YOLO Object Detector interface and scaffolding."""

from abc import ABC, abstractmethod
from typing import Any

from vision_client.events import DetectionEvent


class BaseDetector(ABC):
    """Abstract interface for object detection models."""

    @abstractmethod
    def load_model(self) -> None:
        """Load and initialize model weights into memory."""

    @abstractmethod
    def detect(self, frame: Any, camera_id: str) -> list[DetectionEvent]:
        """Perform object detection on an image frame and return structured events.

        Args:
            frame: Video frame (e.g., numpy ndarray).
            camera_id: Camera identifier emitting the detections.

        Returns:
            List of structured DetectionEvent instances.
        """


class YOLODetector(BaseDetector):
    """YOLO model detector implementation (scaffolding).

    Loads YOLO models (such as yolo11n.pt or yolov8n.pt) and extracts
    bounding boxes, class labels, and confidence metrics.
    """

    def __init__(self, model_path: str = "yolo11n.pt", confidence: float = 0.5) -> None:
        self.model_path = model_path
        self.confidence = confidence
        self._model: Any | None = None

    def load_model(self) -> None:
        try:
            from ultralytics import YOLO
            self._model = YOLO(self.model_path)
        except ImportError as err:
            raise ImportError(
                "Ultralytics is required for YOLO object detection. "
                "Install it with `pip install ultralytics`."
            ) from err

    def detect(self, frame: Any, camera_id: str) -> list[DetectionEvent]:
        if self._model is None:
            raise RuntimeError("YOLO model is not loaded. Call load_model() first.")
        # Full frame inference, NMS parsing, and event wrapping will be implemented in subsequent phases.
        return []
