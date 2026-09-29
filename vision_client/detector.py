"""YOLO Object Detector implementation using Ultralytics."""

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
    """YOLO model detector implementation using Ultralytics.

    Loads YOLO models (such as yolo11n.pt or yolov8n.pt) and extracts
    bounding boxes, class labels, and confidence metrics into structured DetectionEvent objects.
    """

    def __init__(self, model_path: str = "yolo11n.pt", confidence: float = 0.5) -> None:
        self.model_path = model_path
        self.confidence = confidence
        self._model: Any | None = None

    def load_model(self) -> None:
        """Load YOLO model weights into memory.

        Raises:
            ImportError: If ultralytics is not installed.
            RuntimeError: If model loading fails.
        """
        try:
            from ultralytics import YOLO

            self._model = YOLO(self.model_path)
        except ImportError as err:
            raise ImportError(
                "Ultralytics is required for YOLO object detection. "
                "Install it with `pip install ultralytics`."
            ) from err
        except Exception as err:
            raise RuntimeError(
                f"Failed to load YOLO model from '{self.model_path}': {err}"
            ) from err

    def detect(self, frame: Any, camera_id: str) -> list[DetectionEvent]:
        """Run YOLO inference on a video frame and return structured detection events.

        Args:
            frame: Video frame image (numpy ndarray).
            camera_id: Identifier of the camera producing the frame.

        Returns:
            List of DetectionEvent objects, one per detected object meeting confidence threshold.

        Raises:
            RuntimeError: If the model has not been loaded before calling detect.
        """
        if self._model is None:
            raise RuntimeError("YOLO model is not loaded. Call load_model() first.")

        if frame is None:
            return []

        try:
            results = self._model(frame, conf=self.confidence, verbose=False)
        except Exception as err:
            raise RuntimeError(f"YOLO inference failed: {err}") from err

        events: list[DetectionEvent] = []
        for result in results:
            boxes = getattr(result, "boxes", None)
            if boxes is None or len(boxes) == 0:
                continue

            names = getattr(result, "names", None) or getattr(self._model, "names", {})

            for box in boxes:
                # Extract coordinates [x1, y1, x2, y2]
                xyxy_raw = box.xyxy[0] if hasattr(box, "xyxy") and len(box.xyxy) > 0 else box.xyxy
                if hasattr(xyxy_raw, "tolist"):
                    coords = xyxy_raw.tolist()
                elif isinstance(xyxy_raw, (list, tuple)):
                    coords = list(xyxy_raw)
                else:
                    coords = [0, 0, 0, 0]

                if len(coords) == 1 and isinstance(coords[0], (list, tuple)):
                    coords = coords[0]

                bbox = [round(float(c)) for c in coords[:4]]

                # Extract confidence
                conf_raw = box.conf[0] if hasattr(box, "conf") and hasattr(box.conf, "__getitem__") else box.conf
                if hasattr(conf_raw, "item"):
                    conf = float(conf_raw.item())
                else:
                    conf = float(conf_raw)
                conf = max(0.0, min(1.0, conf))

                # Extract class label
                cls_raw = box.cls[0] if hasattr(box, "cls") and hasattr(box.cls, "__getitem__") else box.cls
                if hasattr(cls_raw, "item"):
                    cls_id = int(cls_raw.item())
                else:
                    cls_id = int(cls_raw)

                if isinstance(names, dict):
                    class_label = names.get(cls_id, str(cls_id))
                elif isinstance(names, (list, tuple)) and 0 <= cls_id < len(names):
                    class_label = str(names[cls_id])
                else:
                    class_label = str(cls_id)

                event = DetectionEvent(
                    camera_id=camera_id,
                    object=class_label,
                    confidence=round(conf, 4),
                    bbox=bbox,
                )
                events.append(event)

        return events
