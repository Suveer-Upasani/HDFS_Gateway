"""Vision Client package for real-time edge capture, YOLO inference, and event publishing."""

from vision_client.camera import BaseCameraStream, OpenCVCameraStream
from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.detector import BaseDetector, YOLODetector
from vision_client.events import DetectionEvent
from vision_client.kafka_producer import BaseEventProducer, KafkaVisionProducer
from vision_client.runner import VisionRunner

__all__ = [
    "BaseCameraStream",
    "BaseDetector",
    "BaseEventProducer",
    "DetectionEvent",
    "KafkaVisionProducer",
    "OpenCVCameraStream",
    "VisionClientSettings",
    "VisionRunner",
    "YOLODetector",
    "get_vision_settings",
]
