"""Vision Client package for real-time edge capture, YOLO inference, and event publishing."""

from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.events import DetectionEvent

__all__ = [
    "DetectionEvent",
    "VisionClientSettings",
    "get_vision_settings",
]
