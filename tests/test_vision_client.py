import pytest
from pydantic import ValidationError

from app.api.vision import router as vision_router
from vision_client.camera import BaseCameraStream, OpenCVCameraStream
from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.detector import BaseDetector, YOLODetector
from vision_client.events import DetectionEvent
from vision_client.kafka_producer import BaseEventProducer, KafkaVisionProducer


def test_detection_event_valid():
    """Verify that a valid detection event dictionary creates a valid DetectionEvent."""
    payload = {
        "timestamp": "2026-09-28T22:05:31.521Z",
        "camera_id": "camera-01",
        "object": "person",
        "confidence": 0.94,
        "bbox": [312, 145, 521, 612],
    }
    event = DetectionEvent(**payload)
    assert event.camera_id == "camera-01"
    assert event.object == "person"
    assert event.confidence == 0.94
    assert event.bbox == [312, 145, 521, 612]
    assert event.timestamp == "2026-09-28T22:05:31.521Z"


def test_detection_event_default_timestamp():
    """Verify that DetectionEvent auto-generates an ISO timestamp if none is provided."""
    event = DetectionEvent(
        camera_id="cam-test",
        object="car",
        confidence=0.88,
        bbox=[10, 20, 100, 200],
    )
    assert event.timestamp is not None
    assert isinstance(event.timestamp, str)
    assert event.camera_id == "cam-test"


def test_detection_event_validation_errors():
    """Verify that invalid bounding boxes or confidence scores raise ValidationErrors."""
    # Confidence > 1.0
    with pytest.raises(ValidationError):
        DetectionEvent(
            camera_id="cam-01",
            object="person",
            confidence=1.5,
            bbox=[0, 0, 10, 10],
        )

    # Negative confidence
    with pytest.raises(ValidationError):
        DetectionEvent(
            camera_id="cam-01",
            object="person",
            confidence=-0.1,
            bbox=[0, 0, 10, 10],
        )

    # Incomplete bbox (less than 4 items)
    with pytest.raises(ValidationError):
        DetectionEvent(
            camera_id="cam-01",
            object="person",
            confidence=0.5,
            bbox=[0, 0, 10],
        )

    # Excessive bbox (more than 4 items)
    with pytest.raises(ValidationError):
        DetectionEvent(
            camera_id="cam-01",
            object="person",
            confidence=0.5,
            bbox=[0, 0, 10, 10, 50],
        )


def test_vision_client_settings_defaults():
    """Verify default values in VisionClientSettings."""
    settings = VisionClientSettings()
    assert settings.CAMERA_ID == "camera-01"
    assert settings.KAFKA_TOPIC == "vision-events"
    assert settings.YOLO_MODEL == "yolo11n.pt"
    assert settings.YOLO_CONFIDENCE == 0.5
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"


def test_get_vision_settings_singleton():
    """Verify that get_vision_settings returns a cached instance."""
    s1 = get_vision_settings()
    s2 = get_vision_settings()
    assert s1 is s2


def test_camera_interface():
    """Verify camera class inheritance and initial state."""
    cam = OpenCVCameraStream(source=0)
    assert isinstance(cam, BaseCameraStream)
    assert not cam.is_opened()
    success, frame = cam.read()
    assert success is False
    assert frame is None


def test_detector_interface():
    """Verify detector class inheritance and initial state."""
    detector = YOLODetector(model_path="yolo11n.pt", confidence=0.5)
    assert isinstance(detector, BaseDetector)
    # Detect without loading should raise RuntimeError
    with pytest.raises(RuntimeError, match="not loaded"):
        detector.detect(frame=None, camera_id="cam-01")


def test_kafka_producer_interface():
    """Verify producer class inheritance and initial state."""
    producer = KafkaVisionProducer(bootstrap_servers="localhost:9092", topic="vision-events")
    assert isinstance(producer, BaseEventProducer)
    event = DetectionEvent(
        camera_id="cam-01",
        object="person",
        confidence=0.9,
        bbox=[10, 20, 30, 40],
    )
    # Send without connect should raise RuntimeError
    with pytest.raises(RuntimeError, match="not connected"):
        producer.send_event(event)


def test_vision_router_defined():
    """Verify that vision router is properly initialized without conflicting with v1 endpoints."""
    assert vision_router.prefix == "/vision"
    assert "Real-Time Vision Streaming" in vision_router.tags
