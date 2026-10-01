from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from app.api.vision import router as vision_router
from vision_client.camera import BaseCameraStream, OpenCVCameraStream
from vision_client.config import VisionClientSettings, get_vision_settings
from vision_client.detector import BaseDetector, YOLODetector
from vision_client.events import DetectionEvent
from vision_client.kafka_producer import BaseEventProducer, KafkaVisionProducer
from vision_client.runner import VisionRunner

# ==========================================
# 1. DetectionEvent Tests
# ==========================================

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


# ==========================================
# 2. Configuration Settings Tests
# ==========================================

def test_vision_client_settings_defaults():
    """Verify default values in VisionClientSettings."""
    settings = VisionClientSettings(_env_file=None)
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


# ==========================================
# 3. Camera Stream Tests
# ==========================================

def test_camera_interface():
    """Verify camera class inheritance and initial state."""
    cam = OpenCVCameraStream(source=0)
    assert isinstance(cam, BaseCameraStream)
    assert not cam.is_opened()
    success, frame = cam.read()
    assert success is False
    assert frame is None


def test_camera_source_parsing():
    """Verify that string digits are parsed to int and string paths are kept as str."""
    cam_num = OpenCVCameraStream(source="2")
    assert cam_num.source == 2

    cam_str = OpenCVCameraStream(source="/path/to/video.mp4")
    assert cam_str.source == "/path/to/video.mp4"

    cam_int = OpenCVCameraStream(source=1)
    assert cam_int.source == 1


@patch("cv2.VideoCapture")
def test_camera_open_success(mock_video_capture):
    """Verify camera open success with mocked VideoCapture."""
    mock_cap = MagicMock()
    mock_cap.isOpened.return_value = True
    mock_video_capture.return_value = mock_cap

    cam = OpenCVCameraStream(source=0)
    opened = cam.open()
    assert opened is True
    assert cam.is_opened() is True


@patch("cv2.VideoCapture")
def test_camera_open_failure(mock_video_capture):
    """Verify camera open returns False when device is not opened."""
    mock_cap = MagicMock()
    mock_cap.isOpened.return_value = False
    mock_video_capture.return_value = mock_cap

    cam = OpenCVCameraStream(source=99)
    opened = cam.open()
    assert opened is False
    assert cam.is_opened() is False


@patch("cv2.VideoCapture")
def test_camera_read_and_release(mock_video_capture):
    """Verify camera read and release methods with mock frames."""
    mock_cap = MagicMock()
    mock_cap.isOpened.return_value = True
    fake_frame = MagicMock()
    mock_cap.read.return_value = (True, fake_frame)
    mock_video_capture.return_value = mock_cap

    cam = OpenCVCameraStream(source=0)
    cam.open()

    success, frame = cam.read()
    assert success is True
    assert frame is fake_frame

    cam.release()
    assert cam.is_opened() is False
    mock_cap.release.assert_called_once()

    # Read after release returns (False, None)
    success2, frame2 = cam.read()
    assert success2 is False
    assert frame2 is None


# ==========================================
# 4. YOLO Detector Tests
# ==========================================

def test_detector_interface():
    """Verify detector class inheritance and initial state."""
    detector = YOLODetector(model_path="yolo11n.pt", confidence=0.5)
    assert isinstance(detector, BaseDetector)
    # Detect without loading should raise RuntimeError
    with pytest.raises(RuntimeError, match="not loaded"):
        detector.detect(frame=None, camera_id="cam-01")


def test_detector_detect_none_frame():
    """Verify that detect returns empty list when frame is None."""
    detector = YOLODetector(model_path="yolo11n.pt", confidence=0.5)
    detector._model = MagicMock()
    events = detector.detect(frame=None, camera_id="cam-01")
    assert events == []


def test_detector_detect_parsing_with_mock():
    """Verify YOLO output parsing into DetectionEvent objects without running real model."""
    detector = YOLODetector(model_path="yolo11n.pt", confidence=0.5)

    mock_box1 = MagicMock()
    mock_box1.xyxy = [[10.2, 20.8, 100.1, 200.9]]
    mock_box1.conf = [0.89234]
    mock_box1.cls = [0]

    mock_box2 = MagicMock()
    mock_box2.xyxy = [[50.0, 60.0, 150.0, 250.0]]
    mock_box2.conf = [0.72]
    mock_box2.cls = [2]

    mock_result = MagicMock()
    mock_result.boxes = [mock_box1, mock_box2]
    mock_result.names = {0: "person", 2: "car"}

    mock_model = MagicMock()
    mock_model.return_value = [mock_result]
    detector._model = mock_model

    fake_frame = MagicMock()
    events = detector.detect(frame=fake_frame, camera_id="camera-01")

    assert len(events) == 2
    mock_model.assert_called_once_with(fake_frame, conf=0.5, verbose=False)

    # First detection
    assert isinstance(events[0], DetectionEvent)
    assert events[0].camera_id == "camera-01"
    assert events[0].object == "person"
    assert events[0].confidence == 0.8923
    assert events[0].bbox == [10, 21, 100, 201]

    # Second detection
    assert isinstance(events[1], DetectionEvent)
    assert events[1].camera_id == "camera-01"
    assert events[1].object == "car"
    assert events[1].confidence == 0.72
    assert events[1].bbox == [50, 60, 150, 250]


def test_detector_detect_no_boxes():
    """Verify detect returns [] when model finds no boxes."""
    detector = YOLODetector(model_path="yolo11n.pt", confidence=0.5)
    mock_result = MagicMock()
    mock_result.boxes = []
    mock_model = MagicMock()
    mock_model.return_value = [mock_result]
    detector._model = mock_model

    fake_frame = MagicMock()
    events = detector.detect(frame=fake_frame, camera_id="camera-01")
    assert events == []


# ==========================================
# 5. Kafka Event Producer Tests
# ==========================================

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


@patch("kafka.KafkaProducer")
def test_kafka_producer_send_and_close(mock_kafka_cls):
    """Verify KafkaVisionProducer send_event serializes DetectionEvent and sends to Kafka."""
    mock_inner_producer = MagicMock()
    mock_kafka_cls.return_value = mock_inner_producer

    producer = KafkaVisionProducer(bootstrap_servers="localhost:9092", topic="vision-events")
    producer.connect()
    assert producer._producer is mock_inner_producer

    event = DetectionEvent(
        camera_id="cam-01",
        object="bicycle",
        confidence=0.95,
        bbox=[100, 200, 300, 400],
    )
    result = producer.send_event(event)
    assert result is True
    mock_inner_producer.send.assert_called_once_with("vision-events", value=event.model_dump())

    producer.close()
    mock_inner_producer.flush.assert_called_once()
    mock_inner_producer.close.assert_called_once()
    assert producer._producer is None


# ==========================================
# 6. VisionRunner Orchestration Tests
# ==========================================

def test_vision_runner_setup_success():
    """Verify that VisionRunner setup initializes Kafka, YOLO detector, and Camera."""
    mock_camera = MagicMock(spec=BaseCameraStream)
    mock_camera.open.return_value = True

    mock_detector = MagicMock(spec=BaseDetector)
    mock_producer = MagicMock(spec=BaseEventProducer)

    settings = VisionClientSettings(
        CAMERA_ID="test-cam",
        CAMERA_SOURCE="0",
        KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092",
        KAFKA_TOPIC="vision-events",
        YOLO_MODEL="yolo11n.pt",
        YOLO_CONFIDENCE=0.6,
    )

    runner = VisionRunner(
        settings=settings,
        camera=mock_camera,
        detector=mock_detector,
        producer=mock_producer,
    )

    runner.setup()

    mock_producer.connect.assert_called_once()
    mock_detector.load_model.assert_called_once()
    mock_camera.open.assert_called_once()


def test_vision_runner_setup_camera_failure():
    """Verify that VisionRunner fails with RuntimeError when camera cannot be opened."""
    mock_camera = MagicMock(spec=BaseCameraStream)
    mock_camera.open.return_value = False

    mock_detector = MagicMock(spec=BaseDetector)
    mock_producer = MagicMock(spec=BaseEventProducer)

    runner = VisionRunner(
        camera=mock_camera,
        detector=mock_detector,
        producer=mock_producer,
    )

    with pytest.raises(RuntimeError, match="Could not open camera source"):
        runner.setup()

    mock_producer.close.assert_called_once()


def test_vision_runner_single_loop_execution():
    """Verify that VisionRunner reads frame, detects objects, and sends to Kafka."""
    fake_frame = MagicMock()
    mock_camera = MagicMock(spec=BaseCameraStream)
    mock_camera.open.return_value = True
    # Return one frame then trigger stop
    mock_camera.read.return_value = (True, fake_frame)

    test_event = DetectionEvent(
        camera_id="test-cam",
        object="person",
        confidence=0.91,
        bbox=[10, 20, 30, 40],
    )
    mock_detector = MagicMock(spec=BaseDetector)
    mock_detector.detect.return_value = [test_event]

    mock_producer = MagicMock(spec=BaseEventProducer)

    settings = VisionClientSettings(CAMERA_ID="test-cam")
    runner = VisionRunner(
        settings=settings,
        camera=mock_camera,
        detector=mock_detector,
        producer=mock_producer,
    )

    # Patch read to stop the runner after the first iteration
    def side_effect_read():
        runner.stop()
        return (True, fake_frame)

    mock_camera.read.side_effect = side_effect_read

    runner.run()

    mock_detector.detect.assert_called_once_with(fake_frame, camera_id="test-cam")
    mock_producer.send_event.assert_called_once_with(test_event)
    mock_camera.release.assert_called_once()
    mock_producer.close.assert_called_once()


# ==========================================
# 7. Vision Router Tests
# ==========================================

def test_vision_router_defined():
    """Verify that vision router is properly initialized without conflicting with v1 endpoints."""
    assert vision_router.prefix == "/vision"
    assert "Real-Time Vision Streaming" in vision_router.tags
