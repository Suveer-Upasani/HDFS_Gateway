"""Unit tests for Apache Flink windowed aggregation and streaming analytics engine."""

import json
from datetime import datetime, timezone

from streaming.flink.jobs.window_aggregator import (
    TumblingWindowAggregator,
    WindowedObjectCount,
    compute_window_bounds,
    parse_event_timestamp,
)
from vision_client.events import DetectionEvent

# ==========================================
# 1. Timestamp Parsing & Window Bounds Tests
# ==========================================

def test_parse_event_timestamp_zulu():
    """Verify parsing ISO timestamp with 'Z' suffix."""
    ts_str = "2026-09-29T21:52:23.450Z"
    dt = parse_event_timestamp(ts_str)
    assert dt.tzinfo == timezone.utc
    assert dt.year == 2026
    assert dt.month == 9
    assert dt.day == 29
    assert dt.hour == 21
    assert dt.minute == 52
    assert dt.second == 23
    assert dt.microsecond == 450000


def test_parse_event_timestamp_offset():
    """Verify parsing ISO timestamp with explicit UTC offset."""
    ts_str = "2026-09-29T21:52:23.123456+00:00"
    dt = parse_event_timestamp(ts_str)
    assert dt.tzinfo == timezone.utc
    assert dt.second == 23


def test_parse_event_timestamp_fallback():
    """Verify fallback for non-standard timestamp string."""
    dt = parse_event_timestamp("invalid-timestamp")
    assert isinstance(dt, datetime)
    assert dt.tzinfo == timezone.utc


def test_compute_window_bounds_10s():
    """Verify that timestamps are aligned to 10-second epoch boundaries."""
    # 21:52:23 -> window [21:52:20, 21:52:30)
    dt = datetime(2026, 9, 29, 21, 52, 23, tzinfo=timezone.utc)
    start_dt, end_dt = compute_window_bounds(dt, window_seconds=10)

    assert start_dt.second == 20
    assert end_dt.second == 30
    assert (end_dt - start_dt).total_seconds() == 10.0


def test_compute_window_bounds_exact_boundary():
    """Verify window calculation when timestamp falls exactly on boundary."""
    dt = datetime(2026, 9, 29, 21, 52, 30, 0, tzinfo=timezone.utc)
    start_dt, end_dt = compute_window_bounds(dt, window_seconds=10)

    assert start_dt.second == 30
    assert end_dt.second == 40


# ==========================================
# 2. WindowedObjectCount Model Tests
# ==========================================

def test_windowed_object_count_serialization():
    """Verify dictionary and JSON serialization without camera_id."""
    record = WindowedObjectCount(
        window_start="2026-09-29T21:52:20Z",
        window_end="2026-09-29T21:52:30Z",
        object="person",
        count=287,
    )
    data = record.to_dict()
    assert data == {
        "window_start": "2026-09-29T21:52:20Z",
        "window_end": "2026-09-29T21:52:30Z",
        "object": "person",
        "count": 287,
    }

    json_str = record.to_json()
    parsed = json.loads(json_str)
    assert parsed["count"] == 287
    assert "camera_id" not in parsed


def test_windowed_object_count_with_camera_id():
    """Verify serialization when camera_id grouping is enabled."""
    record = WindowedObjectCount(
        window_start="2026-09-29T21:52:20Z",
        window_end="2026-09-29T21:52:30Z",
        object="car",
        count=14,
        camera_id="camera-01",
    )
    data = record.to_dict()
    assert data["camera_id"] == "camera-01"
    assert data["object"] == "car"
    assert data["count"] == 14


# ==========================================
# 3. Tumbling Window Aggregator Tests
# ==========================================

def test_aggregator_single_window_accumulation():
    """Verify multiple events within the same 10-second window accumulate counts."""
    aggregator = TumblingWindowAggregator(window_seconds=10, group_by_camera=False)

    events = [
        {"timestamp": "2026-09-29T21:52:21Z", "camera_id": "cam-1", "object": "person", "confidence": 0.9, "bbox": [0, 0, 10, 10]},
        {"timestamp": "2026-09-29T21:52:22Z", "camera_id": "cam-1", "object": "person", "confidence": 0.85, "bbox": [0, 0, 10, 10]},
        {"timestamp": "2026-09-29T21:52:25Z", "camera_id": "cam-2", "object": "person", "confidence": 0.95, "bbox": [0, 0, 10, 10]},
        {"timestamp": "2026-09-29T21:52:26Z", "camera_id": "cam-1", "object": "car", "confidence": 0.77, "bbox": [0, 0, 10, 10]},
    ]

    for ev in events:
        aggregator.process_event(ev)

    # Flush all to inspect accumulated counts
    results = aggregator.flush_all()
    assert len(results) == 2

    person_record = next(r for r in results if r.object == "person")
    car_record = next(r for r in results if r.object == "car")

    assert person_record.count == 3
    assert car_record.count == 1
    assert person_record.window_start == "2026-09-29T21:52:20Z"
    assert person_record.window_end == "2026-09-29T21:52:30Z"


def test_aggregator_group_by_camera():
    """Verify that enabling group_by_camera partitions counts by both object and camera_id."""
    aggregator = TumblingWindowAggregator(window_seconds=10, group_by_camera=True)

    events = [
        {"timestamp": "2026-09-29T21:52:21Z", "camera_id": "cam-1", "object": "person", "confidence": 0.9, "bbox": [0, 0, 10, 10]},
        {"timestamp": "2026-09-29T21:52:22Z", "camera_id": "cam-2", "object": "person", "confidence": 0.85, "bbox": [0, 0, 10, 10]},
        {"timestamp": "2026-09-29T21:52:25Z", "camera_id": "cam-1", "object": "person", "confidence": 0.95, "bbox": [0, 0, 10, 10]},
    ]

    for ev in events:
        aggregator.process_event(ev)

    results = aggregator.flush_all()
    assert len(results) == 2

    cam1_person = next(r for r in results if r.camera_id == "cam-1")
    cam2_person = next(r for r in results if r.camera_id == "cam-2")

    assert cam1_person.count == 2
    assert cam2_person.count == 1


def test_aggregator_watermark_window_emission():
    """Verify that progressing event time past window boundary + watermark delay automatically emits window."""
    aggregator = TumblingWindowAggregator(window_seconds=10, watermark_delay_seconds=2.0)

    # Event in Window 1
    w1_event = {
        "timestamp": "2026-09-29T21:52:25Z",
        "camera_id": "cam-1",
        "object": "laptop",
        "confidence": 0.92,
        "bbox": [10, 10, 50, 50],
    }
    emitted = aggregator.process_event(w1_event)
    assert emitted == []  # Not emitted yet

    # Event in Window 2 at 21:52:32 (Watermark = 21:52:32 - 2s = 21:52:30)
    w2_event = {
        "timestamp": "2026-09-29T21:52:32Z",
        "camera_id": "cam-1",
        "object": "laptop",
        "confidence": 0.88,
        "bbox": [10, 10, 50, 50],
    }
    emitted2 = aggregator.process_event(w2_event)
    assert len(emitted2) == 1
    assert emitted2[0].object == "laptop"
    assert emitted2[0].count == 1
    assert emitted2[0].window_start == "2026-09-29T21:52:20Z"
    assert emitted2[0].window_end == "2026-09-29T21:52:30Z"


def test_aggregator_invalid_event_skipped():
    """Verify that invalid payloads (missing required fields) are safely skipped."""
    aggregator = TumblingWindowAggregator()
    invalid_event = {"broken": "payload"}
    emitted = aggregator.process_event(invalid_event)
    assert emitted == []
    assert len(aggregator.flush_all()) == 0


def test_aggregator_with_detection_event_instance():
    """Verify aggregator accepts DetectionEvent instances directly."""
    aggregator = TumblingWindowAggregator(window_seconds=10)
    event = DetectionEvent(
        timestamp="2026-09-29T21:52:25Z",
        camera_id="cam-test",
        object="bottle",
        confidence=0.91,
        bbox=[5, 5, 20, 20],
    )
    aggregator.process_event(event)
    results = aggregator.flush_all()
    assert len(results) == 1
    assert results[0].object == "bottle"
    assert results[0].count == 1
