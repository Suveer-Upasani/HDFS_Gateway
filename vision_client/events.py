from datetime import datetime, timezone

from pydantic import BaseModel, Field


class DetectionEvent(BaseModel):
    """Structured data contract for object detection events transmitted across the streaming pipeline.

    This contract is shared across the vision client (producer), Kafka event bus,
    Apache Flink stream processing jobs, and downstream HDFS/FastAPI consumers.
    """

    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 UTC timestamp when the detection occurred",
        examples=["2026-09-28T22:05:31.521Z"],
    )
    camera_id: str = Field(
        ...,
        description="Unique identifier for the camera source producing the event",
        examples=["camera-01"],
    )
    object: str = Field(
        ...,
        description="Class label of the detected object (e.g., person, car, bicycle)",
        examples=["person"],
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence score of the model detection between 0.0 and 1.0",
        examples=[0.94],
    )
    bbox: list[int] = Field(
        ...,
        min_length=4,
        max_length=4,
        description="Bounding box coordinates in [x1, y1, x2, y2] pixel format",
        examples=[[312, 145, 521, 612]],
    )
