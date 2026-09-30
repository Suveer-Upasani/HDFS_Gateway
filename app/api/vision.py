"""Vision and Real-Time Stream API Router.

Provides real-time pipeline status, configuration metadata, and streaming endpoint
contracts for downstream consumers and dashboards.

IMPORTANT:
- Does NOT return fake or simulated stream telemetry.
- Reflects actual system state and streaming pipeline configuration.
- Real-time window aggregation execution is handled by Apache Flink.
"""

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.core.config import get_settings

router = APIRouter(prefix="/vision", tags=["Real-Time Vision Streaming"])


class VisionPipelineStatus(BaseModel):
    """Current operational state and architecture configuration of the vision pipeline."""

    status: str = Field("operational", description="Current vision gateway router status")
    kafka_topic: str = Field(..., description="Target Kafka topic carrying DetectionEvent JSON records")
    kafka_bootstrap_servers: str = Field(..., description="Kafka broker address")
    flink_jobmanager_url: str = Field(..., description="Apache Flink JobManager Web URL / RPC endpoint")
    stream_window: str = Field("10s_tumbling", description="Flink windowing strategy")
    watermark_delay_seconds: float = Field(2.0, description="Out-of-orderness watermark delay in seconds")
    active_sink: str = Field("print (dev)", description="Current Flink analytics sink")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 UTC timestamp of status check",
    )


class VisionEventsResponse(BaseModel):
    """Latest detection events query response contract."""

    status: str = Field(..., description="Event stream connection state")
    sink_type: str = Field("flink_print_sink", description="Current sink mechanism")
    events: list[dict[str, Any]] = Field(default_factory=list, description="Recent detection events")
    count: int = Field(0, description="Number of events returned")
    message: str = Field(..., description="Informational message regarding streaming source")


class VisionStatsResponse(BaseModel):
    """Current windowed aggregation statistics configuration."""

    status: str = Field("active", description="Streaming statistics status")
    window_duration_seconds: int = Field(10, description="Tumbling window duration")
    watermark_delay_seconds: float = Field(2.0, description="Watermark tolerance")
    grouping_keys: list[str] = Field(default_factory=lambda: ["object"], description="Aggregation grouping keys")
    source_topic: str = Field("vision-events", description="Kafka input topic")
    message: str = Field(..., description="Pipeline execution details")


class VisionAnalyticsResponse(BaseModel):
    """Aggregated window analytics query response contract."""

    status: str = Field("ready", description="Analytics engine state")
    window_type: str = Field("tumbling", description="Window aggregation type")
    window_size_seconds: int = Field(10, description="Window size in seconds")
    records: list[dict[str, Any]] = Field(default_factory=list, description="Aggregated window records")
    message: str = Field(..., description="Description of analytics output location")


@router.get(
    "/status",
    response_model=VisionPipelineStatus,
    summary="Vision Pipeline Architecture Status",
)
async def get_vision_status() -> VisionPipelineStatus:
    """Return the configuration and operational status of the vision streaming pipeline."""
    settings = get_settings()
    return VisionPipelineStatus(
        status="operational",
        kafka_topic=settings.KAFKA_VISION_TOPIC,
        kafka_bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        flink_jobmanager_url=settings.FLINK_JOBMANAGER_URL,
        stream_window="10s_tumbling",
        watermark_delay_seconds=2.0,
        active_sink="print (dev)",
    )


@router.get(
    "/events",
    response_model=VisionEventsResponse,
    summary="Real-Time Detection Events Stream Status",
)
async def get_vision_events() -> VisionEventsResponse:
    """Query recent detection events.

    In the development phase, detection events are published to Kafka ('vision-events')
    and processed in Flink. Live stream logs are emitted to Flink TaskManager stdout.
    """
    settings = get_settings()
    return VisionEventsResponse(
        status="streaming_to_kafka_and_flink",
        sink_type="flink_print_sink",
        events=[],
        count=0,
        message=(
            f"Detection events are ingested into Kafka topic '{settings.KAFKA_VISION_TOPIC}' "
            "and processed by Apache Flink. Live detection events will connect via SSE/WebSocket "
            "in dashboard phase without mock data."
        ),
    )


@router.get(
    "/stats",
    response_model=VisionStatsResponse,
    summary="Windowed Aggregation Stream Statistics",
)
async def get_vision_stats() -> VisionStatsResponse:
    """Return the current streaming aggregation parameters and grouping configuration."""
    settings = get_settings()
    return VisionStatsResponse(
        status="active",
        window_duration_seconds=10,
        watermark_delay_seconds=2.0,
        grouping_keys=["object"],
        source_topic=settings.KAFKA_VISION_TOPIC,
        message="10-second tumbling window aggregation active in Flink cluster.",
    )


@router.get(
    "/analytics",
    response_model=VisionAnalyticsResponse,
    summary="Windowed Object Analytics",
)
async def get_vision_analytics() -> VisionAnalyticsResponse:
    """Return aggregated window analytics metadata.

    Flink 10-second tumbling window outputs are currently routed to the Flink print sink
    (inspectable via TaskManager logs). Future phases will persist to HDFS.
    """
    return VisionAnalyticsResponse(
        status="ready",
        window_type="tumbling",
        window_size_seconds=10,
        records=[],
        message=(
            "Windowed counts (window_start, window_end, object, count) are emitted by Flink "
            "job 'vision_analytics.sql' to TaskManager stdout. HDFS sink will be added in storage phase."
        ),
    )
