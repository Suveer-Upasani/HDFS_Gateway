"""Vision and Real-Time Stream API Router (v2 Scaffolding).

This module provides the route namespace and architectural placeholders for
future real-time stream queries (consuming processed Flink metrics or HDFS analytics).

IMPORTANT:
- Does NOT execute or connect to camera streams directly.
- Does NOT return mock or simulated stream telemetry.
- Actual streaming endpoints will be implemented in subsequent phases.
"""

from fastapi import APIRouter

router = APIRouter(prefix="/vision", tags=["Real-Time Vision Streaming"])


# Planned endpoints for Phase 5 (Real-Time API & Dashboards):
# - GET /api/v1/vision/health        -> Real-time streaming pipeline status
# - GET /api/v1/vision/events/latest -> Latest windowed detections from Flink sink
# - GET /api/v1/vision/cameras       -> Active camera metadata & frame rate stats
# - GET /api/v1/vision/analytics     -> Aggregated historical detection analytics
