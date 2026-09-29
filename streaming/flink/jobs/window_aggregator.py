"""Event-time windowed aggregation engine for Kafka vision detection streams."""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from vision_client.events import DetectionEvent

logger = logging.getLogger("flink_vision_analytics")


def parse_event_timestamp(ts_str: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a timezone-aware UTC datetime.

    Handles ISO formats with 'Z' suffix, '+00:00' offsets, and fractional seconds.

    Args:
        ts_str: ISO 8601 formatted timestamp string.

    Returns:
        timezone-aware datetime in UTC.
    """
    cleaned = ts_str.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        # Fallback for alternative or truncated ISO strings
        try:
            dt = datetime.strptime(cleaned[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = datetime.now(timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def compute_window_bounds(dt: datetime, window_seconds: int = 10) -> tuple[datetime, datetime]:
    """Calculate the aligned tumbling window start and end boundaries for a given datetime.

    The window interval is [start, end) aligned to the Unix epoch.

    Args:
        dt: The event timestamp.
        window_seconds: Tumbling window duration in seconds (default: 10s).

    Returns:
        Tuple of (window_start, window_end) as UTC datetimes.
    """
    ts = dt.timestamp()
    start_ts = (int(ts) // window_seconds) * window_seconds
    end_ts = start_ts + window_seconds

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    return start_dt, end_dt


@dataclass(frozen=True)
class WindowKey:
    """Composite key identifying a specific window partition."""

    window_start: str
    window_end: str
    object: str
    camera_id: str | None = None


@dataclass
class WindowedObjectCount:
    """Structured analytics output representing aggregated object counts per window."""

    window_start: str
    window_end: str
    object: str
    count: int
    camera_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize window result to a clean dictionary."""
        data = asdict(self)
        if self.camera_id is None:
            data.pop("camera_id", None)
        return data

    def to_json(self) -> str:
        """Serialize window result to a JSON string."""
        return json.dumps(self.to_dict())


class TumblingWindowAggregator:
    """Event-time tumbling window aggregator for object detection events.

    Accumulates counts across discrete time windows and emits finalized analytics records
    as watermarks or newer event times progress past the window boundary.
    """

    def __init__(
        self,
        window_seconds: int = 10,
        group_by_camera: bool = False,
        watermark_delay_seconds: float = 2.0,
    ) -> None:
        self.window_seconds = window_seconds
        self.group_by_camera = group_by_camera
        self.watermark_delay_seconds = watermark_delay_seconds

        # Map of WindowKey -> integer count
        self._window_counts: dict[WindowKey, int] = {}
        # Track active window end timestamps
        self._window_end_times: dict[WindowKey, datetime] = {}
        # Current watermark timestamp
        self._max_event_time: datetime | None = None

    def process_event(self, event_data: dict[str, Any] | DetectionEvent) -> list[WindowedObjectCount]:
        """Ingest a single detection event and return any completed window results.

        Args:
            event_data: Raw JSON dictionary or DetectionEvent instance.

        Returns:
            List of WindowedObjectCount instances for windows finalized by this event's watermark.
        """
        if isinstance(event_data, dict):
            try:
                event = DetectionEvent(**event_data)
            except (ValidationError, TypeError, ValueError, KeyError) as err:
                logger.warning("Skipping invalid detection event payload: %s (Error: %s)", event_data, err)
                return []
        else:
            event = event_data

        event_dt = parse_event_timestamp(event.timestamp)
        start_dt, end_dt = compute_window_bounds(event_dt, self.window_seconds)

        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        camera_key = event.camera_id if self.group_by_camera else None
        key = WindowKey(
            window_start=start_str,
            window_end=end_str,
            object=event.object,
            camera_id=camera_key,
        )

        self._window_counts[key] = self._window_counts.get(key, 0) + 1
        self._window_end_times[key] = end_dt

        # Advance watermark
        if self._max_event_time is None or event_dt > self._max_event_time:
            self._max_event_time = event_dt

        watermark = self._max_event_time.timestamp() - self.watermark_delay_seconds
        return self._flush_expired_windows(watermark)

    def _flush_expired_windows(self, watermark_ts: float) -> list[WindowedObjectCount]:
        """Flush windows whose end timestamp is older than or equal to the watermark."""
        completed: list[WindowedObjectCount] = []
        keys_to_remove: list[WindowKey] = []

        for key, end_dt in self._window_end_times.items():
            if end_dt.timestamp() <= watermark_ts:
                count = self._window_counts.get(key, 0)
                record = WindowedObjectCount(
                    window_start=key.window_start,
                    window_end=key.window_end,
                    object=key.object,
                    count=count,
                    camera_id=key.camera_id,
                )
                completed.append(record)
                keys_to_remove.append(key)

        for key in keys_to_remove:
            self._window_counts.pop(key, None)
            self._window_end_times.pop(key, None)

        # Sort completed records chronologically and by object
        completed.sort(key=lambda r: (r.window_start, r.object, r.camera_id or ""))
        return completed

    def flush_all(self) -> list[WindowedObjectCount]:
        """Force flush all remaining active windows (used on job shutdown or EOF)."""
        completed: list[WindowedObjectCount] = []
        for key, count in self._window_counts.items():
            record = WindowedObjectCount(
                window_start=key.window_start,
                window_end=key.window_end,
                object=key.object,
                count=count,
                camera_id=key.camera_id,
            )
            completed.append(record)

        self._window_counts.clear()
        self._window_end_times.clear()
        completed.sort(key=lambda r: (r.window_start, r.object, r.camera_id or ""))
        return completed
