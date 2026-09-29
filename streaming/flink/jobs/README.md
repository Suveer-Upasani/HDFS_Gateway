# 🛠️ Flink Streaming Jobs (v2)

This directory contains streaming analytics jobs that process real-time `DetectionEvent` streams from Kafka.

---

## 📊 Real-Time Windowed Object Counting Job (`object_counting_job.py`)

### Architecture & Operation
The windowed object counting job consumes structured `DetectionEvent` messages from the `vision-events` Kafka topic, performs event-time 10-second tumbling window aggregations grouped by detected object class (e.g., `person`, `car`, `laptop`), and outputs structured JSON records to stdout/logging.

```text
Kafka Topic (vision-events)
          ↓
  [DetectionEvent JSON]
          ↓
  TumblingWindowAggregator (10s Tumbling Windows, 2s Watermark Delay)
          ↓
  [Windowed Analytics Record]
          ↓
  Standard Output / Flink Logging
```

### Schema & Aggregation Contract

#### Input Event (`DetectionEvent`):
```json
{
  "timestamp": "2026-09-29T21:52:23.450Z",
  "camera_id": "camera-01",
  "object": "person",
  "confidence": 0.94,
  "bbox": [312, 145, 521, 612]
}
```

#### Output Record (`WindowedObjectCount`):
```json
{
  "window_start": "2026-09-29T21:52:20Z",
  "window_end": "2026-09-29T21:52:30Z",
  "object": "person",
  "count": 287
}
```

When camera grouping is enabled (`--group-by-camera`), the output includes `camera_id`:
```json
{
  "window_start": "2026-09-29T21:52:20Z",
  "window_end": "2026-09-29T21:52:30Z",
  "camera_id": "camera-01",
  "object": "person",
  "count": 287
}
```

---

## 🚀 Execution Guide

### 1. Running on Host / Linux VM (Direct Streaming Engine)
From the repository root:

```bash
# Connect to local/VM Kafka broker (e.g., inside Kali Linux VM)
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers localhost:9092 --topic vision-events

# Connect to remote Kafka broker from external host
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers 192.168.1.7:9092 --topic vision-events

# Optional: Group by camera ID and read from earliest offset
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers localhost:9092 --group-by-camera --from-beginning
```

### 2. Running inside Docker Network
When running inside the Docker network (e.g. within a container on the streaming network):

```bash
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers kafka:9092 --topic vision-events
```

---

## 🗄️ Planned Downstream Sinks

In subsequent development phases, outputs from this windowing engine will be dispatched to:
1. **Hadoop HDFS (Historical Storage):** `/user/suveer/vision/analytics/` in daily Parquet/JSON partitions.
2. **FastAPI & Live Dashboard:** In-memory rolling metrics buffer for sub-second REST/WebSocket visualization.
