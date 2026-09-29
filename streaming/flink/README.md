# 🌊 Apache Flink Streaming Engine (v2)

This directory contains configuration, architecture notes, and deployment definitions for **Apache Flink** in the real-time computer vision streaming pipeline.

---

## 🎯 Role in Architecture

Apache Flink acts as the low-latency, stateful distributed stream processor. It continuously ingests structured detection events from Kafka, performs sliding/tumbling window aggregations, anomaly detection, and state tracking, and writes dual outputs:
1. **Long-Term Persistence (Future Phase):** High-throughput streaming sink writing raw and structured event batches to existing Hadoop HDFS (`/user/suveer/vision/`).
2. **Real-Time Analytics:** Pushing aggregated telemetry and metrics downstream to FastAPI and the live web dashboard.

```
                  ┌───────────────────────────────┐
                  │ Kafka Topic: vision-events    │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Apache Flink Streaming Cluster                  │
│                                                                 │
│  ┌───────────────────────────┐    ┌──────────────────────────┐  │
│  │ JobManager (Port 8081)    │    │ TaskManager (Slots: 2)   │  │
│  │ • Cluster coordination    │    │ • 10s Window Aggregations│  │
│  │ • Checkpoint coordinator  │    │ • State management       │  │
│  └───────────────────────────┘    └────────────┬─────────────┘  │
└────────────────────────────────────────────────┼────────────────┘
                         ┌───────────────────────┴───────────────────────┐
                         │                                               │
                         ▼                                               ▼
     ┌───────────────────────────────────────┐       ┌───────────────────────────────────────┐
     │ Hadoop HDFS (Historical Storage)      │       │ FastAPI & Live Web Dashboard          │
     │ /user/suveer/vision/raw/              │       │ • Windowed object counts              │
     │ /user/suveer/vision/detections/       │       │ • Active camera alerts                │
     │ /user/suveer/vision/analytics/        │       │ • Real-time traffic analytics         │
     └───────────────────────────────────────┘       └───────────────────────────────────────┘
```

---

## ⚙️ Memory & Resource Optimization

The target host environment (Kali Linux VM) has **~7.7 GiB total RAM** and is already running Apache Hadoop (NameNode, DataNode, ResourceManager, NodeManager).

To ensure high stability without starving Hadoop, Flink is configured with conservative memory limits:
* **JobManager Process Size:** `1024m`
* **TaskManager Process Size:** `1024m`
* **Task Slots:** `2`
* **Parallelism:** `1` (suitable for single-node development & testing)

---

## 🚀 Flink Management & Job Execution

### 1. Starting Kafka & Flink Infrastructure
Kafka and Flink services are defined in `docker-compose.streaming.yml`:

```bash
# Start Kafka (KRaft mode with persistent volume) & topic initializer
docker compose -f docker-compose.streaming.yml up -d kafka init-kafka

# Start Flink cluster (JobManager + TaskManager)
docker compose -f docker-compose.streaming.yml up -d flink-jobmanager flink-taskmanager

# Access Flink Web Dashboard
open http://localhost:8081
```

### 2. Running the 10-Second Object Counting Job
The windowed object counting job consumes `DetectionEvent` records from Kafka topic `vision-events` and emits aggregated object counts every 10 seconds.

```bash
# Inside Kali Linux VM / local host
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers localhost:9092 --topic vision-events

# From external client pointing to Kali Linux VM
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers 192.168.1.7:9092 --topic vision-events

# Inside Docker network
python -m streaming.flink.jobs.object_counting_job --bootstrap-servers kafka:9092 --topic vision-events
```

### 3. Example Output
```json
{"window_start": "2026-09-29T21:52:20Z", "window_end": "2026-09-29T21:52:30Z", "object": "person", "count": 287}
{"window_start": "2026-09-29T21:52:20Z", "window_end": "2026-09-29T21:52:30Z", "object": "car", "count": 14}
```
