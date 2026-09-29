# 🌊 Apache Flink Streaming Engine (v2)

This directory contains configuration, architecture notes, and deployment definitions for **Apache Flink** in the real-time computer vision streaming pipeline.

---

## 🎯 Role in Architecture

Apache Flink acts as the low-latency, stateful distributed stream processor. It continuously ingests structured detection events from Kafka, performs sliding/tumbling window aggregations, anomaly detection, and state tracking, and writes dual outputs:
1. **Long-Term Persistence:** High-throughput streaming sink writing raw and structured event batches to existing Hadoop HDFS.
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
│  │ • Cluster coordination    │    │ • Window aggregations    │  │
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

## 🚀 Flink Management Commands

Flink services are defined in `docker-compose.streaming.yml`:

```bash
# Start Flink cluster
docker compose -f docker-compose.streaming.yml up -d flink-jobmanager flink-taskmanager

# Access Flink Web Dashboard
open http://localhost:8081

# Check Flink TaskManager logs
docker compose -f docker-compose.streaming.yml logs -f flink-taskmanager
```
