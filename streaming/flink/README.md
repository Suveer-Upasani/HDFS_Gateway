# 🌊 Apache Flink Streaming Engine (v2)

This directory contains the production **Apache Flink SQL streaming job**, Docker container specifications, Kafka connector configurations, and execution instructions for real-time windowed computer vision analytics.

---

## 🎯 1. Overview & Architecture

Apache Flink acts as the low-latency, stateful distributed stream processor in the pipeline:

1. **YOLO Detection Producer:** Ingests live video / test stream, generates structured `DetectionEvent` JSON records, and publishes to Kafka.
2. **Kafka Bus (`vision-events`):** High-throughput persistent event log decoupling ingestion from processing.
3. **Apache Flink Cluster (JobManager + TaskManager):**
   - Ingests events from Kafka using `flink-sql-connector-kafka`.
   - Parses ISO 8601 timestamps to event-time attributes with bounded out-of-orderness watermarks (2-second delay).
   - Computes stateful **10-second tumbling window aggregations** grouped by detected object class.
   - Emits windowed counts (`window_start`, `window_end`, `object`, `count`) to the target sink.
4. **Dual Outputs:**
   - **Development Sink:** Flink `print` sink (outputs directly to TaskManager stdout logs).
   - **Persistence Sink (Future Phase):** High-throughput streaming sink to Hadoop HDFS (`/user/suveer/vision/analytics/`).
   - **Real-Time API / Dashboard:** FastAPI backend exposing pipeline status and telemetry.

```
┌──────────────────────────────┐
│  vision_client (YOLO / Cam)  │
└──────────────┬───────────────┘
               │ JSON DetectionEvents
               ▼
┌──────────────────────────────┐
│ Kafka Topic: `vision-events` │ (KRaft mode, Port 9092)
└──────────────┬───────────────┘
               │ flink-sql-connector-kafka:3.0.2-1.18
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Apache Flink Streaming Cluster                  │
│                                                                 │
│  ┌───────────────────────────┐    ┌──────────────────────────┐  │
│  │ JobManager (Port 8081)    │    │ TaskManager (Slots: 2)   │  │
│  │ • Job Coordination        │    │ • Event-Time Watermarking│  │
│  │ • SQL Client Gateway      │    │ • 10s Tumbling Windows   │  │
│  │ • Checkpoint Manager      │    │ • Group By `object`      │  │
│  └───────────────────────────┘    └────────────┬─────────────┘  │
└────────────────────────────────────────────────┼────────────────┘
                                                 │
                                                 ▼
                         ┌───────────────────────────────────────────────┐
                         │ Development Sink: TaskManager Stdout (Print)  │
                         │ Future: HDFS Sink & FastAPI Stream Consumer   │
                         └───────────────────────────────────────────────┘
```

---

## 📋 2. Event Schema & Contracts

Events published to Kafka adhere to the `DetectionEvent` contract defined in [`vision_client/events.py`](file:///Users/suveer/HDFS/vision_client/events.py):

```json
{
  "timestamp": "2026-09-28T22:05:31.521Z",
  "camera_id": "camera-01",
  "object": "person",
  "confidence": 0.94,
  "bbox": [312, 145, 521, 612]
}
```

### Schema Attributes

| Field | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `timestamp` | `STRING` | ISO 8601 UTC timestamp of camera frame detection | `"2026-09-28T22:05:31.521Z"` |
| `camera_id` | `STRING` | Identifier of source camera | `"camera-01"` |
| `object` | `STRING` | YOLO class label | `"person"`, `"car"` |
| `confidence` | `DOUBLE` | Detection confidence score (0.0 – 1.0) | `0.94` |
| `bbox` | `ARRAY<INT>` | Bounding box coordinates `[x1, y1, x2, y2]` | `[312, 145, 521, 612]` |

---

## ⏰ 3. Event-Time & Watermark Specification

In [`streaming/flink/jobs/vision_analytics.sql`](file:///Users/suveer/HDFS/streaming/flink/jobs/vision_analytics.sql), the Flink SQL source parses the string timestamp into an event-time attribute and assigns a bounded watermark:

```sql
`event_time` AS TO_TIMESTAMP(REPLACE(SUBSTRING(`timestamp`, 1, 19), 'T', ' ')),
WATERMARK FOR `event_time` AS `event_time` - INTERVAL '2' SECOND
```

* **Event-Time Extraction:** Robustly converts standard ISO strings (`YYYY-MM-DDTHH:MM:SS`) to Flink `TIMESTAMP(3)`.
* **Watermark Delay (2 seconds):** Accommodates camera network jitter and out-of-order frame delivery up to 2 seconds before finalizing window calculations.

---

## 🪟 4. 10-Second Tumbling Window Aggregation

Flink SQL executes a standard Table-Valued Function (TVF) tumbling window aggregation:

```sql
INSERT INTO window_analytics_sink
SELECT
    window_start,
    window_end,
    `object`,
    COUNT(*) AS `count`
FROM TABLE(
    TUMBLE(TABLE vision_events, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY
    window_start,
    window_end,
    `object`;
```

* Windows are aligned to epoch boundaries: `[XX:XX:00, XX:XX:10)`, `[XX:XX:10, XX:XX:20)`, etc.
* When the watermark passes `window_end + 2 seconds`, the window closes and emits the aggregated counts.

---

## 📦 5. Flink Container & Connector Details

The Flink image builds from [`streaming/flink/Dockerfile`](file:///Users/suveer/HDFS/streaming/flink/Dockerfile):
* **Base Image:** `flink:1.18.1-scala_2.12-java11`
* **Connector JAR:** `flink-sql-connector-kafka:3.0.2-1.18` installed into `/opt/flink/lib/`
* **Volume Mount:** `./streaming/flink/jobs` mounted to `/opt/flink/jobs` inside containers

---

## 🚀 6. Execution & Deployment Guide (Linux VM)

### Step 1: Start Kafka & Flink Infrastructure
```bash
# Start Kafka (KRaft mode) and topic initializer (preserves existing kafka_data volume)
docker-compose -f docker-compose.streaming.yml up -d kafka init-kafka

# Build and start Flink JobManager and TaskManager
docker-compose -f docker-compose.streaming.yml up -d --build flink-jobmanager flink-taskmanager
```

### Step 2: Verify Flink Cluster & Kafka Connector
```bash
# Verify running containers
docker-compose -f docker-compose.streaming.yml ps

# Check Flink JobManager logs
docker-compose -f docker-compose.streaming.yml logs flink-jobmanager

# Confirm Kafka SQL connector JAR is present in JobManager
docker-compose -f docker-compose.streaming.yml exec flink-jobmanager ls -l /opt/flink/lib/flink-sql-connector-kafka*.jar
```

### Step 3: Submit the Real Flink Streaming Job
Submit [`vision_analytics.sql`](file:///Users/suveer/HDFS/streaming/flink/jobs/vision_analytics.sql) via Flink's SQL Client inside the JobManager container:

```bash
docker-compose -f docker-compose.streaming.yml exec flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/vision_analytics.sql
```

### Step 4: Verify Job in Flink UI & CLI
* **Web UI:** Open `http://<LINUX_VM_IP>:8081` or `http://localhost:8081` in your browser.
* **CLI Job List:**
  ```bash
  docker-compose -f docker-compose.streaming.yml exec flink-jobmanager /opt/flink/bin/flink list
  ```
  You should see a RUNNING job named after the `INSERT INTO window_analytics_sink` query.

### Step 5: Generate Real YOLO Detection Events
From your Mac or Linux terminal, start the YOLO vision producer to publish real detection events to Kafka:

```bash
# Example: Run mock/live camera runner pushing to Kafka broker
python -m vision_client.runner --bootstrap-servers <KAFKA_IP>:9092 --topic vision-events --camera-id camera-01
```

### Step 6: Inspect Real Window Output
Follow the Flink TaskManager logs to view the print sink output in real time:

```bash
docker-compose -f docker-compose.streaming.yml logs -f flink-taskmanager
```

**Expected Real Output:**
```text
FLINK-WINDOW-ANALYTICS> +I[2026-09-30 20:25:00.000, 2026-09-30 20:25:10.000, person, 287]
FLINK-WINDOW-ANALYTICS> +I[2026-09-30 20:25:00.000, 2026-09-30 20:25:10.000, car, 14]
FLINK-WINDOW-ANALYTICS> +I[2026-09-30 20:25:10.000, 2026-09-30 20:25:20.000, person, 312]
FLINK-WINDOW-ANALYTICS> +I[2026-09-30 20:25:10.000, 2026-09-30 20:25:20.000, bicycle, 3]
```

---

## ⚠️ 7. Production Job vs Python Test Harness

* **Production Path:** [`streaming/flink/jobs/vision_analytics.sql`](file:///Users/suveer/HDFS/streaming/flink/jobs/vision_analytics.sql) executed directly on the **Apache Flink cluster** (JobManager/TaskManager) with distributed state management, checkpointing, and Kafka connector.
* **Client / Emulation Harness:** [`streaming/flink/jobs/object_counting_job.py`](file:///Users/suveer/HDFS/streaming/flink/jobs/object_counting_job.py) and [`window_aggregator.py`](file:///Users/suveer/HDFS/streaming/flink/jobs/window_aggregator.py) are local Python client-side consumer tools for test simulation and offline verification. They do **not** replace the cluster Flink engine.
