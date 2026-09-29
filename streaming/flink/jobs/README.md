# 🛠️ Flink Streaming Jobs (v2 Scaffolding)

This directory contains the specifications and upcoming job definitions for Apache Flink stream processing tasks.

---

## 📋 Planned Processing Jobs

### 1. Ingestion & Validation Job (`IngestionJob`)
* **Source:** Consumes JSON payloads from Kafka topic `vision-events`.
* **Validation:** Validates timestamp formats, bounding box boundaries, and minimum confidence thresholds.
* **Sink (HDFS Raw):** Persists all raw validated events to `/user/suveer/vision/raw/` in hourly partition buckets (`YYYY/MM/DD/HH`).

### 2. Detection Analytics & Aggregation Job (`AnalyticsJob`)
* **Keying:** Keyed by `camera_id` and detected `object` class.
* **Windowing:**
  * Tumbling 1-minute window: Total object occurrences per camera.
  * Sliding 5-minute window: Traffic density & movement trends.
* **Sinks:**
  * **HDFS Analytics:** Writes structured Parquet/JSON aggregated metrics to `/user/suveer/vision/analytics/`.
  * **Real-time Downstream:** Pushes immediate aggregated metrics for FastAPI endpoint consumption.

---

## 🗄️ Target HDFS Directory Structure

All historical streaming data will be partitioned under the dedicated application user namespace:

```text
/user/suveer/vision/
├── raw/                      # Unmodified, validated detection events
│   └── YYYY/MM/DD/HH/
├── detections/               # Filtered, high-confidence detection records
│   └── YYYY/MM/DD/
└── analytics/                # Windowed rollups, aggregations, and traffic metrics
    └── YYYY/MM/DD/
```

> **Note:** The existing Hadoop 3.4.2 installation on the host machine remains external infrastructure and is not modified.
