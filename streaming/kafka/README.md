# ⚡ Kafka Streaming Infrastructure (v2)

This directory contains configuration, topics documentation, and operational guidelines for the **Apache Kafka** event transport layer in the real-time vision data streaming pipeline.

---

## 🎯 Role in Architecture

Kafka serves as the high-throughput, decoupled event streaming backbone between edge vision clients and the Apache Flink stream processing cluster.

```
┌───────────────────────────┐
│ Edge Vision Client        │
│ (Windows / macOS / Linux) │
└─────────────┬─────────────┘
              │ JSON Detection Events (via TCP :9092)
              ▼
┌───────────────────────────┐
│ Apache Kafka (KRaft Mode) │
│ Topic: vision-events      │
│ Persistent: kafka_data    │
└─────────────┬─────────────┘
              │ Subscribed Streams
              ▼
┌───────────────────────────┐
│ Apache Flink Stream Engine│
└───────────────────────────┘
```

---

## 📐 Architecture Principles

1. **No Raw Video Over Kafka:**
   * Raw video frames (1080p/720p @ 30 FPS) produce gigabytes of data per minute, overwhelming network bandwidth, memory, and broker storage.
   * Object detection inference is executed directly at the edge (camera device). Only structured JSON detection metadata (`DetectionEvent`) is published to Kafka.

2. **KRaft Mode (No ZooKeeper):**
   * Kafka is configured in modern **KRaft (Kafka Raft)** consensus mode, eliminating the operational complexity and memory footprint of a separate ZooKeeper cluster.

3. **Persistent Docker Volume Storage:**
   * Kafka broker logs and metadata are stored in a dedicated named Docker volume `kafka_data` mapped to `/tmp/kraft-combined-logs`.
   * Data and committed offset state survive container restarts without data loss.

4. **External LAN Connectivity (Configurable Advertised Listener):**
   * The Kafka broker runs inside Docker on the Linux VM host.
   * To allow external edge devices (such as a laptop webcam running on macOS or Windows on the LAN) to publish events, Kafka exposes a configurable advertised listener:
     ```bash
     KAFKA_ADVERTISED_HOST=192.168.1.7  # Or target Linux host IP
     ```

---

## 📋 Topic Specifications

| Topic Name | Partitions | Replication Factor | Retention Policy | Message Payload |
| :--- | :--- | :--- | :--- | :--- |
| `vision-events` | 3 | 1 | 24 Hours (`86400000 ms`) | JSON `DetectionEvent` objects |

### Message Schema Contract
```json
{
  "timestamp": "2026-09-28T22:05:31.521Z",
  "camera_id": "camera-01",
  "object": "person",
  "confidence": 0.94,
  "bbox": [312, 145, 521, 612]
}
```

---

## 🚀 Deployment & Management

Kafka is started via `docker-compose.streaming.yml`:

```bash
# Start Kafka & Streaming infrastructure
docker compose -f docker-compose.streaming.yml up -d kafka init-kafka

# Inspect topic creation
docker compose -f docker-compose.streaming.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume live topic messages for testing
docker compose -f docker-compose.streaming.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vision-events --from-beginning
```
