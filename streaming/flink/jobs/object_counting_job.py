"""Real-time Windowed Object Counting Consumer (Client & Test Harness).

NOTE:
- For production Flink cluster execution, use `streaming/flink/jobs/vision_analytics.sql`
  submitted to the Flink JobManager via SQL Client.
- This Python module provides a client-side test consumer and local emulation engine using
  kafka-python and TumblingWindowAggregator.
"""

import argparse
import json
import logging
import os
import signal
import sys
from types import FrameType
from typing import Any

from streaming.flink.jobs.window_aggregator import (
    TumblingWindowAggregator,
    WindowedObjectCount,
)

logger = logging.getLogger("flink_vision_analytics")


def print_window_record(record: WindowedObjectCount) -> None:
    """Output structured window analytics JSON record to standard output."""
    payload = record.to_json()
    print(payload, flush=True)


def run_streaming_job(
    bootstrap_servers: str = "kafka:9092",
    topic: str = "vision-events",
    group_id: str = "flink-vision-analytics-group",
    window_seconds: int = 10,
    group_by_camera: bool = False,
    auto_offset_reset: str = "latest",
    max_events: int | None = None,
) -> None:
    """Execute real-time streaming analytics consumer and window aggregation loop.

    Args:
        bootstrap_servers: Kafka broker connection string (e.g., 'kafka:9092' or '192.168.1.7:9092').
        topic: Kafka topic to consume.
        group_id: Kafka consumer group identifier.
        window_seconds: Duration of tumbling window in seconds (default: 10).
        group_by_camera: Whether to partition counts by camera_id in addition to object.
        auto_offset_reset: Position to start reading ('earliest' or 'latest').
        max_events: Optional cap on processed events (useful for testing/benchmarks).
    """
    try:
        from kafka import KafkaConsumer
    except ImportError as err:
        raise ImportError(
            "kafka-python or kafka-python-ng is required to run the streaming consumer. "
            "Install it via `pip install kafka-python-ng`."
        ) from err

    logger.info("Initializing Flink Vision Analytics Streaming Job...")
    logger.info("• Kafka Brokers: %s", bootstrap_servers)
    logger.info("• Topic: %s", topic)
    logger.info("• Consumer Group: %s", group_id)
    logger.info("• Window Duration: %d seconds (Tumbling)", window_seconds)
    logger.info("• Group By Camera: %s", group_by_camera)

    aggregator = TumblingWindowAggregator(
        window_seconds=window_seconds,
        group_by_camera=group_by_camera,
        watermark_delay_seconds=2.0,
    )

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
    except Exception as err:
        raise RuntimeError(
            f"Failed to connect Kafka consumer to '{bootstrap_servers}' on topic '{topic}': {err}"
        ) from err

    running = True

    def handle_signal(sig: int, frame: FrameType | None) -> None:
        nonlocal running
        logger.info("\nShutdown signal (%s) received. Finalizing remaining windows...", sig)
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("✓ Connected to Kafka. Listening for DetectionEvent stream...")
    event_count = 0

    try:
        while running:
            for message in consumer:
                if not running:
                    break

                event_count += 1
                raw_event = message.value

                completed_windows = aggregator.process_event(raw_event)
                for record in completed_windows:
                    print_window_record(record)

                if max_events is not None and event_count >= max_events:
                    logger.info("Reached maximum event threshold (%d). Exiting loop.", max_events)
                    running = False
                    break

    except KeyboardInterrupt:
        logger.info("\nKeyboardInterrupt received.")
    finally:
        # Flush any remaining active windows on exit
        final_windows = aggregator.flush_all()
        for record in final_windows:
            print_window_record(record)

        consumer.close()
        logger.info("✓ Kafka consumer closed. Processed %d total events.", event_count)


def build_pyflink_table_environment(
    bootstrap_servers: str = "kafka:9092",
    topic: str = "vision-events",
    group_id: str = "flink-vision-analytics-group",
    window_seconds: int = 10,
) -> Any:
    """Build and return PyFlink TableEnvironment SQL job definition (for Flink CLI submissions)."""
    try:
        from pyflink.table import EnvironmentSettings, TableEnvironment
    except ImportError as err:
        raise ImportError("PyFlink (apache-flink) is required for Table API execution.") from err

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Register Kafka Source Table with Event-Time Watermarking
    table_env.execute_sql(f"""
        CREATE TABLE vision_events (
            `timestamp` STRING,
            `camera_id` STRING,
            `object` STRING,
            `confidence` DOUBLE,
            `bbox` ARRAY<INT>,
            `event_time` AS TO_TIMESTAMP(REPLACE(SUBSTRING(`timestamp`, 1, 19), 'T', ' ')),
            WATERMARK FOR `event_time` AS `event_time` - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Register Print Sink Table
    table_env.execute_sql("""
        CREATE TABLE window_analytics_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            `object` STRING,
            `count` BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    return table_env


def main() -> None:
    """CLI entrypoint for running the Flink vision analytics streaming job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    default_topic = os.getenv("KAFKA_TOPIC", "vision-events")

    parser = argparse.ArgumentParser(
        description="Real-Time Flink Vision Analytics - 10-second Windowed Object Counter"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap,
        help=f"Kafka bootstrap servers (default: {default_bootstrap})",
    )
    parser.add_argument(
        "--topic",
        default=default_topic,
        help=f"Kafka topic name (default: {default_topic})",
    )
    parser.add_argument(
        "--group-id",
        default="flink-vision-analytics-group",
        help="Kafka consumer group ID (default: flink-vision-analytics-group)",
    )
    parser.add_argument(
        "--window-seconds",
        type=int,
        default=10,
        help="Tumbling window duration in seconds (default: 10)",
    )
    parser.add_argument(
        "--group-by-camera",
        action="store_true",
        help="Include camera_id in aggregation grouping",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start reading from earliest offset instead of latest",
    )

    args = parser.parse_args()

    try:
        run_streaming_job(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            window_seconds=args.window_seconds,
            group_by_camera=args.group_by_camera,
            auto_offset_reset="earliest" if args.from_beginning else "latest",
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Streaming job terminated with error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
