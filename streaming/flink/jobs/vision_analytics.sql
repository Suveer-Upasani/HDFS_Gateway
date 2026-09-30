-- Apache Flink 1.18.1 Streaming Analytics Job
-- -----------------------------------------------------------------------------
-- Real-time 10-second Tumbling Event-Time Window Aggregation on Object Detections
-- Consumes: Kafka topic 'vision-events' (JSON payload from YOLO vision_client)
-- Produces: Windowed object detection counts emitted to Flink print sink
-- -----------------------------------------------------------------------------

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

-- 1. Define Kafka Source Table with Event-Time Watermarking
CREATE TABLE IF NOT EXISTS vision_events (
    `timestamp` STRING,
    `camera_id` STRING,
    `object` STRING,
    `confidence` DOUBLE,
    `bbox` ARRAY<INT>,
    `event_time` AS TO_TIMESTAMP(REPLACE(SUBSTRING(`timestamp`, 1, 19), 'T', ' ')),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '2' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'vision-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-vision-analytics-sql-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. Define Development Print Sink Table
-- Emits structured windowed counts to TaskManager standard output logs.
CREATE TABLE IF NOT EXISTS window_analytics_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `object` STRING,
    `count` BIGINT
) WITH (
    'connector' = 'print',
    'print-identifier' = 'FLINK-WINDOW-ANALYTICS'
);

-- 3. Execute Continuous 10-Second Tumbling Window Aggregation Pipeline
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
