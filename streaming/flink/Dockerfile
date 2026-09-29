# Apache Flink 1.18.1 with Apache Kafka SQL Connector
FROM flink:1.18.1-scala_2.12-java11

# Version of the official Apache Flink Kafka SQL connector
ARG KAFKA_SQL_CONNECTOR_VERSION=3.0.2-1.18
ARG MAVEN_REPO_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_SQL_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_SQL_CONNECTOR_VERSION}.jar

USER root

# Install curl, download the official Kafka SQL connector JAR to /opt/flink/lib/, and set ownership
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_SQL_CONNECTOR_VERSION}.jar ${MAVEN_REPO_URL} && \
    chown -R flink:flink /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_SQL_CONNECTOR_VERSION}.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER flink
