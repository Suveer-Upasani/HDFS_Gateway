"""Kafka producer client for publishing structured detection events."""

import json
from abc import ABC, abstractmethod
from typing import Any

from vision_client.events import DetectionEvent


class BaseEventProducer(ABC):
    """Abstract interface for event streaming producers."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the event streaming broker."""

    @abstractmethod
    def send_event(self, event: DetectionEvent) -> bool:
        """Publish a detection event to the configured stream topic."""

    @abstractmethod
    def close(self) -> None:
        """Flush remaining buffers and safely close broker connections."""


class KafkaVisionProducer(BaseEventProducer):
    """Kafka event producer for publishing DetectionEvent records."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "vision-events",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer: Any | None = None

    def connect(self) -> None:
        try:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except ImportError as err:
            raise ImportError(
                "A Kafka client library (kafka-python or kafka-python-ng) is required. "
                "Install it with `pip install kafka-python-ng`."
            ) from err

    def send_event(self, event: DetectionEvent) -> bool:
        if self._producer is None:
            raise RuntimeError("Producer is not connected. Call connect() first.")
        payload = event.model_dump()
        self._producer.send(self.topic, value=payload)
        return True

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None
