from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class VisionClientSettings(BaseSettings):
    """Configuration settings for edge vision capture and Kafka event streaming."""

    CAMERA_ID: str = "camera-01"
    CAMERA_SOURCE: str = "0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "vision-events"
    YOLO_MODEL: str = "yolo11n.pt"
    YOLO_CONFIDENCE: float = 0.5

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_vision_settings() -> VisionClientSettings:
    """Return a cached instance of VisionClientSettings."""
    return VisionClientSettings()
