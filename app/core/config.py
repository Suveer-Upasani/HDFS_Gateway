from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "HDFS Gateway API"
    APP_ENV: str = "production"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 5005

    # Hadoop WebHDFS Configuration
    HDFS_NAMENODE_URL: str = "http://localhost:9870"
    HDFS_USER: str = "suveer"
    HDFS_DEFAULT_DIR: str = "/"
    HDFS_TIMEOUT_SECONDS: float = 30.0

    # Kafka & Flink Streaming Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_VISION_TOPIC: str = "vision-events"
    FLINK_JOBMANAGER_URL: str = "http://localhost:8081"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
