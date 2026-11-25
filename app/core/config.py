from functools import lru_cache
from typing import Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application Settings and Environment Configuration.

    Attributes are validated via Pydantic v2 BaseSettings and loaded from
    the root `.env` file if present.
    """

    # --- Application ---
    APP_NAME: str = "FlashLogix"
    APP_ENV: str = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Concurrency Strategy:
    # - "redis_async_lazy": Async Redis Gate + Idempotency/Product Cache + Lazy DB session (Default)
    # - "db_only": Pure PostgreSQL SELECT ... FOR UPDATE (for controlled baseline benchmarking)
    CONCURRENCY_MODE: Literal["redis_async_lazy", "db_only"] = "redis_async_lazy"

    # --- PostgreSQL OLTP ---
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "password123"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5433
    POSTGRES_DB: str = "flashlogix_db"
    DATABASE_URL: Optional[str] = None

    # Connection Pool Settings (12 + 8 = 20 max per worker -> 80 max across 4 workers < PG max_connections=100)
    DB_POOL_SIZE: int = 12
    DB_MAX_OVERFLOW: int = 8
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800

    # --- Redis In-Memory Cache ---
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6380
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_MAX_CONNECTIONS: int = 100
    PRODUCT_CACHE_TTL_SEC: int = 300
    IDEMPOTENCY_TTL_SEC: int = 86400

    # --- Apache Kafka ---
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9094"
    KAFKA_TOPIC_ORDERS: str = "flashsale.orders"
    KAFKA_TOPIC_ORDERS_DLQ: str = "flashsale.orders.dlq"
    KAFKA_CONSUMER_GROUP: str = "flashlogix_audit_group"

    # --- MinIO Lakehouse (S3-Compatible) ---
    MINIO_ENDPOINT: str = "http://localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadminpassword"
    MINIO_BUCKET: str = "lakehouse"

    # --- Data Governance, Fraud & Compliance ---
    PII_SALT: str = "FLASH_SECRET_SALT_2026"
    FRAUD_VELOCITY_THRESHOLD_PER_MIN: int = 5

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    def get_database_url(self) -> str:
        """Returns the fully qualified SQLAlchemy PostgreSQL connection string."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


@lru_cache()
def get_settings() -> Settings:
    """Cached singleton instance of application settings."""
    return Settings()


settings = get_settings()
