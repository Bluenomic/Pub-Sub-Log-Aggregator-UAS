import os
from dataclasses import dataclass, field

@dataclass(frozen=True, slots=True)
class AppConfig:
    # ── PostgreSQL Engine Settings ──
    postgres_dsn: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL", "postgresql://user:pass@storage:5432/logdb"
        )
    )
    postgres_pool_min: int = field(
        default_factory=lambda: int(os.getenv("DB_MIN_POOL", "5"))
    )
    postgres_pool_max: int = field(
        default_factory=lambda: int(os.getenv("DB_MAX_POOL", "20"))
    )

    # ── Redis Stream Broker Settings ──
    redis_dsn: str = field(
        default_factory=lambda: os.getenv("BROKER_URL", "redis://broker:6379")
    )
    stream_topic: str = field(
        default_factory=lambda: os.getenv("REDIS_STREAM", "events")
    )
    consumer_group_name: str = field(
        default_factory=lambda: os.getenv("REDIS_CONSUMER_GROUP", "aggregator-group")
    )

    # ── Worker Pool Settings ──
    consumer_workers_count: int = field(
        default_factory=lambda: int(os.getenv("WORKER_COUNT", "4"))
    )
    batch_read_limit: int = field(
        default_factory=lambda: int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    )
    read_block_duration_ms: int = field(
        default_factory=lambda: int(os.getenv("CONSUMER_BLOCK_MS", "2000"))
    )

    # ── Outbox Poller Settings ──
    outbox_check_interval: float = field(
        default_factory=lambda: float(os.getenv("OUTBOX_POLL_INTERVAL", "1.5"))
    )
    outbox_fetch_limit: int = field(
        default_factory=lambda: int(os.getenv("OUTBOX_BATCH_SIZE", "200"))
    )

    # ── FastAPI Host/Port ──
    server_host: str = field(
        default_factory=lambda: os.getenv("APP_HOST", "0.0.0.0")
    )
    server_port: int = field(
        default_factory=lambda: int(os.getenv("APP_PORT", "8080"))
    )

    # ── Logger Level ──
    app_log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )

def load_app_settings() -> AppConfig:
    return AppConfig()
