import os
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator
import asyncpg
import httpx
import pytest

BASE_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")
PG_DSN = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/logdb")

def make_event(
    *,
    topic: str = "test-topic",
    event_id: str | None = None,
    source: str = "test-suite",
    payload: dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": payload or {"info": "test-event"},
    }

def make_events(n: int, *, topic: str = "test-topic", source: str = "test-suite") -> list[dict[str, Any]]:
    return [make_event(topic=topic, source=source) for _ in range(n)]

@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=httpx.Timeout(30.0)) as c:
        yield c

@pytest.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
    assert pool is not None
    yield pool
    await pool.close()

@pytest.fixture
def unique_topic() -> str:
    return f"test-topic-{uuid.uuid4().hex[:12]}"

@pytest.fixture
def sample_event(unique_topic: str) -> dict[str, Any]:
    return make_event(topic=unique_topic)

@pytest.fixture
def sample_events(unique_topic: str) -> list[dict[str, Any]]:
    return make_events(5, topic=unique_topic)

def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: integration tests requiring running services")
    config.addinivalue_line("markers", "stress: stress / performance tests")
