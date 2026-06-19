import asyncio
import uuid
import httpx
import pytest
from conftest import make_event

pytestmark = pytest.mark.integration

async def _wait_for_processing(
    client: httpx.AsyncClient,
    topic: str,
    *,
    expected_unique: int,
    timeout: float = 10.0,
    interval: float = 0.3,
) -> list[dict]:
    deadline = asyncio.get_event_loop().time() + timeout
    events: list[dict] = []
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get("/events", params={"topic": topic})
        if resp.status_code == 200:
            events = resp.json() if isinstance(resp.json(), list) else resp.json().get("events", [])
            if len(events) >= expected_unique:
                return events
        await asyncio.sleep(interval)
    return events

async def test_single_event_processed_once(client: httpx.AsyncClient) -> None:
    """Sending the exact same event twice must result in only one stored record."""
    topic = f"dedup-single-{uuid.uuid4().hex[:8]}"
    event = make_event(topic=topic)

    r1 = await client.post("/publish", json=event)
    assert r1.status_code in (200, 201)

    r2 = await client.post("/publish", json=event)
    assert r2.status_code in (200, 201)

    events = await _wait_for_processing(client, topic, expected_unique=1)
    matching = [e for e in events if e.get("event_id") == event["event_id"]]
    assert len(matching) == 1, f"Expected exactly 1 stored event, found {len(matching)}"

async def test_batch_with_duplicates(client: httpx.AsyncClient) -> None:
    """A batch containing duplicate event IDs must deduplicate atomically."""
    topic = f"dedup-batch-{uuid.uuid4().hex[:8]}"
    shared_id = str(uuid.uuid4())

    events = [
        make_event(topic=topic, event_id=shared_id),
        make_event(topic=topic, event_id=shared_id),
        make_event(topic=topic),
        make_event(topic=topic),
    ]

    resp = await client.post("/publish", json={"events": events})
    assert resp.status_code in (200, 201)

    stored = await _wait_for_processing(client, topic, expected_unique=3)
    assert len(stored) == 3, f"Expected 3 unique events after batch dedup, got {len(stored)}"

async def test_cross_topic_same_event_id(client: httpx.AsyncClient) -> None:
    """Using same event ID on different topics must be treated as independent events."""
    shared_id = str(uuid.uuid4())
    topic_a = f"dedup-cross-a-{uuid.uuid4().hex[:8]}"
    topic_b = f"dedup-cross-b-{uuid.uuid4().hex[:8]}"

    event_a = make_event(topic=topic_a, event_id=shared_id)
    event_b = make_event(topic=topic_b, event_id=shared_id)

    resp_a = await client.post("/publish", json=event_a)
    resp_b = await client.post("/publish", json=event_b)
    assert resp_a.status_code in (200, 201)
    assert resp_b.status_code in (200, 201)

    events_a = await _wait_for_processing(client, topic_a, expected_unique=1)
    events_b = await _wait_for_processing(client, topic_b, expected_unique=1)

    assert len(events_a) >= 1
    assert len(events_b) >= 1
