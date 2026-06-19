import asyncio
import uuid
import httpx
import pytest
from conftest import BASE_URL, make_event, make_events

pytestmark = pytest.mark.integration

CONCURRENCY = 20

async def _publish_event(event: dict) -> httpx.Response:
    # Use a per-call client to simulate distinct independent callers racing
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as c:
        return await c.post("/publish", json=event)

async def _poll_events(
    client: httpx.AsyncClient,
    topic: str,
    *,
    expected: int,
    timeout: float = 15.0,
    interval: float = 0.4,
) -> list[dict]:
    deadline = asyncio.get_event_loop().time() + timeout
    events: list[dict] = []
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get("/events", params={"topic": topic})
        if resp.status_code == 200:
            body = resp.json()
            events = body if isinstance(body, list) else body.get("events", [])
            if len(events) >= expected:
                return events
        await asyncio.sleep(interval)
    return events

async def test_concurrent_duplicate_publish(client: httpx.AsyncClient) -> None:
    """Publish the exact same event concurrently from N tasks. Exactly one copy must be stored."""
    topic = f"conc-dup-{uuid.uuid4().hex[:8]}"
    event = make_event(topic=topic)

    results = await asyncio.gather(
        *[_publish_event(event) for _ in range(CONCURRENCY)],
        return_exceptions=True,
    )

    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish threw exception: {r}")
        assert r.status_code in (200, 201)

    events = await _poll_events(client, topic, expected=1)
    matching = [e for e in events if e.get("event_id") == event["event_id"]]
    assert len(matching) == 1, f"Expected exactly 1 stored event, got {len(matching)}"

async def test_concurrent_different_events(client: httpx.AsyncClient) -> None:
    """Publish N distinct events concurrently. All must be stored."""
    topic = f"conc-diff-{uuid.uuid4().hex[:8]}"
    events = make_events(CONCURRENCY, topic=topic)

    results = await asyncio.gather(
        *[_publish_event(e) for e in events],
        return_exceptions=True,
    )

    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish threw exception: {r}")
        assert r.status_code in (200, 201)

    stored = await _poll_events(client, topic, expected=CONCURRENCY)
    assert len(stored) == CONCURRENCY, f"Expected {CONCURRENCY} events stored, got {len(stored)}"

async def test_concurrent_stats_consistency(client: httpx.AsyncClient) -> None:
    """Stats received == unique_processed + duplicate_dropped remains invariant under concurrency."""
    topic = f"conc-stats-{uuid.uuid4().hex[:8]}"

    # baseline stats
    baseline_resp = await client.get("/stats")
    assert baseline_resp.status_code == 200
    baseline = baseline_resp.json()
    base_received = baseline.get("received", 0)
    base_unique = baseline.get("unique_processed", 0)
    base_dups = baseline.get("duplicate_dropped", 0)

    # 10 unique events, each sent twice -> total 20 concurrent publishes
    unique_events = make_events(10, topic=topic)
    all_publishes = unique_events + unique_events

    results = await asyncio.gather(
        *[_publish_event(e) for e in all_publishes],
        return_exceptions=True,
    )
    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish exception: {r}")

    await asyncio.sleep(2)

    stats_resp = await client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    received = stats.get("received", 0) - base_received
    unique = stats.get("unique_processed", 0) - base_unique
    dups = stats.get("duplicate_dropped", 0) - base_dups

    # Invariant checks
    assert received == unique + dups, f"Invariant violation: received={received} != unique({unique}) + dups({dups})"
    assert unique == 10, f"Expected exactly 10 unique, got {unique}"
    assert dups == 10, f"Expected exactly 10 duplicates, got {dups}"
