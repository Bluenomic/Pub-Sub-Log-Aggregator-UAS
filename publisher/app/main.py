import asyncio
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
import httpx
import redis.asyncio as aioredis
from app import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("publisher")

def _get_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()

def _generate_single_event(topic: str, source: str = "publisher-simulator") -> Dict[str, Any]:
    return {
        "topic": topic,
        "event_id": str(uuid.uuid4()),
        "timestamp": _get_iso_timestamp(),
        "source": source,
        "payload": {
            "msg": f"Auto-generated log event for {topic}",
            "metric": round(random.uniform(10.0, 100.0), 3),
            "status": random.choice(["SUCCESS", "WARNING", "ERROR"])
        }
    }

def _build_simulation_events(count: int, dup_rate: float, topics: List[str]) -> List[Dict[str, Any]]:
    unique_count = int(count * (1 - dup_rate))
    logger.info("Generating %d events (%d unique, %d duplicates)", count, unique_count, count - unique_count)
    
    unique_events = []
    events_pool = []
    
    # Generate unique events
    for _ in range(unique_count):
        topic = random.choice(topics)
        event = _generate_single_event(topic)
        unique_events.append(event)
        events_pool.append(event)
        
    # Generate duplicate events from the unique pool
    duplicates_to_create = count - unique_count
    for _ in range(duplicates_to_create):
        original = random.choice(unique_events)
        # Duplicate retains topic and event_id but updates timestamp/source to simulate client retry
        duplicate = {
            **original,
            "timestamp": _get_iso_timestamp(),
            "source": "publisher-simulator-retry"
        }
        events_pool.append(duplicate)
        
    random.shuffle(events_pool)
    return events_pool

async def _wait_for_aggregator(client: httpx.AsyncClient) -> None:
    logger.info("Awaiting aggregator readiness at %s ...", config.HEALTH_URL)
    deadline = time.monotonic() + config.HEALTH_TIMEOUT
    backoff = 1.0
    
    while time.monotonic() < deadline:
        try:
            resp = await client.get(config.HEALTH_URL, timeout=3.0)
            if resp.status_code == 200:
                logger.info("Aggregator is healthy and ready!")
                return
            logger.warning("Aggregator health check returned status code %d. Retrying...", resp.status_code)
        except (httpx.ConnectError, httpx.TimeoutException, OSError) as exc:
            logger.warning("Failed to connect to aggregator (%s). Retrying in %.1fs...", exc, backoff)
            
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, config.MAX_BACKOFF)
        
    logger.error("Timeout: Aggregator failed to report healthy within %d seconds.", config.HEALTH_TIMEOUT)
    sys.exit(1)

async def _post_batch_http(
    client: httpx.AsyncClient,
    batch: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
    stats: Dict[str, int]
) -> None:
    payload = {"events": batch}
    backoff = config.INITIAL_BACKOFF
    
    for attempt in range(1, config.MAX_RETRIES + 1):
        async with semaphore:
            try:
                resp = await client.post(config.TARGET_URL, json=payload, timeout=20.0)
                if resp.status_code in (200, 201, 207):
                    stats["http_success"] += len(batch)
                    return
                logger.warning("Aggregator HTTP POST failed (Status %d) on attempt %d for batch", resp.status_code, attempt)
            except httpx.RequestError as exc:
                logger.warning("HTTP POST error on attempt %d: %s. Retrying in %.1fs...", attempt, exc, backoff)
                
        # Retry backoff with minor jitter
        await asyncio.sleep(backoff + random.uniform(0.1, 0.5))
        backoff = min(backoff * 2, config.MAX_BACKOFF)
        
    stats["http_failed"] += len(batch)
    logger.error("Gave up publishing batch of %d events after %d attempts.", len(batch), config.MAX_RETRIES)

async def _publish_to_redis(rds: aioredis.Redis, events: List[Dict[str, Any]]) -> int:
    published = 0
    logger.info("Starting Redis Stream publication to stream '%s'...", config.STREAM_NAME)
    for event in events:
        try:
            await rds.xadd(config.STREAM_NAME, {"data": json.dumps(event)})
            published += 1
        except Exception as exc:
            logger.error("Redis XADD failed for event %s: %s", event.get("event_id"), exc)
    return published

async def run() -> None:
    logger.info("=" * 60)
    logger.info("Starting Publisher Simulator (UAS Sistem Terdistribusi)")
    logger.info("=" * 60)
    logger.info("Configuration: Total Events=%d, Duplicate Rate=%.1f%%, Batch Size=%d", 
                config.EVENT_COUNT, config.DUPLICATE_RATE * 100, config.BATCH_SIZE)
    
    # ── Generate event pool ──
    events = _build_simulation_events(config.EVENT_COUNT, config.DUPLICATE_RATE, config.TOPICS)
    total_events = len(events)
    unique_ids = {(e["topic"], e["event_id"]) for e in events}
    unique_count = len(unique_ids)
    duplicate_count = total_events - unique_count
    
    # ── Initialize client connections ──
    rds = aioredis.from_url(config.BROKER_URL, decode_responses=True)
    semaphore = asyncio.Semaphore(config.HTTP_CONCURRENCY)
    stats = {"http_success": 0, "http_failed": 0, "redis_published": 0}
    
    async with httpx.AsyncClient() as client:
        # Await aggregator readiness
        await _wait_for_aggregator(client)
        
        start_time = time.perf_counter()
        
        # ── 1. Publish to Redis Stream ──
        if config.PUBLISH_MODE in ("both", "redis"):
            stats["redis_published"] = await _publish_to_redis(rds, events)
        else:
            logger.info("Skipping Redis Stream publication (PUBLISH_MODE=%s)", config.PUBLISH_MODE)
        
        # ── 2. Publish to HTTP POST ──
        if config.PUBLISH_MODE in ("both", "http"):
            batches = [
                events[i : i + config.BATCH_SIZE]
                for i in range(0, total_events, config.BATCH_SIZE)
            ]
            logger.info("Sending %d batches via concurrent HTTP POST to %s...", len(batches), config.TARGET_URL)
            
            tasks = [
                _post_batch_http(client, batch, semaphore, stats)
                for batch in batches
            ]
            await asyncio.gather(*tasks)
        else:
            logger.info("Skipping HTTP POST publication (PUBLISH_MODE=%s)", config.PUBLISH_MODE)
        
        duration = time.perf_counter() - start_time
        
    await rds.aclose()
    
    # ── Print Performance Summary ──
    throughput = total_events / duration if duration > 0 else 0
    logger.info("=" * 60)
    logger.info("                    PUBLISHER RUN SUMMARY")
    logger.info("=" * 60)
    logger.info("  Total Generated Events   : %d", total_events)
    logger.info("  Unique Event IDs         : %d", unique_count)
    logger.info("  Duplicate Events         : %d (%.2f%%)", duplicate_count, (duplicate_count / total_events) * 100)
    logger.info("  Successful HTTP Logs     : %d", stats["http_success"])
    logger.info("  Failed HTTP Logs         : %d", stats["http_failed"])
    logger.info("  Published to Redis Stream: %d", stats["redis_published"])
    logger.info("  Execution Duration       : %.2f seconds", duration)
    logger.info("  Average Throughput       : %.1f events/second", throughput)
    logger.info("=" * 60)

def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Publisher interrupted by user.")
    except Exception as exc:
        logger.exception("Publisher crashed with unhandled exception: %s", exc)
        sys.exit(1)

if __name__ == "__main__":
    main()
