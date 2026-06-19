import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
from app.core.config import AppConfig
from app.repositories.event_repo import EventRepository
from app.models.schemas import LogEvent

logger = logging.getLogger("aggregator.broker_consumer")

redis_client_instance: Optional[aioredis.Redis] = None
consumer_tasks_pool: List[asyncio.Task[None]] = []

async def _fetch_redis_client(config: AppConfig) -> aioredis.Redis:
    global redis_client_instance
    if redis_client_instance is None:
        redis_client_instance = aioredis.from_url(
            config.redis_dsn,
            decode_responses=True,
            max_connections=config.consumer_workers_count + 5
        )
    return redis_client_instance

async def _setup_consumer_group_idempotently(
    client: aioredis.Redis,
    stream: str,
    group: str
) -> None:
    try:
        await client.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        logger.info("Created stream consumer group '%s' on stream '%s' successfully.", group, stream)
    except aioredis.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            logger.debug("Stream consumer group '%s' already active.", group)
        else:
            raise

def _decode_stream_payload(data: Dict[str, Any]) -> Optional[LogEvent]:
    try:
        if "data" in data:
            raw_event = json.loads(data["data"])
        else:
            raw_event = dict(data)
            if "payload" in raw_event and isinstance(raw_event["payload"], str):
                raw_event["payload"] = json.loads(raw_event["payload"])
        return LogEvent(**raw_event)
    except Exception as exc:
        logger.exception("Stream message decoding failed: %s", exc)
        return None

async def _run_consumer_worker(worker_id: int, config: AppConfig) -> None:
    worker_label = f"worker-{worker_id}"
    logger.info("Starting background consumer worker loop: %s", worker_label)
    
    client = await _fetch_redis_client(config)
    stream = config.stream_topic
    group = config.consumer_group_name
    batch_size = config.batch_read_limit
    block_ms = config.read_block_duration_ms

    while True:
        try:
            # 1. Look for Pending Entries List (PEL) first to recover from crash (ID "0")
            entries = await client.xreadgroup(
                groupname=group,
                consumername=worker_label,
                streams={stream: "0"},
                count=batch_size
            )
            
            # 2. If no pending items, poll new entries (ID ">")
            if not entries or not entries[0][1]:
                entries = await client.xreadgroup(
                    groupname=group,
                    consumername=worker_label,
                    streams={stream: ">"},
                    count=batch_size,
                    block=block_ms
                )
                
            if not entries:
                continue
                
            for _stream_name, messages in entries:
                for message_id, data in messages:
                    event = _decode_stream_payload(data)
                    if event is None:
                        # Dead-letter/malformed event, ACK to discard
                        await client.xack(stream, group, message_id)
                        continue
                        
                    try:
                        await EventRepository.persist_single_event(event)
                    except Exception as exc:
                        # Transaction failed, do NOT ACK to retry on next loop
                        logger.error("%s: Database transaction error processing event id=%s: %s", 
                                     worker_label, event.event_id, exc)
                        continue
                        
                    await client.xack(stream, group, message_id)
                    
        except asyncio.CancelledError:
            logger.info("Consumer worker loop %s cancelled.", worker_label)
            return
        except Exception as exc:
            logger.exception("Consumer worker %s loop encountered error, restarting in 1s: %s", worker_label, exc)
            await asyncio.sleep(1.0)

async def launch_stream_consumers(config: AppConfig) -> List[asyncio.Task[None]]:
    global consumer_tasks_pool
    client = await _fetch_redis_client(config)
    await _setup_consumer_group_idempotently(client, config.stream_topic, config.consumer_group_name)
    
    consumer_tasks_pool = [
        asyncio.create_task(_run_consumer_worker(i, config), name=f"consumer-worker-{i}")
        for i in range(config.consumer_workers_count)
    ]
    logger.info("Fired up %d concurrent consumer workers.", len(consumer_tasks_pool))
    return consumer_tasks_pool

async def shutdown_stream_consumers() -> None:
    global consumer_tasks_pool, redis_client_instance
    
    for task in consumer_tasks_pool:
        if not task.done():
            task.cancel()
            
    if consumer_tasks_pool:
        await asyncio.gather(*consumer_tasks_pool, return_exceptions=True)
    consumer_tasks_pool.clear()
    
    if redis_client_instance is not None:
        await redis_client_instance.aclose()
        redis_client_instance = None
        
    logger.info("All stream consumers stopped.")

async def extract_active_redis_client(config: AppConfig) -> aioredis.Redis:
    return await _fetch_redis_client(config)
