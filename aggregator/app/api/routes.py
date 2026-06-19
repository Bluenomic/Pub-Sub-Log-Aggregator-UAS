import json
import logging
import time
from typing import Optional, Union, List
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
import redis.asyncio as aioredis

from app.core.config import load_app_settings
from app.core.database import get_database_pool
from app.models.schemas import (
    LogEvent,
    BulkPublishPayload,
    PublishResult,
    LogEventDetail,
    LogEventList,
    StatsSummary,
    AppHealthStatus
)
from app.repositories.event_repo import EventRepository, TransactionResult
from app.services.broker_consumer import extract_active_redis_client

logger = logging.getLogger("aggregator.api")
router = APIRouter()
_app_config = load_app_settings()
_router_startup_time = time.monotonic()

@router.post("/publish", response_model=PublishResult)
async def publish_logs(body: Union[LogEvent, BulkPublishPayload]) -> PublishResult:
    """
    Ingest a single event or a batch of events.
    Deduplication is performed atomically in PostgreSQL.
    """
    events_list: List[LogEvent]
    if isinstance(body, BulkPublishPayload):
        events_list = body.events
    else:
        events_list = [body]

    if len(events_list) == 1:
        is_new = await EventRepository.persist_single_event(events_list[0])
        result = TransactionResult(
            total_received=1,
            total_inserted=1 if is_new else 0,
            total_duplicates=0 if is_new else 1
        )
    else:
        result = await EventRepository.persist_batch_events(events_list)

    return PublishResult(
        status="accepted",
        received=result.total_received,
        duplicates=result.total_duplicates,
        processed=result.total_inserted
    )

@router.get("/events", response_model=LogEventList)
async def fetch_unique_events(
    topic: Optional[str] = Query(None, description="Topic name filter."),
    limit: int = Query(100, ge=1, le=5000, description="Max return limit."),
    offset: int = Query(0, ge=0, description="Offset index.")
) -> LogEventList:
    """
    Query processed unique events.
    """
    pool = get_database_pool()
    
    if topic:
        rows = await pool.fetch(
            """
            SELECT id, topic, event_id, timestamp, source, payload, processed_at
            FROM processed_events
            WHERE topic = $1
            ORDER BY processed_at DESC
            LIMIT $2 OFFSET $3;
            """,
            topic, limit, offset
        )
    else:
        rows = await pool.fetch(
            """
            SELECT id, topic, event_id, timestamp, source, payload, processed_at
            FROM processed_events
            ORDER BY processed_at DESC
            LIMIT $1 OFFSET $2;
            """,
            limit, offset
        )

    events_payload = [
        LogEventDetail(
            id=r["id"],
            topic=r["topic"],
            event_id=r["event_id"],
            timestamp=r["timestamp"],
            source=r["source"],
            payload=json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"],
            processed_at=r["processed_at"]
        )
        for r in rows
    ]
    return LogEventList(topic=topic, count=len(events_payload), events=events_payload)

@router.get("/stats", response_model=StatsSummary)
async def fetch_system_statistics() -> StatsSummary:
    """
    Retrieve live aggregation stats.
    """
    pool = get_database_pool()
    stats_row = await pool.fetchrow("SELECT * FROM stats WHERE id = 1;")
    distinct_topics = await pool.fetch("SELECT DISTINCT topic FROM processed_events ORDER BY topic;")
    uptime = time.monotonic() - _router_startup_time
    
    return StatsSummary(
        received=stats_row["received"] if stats_row else 0,
        unique_processed=stats_row["unique_processed"] if stats_row else 0,
        duplicate_dropped=stats_row["duplicate_dropped"] if stats_row else 0,
        topics=[r["topic"] for r in distinct_topics],
        uptime_seconds=round(uptime, 2)
    )

@router.get("/health", response_model=AppHealthStatus)
async def fetch_health_status() -> AppHealthStatus:
    """
    Liveness and readiness health checks.
    """
    pg_status = "ok"
    redis_status = "ok"
    
    try:
        pool = get_database_pool()
        await pool.fetchval("SELECT 1;")
    except Exception as exc:
        pg_status = f"error: {exc}"
        
    try:
        client = await extract_active_redis_client(_app_config)
        await client.ping()
    except Exception as exc:
        redis_status = f"error: {exc}"
        
    overall = "healthy" if (pg_status == "ok" and redis_status == "ok") else "unhealthy"
    status_code = 200 if overall == "healthy" else 503
    
    resp = AppHealthStatus(status=overall, postgres=pg_status, redis=redis_status)
    if status_code != 200:
        return JSONResponse(content=resp.model_dump(), status_code=status_code)
    return resp
