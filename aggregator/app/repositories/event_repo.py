import json
import logging
import random
import asyncio
from dataclasses import dataclass
from typing import List, Optional
import asyncpg
from app.core.database import get_database_pool
from app.models.schemas import LogEvent

logger = logging.getLogger("aggregator.event_repo")

@dataclass
class TransactionResult:
    total_received: int = 0
    total_inserted: int = 0
    total_duplicates: int = 0

class EventRepository:
    _SQL_INSERT_LOG = """
    INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
    VALUES ($1, $2, $3, $4, $5::jsonb)
    ON CONFLICT (topic, event_id) DO NOTHING
    RETURNING id;
    """

    _SQL_ADD_OUTBOX = """
    INSERT INTO outbox (topic, event_id, payload)
    VALUES ($1, $2, $3::jsonb);
    """

    _SQL_UPDATE_STATS_NEW = """
    UPDATE stats
    SET received = received + 1,
        unique_processed = unique_processed + 1
    WHERE id = 1;
    """

    _SQL_UPDATE_STATS_DUP = """
    UPDATE stats
    SET received = received + 1,
        duplicate_dropped = duplicate_dropped + 1
    WHERE id = 1;
    """

    @classmethod
    async def persist_single_event(cls, event: LogEvent) -> bool:
        """
        Attempt to insert a single log event.
        If unique, write to outbox and increment new stats.
        If duplicate, increment duplicate stats.
        Retries up to 3 times on DeadlockDetectedError.
        """
        pool = get_database_pool()
        payload_str = json.dumps(event.payload)

        for attempt in range(1, 4):
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        row = await conn.fetchrow(
                            cls._SQL_INSERT_LOG,
                            event.topic,
                            event.event_id,
                            event.timestamp,
                            event.source,
                            payload_str
                        )
                        
                        if row is not None:
                            # Unique event
                            await conn.execute(cls._SQL_ADD_OUTBOX, event.topic, event.event_id, payload_str)
                            await conn.execute(cls._SQL_UPDATE_STATS_NEW)
                            logger.debug("Event repository: stored unique event topic=%s id=%s", event.topic, event.event_id)
                            return True
                        else:
                            # Duplicate event
                            await conn.execute(cls._SQL_UPDATE_STATS_DUP)
                            logger.info("Event repository: ignored duplicate event topic=%s id=%s", event.topic, event.event_id)
                            return False
            except asyncpg.exceptions.DeadlockDetectedError as exc:
                if attempt == 3:
                    raise
                backoff_time = random.uniform(0.05, 0.2)
                logger.warning("Event repository deadlock detected (attempt %d/3), retrying in %.3fs: %s",
                               attempt, backoff_time, exc)
                await asyncio.sleep(backoff_time)
        return False

    @classmethod
    async def persist_batch_events(cls, events: List[LogEvent]) -> TransactionResult:
        """
        Atomically process a batch of events inside a single database transaction.
        Accumulates statistics and runs a single stats UPDATE statement at the end to minimize lock contention.
        Retries up to 3 times on deadlock.
        """
        pool = get_database_pool()
        result = TransactionResult(total_received=len(events))

        for attempt in range(1, 4):
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        result.total_inserted = 0
                        result.total_duplicates = 0
                        for event in events:
                            payload_str = json.dumps(event.payload)
                            row = await conn.fetchrow(
                                cls._SQL_INSERT_LOG,
                                event.topic,
                                event.event_id,
                                event.timestamp,
                                event.source,
                                payload_str
                            )
                            
                            if row is not None:
                                result.total_inserted += 1
                                await conn.execute(cls._SQL_ADD_OUTBOX, event.topic, event.event_id, payload_str)
                                logger.debug("Event repository batch: stored event topic=%s id=%s", event.topic, event.event_id)
                            else:
                                result.total_duplicates += 1
                                logger.info("Event repository batch: duplicate dropped topic=%s id=%s", event.topic, event.event_id)
                        
                        # Apply stats batch increment
                        await conn.execute(
                            """
                            UPDATE stats
                            SET received = received + $1,
                                unique_processed = unique_processed + $2,
                                duplicate_dropped = duplicate_dropped + $3
                            WHERE id = 1;
                            """,
                            result.total_received,
                            result.total_inserted,
                            result.total_duplicates
                        )
                        break  # Success
            except asyncpg.exceptions.DeadlockDetectedError as exc:
                if attempt == 3:
                    raise
                backoff_time = random.uniform(0.05, 0.2)
                logger.warning("Event repository batch deadlock detected (attempt %d/3), retrying in %.3fs: %s",
                               attempt, backoff_time, exc)
                await asyncio.sleep(backoff_time)
                
        logger.info("Event repository batch finished: total=%d, inserted=%d, duplicates=%d",
                    result.total_received, result.total_inserted, result.total_duplicates)
        return result
