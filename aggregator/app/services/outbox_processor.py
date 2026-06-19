import asyncio
import logging
from typing import List, Optional
import asyncpg
from app.core.config import AppConfig
from app.core.database import get_database_pool

logger = logging.getLogger("aggregator.outbox_processor")

_SQL_SELECT_UNPROCESSED_OUTBOX = """
SELECT id, topic, event_id, payload, created_at
FROM outbox
WHERE processed = FALSE
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;
"""

_SQL_MARK_OUTBOX_DONE = """
UPDATE outbox
SET processed = TRUE
WHERE id = ANY($1::int[]);
"""

async def process_outbox_queue_batch(limit: int = 200) -> int:
    """
    Fetch unprocessed outbox tasks and mark them done inside a transaction.
    Uses FOR UPDATE SKIP LOCKED to prevent lock conflicts between workers.
    """
    pool = get_database_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            records: List[asyncpg.Record] = await conn.fetch(_SQL_SELECT_UNPROCESSED_OUTBOX, limit)
            if not records:
                return 0
                
            record_ids = [r["id"] for r in records]
            
            # Simulated side effects
            for r in records:
                logger.debug("Outbox processor: pushing event topic=%s id=%s", r["topic"], r["event_id"])
                
            await conn.execute(_SQL_MARK_OUTBOX_DONE, record_ids)
            
    logger.info("Outbox processor: processed %d events successfully.", len(records))
    return len(records)

_outbox_worker_task: Optional[asyncio.Task[None]] = None

async def _outbox_loop_worker(config: AppConfig) -> None:
    logger.info("Outbox background poller started. Interval=%.1fs, Batch Size=%d", 
                config.outbox_check_interval, config.outbox_fetch_limit)
    while True:
        try:
            processed = await process_outbox_queue_batch(config.outbox_fetch_limit)
            if processed < config.outbox_fetch_limit:
                await asyncio.sleep(config.outbox_check_interval)
        except asyncio.CancelledError:
            logger.info("Outbox background poller cancelled.")
            return
        except Exception as exc:
            logger.exception("Error inside outbox poller loop, restarting...: %s", exc)
            await asyncio.sleep(config.outbox_check_interval)

def start_outbox_processor(config: AppConfig) -> asyncio.Task[None]:
    global _outbox_worker_task
    _outbox_worker_task = asyncio.create_task(_outbox_loop_worker(config), name="outbox-processor-task")
    return _outbox_worker_task

async def stop_outbox_processor() -> None:
    global _outbox_worker_task
    if _outbox_worker_task is not None and not _outbox_worker_task.done():
        _outbox_worker_task.cancel()
        try:
            await _outbox_worker_task
        except asyncio.CancelledError:
            pass
        _outbox_worker_task = None
        logger.info("Outbox background poller stopped.")
