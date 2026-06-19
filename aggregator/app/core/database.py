import logging
from typing import Optional
import asyncpg
from app.core.config import AppConfig

logger = logging.getLogger("aggregator.database")

db_connection_pool: Optional[asyncpg.Pool] = None

# SQL schema migrations
_SCHEMAS_DDL = """
-- Table to store processed unique log events
CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    source VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_topic
    ON processed_events (topic, processed_at DESC);

-- Table for outbox event tasks
CREATE TABLE IF NOT EXISTS outbox (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
    ON outbox (processed, created_at)
    WHERE processed = FALSE;

-- Table for atomic system stats
CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    received BIGINT NOT NULL DEFAULT 0,
    unique_processed BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped BIGINT NOT NULL DEFAULT 0,
    CHECK (id = 1)
);

INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""

async def initialize_database(config: AppConfig) -> asyncpg.Pool:
    global db_connection_pool
    logger.info("Initializing asyncpg database pool using target: %s", config.postgres_dsn)
    
    db_connection_pool = await asyncpg.create_pool(
        dsn=config.postgres_dsn,
        min_size=config.postgres_pool_min,
        max_size=config.postgres_pool_max,
        command_timeout=30.0,
        server_settings={"default_transaction_isolation": "read committed"},
    )
    
    async with db_connection_pool.acquire() as conn:
        await conn.execute(_SCHEMAS_DDL)
        
    logger.info("Database schema checked/applied and pool initialized successfully.")
    return db_connection_pool

async def terminate_database_pool() -> None:
    global db_connection_pool
    if db_connection_pool is not None:
        await db_connection_pool.close()
        logger.info("Database connection pool terminated.")
        db_connection_pool = None

def get_database_pool() -> asyncpg.Pool:
    if db_connection_pool is None:
        raise RuntimeError("Database pool has not been initialized. Call initialize_database() first.")
    return db_connection_pool
