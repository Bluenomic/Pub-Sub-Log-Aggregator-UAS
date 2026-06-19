import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.config import load_app_settings
from app.core.database import initialize_database, terminate_database_pool
from app.services.broker_consumer import launch_stream_consumers, shutdown_stream_consumers
from app.services.outbox_processor import start_outbox_processor, stop_outbox_processor
from app.api.routes import router

# Load app configurations
_config = load_app_settings()

# Basic logging configurations
logging.basicConfig(
    level=logging.INFO if _config.app_log_level == "INFO" else logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
logger = logging.getLogger("aggregator.main")

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    logger.info("=== Initializing Pub-Sub Log Aggregator Subsystems ===")
    
    # 1. Connect to PostgreSQL & run schema DDL
    await initialize_database(_config)
    
    # 2. Fire up background stream consumer workers
    await launch_stream_consumers(_config)
    
    # 3. Start background outbox polling loop
    start_outbox_processor(_config)
    
    logger.info("=== Subsystems initialized and ready ===")
    
    yield  # Runs application
    
    logger.info("=== Shutting down Pub-Sub Log Aggregator Subsystems ===")
    await shutdown_stream_consumers()
    await stop_outbox_processor()
    await terminate_database_pool()
    logger.info("=== Subsystems shut down completely ===")

app = FastAPI(
    title="Pub-Sub Log Aggregator",
    version="1.1.0",
    description="High-throughput log aggregation system with robust deduplication, outbox pattern, and transactional resilience.",
    lifespan=app_lifespan,
)

# Connect router
app.include_router(router)
