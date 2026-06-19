import os

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379")
STREAM_NAME = os.getenv("REDIS_STREAM", "events")
PUBLISH_MODE = os.getenv("PUBLISH_MODE", "both").lower()


EVENT_COUNT = int(os.getenv("EVENT_COUNT", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

HTTP_CONCURRENCY = int(os.getenv("HTTP_CONCURRENCY", "10"))
HEALTH_URL = os.getenv("HEALTH_URL", "http://aggregator:8080/health")
HEALTH_TIMEOUT = int(os.getenv("HEALTH_TIMEOUT", "120"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
INITIAL_BACKOFF = float(os.getenv("INITIAL_BACKOFF", "1.0"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "10.0"))

TOPICS = ["auth.login", "payment.checkout", "user.signup", "system.metric"]
