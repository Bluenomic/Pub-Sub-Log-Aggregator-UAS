from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

class LogEvent(BaseModel):
    topic: str = Field(..., min_length=1, max_length=256)
    event_id: str = Field(..., min_length=1, max_length=256)
    timestamp: datetime
    source: str = Field(..., min_length=1, max_length=256)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("topic", "event_id", "source")
    @classmethod
    def strip_whitespace(cls, val: str) -> str:
        return val.strip()

class BulkPublishPayload(BaseModel):
    events: List[LogEvent] = Field(..., min_length=1, max_length=5000)

class PublishResult(BaseModel):
    status: str
    received: int
    duplicates: int
    processed: int

class LogEventDetail(BaseModel):
    id: int
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]
    processed_at: datetime

class LogEventList(BaseModel):
    topic: Optional[str] = None
    count: int
    events: List[LogEventDetail]

class StatsSummary(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float

class AppHealthStatus(BaseModel):
    status: str
    postgres: str
    redis: str
