"""Backend-neutral reliable messaging models."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ClaimStatus(str, Enum):
    ACQUIRED = "acquired"
    BUSY = "busy"
    COMPLETED = "completed"


class ClaimResult(BaseModel):
    """Result of claiming an inbound receipt lease."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: ClaimStatus
    fencing_token: int = Field(default=0, ge=0)


class OutboxMessage(BaseModel):
    """One durable reply intent with resumable part delivery."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    tenant_id: str
    channel: str
    message_id: str
    turn_id: str
    config_revision: Optional[int] = None
    inbound: dict[str, Any]
    parts: list[str]
    next_part: int = Field(default=0, ge=0)
    attempt_count: int = Field(default=0, ge=0)
    status: str = "pending"
    lease_owner: Optional[str] = None
    available_at: datetime = Field(default_factory=utcnow)
    created_at: datetime = Field(default_factory=utcnow)
