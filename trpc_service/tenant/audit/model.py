"""审计事件模型。"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class AuditEvent(BaseModel):
    """一条审计记录。"""

    tenant_id: str
    channel: str = "web"
    user_id: str = ""
    session_id: str = ""
    agent_name: str = ""
    tool_name: str = ""
    decision: str = "allow"  # allow / block / denied
    latency_ms: int = 0
    error_type: str = ""
    cost: float = 0.0
    trace_id: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
