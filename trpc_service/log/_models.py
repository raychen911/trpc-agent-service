# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Audit log data model."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AuditLogEntry(BaseModel):
    """A single immutable audit record.

    Field set follows the project application spec: tenant, channel, user,
    session, agent, tool, decision, latency, error, cost and trace correlation.
    """

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    """Tenant that owns the action."""
    channel: Optional[str] = None
    """Originating IM channel (WeCom/WeChat KF/DingTalk/Feishu)."""
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    agent_name: Optional[str] = None
    tool_name: Optional[str] = None
    decision: str = "allow"
    """Governance decision: ``allow`` / ``deny`` / ``confirm`` / ``error``."""
    latency_ms: Optional[int] = None
    error_type: Optional[str] = None
    cost: Optional[float] = None
    trace_id: Optional[str] = None
    detail: dict[str, Any] = Field(default_factory=dict)
    """Additional redacted context (model name, token counts, ...)."""
    created_at: datetime = Field(default_factory=_utcnow)
