# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Structured, redacted audit events and sinks."""

from __future__ import annotations

import logging
import json
import re
from datetime import datetime
from datetime import timezone
from typing import Protocol
from typing import Any

from pydantic import BaseModel
from pydantic import Field

_EMAIL = re.compile(r"(?<![\w.+-])([\w.+-]{1,64})@([\w.-]+\.[A-Za-z]{2,})")
_PHONE = re.compile(r"(?<!\d)(1[3-9]\d{9})(?!\d)")
_SECRET = re.compile(r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*([^\s,;]+)")
_BEARER = re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+")
_URL_CREDENTIAL = re.compile(r"(?i)(https?://[^\s:/]+:)([^@\s]+)@")
_DATABASE_PASSWORD = re.compile(r"(?i)(postgres(?:ql)?://[^\s:/]+:)([^@\s]+)@")
_LOADED_SECRETS: set[str] = set()


def register_secret(value: str) -> None:
    """Redact resolved credential values even when no key name precedes them."""
    if len(value) >= 6:
        _LOADED_SECRETS.add(value)


def mask_sensitive_text(value: str) -> str:
    """Best-effort masking for logs; raw prompts should not be audited at all."""
    for secret in sorted(_LOADED_SECRETS, key=len, reverse=True):
        value = value.replace(secret, "***")
    value = _EMAIL.sub(lambda match: f"{match.group(1)[:2]}***@{match.group(2)}", value)
    value = _PHONE.sub(lambda match: f"{match.group(1)[:3]}****{match.group(1)[-4:]}", value)
    value = _BEARER.sub("Bearer ***", value)
    value = _URL_CREDENTIAL.sub(r"\1***@", value)
    value = _DATABASE_PASSWORD.sub(r"\1***@", value)
    return _SECRET.sub(lambda match: f"{match.group(1)}=***", value)


class AuditEvent(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tenant_id: str
    channel: str
    user_id: str
    session_id: str
    agent_name: str
    action: str
    decision: str = "allow"
    tool_name: str = ""
    latency_ms: float = 0
    error_type: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0
    trace_id: str = ""
    request_id: str = ""


class AuditSink(Protocol):

    async def write(self, event: AuditEvent) -> None:
        """Persist one append-only audit event."""


class LoggingAuditSink:
    """Development sink emitting one JSON object through standard logging."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("trpc_service.audit")

    async def write(self, event: AuditEvent) -> None:
        safe = event.model_copy(update={
            key: mask_sensitive_text(value)
            for key, value in event.model_dump().items() if isinstance(value, str)
        })
        self._logger.info("audit %s", safe.model_dump_json())


class PostgresAuditSink:
    """Append-only production audit sink; prompt and secret bodies are not fields."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def write(self, event: AuditEvent) -> None:
        await self._pool.execute(
            """
            INSERT INTO audit_log
                (occurred_at,tenant_id,channel,user_id,session_id,agent_name,
                 tool_name,action,decision,latency_ms,error_type,cost_usd,
                 trace_id,request_id,details)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
            """,
            event.timestamp,
            event.tenant_id,
            event.channel,
            event.user_id,
            event.session_id,
            event.agent_name,
            event.tool_name or None,
            event.action,
            event.decision,
            event.latency_ms,
            event.error_type or None,
            event.cost_usd,
            event.trace_id or None,
            event.request_id,
            json.dumps({
                "input_tokens": event.input_tokens,
                "output_tokens": event.output_tokens,
            }),
        )
