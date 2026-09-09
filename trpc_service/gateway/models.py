# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Channel-neutral contracts crossing Gateway and Worker boundaries."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from trpc_service.config import ChannelType
from trpc_service._compat import StrEnum


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(timezone.utc)


class MessageKind(StrEnum):
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    AUDIO = "audio"


class StreamEventType(StrEnum):
    STARTED = "started"
    DELTA = "delta"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    COMPLETED = "completed"
    ERROR = "error"


class RequestState(StrEnum):
    """Durable lifecycle for one accepted platform request."""

    RESERVED = "reserved"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYABLE_FAILED = "retryable_failed"
    FAILED = "failed"


class Attachment(BaseModel):
    """Transport-neutral attachment metadata; binary data lives in object storage."""

    model_config = ConfigDict(extra="forbid")

    attachment_id: str
    kind: MessageKind
    name: str = ""
    mime_type: str = "application/octet-stream"
    size_bytes: int = Field(default=0, ge=0)
    source_url: str = ""
    object_uri: str = ""
    checksum_sha256: str = ""


class TraceContext(BaseModel):
    """W3C trace headers propagated through queues and channel delivery."""

    model_config = ConfigDict(extra="forbid")

    traceparent: str = ""
    tracestate: str = ""
    baggage: str = ""


class NormalizedInboundMessage(BaseModel):
    """Message emitted by every Channel Adapter."""

    model_config = ConfigDict(extra="forbid")

    message_id: str
    binding_id: str
    channel: ChannelType
    external_user_id: str
    external_conversation_id: str
    text: str = ""
    kind: MessageKind = MessageKind.TEXT
    is_group: bool = False
    reply_to_message_id: str = ""
    occurred_at: datetime = Field(default_factory=utc_now)
    attachments: list[Attachment] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    trace: TraceContext = Field(default_factory=TraceContext)


class AgentRequest(BaseModel):
    """Immutable task persisted before a Worker starts execution."""

    model_config = ConfigDict(extra="forbid")

    request_id: str
    tenant_id: str
    config_version: int
    storage_route_version: int = 0
    app_id: str
    user_id: str
    session_id: str
    text: str
    channel: ChannelType = ChannelType.WEB
    source_message_id: str = ""
    binding_id: str = ""
    attachments: list[Attachment] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    trace: TraceContext = Field(default_factory=TraceContext)
    created_at: datetime = Field(default_factory=utc_now)


class AgentStreamEvent(BaseModel):
    """Stable event format exposed by SSE and outbound delivery."""

    model_config = ConfigDict(extra="forbid")

    request_id: str
    sequence: int = Field(ge=0)
    type: StreamEventType
    text: str = ""
    event_id: str = ""
    author: str = ""
    partial: bool = False
    tool_name: str = ""
    data: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=utc_now)


class UsageSummary(BaseModel):
    """Provider-neutral token and estimated cost summary."""

    model_config = ConfigDict(extra="forbid")

    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)
    cost_usd: float = Field(default=0, ge=0)


class OutboundMessage(BaseModel):
    """Message waiting to be delivered by a Channel Adapter."""

    model_config = ConfigDict(extra="forbid")

    outbound_id: str
    request_id: str
    tenant_id: str
    binding_id: str
    channel: ChannelType
    external_conversation_id: str
    text: str
    reply_to_message_id: str = ""
    attachments: list[Attachment] = Field(default_factory=list)
    trace: TraceContext = Field(default_factory=TraceContext)
    created_at: datetime = Field(default_factory=utc_now)


class ChatResult(BaseModel):
    """Non-streaming result returned by the Chat API."""

    model_config = ConfigDict(extra="forbid")

    request_id: str
    tenant_id: str
    app_id: str
    user_id: str
    session_id: str
    text: str
    events: list[AgentStreamEvent] = Field(default_factory=list)
    usage: UsageSummary = Field(default_factory=UsageSummary)


class RequestRecord(BaseModel):
    """Observable request state shared by asynchronous API and Workers."""

    model_config = ConfigDict(extra="forbid")

    request_id: str
    tenant_id: str
    state: RequestState
    payload_hash: str = ""
    config_version: int = 0
    storage_route_version: int = 0
    attempts: int = Field(default=0, ge=0)
    model_attempts: int = Field(default=0, ge=0)
    successful_model_calls: int = Field(default=0, ge=0)
    recovery_count: int = Field(default=0, ge=0)
    result: ChatResult | None = None
    error_code: str = ""
    retryable: bool = False
    request: AgentRequest | None = None
    idempotency_key: str = ""
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class TaskAccepted(BaseModel):
    """Response returned when a request has been durably queued."""

    model_config = ConfigDict(extra="forbid")

    request_id: str
    state: RequestState
    status_url: str


class ErrorBody(BaseModel):
    """Public error envelope. Internal messages and secrets are never exposed."""

    code: str
    message: str
    request_id: str = ""
    retryable: bool = False
