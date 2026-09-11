"""Tenant-scoped configuration and message models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ChannelBinding:
    channel: str
    account_id: str
    verify_token: str | None = None
    secret_ref: str | None = None
    enabled: bool = True
    allowed_users: frozenset[str] = frozenset()
    encrypt_key_ref: str | None = None
    api_base_url: str | None = None
    corp_id: str | None = None
    agent_id: str | None = None


@dataclass(frozen=True)
class AgentApp:
    app_id: str
    name: str
    instruction: str = "You are a helpful enterprise assistant."
    model_name: str | None = None
    model_api_key_ref: str | None = None
    model_base_url: str | None = None
    fallback_model_name: str | None = None
    fallback_api_key_ref: str | None = None
    fallback_base_url: str | None = None
    timeout_seconds: float | None = None
    tools: frozenset[str] = frozenset()
    knowledge_collections: frozenset[str] = frozenset()
    input_cost_per_million: float = 0.0
    output_cost_per_million: float = 0.0


@dataclass(frozen=True)
class TenantPolicy:
    allowed_tools: frozenset[str] = frozenset()
    dangerous_tools: frozenset[str] = frozenset()
    max_input_chars: int = 8000
    daily_token_budget: int = 100_000
    requests_per_minute: int = 120
    redact_output: bool = True


@dataclass(frozen=True)
class StorageProfile:
    session: str = "inmemory"
    memory: str = "inmemory"
    audit: str = "inmemory"
    vector: str = "none"
    object_store: str = "none"


@dataclass(frozen=True)
class AuditPolicy:
    retention_days: int = 90
    allow_export: bool = False


@dataclass(frozen=True)
class TenantConfig:
    tenant_id: str
    name: str
    apps: dict[str, AgentApp]
    channels: dict[str, ChannelBinding]
    policy: TenantPolicy = field(default_factory=TenantPolicy)
    audit: AuditPolicy = field(default_factory=AuditPolicy)
    storage: StorageProfile = field(default_factory=StorageProfile)
    version: int = 1
    status: str = "active"


@dataclass(frozen=True)
class InboundMessage:
    tenant_id: str
    channel: str
    account_id: str
    external_message_id: str
    external_user_id: str
    chat_id: str
    chat_type: str
    text: str
    app_id: str = "default"
    received_at: datetime = field(default_factory=utcnow)
    trace_id: str = ""
    session_id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    config_version: int = 1


@dataclass(frozen=True)
class OutboundMessage:
    tenant_id: str
    channel: str
    account_id: str
    external_user_id: str
    chat_id: str
    text: str
    in_reply_to: str
    trace_id: str
    part: int = 1
    total_parts: int = 1
    # Process-local reply metadata; never persisted as a credential.
    reply_context: dict[str, Any] = field(default_factory=dict)
    message_type: str = "text"
    media_url: str | None = None
    media_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SessionEvent:
    tenant_id: str
    session_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any]
    trace_id: str
    created_at: datetime = field(default_factory=utcnow)
    event_id: str = ""


@dataclass
class SessionSnapshot:
    tenant_id: str
    session_id: str
    app_id: str
    user_id: str
    state: dict[str, Any] = field(default_factory=dict)
    version: int = 0
    updated_at: datetime = field(default_factory=utcnow)


def inbound_to_dict(message: InboundMessage) -> dict[str, Any]:
    value = asdict(message)
    value["received_at"] = message.received_at.isoformat()
    return value


def inbound_from_dict(value: dict[str, Any]) -> InboundMessage:
    data = dict(value)
    received_at = data.get("received_at")
    if isinstance(received_at, str):
        data["received_at"] = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    return InboundMessage(**data)
