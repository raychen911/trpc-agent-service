"""
Data models and configuration schemas for Multi-tenant Agent Service.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StorageType(str, Enum):
    IN_MEMORY = "in_memory"
    SQL = "sql"
    REDIS = "redis"
    HYBRID = "hybrid"


class ChannelType(str, Enum):
    WECOM = "wecom"
    TELEGRAM = "telegram"
    WECHAT_KF = "wechat_kf"
    CUSTOM = "custom"


class EventType(str, Enum):
    USER_MESSAGE = "user_message"
    AGENT_MESSAGE = "agent_message"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SYSTEM_EVENT = "system_event"


class AuditDecision(str, Enum):
    ALLOWED = "ALLOWED"
    BLOCKED = "BLOCKED"
    CONFIRM_REQUIRED = "CONFIRM_REQUIRED"


# =====================================================================
# 1. Configuration Schemas (Tenant, Model, Agent, Channel, Storage)
# =====================================================================

class ModelConfig(BaseModel):
    """Model endpoint and parameter configuration."""
    provider: str = Field(default="mock", description="LLM provider: openai, deepseek, qwen, mock, etc.")
    model_name: str = Field(default="gpt-4o-mini", description="Model identifier")
    api_key_encrypted: Optional[str] = Field(default=None, description="Encrypted API key via KMS/AES")
    api_base: Optional[str] = Field(default=None, description="Custom base URL for LLM endpoint")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=2048, gt=0)
    timeout_seconds: float = Field(default=30.0, gt=0)


class QuotaPolicy(BaseModel):
    """Per-tenant resource quota and rate limit policy."""
    daily_token_budget: int = Field(default=1_000_000, description="Max prompt+completion tokens per day")
    monthly_token_budget: int = Field(default=30_000_000, description="Max tokens per month")
    max_qps: int = Field(default=50, description="Max queries per second")
    max_concurrent_sessions: int = Field(default=200, description="Max concurrent active sessions")


class StorageBackendConfig(BaseModel):
    """Storage backend configuration for a tenant."""
    storage_type: StorageType = Field(default=StorageType.IN_MEMORY)
    redis_url: Optional[str] = Field(default="redis://localhost:6379/0")
    sql_url: Optional[str] = Field(default="sqlite:///./data/trpc_agent.db")
    vector_url: Optional[str] = Field(default=None)
    object_store_bucket: Optional[str] = Field(default=None)


class ChannelBindingConfig(BaseModel):
    """Configuration for binding an IM channel to a tenant."""
    binding_id: str = Field(..., description="Unique binding ID")
    tenant_id: str = Field(..., description="Belonging tenant ID")
    channel_type: ChannelType = Field(..., description="Channel type (wecom, telegram, etc.)")
    bot_id: str = Field(..., description="Bot/Corp ID")
    encrypted_token: Optional[str] = Field(default=None, description="Encrypted webhook verification token")
    encrypted_secret: Optional[str] = Field(default=None, description="Encrypted bot secret / CorpSecret")
    encrypted_aes_key: Optional[str] = Field(default=None, description="Encrypted AES encoding key (for WeCom)")
    webhook_path: str = Field(default="", description="Webhook path suffix")
    is_active: bool = Field(default=True)


class AgentAppConfig(BaseModel):
    """Agent application configuration within a tenant."""
    app_id: str = Field(..., description="Unique Agent App ID")
    tenant_id: str = Field(..., description="Owner tenant ID")
    name: str = Field(..., description="Agent name")
    description: str = Field(default="")
    llm_config: ModelConfig = Field(default_factory=ModelConfig)
    system_prompt: str = Field(default="You are a helpful and secure AI assistant.")
    allowed_tools: List[str] = Field(default_factory=list, description="Whitelist of allowed tool names")
    require_confirmation_tools: List[str] = Field(default_factory=list, description="High-risk tools requiring human check")
    max_context_turns: int = Field(default=15, description="Max recent conversation turns before summary truncation")
    is_active: bool = Field(default=True)


class TenantConfig(BaseModel):
    """Root Tenant configuration entity."""
    tenant_id: str = Field(..., description="Unique tenant identifier")
    name: str = Field(..., description="Tenant display name")
    is_active: bool = Field(default=True)
    quota_policy: QuotaPolicy = Field(default_factory=QuotaPolicy)
    storage_config: StorageBackendConfig = Field(default_factory=StorageBackendConfig)
    custom_metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


# =====================================================================
# 2. Runtime Session, Event, Memory, and Audit Entities
# =====================================================================

class Session(BaseModel):
    """Conversation session entity."""
    session_id: str = Field(..., description="Globally unique session ID, e.g. tenant:wecom:direct:user123")
    tenant_id: str = Field(..., description="Tenant ID")
    app_id: str = Field(..., description="Agent App ID")
    channel_type: str = Field(..., description="Channel type (wecom, telegram, etc.)")
    external_user_id: str = Field(..., description="User ID in external IM")
    external_chat_id: Optional[str] = Field(default=None, description="Group / Chat ID in external IM")
    is_group: bool = Field(default=False)
    current_version: int = Field(default=0, description="Optimistic locking version")
    state_metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class SessionEvent(BaseModel):
    """Immutable session event record (Event Sourcing)."""
    id: Optional[int] = Field(default=None, description="Auto-increment sequence or ID")
    session_id: str = Field(..., description="Belonging session ID")
    tenant_id: str = Field(..., description="Tenant ID")
    sequence_num: int = Field(default=0, description="Strict sequence order within session")
    event_type: EventType = Field(..., description="Event type")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Event payload content")
    created_at: datetime = Field(default_factory=utc_now)


class MemoryItem(BaseModel):
    """Long-term semantic memory item."""
    memory_id: str = Field(..., description="Unique memory ID")
    tenant_id: str = Field(..., description="Tenant ID")
    user_id: str = Field(..., description="User ID associated with memory")
    content: str = Field(..., description="Extracted memory fact/profile text")
    embedding: Optional[List[float]] = Field(default=None, description="Vector embedding")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class SummaryItem(BaseModel):
    """Periodic conversation summary."""
    summary_id: str = Field(..., description="Unique summary ID")
    tenant_id: str = Field(..., description="Tenant ID")
    session_id: str = Field(..., description="Session ID")
    summary_text: str = Field(..., description="Condensed summary text")
    covered_sequence_end: int = Field(..., description="Event sequence_num up to which summary covers")
    created_at: datetime = Field(default_factory=utc_now)


class ArtifactItem(BaseModel):
    """Artifact produced by Agent/Tools (files, reports, charts)."""
    artifact_id: str = Field(..., description="Unique artifact ID")
    tenant_id: str = Field(..., description="Tenant ID")
    session_id: str = Field(..., description="Session ID")
    name: str = Field(..., description="Artifact filename or title")
    content_type: str = Field(default="text/plain")
    storage_uri: str = Field(..., description="URI or path in object store / filesystem")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class AuditLogEntry(BaseModel):
    """Structured security and governance audit log record."""
    log_id: str = Field(..., description="Unique audit record ID")
    trace_id: str = Field(..., description="Distributed trace ID")
    tenant_id: str = Field(..., description="Tenant ID")
    channel: str = Field(..., description="Channel type")
    user_id: str = Field(..., description="External user ID")
    session_id: str = Field(..., description="Session ID")
    agent_name: str = Field(..., description="Agent name or App ID")
    tool_name: Optional[str] = Field(default=None, description="Invoked tool name if any")
    decision: AuditDecision = Field(default=AuditDecision.ALLOWED)
    prompt_tokens: int = Field(default=0)
    completion_tokens: int = Field(default=0)
    cost_usd: float = Field(default=0.0)
    latency_ms: int = Field(default=0)
    error_type: Optional[str] = Field(default=None)
    created_at: datetime = Field(default_factory=utc_now)


# =====================================================================
# 3. IM Inbound / Outbound DTOs
# =====================================================================

class InboundMessage(BaseModel):
    """Standardized Inbound Message from any IM channel."""
    trace_id: str = Field(..., description="Unique Trace ID generated for this message")
    tenant_id: str = Field(..., description="Matched tenant ID")
    channel_type: ChannelType = Field(..., description="Source channel")
    raw_user_id: str = Field(..., description="Sender ID in IM")
    raw_chat_id: Optional[str] = Field(default=None, description="Group chat ID if applicable")
    is_group: bool = Field(default=False)
    message_id: str = Field(..., description="Original message ID in IM for idempotency")
    content: str = Field(..., description="Text content or transcription")
    content_type: str = Field(default="text", description="text, image, audio, file, event")
    raw_payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=utc_now)


class OutboundMessage(BaseModel):
    """Standardized Outbound Message to IM channel."""
    trace_id: str = Field(..., description="Trace ID")
    tenant_id: str = Field(..., description="Tenant ID")
    channel_type: ChannelType = Field(..., description="Target channel")
    target_user_id: str = Field(..., description="Recipient user ID")
    target_chat_id: Optional[str] = Field(default=None, description="Recipient chat/group ID")
    is_group: bool = Field(default=False)
    content: str = Field(..., description="Response text or markdown")
    msg_type: str = Field(default="markdown", description="text, markdown, card, image")
    extra_cards: Optional[Dict[str, Any]] = Field(default=None)
    quote_msg_id: Optional[str] = Field(default=None)
