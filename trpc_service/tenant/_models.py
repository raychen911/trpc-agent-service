# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant data models for the multi-tenant enterprise layer.

These models define the tenant-level configuration surface required by the
multi-tenant deployment: application config, model endpoints, tool permissions,
IM channel bindings, storage backends and audit policy. They intentionally stay
backend-agnostic so the same ``Tenant`` object can select Redis or MySQL
storage, multiple LLM providers and the supported enterprise IM channels.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from enum import Enum
from typing import Annotated
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr
from pydantic import field_validator
from pydantic import model_validator


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp used as a default value."""
    return datetime.now(timezone.utc)


class TenantStatus(str, Enum):
    """Lifecycle status of a tenant."""

    ACTIVE = "active"
    DISABLED = "disabled"


class ModelEndpoint(BaseModel):
    """Model provider configuration for a tenant."""

    model_config = ConfigDict(extra="forbid")

    provider: str = "openai"
    """Model provider identifier (``openai`` / ``anthropic`` / ``deepseek`` ...)."""
    model_name: str
    """Name of the default model for the tenant."""
    api_endpoint: Optional[str] = None
    """Optional custom API base URL."""
    timeout: int = 30
    """Per-request timeout in seconds."""
    retry: int = 2
    """Number of retries on transient failures."""
    fallback_model: Optional[str] = None
    """Fallback model used when the primary model times out."""
    api_key_env: str = "TRPC_AGENT_API_KEY"
    """Environment/Kubernetes Secret key containing this tenant's model credential."""
    daily_token_budget: Optional[int] = None
    """Optional daily token budget ceiling for this tenant."""


class ToolPermissions(BaseModel):
    """Tool-level permissions granted to a tenant."""

    model_config = ConfigDict(extra="forbid")

    tool_whitelist: list[str] = Field(default_factory=list)
    """Tools the tenant is allowed to call. Empty means 'allow all'."""
    tool_denylist: list[str] = Field(default_factory=list)
    """Tools explicitly forbidden for the tenant."""
    dangerous_tools: list[str] = Field(default_factory=list)
    """Tools requiring a second confirmation before execution."""
    max_tool_execution_time: int = 60
    """Per-tool execution timeout in seconds."""
    max_tool_calls_per_turn: int = 0
    """Maximum tool calls per turn. ``0`` disables the limit."""


class BaseChannelConfig(BaseModel):
    """Shared validation rules for an IM channel binding.

    Secret material is wrapped in :class:`pydantic.SecretStr` so its repr/str
    never leaks the plaintext value.
    """

    model_config = ConfigDict(extra="forbid")


class WeComChannelConfig(BaseChannelConfig):
    """Enterprise WeChat (WeCom) callback and application credentials."""

    channel_type: Literal["wecom"] = "wecom"
    token: SecretStr
    """Callback verification token."""
    secret: Optional[SecretStr] = None
    """Optional application secret used to obtain an access token."""
    aes_key: SecretStr
    """Callback EncodingAESKey."""
    corp_id: str
    """Corporation id and callback receive id."""
    agent_id: str
    """Enterprise application agent id."""
    access_token: Optional[SecretStr] = None
    """Optional pre-issued access token."""


class WechatCustomerServiceChannelConfig(BaseChannelConfig):
    """WeChat Customer Service callback and account configuration."""

    channel_type: Literal["wechat_kf"] = "wechat_kf"
    token: SecretStr
    """Callback verification token."""
    aes_key: SecretStr
    """Callback EncodingAESKey."""
    corp_id: str
    """Corporation id."""
    open_kfid: str
    """Customer service account id."""
    webhook_url: Optional[str] = None
    """Optional outbound webhook override."""


class DingTalkChannelConfig(BaseChannelConfig):
    """DingTalk robot application configuration."""

    channel_type: Literal["dingtalk"] = "dingtalk"
    app_id: str
    """Application client id."""
    robot_code: str
    """Robot code."""
    secret: SecretStr
    """Application secret."""
    webhook_url: Optional[str] = None
    """Optional outbound webhook override."""


class FeishuChannelConfig(BaseChannelConfig):
    """Feishu application and event callback configuration."""

    channel_type: Literal["feishu"] = "feishu"
    app_id: str
    """Application id."""
    verification_token: Optional[SecretStr] = None
    """Event callback verification token."""
    encrypt_key: Optional[SecretStr] = None
    """Event callback encryption key."""
    secret: Optional[SecretStr] = None
    """Optional application secret used for outbound API access."""
    webhook_url: Optional[str] = None
    """Optional outbound webhook override."""

    @model_validator(mode="after")
    def _validate_callback_credential(self) -> FeishuChannelConfig:
        if self.verification_token is None and self.encrypt_key is None:
            raise ValueError("feishu requires verification_token or encrypt_key")
        return self


class QQChannelConfig(BaseChannelConfig):
    """QQ bot application configuration."""

    channel_type: Literal["qq"] = "qq"
    app_id: str
    """Application id."""
    secret: SecretStr
    """Application secret."""
    access_token: Optional[SecretStr] = None
    """Optional pre-issued access token."""


ChannelConfig = Annotated[
    WeComChannelConfig
    | WechatCustomerServiceChannelConfig
    | DingTalkChannelConfig
    | FeishuChannelConfig
    | QQChannelConfig,
    Field(discriminator="channel_type"),
]
"""Discriminated union of all supported IM channel configurations."""


class VectorBackendConfig(BaseModel):
    """Knowledge-vector backend selected for one tenant.

    ``memory`` is intended for tests and demos. Qdrant is built in; Milvus and
    pgvector can be added as backend factories on
    :class:`trpc_service.workspace.TenantStorageRouter`.
    """

    model_config = ConfigDict(extra="forbid")

    backend: Literal["memory", "qdrant", "milvus", "pgvector"] = "memory"
    url: Optional[SecretStr] = None
    api_key: Optional[SecretStr] = None
    collection: str = "agent_knowledge"
    dimensions: Optional[int] = Field(default=None, gt=0)
    embedding_model: Optional[str] = None


class ObjectBackendConfig(BaseModel):
    """Artifact payload backend selected for one tenant."""

    model_config = ConfigDict(extra="forbid")

    backend: Literal["local", "s3", "minio", "cos"] = "local"
    endpoint_url: Optional[str] = None
    bucket: str = "agent-artifacts"
    region: Optional[str] = None
    access_key: Optional[SecretStr] = None
    secret_key: Optional[SecretStr] = None
    local_path: str = "./data/objects"


class StorageBackendConfig(BaseModel):
    """Per-tenant production storage selection.

    Session and memory may use Redis or MySQL. Vector knowledge and artifact
    payloads have independent backends because they have different consistency
    and capacity characteristics. Summary events follow the session backend;
    audit history is fixed to append-only MySQL.
    """

    model_config = ConfigDict(extra="forbid")

    session_backend: Literal["redis", "mysql"] = "redis"
    memory_backend: Literal["redis", "mysql"] = "redis"
    summary_backend: Literal["redis", "mysql"] = "redis"
    """Compatibility field; summaries are session events and follow the session backend."""
    audit_backend: Literal["mysql"] = "mysql"
    """Audit logs use durable append-only MySQL storage, never Redis as a database."""
    redis_url: Optional[SecretStr] = None
    mysql_url: Optional[SecretStr] = None
    vector: VectorBackendConfig = Field(default_factory=VectorBackendConfig)
    object: ObjectBackendConfig = Field(default_factory=ObjectBackendConfig)

    @model_validator(mode="after")
    def _align_summary_with_session(self) -> StorageBackendConfig:
        self.summary_backend = self.session_backend
        return self


class DesensitizeRule(BaseModel):
    """A single regex-based redaction rule used by audit/log redaction."""

    model_config = ConfigDict(extra="forbid")

    pattern: str
    replace: str


class AuditPolicy(BaseModel):
    """Audit and redaction policy for a tenant."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    retention_days: int = 90
    desensitize_rules: list[DesensitizeRule] = Field(default_factory=list)


class AppInfo(BaseModel):
    """A single agent application bound to a tenant."""

    model_config = ConfigDict(extra="forbid")

    app_id: str
    instruction: Optional[str] = None


class AppConfig(BaseModel):
    """Application-level configuration for a tenant."""

    model_config = ConfigDict(extra="forbid")

    app_list: list[AppInfo] = Field(default_factory=list)
    default_instruction: str = ""
    max_concurrent_sessions: int = 500


class IMAccessPolicy(BaseModel):
    """Tenant rules for mapping and authorizing external IM identities."""

    model_config = ConfigDict(extra="forbid")

    require_verified_identity: bool = False
    allowed_users: list[str] = Field(default_factory=list)
    denied_users: list[str] = Field(default_factory=list)
    allowed_groups: list[str] = Field(default_factory=list)


class BudgetConfig(BaseModel):
    """Budget ceilings enforced by the model budget filter."""

    model_config = ConfigDict(extra="forbid")

    daily_token_budget: Optional[int] = None
    daily_cost_limit: Optional[float] = None


class Tenant(BaseModel):
    """A tenant in the multi-tenant enterprise layer."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    """Globally unique tenant identifier."""
    name: str
    """Human-readable tenant name."""
    status: TenantStatus = TenantStatus.ACTIVE
    app_config: AppConfig = Field(default_factory=AppConfig)
    # NOTE: the model endpoint field is named ``model`` (not ``model_config``)
    # because ``model_config`` is reserved by Pydantic for model configuration.
    model: ModelEndpoint
    tool_permissions: ToolPermissions = Field(default_factory=ToolPermissions)
    channel_configs: dict[str, ChannelConfig] = Field(default_factory=dict)
    """Bindings keyed by ``wecom``/``wechat_kf``/``dingtalk``/``feishu``/``qq``."""
    storage_config: StorageBackendConfig = Field(default_factory=StorageBackendConfig)
    im_access_policy: IMAccessPolicy = Field(default_factory=IMAccessPolicy)
    audit_policy: AuditPolicy = Field(default_factory=AuditPolicy)
    budget: BudgetConfig = Field(default_factory=BudgetConfig)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    @field_validator("tenant_id")
    @classmethod
    def _validate_tenant_id(cls, value: str) -> str:
        if ":" in value or "/" in value:
            raise ValueError("tenant_id must not contain ':' or '/' (they are storage key delimiters)")
        return value
