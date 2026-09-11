"""Immutable tenant configuration and policy models."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Literal
from urllib.parse import urlparse

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from trpc_service.channels import ChannelType


_IDENTIFIER_PATTERN = r"^[a-z][a-z0-9_-]{2,63}$"
_SECRET_SCHEMES = {"vault", "secret", "kms", "env"}


class TenantStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DISABLED = "disabled"


class BackendKind(str, Enum):
    """ Type of backend storage """

    IN_MEMORY = "in_memory"
    REDIS = "redis"
    SQL = "sql"
    VECTOR = "vector"
    OBJECT = "object"
    EXTERNAL_MEMORY = "external_memory"


class ReplyMode(str, Enum):
    """ IM response """

    ASYNC = "async"
    STREAM = "stream"
    CARD = "card"


class SecretRef(BaseModel):
    """Reference to a secret; plaintext credentials are intentionally rejected."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    uri: str = Field(min_length=8, max_length=1024)

    @field_validator("uri")
    @classmethod
    def validate_secret_uri(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in _SECRET_SCHEMES or not parsed.netloc:
            raise ValueError("secret must use a supported reference URI, not a plaintext value")
        return value


class MCPServerConfig(BaseModel):
    """Tenant application MCP endpoint and its trusted server identifier."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    server_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    endpoint: str = Field(min_length=8, max_length=2048)
    secret_ref: SecretRef | None = None
    enabled: bool = True

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        parsed = urlparse(value)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment):
            raise ValueError("MCP endpoint must be an absolute HTTP(S) URL")
        return value.rstrip("/")


class EmbeddingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    model_name: str = Field(min_length=1, max_length=255)
    api_key_ref: SecretRef
    base_url: str = "https://openrouter.ai/api/v1"
    dimensions: int = Field(ge=1, le=65536)

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, value):
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("embedding endpoint must be HTTP(S) without credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("embedding endpoint cannot contain a query or fragment")
        return value.rstrip("/")


class ModelConfig(BaseModel):
    """Tenant-owned LLM routing, reliability and budget settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    model_config_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    provider: str = Field(min_length=1, max_length=64)
    model_name: str = Field(min_length=1, max_length=255)
    base_url: str | None = Field(default=None, max_length=2048)
    api_key_ref: SecretRef
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)
    max_output_tokens: int = Field(default=4096, ge=1)
    request_timeout_seconds: float = Field(default=60.0, gt=0.0)
    max_retries: int = Field(default=2, ge=0, le=10)
    monthly_budget: Decimal | None = Field(default=None, ge=Decimal("0"))
    currency: str = Field(default="CNY", pattern=r"^[A-Z]{3}$")


class ToolPolicy(BaseModel):
    """Tool visibility and execution policy for one Agent application."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    default_deny: bool = True
    allow: frozenset[str] = frozenset()
    deny: frozenset[str] = frozenset()
    require_confirmation: frozenset[str] = frozenset()
    max_calls_per_run: int = Field(default=20, ge=0, le=500)

    @model_validator(mode="after")
    def validate_disjoint_sets(self) -> "ToolPolicy":
        overlaps = (self.allow & self.deny) | (self.deny & self.require_confirmation)
        if overlaps:
            raise ValueError(f"tool policy sets overlap: {sorted(overlaps)}")
        if not self.require_confirmation <= self.allow:
            raise ValueError("tools requiring confirmation must also be present in allow")
        return self


class AgentApplicationConfig(BaseModel):
    """A versioned Agent application inside one tenant."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    app_id: str = Field(pattern=_IDENTIFIER_PATTERN)  # 平台内的应用id
    name: str = Field(min_length=1, max_length=255)
    agent_name: str = Field(pattern=_IDENTIFIER_PATTERN)  # agent名
    model_config_id: str = Field(pattern=_IDENTIFIER_PATTERN)  # 指向同一个租户的model config
    instruction: str = Field(min_length=1, max_length=100_000)
    instruction_version: int = Field(default=1, ge=1)
    tool_policy: ToolPolicy = ToolPolicy()   # 只属于本应用的工具策略
    mcp_servers: tuple[MCPServerConfig, ...] = ()
    knowledge_base_ids: tuple[str, ...] = ()  # 知识库
    max_tool_iterations: int = Field(default=20, ge=0, le=500)
    run_timeout_seconds: float = Field(default=120.0, gt=0.0)
    enabled: bool = True

    @model_validator(mode="after")
    def validate_mcp_servers(self) -> "AgentApplicationConfig":
        ids = [item.server_id for item in self.mcp_servers]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate MCP server identifier")
        return self


class ChannelCredentialRef(BaseModel):
    """Named credential used by one channel binding."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=_IDENTIFIER_PATTERN)
    secret_ref: SecretRef


class ChannelBindingConfig(BaseModel):
    """Trusted mapping from one external IM account to a tenant Agent app.

    某个外部 IM 账号通过哪个 webhook 绑定到哪个租户的哪个 Agent 应用。
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    tenant_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    agent_app_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    channel: ChannelType
    external_account_id: str = Field(min_length=1, max_length=255)
    webhook_public_id: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    credential_refs: tuple[ChannelCredentialRef, ...] = ()
    reply_mode: ReplyMode = ReplyMode.ASYNC
    enabled: bool = True
    group_mode: Literal["per_user", "shared"] = "per_user"
    identity_version: Literal[2] = 2
    conversation_epoch: int = Field(default=1, ge=1)
    transport: Literal["none", "wecom_ws", "telegram_polling", "telegram_webhook", "feishu_ws"] = "none"

    @model_validator(mode="after")
    def validate_transport(self):
        if self.transport != "none" and not self.transport.startswith(self.channel.value + "_"):
            raise ValueError("transport does not belong to the selected channel")
        return self

    @field_validator("credential_refs")
    @classmethod
    def validate_unique_credentials(cls, value: tuple[ChannelCredentialRef, ...]) -> tuple[ChannelCredentialRef, ...]:
        names = [item.name for item in value]
        if len(names) != len(set(names)):
            raise ValueError("duplicate channel credential name")
        return value


class BackendRef(BaseModel):
    """Reference to an operator-managed backend profile and tenant namespace."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: BackendKind  # 数据库类型
    profile_id: str = Field(pattern=_IDENTIFIER_PATTERN)  # 连接配置
    namespace: str = Field(min_length=1, max_length=255)  # 租户在这个后端中的数据作用域


class DataBackendConfig(BaseModel):
    """Storage placement for each tRPC-Agent state category."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    session: BackendRef
    memory: BackendRef
    summary: BackendRef
    knowledge: BackendRef
    artifact: BackendRef
    audit: BackendRef

    @model_validator(mode="after")
    def validate_backend_roles(self) -> "DataBackendConfig":
        if self.session.kind not in {BackendKind.IN_MEMORY, BackendKind.REDIS, BackendKind.SQL}:
            raise ValueError("session backend must be in_memory, redis, or sql")
        if self.summary.kind not in {BackendKind.IN_MEMORY, BackendKind.REDIS, BackendKind.SQL}:
            raise ValueError("summary backend must be in_memory, redis, or sql")
        if self.knowledge.kind is not BackendKind.VECTOR:
            raise ValueError("knowledge backend must be vector")
        if self.artifact.kind is not BackendKind.OBJECT:
            raise ValueError("artifact backend must be object")
        if self.audit.kind is not BackendKind.SQL:
            raise ValueError("audit backend must be sql")
        if (self.summary.kind, self.summary.profile_id) != (self.session.kind, self.session.profile_id):
            raise ValueError("summary must use the same backend kind and profile as session")
        return self


class AuditPolicy(BaseModel):
    """Tenant-specific retention, content capture and redaction policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    retention_days: int = Field(default=180, ge=1, le=3650)
    # SQL hot retention is applied only after an authenticated archive is read
    # back successfully. Archive objects have a separate operator lifecycle.
    archive_enabled: bool = False
    record_prompt_body: bool = False
    record_response_body: bool = False
    hash_external_identifiers: bool = True
    redact_pii: bool = True
    redact_secrets: bool = True
    allowed_viewer_roles: frozenset[str] = frozenset({"tenant_auditor"})


class ResourcePolicy(BaseModel):
    """Persisted admission limits used by every worker for a tenant."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_concurrent_runs: int = Field(default=4, ge=1, le=1000)
    max_queued_messages: int = Field(default=1000, ge=1, le=1_000_000)
    requests_per_minute: int = Field(default=600, ge=1, le=1_000_000)
    priority: int = Field(default=100, ge=0, le=1000)


class TenantConfig(BaseModel):
    """Complete, immutable tenant runtime configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    name: str = Field(min_length=1, max_length=255)
    status: TenantStatus = TenantStatus.ACTIVE
    config_version: int = Field(default=1, ge=1)
    storage_revision: int = Field(default=1, ge=1)
    applications: tuple[AgentApplicationConfig, ...]
    models: tuple[ModelConfig, ...]
    channel_bindings: tuple[ChannelBindingConfig, ...]
    data_backends: DataBackendConfig
    audit_policy: AuditPolicy = AuditPolicy()
    resource_policy: ResourcePolicy = ResourcePolicy()
    embedding: EmbeddingConfig | None = None

    @model_validator(mode="after")
    def validate_references_and_uniqueness(self) -> "TenantConfig":
        app_ids = [app.app_id for app in self.applications]
        model_ids = [model.model_config_id for model in self.models]
        binding_ids = [binding.binding_id for binding in self.channel_bindings]
        webhook_ids = [binding.webhook_public_id for binding in self.channel_bindings]

        for label, values in {
                "application": app_ids,
                "model": model_ids,
                "channel binding": binding_ids,
                "webhook public": webhook_ids,
        }.items():
            if len(values) != len(set(values)):
                raise ValueError(f"duplicate {label} identifier")

        known_models = set(model_ids)
        for app in self.applications:
            if app.model_config_id not in known_models:
                raise ValueError(f"application {app.app_id} references an unknown model config")

        known_apps = set(app_ids)
        for binding in self.channel_bindings:
            if binding.tenant_id != self.tenant_id:
                raise ValueError(f"channel binding {binding.binding_id} belongs to another tenant")
            if binding.agent_app_id not in known_apps:
                raise ValueError(f"channel binding {binding.binding_id} references an unknown application")

        for backend_role, backend in (
                ("session", self.data_backends.session),
                ("memory", self.data_backends.memory),
                ("summary", self.data_backends.summary),
                ("knowledge", self.data_backends.knowledge),
                ("artifact", self.data_backends.artifact),
                ("audit", self.data_backends.audit),
        ):
            if backend.namespace != self.tenant_id:
                raise ValueError(f"{backend_role} backend namespace must equal tenant_id")

        return self
