"""Validated control-plane and message contracts."""

from __future__ import annotations

import base64
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import parse_qs, unquote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(UTC)


def tenant_environment_prefix(tenant_id: str) -> str:
    """Return an injective, environment-safe prefix for one tenant ID."""

    if re.fullmatch(r"[a-z0-9]+", tenant_id):
        encoded = tenant_id.upper()
    else:
        encoded = "ENC_" + base64.b32encode(tenant_id.encode()).decode().rstrip("=")
    return f"TENANT_{encoded}_"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TenantStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class ChannelType(StrEnum):
    WEB = "web"
    TELEGRAM = "telegram"
    WECOM = "wecom"
    WECOM_BOT = "wecom_bot"


class ChatType(StrEnum):
    DIRECT = "direct"
    GROUP = "group"
    CHANNEL = "channel"


class BackendKind(StrEnum):
    INMEMORY = "inmemory"
    REDIS = "redis"
    SQL = "sql"
    FILESYSTEM = "filesystem"
    S3 = "s3"
    LOCAL_VECTOR = "local_vector"
    QDRANT = "qdrant"
    EXTERNAL_MEMORY = "external_memory"


class SecretRef(StrictModel):
    """A reference to a secret, never the secret value itself."""

    uri: str

    @field_validator("uri")
    @classmethod
    def validate_reference(cls, value: str) -> str:
        allowed = (
            "env://",
            "file://",
            "vault://",
            "aws-secretsmanager://",
            "gcp-secretmanager://",
            "azure-keyvault://",
        )
        if not value.startswith(allowed):
            raise ValueError("secret values must use a supported secret-reference URI")
        return value

    def __str__(self) -> str:
        return "<secret-ref>"


class BackendRef(StrictModel):
    kind: BackendKind
    dsn_ref: SecretRef | None = None
    native_dsn_ref: SecretRef | None = None
    namespace: str = Field(default="default", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_backend(self) -> BackendRef:
        remote = {
            BackendKind.REDIS,
            BackendKind.SQL,
            BackendKind.S3,
            BackendKind.QDRANT,
            BackendKind.EXTERNAL_MEMORY,
        }
        if self.kind in remote and self.dsn_ref is None:
            raise ValueError(f"{self.kind.value} requires dsn_ref")
        _reject_inline_secrets(self.options)
        return self


class DataBackendConfig(StrictModel):
    session: BackendRef = Field(default_factory=lambda: BackendRef(kind=BackendKind.INMEMORY))
    memory: BackendRef = Field(default_factory=lambda: BackendRef(kind=BackendKind.INMEMORY))
    summary: BackendRef = Field(default_factory=lambda: BackendRef(kind=BackendKind.INMEMORY))
    artifact: BackendRef = Field(default_factory=lambda: BackendRef(kind=BackendKind.FILESYSTEM))
    knowledge: BackendRef = Field(default_factory=lambda: BackendRef(kind=BackendKind.LOCAL_VECTOR))
    audit: BackendRef = Field(
        default_factory=lambda: BackendRef(
            kind=BackendKind.SQL, dsn_ref=SecretRef(uri="env://TAP_CONTROL_DATABASE_URL")
        )
    )

    @model_validator(mode="after")
    def validate_resource_backends(self) -> DataBackendConfig:
        allowed = {
            "session": {BackendKind.INMEMORY, BackendKind.REDIS, BackendKind.SQL},
            "memory": {
                BackendKind.INMEMORY,
                BackendKind.REDIS,
                BackendKind.SQL,
                BackendKind.EXTERNAL_MEMORY,
            },
            "summary": {BackendKind.INMEMORY, BackendKind.REDIS, BackendKind.SQL},
            "artifact": {
                BackendKind.INMEMORY,
                BackendKind.SQL,
                BackendKind.FILESYSTEM,
                BackendKind.S3,
            },
            "knowledge": {
                BackendKind.INMEMORY,
                BackendKind.SQL,
                BackendKind.LOCAL_VECTOR,
                BackendKind.QDRANT,
            },
            "audit": {BackendKind.INMEMORY, BackendKind.SQL},
        }
        for resource, kinds in allowed.items():
            reference = getattr(self, resource)
            if reference.kind not in kinds:
                raise ValueError(f"{reference.kind.value} cannot back the {resource} resource")
        return self


class ModelConfig(StrictModel):
    provider: Literal["openai-compatible", "anthropic", "litellm", "deterministic"]
    model_name: str
    api_key_ref: SecretRef | None = None
    base_url: str | None = None
    timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    max_output_tokens: int = Field(default=2_048, gt=0, le=128_000)
    context_window_tokens: int = Field(default=128_000, ge=1_024, le=2_000_000)
    retry_count: int = Field(default=2, ge=0, le=10)
    retry_initial_seconds: float = Field(default=1.0, ge=0, le=60)
    retry_max_seconds: float = Field(default=10.0, ge=0, le=300)
    input_cost_per_million: float = Field(default=0.0, ge=0)
    output_cost_per_million: float = Field(default=0.0, ge=0)

    @model_validator(mode="after")
    def require_key_for_remote_model(self) -> ModelConfig:
        if self.provider != "deterministic" and self.api_key_ref is None:
            raise ValueError("remote model providers require api_key_ref")
        if self.base_url:
            parsed = urlsplit(self.base_url)
            if parsed.username or parsed.password:
                raise ValueError("model base_url cannot contain inline credentials")
            if not parsed.hostname or parsed.scheme not in {"http", "https"}:
                raise ValueError("model base_url must be an absolute HTTP(S) URL")
            loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
            if parsed.scheme != "https" and not loopback:
                raise ValueError("remote model base_url must use HTTPS")
            _reject_inline_secrets({key: values for key, values in parse_qs(parsed.query).items()})
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError("retry_max_seconds cannot be less than retry_initial_seconds")
        if self.max_output_tokens > self.context_window_tokens:
            raise ValueError("max_output_tokens cannot exceed the model context window")
        return self


class AgentAppConfig(StrictModel):
    app_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{1,62}$")
    agent_name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{1,62}$")
    description: str = "Tenant assistant"
    instruction: str
    model_profile: str
    allowed_tools: frozenset[str] = Field(default_factory=frozenset)
    enabled: bool = True


class ToolPermissionConfig(StrictModel):
    allow: frozenset[str] = Field(default_factory=frozenset)
    deny: frozenset[str] = Field(default_factory=frozenset)
    dangerous: frozenset[str] = Field(default_factory=frozenset)
    confirmation_ttl_seconds: int = Field(default=300, ge=30, le=3_600)

    @model_validator(mode="after")
    def deny_wins(self) -> ToolPermissionConfig:
        if self.allow & self.deny:
            raise ValueError("a tool cannot be both allowed and denied")
        if not self.dangerous <= self.allow:
            raise ValueError("dangerous tools must also appear in allow")
        return self


class UserAccessPolicy(StrictModel):
    allow_users: frozenset[str] = Field(default_factory=frozenset)
    deny_users: frozenset[str] = Field(default_factory=frozenset)
    allow_groups: frozenset[str] = Field(default_factory=frozenset)
    group_session_scope: Literal["conversation", "per_user"] = "conversation"


class BudgetPolicy(StrictModel):
    monthly_tokens: int = Field(default=10_000_000, ge=0)
    monthly_cost_usd: float = Field(default=100.0, ge=0)
    max_tokens_per_request: int = Field(default=16_000, ge=128)
    max_concurrent_sessions: int = Field(default=100, ge=1)
    max_llm_calls_per_request: int = Field(default=8, ge=1, le=100)
    max_tool_calls_per_request: int = Field(default=16, ge=0, le=1_000)


class RedactionPolicy(StrictModel):
    redact_email: bool = True
    redact_phone: bool = True
    redact_credentials: bool = True
    redact_before_model: bool = False
    replacement: str = "[REDACTED]"


class AuditPolicy(StrictModel):
    enabled: bool = True
    retention_days: int = Field(default=180, ge=1, le=3_650)
    include_prompt_hash: bool = True
    include_content: bool = False
    export_sink: str | None = None
    export_auth_ref: SecretRef | None = None

    @model_validator(mode="after")
    def validate_export(self) -> AuditPolicy:
        if self.export_sink:
            parsed = urlsplit(self.export_sink)
            if parsed.scheme != "https" or not parsed.hostname:
                raise ValueError("audit export_sink must be an absolute HTTPS URL")
            if parsed.username or parsed.password:
                raise ValueError("audit export_sink cannot contain inline credentials")
            _reject_inline_secrets({key: values for key, values in parse_qs(parsed.query).items()})
        if self.export_auth_ref and not self.export_sink:
            raise ValueError("audit export_auth_ref requires export_sink")
        return self


class GovernanceConfig(StrictModel):
    tools: ToolPermissionConfig = Field(default_factory=ToolPermissionConfig)
    users: UserAccessPolicy = Field(default_factory=UserAccessPolicy)
    budget: BudgetPolicy = Field(default_factory=BudgetPolicy)
    redaction: RedactionPolicy = Field(default_factory=RedactionPolicy)


class ChannelBindingConfig(StrictModel):
    binding_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{8,64}$")
    channel: ChannelType
    app_id: str
    external_account_id: str
    credential_refs: dict[str, SecretRef] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True

    @model_validator(mode="after")
    def reject_inline_channel_secrets(self) -> ChannelBindingConfig:
        _reject_inline_secrets(self.settings)
        required = {
            ChannelType.WEB: {"webhook_token"},
            ChannelType.TELEGRAM: {"webhook_secret", "bot_token"},
            ChannelType.WECOM_BOT: {"bot_id", "bot_secret"},
            ChannelType.WECOM: {
                "callback_token",
                "encoding_aes_key",
                "corp_id",
                "corp_secret",
                "agent_id",
            },
        }[self.channel]
        missing = required - self.credential_refs.keys()
        if self.enabled and missing:
            raise ValueError(f"enabled {self.channel.value} binding is missing credential references")
        if self.channel is ChannelType.WECOM_BOT and self.settings:
            raise ValueError("WeCom bot transport uses fixed secure defaults; settings must be empty")
        return self


class TenantConfig(StrictModel):
    tenant_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{2,62}$")
    display_name: str
    revision: int = Field(default=1, ge=1)
    status: TenantStatus = TenantStatus.ACTIVE
    models: dict[str, ModelConfig]
    apps: dict[str, AgentAppConfig]
    channels: tuple[ChannelBindingConfig, ...]
    data_backends: DataBackendConfig = Field(default_factory=DataBackendConfig)
    governance: GovernanceConfig = Field(default_factory=GovernanceConfig)
    audit: AuditPolicy = Field(default_factory=AuditPolicy)
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_references(self) -> TenantConfig:
        for key, app in self.apps.items():
            if key != app.app_id:
                raise ValueError(f"application key {key!r} must equal app_id {app.app_id!r}")
            if app.model_profile not in self.models:
                raise ValueError(f"application {app.app_id!r} references an unknown model profile")
            if not app.allowed_tools <= self.governance.tools.allow:
                raise ValueError(f"application {app.app_id!r} enables tools outside the tenant allow-list")
        seen: set[str] = set()
        for channel in self.channels:
            if channel.binding_id in seen:
                raise ValueError(f"duplicate channel binding {channel.binding_id!r}")
            seen.add(channel.binding_id)
            if channel.app_id not in self.apps:
                raise ValueError(f"channel {channel.binding_id!r} references an unknown application")
            for purpose, reference in channel.credential_refs.items():
                _validate_tenant_secret_scope(
                    self.tenant_id,
                    reference,
                    purpose=f"channel.{purpose}",
                )
        for profile_name, model in self.models.items():
            if model.api_key_ref:
                _validate_tenant_secret_scope(
                    self.tenant_id,
                    model.api_key_ref,
                    purpose=f"model.{profile_name}",
                )
        for resource in ("session", "memory", "summary", "artifact", "knowledge", "audit"):
            backend = getattr(self.data_backends, resource)
            reference = backend.dsn_ref
            if reference:
                _validate_tenant_secret_scope(
                    self.tenant_id,
                    reference,
                    purpose=f"backend.{resource}",
                )
            if resource == "session" and backend.native_dsn_ref:
                _validate_tenant_secret_scope(
                    self.tenant_id,
                    backend.native_dsn_ref,
                    purpose="backend.session.native",
                )
            if resource == "session" and backend.kind is BackendKind.SQL:
                if backend.native_dsn_ref is None:
                    raise ValueError("SQL Session requires an isolated native_dsn_ref")
            if resource != "session" and backend.native_dsn_ref is not None:
                raise ValueError("native_dsn_ref is supported only by the Session backend")
        if self.audit.export_auth_ref:
            _validate_tenant_secret_scope(
                self.tenant_id,
                self.audit.export_auth_ref,
                purpose="audit.export",
            )
        _reject_inline_secrets(self.metadata)
        return self


class Attachment(StrictModel):
    kind: Literal["image", "file", "audio", "video"]
    external_id: str
    filename: str | None = None
    mime_type: str | None = None
    size_bytes: int | None = None
    download_url: str | None = None


class InboundEnvelope(StrictModel):
    message_id: str
    tenant_id: str
    app_id: str
    binding_id: str
    channel: ChannelType
    external_account_id: str
    external_user_id: str
    external_chat_id: str
    chat_type: ChatType
    thread_id: str | None = None
    text: str = ""
    attachments: tuple[Attachment, ...] = ()
    occurred_at: datetime = Field(default_factory=utc_now)
    received_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)
    trace_context: dict[str, str] = Field(default_factory=dict)


class RoutedEnvelope(StrictModel):
    inbound: InboundEnvelope
    internal_user_id: str
    session_id: str
    config_revision: int


class AgentEventType(StrEnum):
    START = "start"
    TEXT_DELTA = "text_delta"
    TEXT_FINAL = "text_final"
    TOOL_START = "tool_start"
    TOOL_RESULT = "tool_result"
    ARTIFACT = "artifact"
    ERROR = "error"


class AgentEvent(StrictModel):
    event_id: str
    event_type: AgentEventType
    text: str | None = None
    tool_name: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    partial: bool = False
    token_input: int = 0
    token_output: int = 0
    cost_usd: float = 0.0


class OutboundMessage(StrictModel):
    tenant_id: str
    binding_id: str
    channel: ChannelType
    external_chat_id: str
    reply_to_message_id: str | None = None
    text: str = ""
    cards: tuple[dict[str, Any], ...] = ()
    attachments: tuple[Attachment, ...] = ()
    stream_key: str | None = None
    is_final: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReceiptStatus(StrEnum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ProcessingReceipt(StrictModel):
    tenant_id: str
    dedupe_key: str
    status: ReceiptStatus
    owner: str
    lease_expires_at: datetime
    response: tuple[OutboundMessage, ...] = ()
    error_type: str | None = None
    updated_at: datetime = Field(default_factory=utc_now)


class AuditRecord(StrictModel):
    audit_id: str
    occurred_at: datetime = Field(default_factory=utc_now)
    tenant_id: str
    channel: str
    user_id: str
    session_id: str
    agent_name: str
    tool_name: str | None = None
    decision: str
    latency_ms: float = 0.0
    error_type: str | None = None
    cost_usd: float = 0.0
    token_input: int = 0
    token_output: int = 0
    trace_id: str
    message_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class SessionSnapshot(StrictModel):
    tenant_id: str
    app_id: str
    session_id: str
    user_id: str
    channel: str
    state: dict[str, Any] = Field(default_factory=dict)
    revision: int = 0
    last_event_sequence: int = 0
    summary_version: int = 0
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class SessionEvent(StrictModel):
    event_id: str
    tenant_id: str
    session_id: str
    sequence: int
    kind: str
    actor_id: str
    payload: dict[str, Any]
    state_delta: dict[str, Any] = Field(default_factory=dict)
    trace_id: str = ""
    created_at: datetime = Field(default_factory=utc_now)


class MemoryRecord(StrictModel):
    memory_id: str
    tenant_id: str
    user_id: str
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    revision: int = 1
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class SummaryRecord(StrictModel):
    tenant_id: str
    session_id: str
    version: int
    through_event_sequence: int
    content: str
    created_at: datetime = Field(default_factory=utc_now)


class ArtifactRecord(StrictModel):
    tenant_id: str
    session_id: str
    artifact_id: str
    filename: str
    content_type: str
    size_bytes: int
    checksum_sha256: str
    storage_uri: str
    version: int = Field(default=1, ge=1, le=9_223_372_036_854_775_807)
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeRecord(StrictModel):
    tenant_id: str
    document_id: str
    chunk_id: str
    text: str
    embedding: tuple[float, ...] = ()
    metadata: dict[str, Any] = Field(default_factory=dict)
    embedding_model: str | None = None
    updated_at: datetime = Field(default_factory=utc_now)


class UsageDelta(StrictModel):
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cost_usd: float = Field(default=0.0, ge=0)


class ConfigVersion(StrictModel):
    tenant_id: str
    revision: int
    config: TenantConfig
    status: Literal["draft", "active", "superseded", "rejected"]
    created_by: str
    created_at: datetime = Field(default_factory=utc_now)
    activated_at: datetime | None = None
    checksum_sha256: str


def _reject_inline_secrets(values: Any) -> None:
    sensitive = {
        "api_key",
        "access_key",
        "access_key_id",
        "secret_access_key",
        "password",
        "token",
        "secret",
        "authorization",
        "connection_string",
    }
    if isinstance(values, (list, tuple, set, frozenset)):
        for value in values:
            _reject_inline_secrets(value)
        return
    if not isinstance(values, dict):
        return
    for key, value in values.items():
        normalized = key.casefold()
        if normalized in sensitive or normalized.endswith(("_password", "_token", "_secret", "_api_key")):
            raise ValueError(f"{key} must be supplied through a secret reference")
        _reject_inline_secrets(value)


def _validate_tenant_secret_scope(
    tenant_id: str,
    reference: SecretRef,
    *,
    purpose: str,
) -> None:
    parsed = urlsplit(reference.uri)
    raw_path = unquote(parsed.netloc + parsed.path)
    path = raw_path.strip("/")
    if parsed.scheme == "vault":
        segments = path.split("/")
        if "\\" in raw_path or any(segment in {"", ".", ".."} for segment in segments):
            raise ValueError("tenant Vault references must use a canonical path")
        kv_v1 = len(segments) >= 4 and segments[1:3] == ["tenants", tenant_id]
        kv_v2 = len(segments) >= 5 and segments[1:4] == ["data", "tenants", tenant_id]
        if not (kv_v1 or kv_v2):
            raise ValueError("tenant Vault references must stay under their tenant namespace")
    if parsed.scheme == "file":
        segments = raw_path.split("/")
        if (
            raw_path.startswith(("/", "\\"))
            or "\\" in raw_path
            or any(segment in {"", ".", ".."} for segment in segments)
            or len(segments) < 2
            or segments[0] != tenant_id
        ):
            raise ValueError("tenant file references must stay under their tenant directory")
    if parsed.scheme == "env":
        name = path
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
            raise ValueError("tenant environment references must use a canonical variable name")
        if name == "TAP_CONTROL_DATABASE_URL" and purpose != "backend.audit":
            raise ValueError("the control database reference is allowed only for the audit backend")
        if name != "TAP_CONTROL_DATABASE_URL":
            tenant_prefix = tenant_environment_prefix(tenant_id)
            if not name.startswith(tenant_prefix):
                raise ValueError(f"tenant environment references must start with {tenant_prefix}")
