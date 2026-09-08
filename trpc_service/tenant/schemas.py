from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from trpc_service.domain import (
    AppStatus,
    BackendKind,
    ChannelType,
    PermissionEffect,
    TenantStatus,
)

Slug = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")]
SecretRef = Annotated[
    str,
    StringConstraints(
        min_length=7,
        max_length=512,
        pattern=r"^(env|vault|secret|kms|aws-secretsmanager|gcp-secretmanager|azure-keyvault)://.+$",
    ),
]

_sensitive_keys = {
    "access_key",
    "api_key",
    "apikey",
    "client_secret",
    "password",
    "private_key",
    "secret",
    "token",
}


def reject_inline_secrets(value: Any, path: str = "config") -> Any:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _sensitive_keys or normalized.endswith("_password"):
                raise ValueError(f"{path}.{key} must use a dedicated secret_ref field")
            reject_inline_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_inline_secrets(child, f"{path}[{index}]")
    return value


class TenantCreate(BaseModel):
    slug: Slug
    name: str = Field(min_length=1, max_length=128)
    audit_policy: dict[str, Any] = Field(default_factory=dict)
    key_namespace: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def validate_policy(self) -> "TenantCreate":
        reject_inline_secrets(self.audit_policy, "audit_policy")
        return self


class TenantUpdate(BaseModel):
    expected_version: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=128)
    status: TenantStatus | None = None
    audit_policy: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_policy(self) -> "TenantUpdate":
        if self.audit_policy is not None:
            reject_inline_secrets(self.audit_policy, "audit_policy")
        return self


class TenantRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    slug: str
    name: str
    status: TenantStatus
    audit_policy: dict[str, Any]
    key_namespace: str
    version: int
    created_at: datetime
    updated_at: datetime


class AgentAppCreate(BaseModel):
    slug: Slug
    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=512)
    instruction: str = Field(default="", max_length=100_000)
    application_config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_config(self) -> "AgentAppCreate":
        reject_inline_secrets(self.application_config, "application_config")
        return self


class ModelConfigDraft(BaseModel):
    provider: str = Field(min_length=1, max_length=64)
    model_name: str = Field(min_length=1, max_length=128)
    base_url: str | None = Field(default=None, max_length=512)
    api_key_secret_ref: SecretRef | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_parameters(self) -> "ModelConfigDraft":
        reject_inline_secrets(self.parameters, "model.parameters")
        return self


class ToolPermissionDraft(BaseModel):
    tool_name: str = Field(min_length=1, max_length=128)
    effect: PermissionEffect = PermissionEffect.DENY
    requires_confirmation: bool = False
    constraints: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_constraints(self) -> "ToolPermissionDraft":
        reject_inline_secrets(self.constraints, f"tools.{self.tool_name}.constraints")
        return self


class ChannelBindingDraft(BaseModel):
    channel_type: ChannelType
    account_id: str = Field(min_length=1, max_length=255)
    webhook_path: str = Field(pattern=r"^/[^?#]*$", max_length=255)
    token_secret_ref: SecretRef | None = None
    secret_ref: SecretRef | None = None
    enabled: bool = True
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_options(self) -> "ChannelBindingDraft":
        reject_inline_secrets(self.options, f"channels.{self.channel_type}.options")
        identity_mode = self.options.get("identity_mode", "passthrough")
        if identity_mode not in {"passthrough", "strict", "auto"}:
            raise ValueError("options.identity_mode must be passthrough, strict, or auto")
        if self.channel_type == ChannelType.WECOM:
            mode = self.options.get("mode", "app")
            if mode not in {"app", "aibot"}:
                raise ValueError("WeCom options.mode must be app or aibot")
            if mode == "aibot" and not self.options.get("aibot_id"):
                raise ValueError("WeCom AIBot options.aibot_id is required")
        return self


class BackendConfigDraft(BaseModel):
    backend_kind: BackendKind
    backend_type: str = Field(min_length=1, max_length=64)
    secret_ref: SecretRef | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_options(self) -> "BackendConfigDraft":
        reject_inline_secrets(self.options, f"backends.{self.backend_kind}.options")
        return self


class DraftConfigUpdate(BaseModel):
    expected_lock_version: int = Field(ge=1)
    description: str = Field(default="", max_length=512)
    instruction: str = Field(default="", max_length=100_000)
    application_config: dict[str, Any] = Field(default_factory=dict)
    model: ModelConfigDraft | None = None
    tools: list[ToolPermissionDraft] = Field(default_factory=list, max_length=500)
    channels: list[ChannelBindingDraft] = Field(default_factory=list, max_length=100)
    backends: list[BackendConfigDraft] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def validate_aggregate(self) -> "DraftConfigUpdate":
        reject_inline_secrets(self.application_config, "application_config")
        tool_names = [tool.tool_name for tool in self.tools]
        if len(tool_names) != len(set(tool_names)):
            raise ValueError("tool_name must be unique within a configuration version")
        channel_keys = [(item.channel_type, item.account_id) for item in self.channels]
        if len(channel_keys) != len(set(channel_keys)):
            raise ValueError("channel_type and account_id must be unique within a version")
        backend_kinds = [item.backend_kind for item in self.backends]
        if len(backend_kinds) != len(set(backend_kinds)):
            raise ValueError("backend_kind must be unique within a configuration version")
        return self


class AgentAppRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    tenant_id: str
    slug: str
    name: str
    status: AppStatus
    active_version: int | None
    draft_version: int
    lock_version: int
    created_at: datetime
    updated_at: datetime


class DraftConfigRead(BaseModel):
    agent_app_id: str
    tenant_id: str
    version: int
    description: str
    instruction: str
    application_config: dict[str, Any]
    model: ModelConfigDraft | None
    tools: list[ToolPermissionDraft]
    channels: list[ChannelBindingDraft]
    backends: list[BackendConfigDraft]


class PublishRequest(BaseModel):
    expected_lock_version: int = Field(ge=1)


class RollbackRequest(BaseModel):
    expected_lock_version: int = Field(ge=1)
    target_version: int = Field(ge=1)


class EffectiveBackendRead(BaseModel):
    backend_kind: BackendKind
    configured_type: str | None
    effective_type: str
    source: Literal["agent-app", "platform-default"]
    runtime_supported: bool
    secret_ref: str | None
    options: dict[str, Any]


class ActiveChannelBindingRead(BaseModel):
    channel_type: ChannelType
    account_id: str
    webhook_url: str
    token_secret_ref: str | None
    secret_ref: str | None
    identity_mode: Literal["passthrough", "strict", "auto"]
    options: dict[str, Any]


class ImUserIdentityUpsert(BaseModel):
    channel_type: ChannelType
    account_id: str = Field(min_length=1, max_length=255)
    external_user_id: str = Field(min_length=1, max_length=255)
    internal_user_id: str = Field(min_length=1, max_length=255)
    display_name: str | None = Field(default=None, max_length=255)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_attributes(self) -> "ImUserIdentityUpsert":
        reject_inline_secrets(self.attributes, "attributes")
        return self


class ImUserIdentityRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    tenant_id: str
    channel_type: ChannelType
    account_id: str
    external_user_id: str
    internal_user_id: str
    display_name: str | None
    status: str
    attributes: dict[str, Any]
    created_at: datetime
    updated_at: datetime
