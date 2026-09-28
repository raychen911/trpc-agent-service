"""Validated management API contracts with write-only Secret references."""

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from trpc_service.config.models import (
    NonNullableUpdateModel,
    SecretRef,
    validate_secret_bearing_config,
)
from trpc_service.config.secret_scope import validate_platform_model_secret_ref

_RUNTIME_MODEL_SECRET_SCHEMES = frozenset({"env", "file"})


def _validate_runtime_model_secret_ref(value: str) -> str:
    """Accept only references that the current model runtime can resolve."""

    reference = SecretRef(uri=value)
    scheme = reference.uri.partition("://")[0]
    if scheme not in _RUNTIME_MODEL_SECRET_SCHEMES:
        supported = ", ".join(sorted(_RUNTIME_MODEL_SECRET_SCHEMES))
        raise ValueError(f"model runtime SecretRef must use one of: {supported}")
    return value


class PrincipalType(StrEnum):
    HUMAN = "human"
    SERVICE = "service"


class ManagementRole(StrEnum):
    PLATFORM_ADMIN = "platform_admin"
    TENANT_ADMIN = "tenant_admin"


class ResourceStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


class CredentialMode(StrEnum):
    TENANT_MANAGED = "tenant_managed"
    PLATFORM_MANAGED = "platform_managed"


class PrincipalCreate(BaseModel):
    display_name: str = Field(min_length=1, max_length=120)
    principal_type: PrincipalType
    external_subject: str = Field(min_length=3, max_length=255)

    @field_validator("external_subject")
    @classmethod
    def reject_reserved_subject(cls, value: str) -> str:
        """Reserve tenant-console subjects for atomic tenant account creation."""

        if value.casefold().startswith("tenant-console:"):
            raise ValueError("tenant-console subject is reserved")
        return value


class PrincipalRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    management_principal_id: UUID
    display_name: str
    principal_type: PrincipalType
    external_subject: str
    status: ResourceStatus
    created_at: datetime
    updated_at: datetime


class PrincipalUpdate(NonNullableUpdateModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=120)
    status: ResourceStatus | None = None


class PrincipalList(BaseModel):
    items: list[PrincipalRead]
    total: int


class RoleAssignmentCreate(BaseModel):
    role: ManagementRole
    tenant_id: UUID | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> "RoleAssignmentCreate":
        # Tenant administrators must be provisioned atomically with a password
        # through /admin/tenant-accounts. The generic role API is deliberately
        # limited to platform roles so it cannot create an unusable tenant login.
        if self.role is not ManagementRole.PLATFORM_ADMIN:
            raise ValueError(
                "tenant administrators must be provisioned through /admin/tenant-accounts")
        if self.tenant_id is not None:
            raise ValueError("platform_admin cannot be tenant-scoped")
        return self


class RoleAssignmentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    role_assignment_id: UUID
    management_principal_id: UUID
    tenant_id: UUID | None
    role: ManagementRole
    created_at: datetime


class RoleAssignmentList(BaseModel):
    items: list[RoleAssignmentRead]
    total: int


class CredentialCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    expires_at: datetime | None = None

    @field_validator("expires_at")
    @classmethod
    def validate_expiry(cls, value: datetime | None) -> datetime | None:
        """Reject credentials that would be expired at issuance time."""

        if value is None:
            return None
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if normalized <= datetime.now(timezone.utc):
            raise ValueError("credential expiry must be in the future")
        return value


class CredentialRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    credential_id: UUID
    management_principal_id: UUID
    name: str
    token_prefix: str
    status: ResourceStatus
    expires_at: datetime | None
    last_used_at: datetime | None
    created_at: datetime


class CredentialIssued(CredentialRead):
    token: str


class CredentialList(BaseModel):
    items: list[CredentialRead]
    total: int


class PasswordCredentialSet(BaseModel):
    """Write-only browser login credential configured by a platform administrator."""

    username: str = Field(min_length=3, max_length=120, pattern=r"^[a-zA-Z0-9._@+-]+$")
    password: SecretStr

    @field_validator("username")
    @classmethod
    def normalize_username(cls, value: str) -> str:
        return value.strip().casefold()

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: SecretStr) -> SecretStr:
        plaintext = value.get_secret_value()
        if len(plaintext) < 12 or len(plaintext) > 256:
            raise ValueError("password must contain 12 to 256 characters")
        return value


class PasswordLogin(BaseModel):
    username: str = Field(min_length=3, max_length=120)
    password: SecretStr = Field(min_length=1, max_length=256)

    @field_validator("username")
    @classmethod
    def normalize_username(cls, value: str) -> str:
        return value.strip().casefold()


class TenantAccountCreate(PasswordCredentialSet):
    """Provision the single administrator login associated with a tenant."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: UUID


class ModelCatalogCreate(BaseModel):
    # The catalog is a deployable capability list, so unsupported providers must
    # not become active entries that can only fail later during execution.
    provider: Literal["bailian", "bailian_openai"]
    model_name: str = Field(min_length=1, max_length=120)
    display_name: str = Field(min_length=1, max_length=120)
    capabilities: dict[str, Any] = Field(default_factory=dict)
    default_limits: dict[str, Any] = Field(default_factory=dict)
    platform_secret_ref: str | None = Field(default=None, max_length=500)

    @field_validator("platform_secret_ref")
    @classmethod
    def validate_secret_ref(cls, value: str | None) -> str | None:
        if value is not None:
            _validate_runtime_model_secret_ref(value)
            validate_platform_model_secret_ref(value)
        return value

    @field_validator("default_limits")
    @classmethod
    def validate_default_limits(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Reject model defaults that would be coerced differently at runtime."""

        validated = validate_secret_bearing_config(value)
        if "enable_thinking" in validated and not isinstance(validated["enable_thinking"], bool):
            raise ValueError("enable_thinking must be a boolean")
        for field in ("max_output_tokens", "context_window_tokens"):
            configured = validated.get(field)
            if configured is not None and (isinstance(configured, bool)
                                           or not isinstance(configured, int) or configured <= 0):
                raise ValueError(f"{field} must be a positive integer")
        return validated


class ModelCatalogUpdate(NonNullableUpdateModel):
    model_name: str | None = Field(default=None, min_length=1, max_length=120)
    display_name: str | None = Field(default=None, min_length=1, max_length=120)
    capabilities: dict[str, Any] | None = None
    default_limits: dict[str, Any] | None = None
    platform_secret_ref: str | None = Field(default=None, max_length=500)
    status: ResourceStatus | None = None

    @field_validator("platform_secret_ref")
    @classmethod
    def validate_secret_ref(cls, value: str | None) -> str | None:
        if value is not None:
            _validate_runtime_model_secret_ref(value)
            validate_platform_model_secret_ref(value)
        return value

    @field_validator("default_limits")
    @classmethod
    def validate_default_limits(
        cls,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if value is None:
            return None
        return ModelCatalogCreate.validate_default_limits(value)


class ModelCatalogRead(BaseModel):
    model_catalog_id: UUID
    provider: str
    model_name: str
    display_name: str
    capabilities: dict[str, Any]
    default_limits: dict[str, Any]
    platform_credential_configured: bool
    status: ResourceStatus
    created_at: datetime
    updated_at: datetime


class ModelCatalogList(BaseModel):
    items: list[ModelCatalogRead]
    total: int


class ModelCredentialCreate(BaseModel):
    provider: Literal["bailian", "bailian_openai"]
    name: str = Field(min_length=1, max_length=120)
    secret_ref: str = Field(max_length=500)

    @field_validator("secret_ref")
    @classmethod
    def validate_secret_ref(cls, value: str) -> str:
        _validate_runtime_model_secret_ref(value)
        validate_platform_model_secret_ref(value)
        return value


class ModelCredentialUpdate(NonNullableUpdateModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    secret_ref: str | None = Field(default=None, max_length=500)
    status: ResourceStatus | None = None

    @field_validator("secret_ref")
    @classmethod
    def validate_secret_ref(cls, value: str | None) -> str | None:
        if value is not None:
            _validate_runtime_model_secret_ref(value)
            validate_platform_model_secret_ref(value)
        return value


class ModelCredentialRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    model_credential_id: UUID
    provider: str
    name: str
    secret_configured: bool = True
    status: ResourceStatus
    created_at: datetime
    updated_at: datetime


class ModelCredentialList(BaseModel):
    items: list[ModelCredentialRead]
    total: int


class ModelProfileCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    model_catalog_id: UUID
    credential_id: UUID
    parameter_config: dict[str, Any] = Field(default_factory=dict)
    limits: dict[str, Any] = Field(default_factory=dict)

    @field_validator("parameter_config", "limits")
    @classmethod
    def reject_plaintext_secrets(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Prevent flexible profile JSON from bypassing write-only SecretRef."""

        validated = validate_secret_bearing_config(value)
        if "enable_thinking" in validated and not isinstance(validated["enable_thinking"], bool):
            raise ValueError("enable_thinking must be a boolean")
        if "temperature" in validated:
            temperature = validated["temperature"]
            if (isinstance(temperature, bool) or not isinstance(temperature, (int, float))
                    or not 0 <= temperature <= 2):
                raise ValueError("temperature must be a number between 0 and 2")
        for field in ("max_output_tokens", "context_window_tokens"):
            configured = validated.get(field)
            if configured is not None and (isinstance(configured, bool)
                                           or not isinstance(configured, int) or configured <= 0):
                raise ValueError(f"{field} must be a positive integer")
        timeout = validated.get("timeout_seconds")
        if timeout is not None and (isinstance(timeout, bool)
                                    or not isinstance(timeout, (int, float)) or timeout <= 0):
            raise ValueError("timeout_seconds must be positive")
        for field in ("input_cost_per_million", "output_cost_per_million"):
            configured = validated.get(field)
            if configured is not None and (isinstance(configured, bool)
                                           or not isinstance(configured,
                                                             (int, float)) or configured < 0):
                raise ValueError(f"{field} must be a non-negative number")
        return validated

    @field_validator("limits")
    @classmethod
    def validate_limits(cls, value: dict[str, Any]) -> dict[str, Any]:
        validated = validate_secret_bearing_config(value)
        supported = {"max_input_chars", "daily_tokens", "daily_calls"}
        unsupported = sorted(set(validated) - supported)
        if unsupported:
            raise ValueError(f"unsupported model limits: {', '.join(unsupported)}")
        for field, configured in validated.items():
            if (isinstance(configured, bool) or not isinstance(configured, int) or configured < 1):
                raise ValueError(f"{field} must be a positive integer")
        return validated

    @model_validator(mode="after")
    def validate_context_window(self) -> "ModelProfileCreate":
        """Ensure the configured context can contain the maximum response."""

        context_window = self.parameter_config.get("context_window_tokens")
        maximum_output = self.parameter_config.get("max_output_tokens")
        if (isinstance(context_window, int) and isinstance(maximum_output, int)
                and context_window < maximum_output):
            raise ValueError("context_window_tokens must be at least max_output_tokens")
        return self


class ModelProfileUpdate(NonNullableUpdateModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    model_catalog_id: UUID | None = None
    credential_id: UUID | None = None
    parameter_config: dict[str, Any] | None = None
    limits: dict[str, Any] | None = None
    status: ResourceStatus | None = None

    @field_validator("parameter_config")
    @classmethod
    def reject_plaintext_secrets(
        cls,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Apply the same secret policy during partial profile updates."""

        if value is None:
            return None
        validated = validate_secret_bearing_config(value)
        if "enable_thinking" in validated and not isinstance(validated["enable_thinking"], bool):
            raise ValueError("enable_thinking must be a boolean")
        temperature = validated.get("temperature")
        if temperature is not None and (isinstance(temperature, bool) or
                                        not isinstance(temperature,
                                                       (int, float)) or not 0 <= temperature <= 2):
            raise ValueError("temperature must be a number between 0 and 2")
        for field in ("max_output_tokens", "context_window_tokens"):
            configured = validated.get(field)
            if configured is not None and (isinstance(configured, bool)
                                           or not isinstance(configured, int) or configured <= 0):
                raise ValueError(f"{field} must be a positive integer")
        timeout = validated.get("timeout_seconds")
        if timeout is not None and (isinstance(timeout, bool)
                                    or not isinstance(timeout, (int, float)) or timeout <= 0):
            raise ValueError("timeout_seconds must be positive")
        for field in ("input_cost_per_million", "output_cost_per_million"):
            configured = validated.get(field)
            if configured is not None and (isinstance(configured, bool)
                                           or not isinstance(configured,
                                                             (int, float)) or configured < 0):
                raise ValueError(f"{field} must be a non-negative number")
        return validated

    @field_validator("limits")
    @classmethod
    def validate_limits(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return None
        validated = validate_secret_bearing_config(value)
        supported = {"max_input_chars", "daily_tokens", "daily_calls"}
        unsupported = sorted(set(validated) - supported)
        if unsupported:
            raise ValueError(f"unsupported model limits: {', '.join(unsupported)}")
        for field, configured in validated.items():
            if (isinstance(configured, bool) or not isinstance(configured, int) or configured < 1):
                raise ValueError(f"{field} must be a positive integer")
        return validated

    @model_validator(mode="after")
    def validate_context_window(self) -> "ModelProfileUpdate":
        """Validate both bounds when a partial request supplies them together."""

        configured = self.parameter_config or {}
        context_window = configured.get("context_window_tokens")
        maximum_output = configured.get("max_output_tokens")
        if (isinstance(context_window, int) and isinstance(maximum_output, int)
                and context_window < maximum_output):
            raise ValueError("context_window_tokens must be at least max_output_tokens")
        return self


class ModelProfileRead(BaseModel):
    model_profile_id: UUID
    tenant_id: UUID
    model_catalog_id: UUID
    credential_id: UUID | None
    name: str
    credential_mode: CredentialMode
    secret_configured: bool
    parameter_config: dict[str, Any]
    limits: dict[str, Any]
    status: ResourceStatus
    created_at: datetime
    updated_at: datetime


class ModelProfileList(BaseModel):
    items: list[ModelProfileRead]
    total: int


class ChannelAdapterCreate(BaseModel):
    channel_type: str = Field(pattern=r"^[a-z][a-z0-9_-]{1,39}$")
    display_name: str = Field(min_length=1, max_length=120)
    adapter_version: str = Field(min_length=1, max_length=40)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    secret_schema: dict[str, Any] = Field(default_factory=dict)
    capabilities: dict[str, Any] = Field(default_factory=dict)
    status: ResourceStatus = ResourceStatus.DISABLED

    @field_validator("config_schema", "secret_schema")
    @classmethod
    def validate_json_schema(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Reject invalid adapter contracts before they reach tenant bindings."""

        try:
            Draft202012Validator.check_schema(value)
        except SchemaError as error:
            raise ValueError("adapter contract is not valid JSON Schema") from error
        return value


class ChannelAdapterUpdate(NonNullableUpdateModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=120)
    adapter_version: str | None = Field(default=None, min_length=1, max_length=40)
    config_schema: dict[str, Any] | None = None
    secret_schema: dict[str, Any] | None = None
    capabilities: dict[str, Any] | None = None
    status: ResourceStatus | None = None

    @field_validator("config_schema", "secret_schema")
    @classmethod
    def validate_json_schema(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """Apply contract validation to adapter schema replacements."""

        if value is not None:
            try:
                Draft202012Validator.check_schema(value)
            except SchemaError as error:
                raise ValueError("adapter contract is not valid JSON Schema") from error
        return value


class ChannelAdapterRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    channel_type: str
    display_name: str
    adapter_version: str
    config_schema: dict[str, Any]
    secret_schema: dict[str, Any]
    capabilities: dict[str, Any]
    status: ResourceStatus
    created_at: datetime
    updated_at: datetime


class ChannelAdapterList(BaseModel):
    items: list[ChannelAdapterRead]
    total: int


class ManagementAuditRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    audit_id: UUID
    tenant_id: UUID | None
    actor_subject: str
    actor_roles: list[str]
    action: str
    resource_type: str
    resource_id: str
    decision: str
    reason: str | None
    details_redacted: dict[str, Any]
    occurred_at: datetime


class ManagementAuditList(BaseModel):
    items: list[ManagementAuditRead]
    total: int


class UsageLedgerRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    usage_id: UUID
    tenant_id: UUID
    agent_app_id: UUID
    request_id: str
    trace_id: str
    model_provider: str
    model_name: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost: Decimal
    occurred_at: datetime


class UsageLedgerSummary(BaseModel):
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost: Decimal


class UsageLedgerList(BaseModel):
    items: list[UsageLedgerRead]
    total: int
    summary: UsageLedgerSummary


class ManagementActorRead(BaseModel):
    subject: str
    roles: list[str]
    tenant_roles: dict[str, list[str]]
    bootstrap: bool
