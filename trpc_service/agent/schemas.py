"""Agent application API schemas and configuration validation."""

from datetime import datetime
from enum import StrEnum
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trpc_service.config.models import NonNullableUpdateModel, validate_secret_bearing_config

_CAPABILITY_KINDS = frozenset({"tool", "mcp", "skill", "workspace"})


def _string_array(value: object, field: str) -> list[str]:
    """Validate flexible JSON arrays before they become runtime policy."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{field} must be an array of strings")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{field} must contain non-empty strings")
    return [item.strip() for item in value]


def validate_tool_permissions(value: dict[str, Any]) -> dict[str, Any]:
    """Reject malformed capability policy at write time, not during an IM turn."""

    validated = validate_secret_bearing_config(value)
    if "allowlist" in validated:
        _string_array(validated["allowlist"], "tool allowlist")
    if "http_allowed_hosts" in validated:
        hosts = _string_array(validated["http_allowed_hosts"], "http_allowed_hosts")
        if any("://" in host or "/" in host for host in hosts):
            raise ValueError("http_allowed_hosts must contain host names, not URLs")
    raw_grants = validated.get("grants")
    if raw_grants is None:
        return validated
    if not isinstance(raw_grants, Sequence) or isinstance(raw_grants, (str, bytes)):
        raise ValueError("capability grants must be an array")
    for raw_grant in raw_grants:
        if not isinstance(raw_grant, Mapping):
            raise ValueError("capability grant must be an object")
        kind = raw_grant.get("kind")
        name = raw_grant.get("name")
        if kind not in _CAPABILITY_KINDS or not isinstance(name, str) or not name.strip():
            raise ValueError("capability grant kind or name is invalid")
        actions = _string_array(raw_grant.get("actions"), "capability grant actions")
        resources = _string_array(raw_grant.get("resources", []), "capability grant resources")
        expected_action = "load" if kind == "skill" else "execute"
        if actions != [expected_action]:
            raise ValueError(f"{kind} capability must use only the {expected_action} action")
        if kind == "skill" and resources:
            raise ValueError("skill capability cannot declare resources")
        if kind == "mcp":
            if len(resources) != 1:
                raise ValueError("MCP capability requires exactly one connection resource")
            try:
                UUID(resources[0])
            except ValueError as error:
                raise ValueError("MCP capability resource must be a connection UUID") from error
        risk_level = raw_grant.get("risk_level", 0)
        if isinstance(risk_level,
                      bool) or not isinstance(risk_level, int) or risk_level not in range(4):
            raise ValueError("capability risk_level must be an integer from 0 to 3")
        if kind == "mcp" and risk_level not in {0, 2}:
            # MCP has two interaction classes: direct read and confirmed write.
            # Runtime recomputes the effective class from the refreshed catalog,
            # so clients cannot downgrade a mutating Tool by editing this value.
            raise ValueError("MCP capability risk_level must be 0 or 2")
    return validated


class AgentAppStatus(StrEnum):
    """Lifecycle states exposed by the Agent application API."""

    ACTIVE = "active"
    DISABLED = "disabled"


class AgentAppCreate(BaseModel):
    """Tenant-scoped Agent configuration accepted during creation."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(min_length=1, max_length=120)
    model_profile_id: UUID | None = None
    application_config: dict[str, Any] = Field(default_factory=dict)
    model_settings: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias="model_config",
        serialization_alias="model_config",
    )
    tool_permissions: dict[str, Any] = Field(default_factory=dict)
    knowledge_config: dict[str, Any] = Field(default_factory=dict)
    backend_config: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "application_config",
        "model_settings",
        "tool_permissions",
        "knowledge_config",
        "backend_config",
    )
    @classmethod
    def reject_plaintext_secrets(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Require SecretRef URIs for conventionally named credential fields."""

        return validate_secret_bearing_config(value)

    @field_validator("tool_permissions")
    @classmethod
    def validate_capabilities(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_tool_permissions(value)


class AgentAppUpdate(NonNullableUpdateModel):
    """Mutable Agent fields; omitted values remain unchanged."""

    model_config = ConfigDict(populate_by_name=True)

    name: str | None = Field(default=None, min_length=1, max_length=120)
    status: AgentAppStatus | None = None
    model_profile_id: UUID | None = None
    application_config: dict[str, Any] | None = None
    model_settings: dict[str, Any] | None = Field(
        default=None,
        validation_alias="model_config",
        serialization_alias="model_config",
    )
    tool_permissions: dict[str, Any] | None = None
    knowledge_config: dict[str, Any] | None = None
    backend_config: dict[str, Any] | None = None

    @field_validator(
        "application_config",
        "model_settings",
        "tool_permissions",
        "knowledge_config",
        "backend_config",
    )
    @classmethod
    def reject_plaintext_secrets(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """Apply the same secret policy to partial updates."""

        return validate_secret_bearing_config(value)

    @field_validator("tool_permissions")
    @classmethod
    def validate_capabilities(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        return None if value is None else validate_tool_permissions(value)


class AgentAppRead(BaseModel):
    """Public Agent application representation returned by the API."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    agent_app_id: UUID
    tenant_id: UUID
    name: str
    model_profile_id: UUID | None
    status: AgentAppStatus
    application_config: dict[str, Any]
    model_settings: dict[str, Any] = Field(serialization_alias="model_config")
    tool_permissions: dict[str, Any]
    knowledge_config: dict[str, Any]
    backend_config: dict[str, Any]
    stable_config_version: int
    canary_config_version: int | None
    canary_percent: int
    created_at: datetime
    updated_at: datetime


class AgentAppList(BaseModel):
    """Paginated Agent application collection."""

    items: list[AgentAppRead]
    total: int


class AgentConfigVersionCreate(NonNullableUpdateModel):
    """Partial snapshot used to draft one immutable Agent configuration."""

    model_config = ConfigDict(populate_by_name=True)

    model_profile_id: UUID | None = None
    application_config: dict[str, Any] | None = None
    model_settings: dict[str, Any] | None = Field(
        default=None,
        validation_alias="model_config",
        serialization_alias="model_config",
    )
    tool_permissions: dict[str, Any] | None = None
    knowledge_config: dict[str, Any] | None = None
    backend_config: dict[str, Any] | None = None
    reason: str = Field(min_length=12, max_length=500)

    @field_validator(
        "application_config",
        "model_settings",
        "tool_permissions",
        "knowledge_config",
        "backend_config",
    )
    @classmethod
    def validate_snapshot_secrets(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        return validate_secret_bearing_config(value)

    @field_validator("tool_permissions")
    @classmethod
    def validate_capabilities(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        return None if value is None else validate_tool_permissions(value)


class AgentConfigVersionRead(BaseModel):
    """Immutable configuration history returned to tenant operators."""

    model_config = ConfigDict(from_attributes=True)

    tenant_id: UUID
    agent_app_id: UUID
    version: int
    status: str
    snapshot: dict[str, Any]
    created_by: str
    reason: str
    created_at: datetime


class AgentConfigVersionList(BaseModel):
    items: list[AgentConfigVersionRead]
    total: int


class AgentReleaseMode(StrEnum):
    STABLE = "stable"
    CANARY = "canary"


class AgentConfigRelease(BaseModel):
    """Pointer update for stable release or a bounded canary cohort."""

    mode: AgentReleaseMode
    canary_percent: int | None = Field(default=None, ge=1, le=99)
    reason: str = Field(min_length=12, max_length=500)

    @model_validator(mode="after")
    def validate_percentage(self) -> "AgentConfigRelease":
        if self.mode is AgentReleaseMode.CANARY and self.canary_percent is None:
            raise ValueError("canary release requires canary_percent")
        if self.mode is AgentReleaseMode.STABLE and self.canary_percent is not None:
            raise ValueError("stable release cannot set canary_percent")
        return self


class AgentConfigRollback(BaseModel):
    reason: str = Field(min_length=12, max_length=500)


class AgentRolloutRead(BaseModel):
    agent_app_id: UUID
    stable_config_version: int
    canary_config_version: int | None
    canary_percent: int
