"""Backend profiles, capabilities, and immutable tenant bindings."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from trpc_service.tenant import BackendKind
from trpc_service.tenant import SecretRef


_IDENTIFIER_PATTERN = r"^[a-z][a-z0-9_-]{2,63}$"


class ResourceType(str, Enum):
    SESSION = "session"
    MEMORY = "memory"
    SUMMARY = "summary"
    KNOWLEDGE = "knowledge"
    ARTIFACT = "artifact"
    AUDIT = "audit"


class BackendCapability(str, Enum):
    SESSION = "session"
    MEMORY = "memory"
    SUMMARY = "summary"
    KNOWLEDGE = "knowledge"
    ARTIFACT = "artifact"
    AUDIT = "audit"
    LEASE = "lease"


class BackendHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


_KIND_CAPABILITIES = {
    BackendKind.IN_MEMORY: {
        BackendCapability.SESSION,
        BackendCapability.MEMORY,
        BackendCapability.SUMMARY,
        BackendCapability.KNOWLEDGE,
        BackendCapability.ARTIFACT,
    },
    BackendKind.REDIS: {
        BackendCapability.SESSION,
        BackendCapability.MEMORY,
        BackendCapability.SUMMARY,
        BackendCapability.LEASE,
    },
    BackendKind.SQL: {
        BackendCapability.SESSION,
        BackendCapability.MEMORY,
        BackendCapability.SUMMARY,
        BackendCapability.AUDIT,
        BackendCapability.LEASE,
    },
    BackendKind.VECTOR: {BackendCapability.KNOWLEDGE, BackendCapability.MEMORY},
    BackendKind.OBJECT: {BackendCapability.ARTIFACT},
    BackendKind.EXTERNAL_MEMORY: {BackendCapability.MEMORY},
}


class BackendProfile(BaseModel):
    """Operator-managed connection metadata; credentials remain references."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    kind: BackendKind
    provider: str = Field(min_length=1, max_length=64)
    endpoint: str = Field(min_length=1, max_length=2048)
    region: str | None = Field(default=None, max_length=128)
    secret_ref: SecretRef
    options: dict[str, Any] = Field(default_factory=dict)
    capabilities: frozenset[BackendCapability]
    profile_revision: int = Field(ge=1)
    enabled: bool = True
    health: BackendHealth = BackendHealth.HEALTHY

    @model_validator(mode="after")
    def validate_capabilities(self) -> "BackendProfile":
        unsupported = self.capabilities - _KIND_CAPABILITIES[self.kind]
        if unsupported:
            raise ValueError(f"capabilities {sorted(item.value for item in unsupported)} not supported by backend kind")
        return self


class TenantBackendBinding(BaseModel):
    """One immutable resource placement in a published tenant config."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    agent_app_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    config_version: int = Field(ge=1)
    resource_type: ResourceType
    profile_id: str = Field(pattern=_IDENTIFIER_PATTERN)
    profile_revision: int = Field(ge=1)
    namespace: str = Field(min_length=1, max_length=255)
    storage_revision: int = Field(ge=1)


class BackendProfileRegistry:
    """Exact, immutable profile revision lookup."""

    def __init__(self, profiles=()) -> None:
        self._profiles: dict[tuple[str, int], BackendProfile] = {}
        for profile in profiles:
            self.register(profile)

    def register(self, profile: BackendProfile) -> None:
        key = (profile.profile_id, profile.profile_revision)
        if key in self._profiles:
            raise ValueError(f"duplicate backend profile revision: {key}")
        self._profiles[key] = profile

    def resolve(self, profile_id: str, profile_revision: int) -> BackendProfile:
        try:
            profile = self._profiles[(profile_id, profile_revision)]
        except KeyError as error:
            raise LookupError(f"backend profile {profile_id} revision {profile_revision} not found") from error
        if not profile.enabled:
            raise LookupError(f"backend profile {profile_id} revision {profile_revision} is disabled")
        return profile


def validate_tenant_bindings(
    tenant_id: str,
    bindings: list[TenantBackendBinding] | tuple[TenantBackendBinding, ...],
    registry: BackendProfileRegistry,
    *,
    production: bool = False,
    worker_count: int = 1,
) -> dict[ResourceType, tuple[TenantBackendBinding, BackendProfile]]:
    """Validate tenant scope, capability, uniqueness, and production safety."""

    resolved: dict[ResourceType, tuple[TenantBackendBinding, BackendProfile]] = {}
    for binding in bindings:
        if binding.tenant_id != tenant_id or binding.namespace != tenant_id:
            raise ValueError("backend binding namespace and tenant_id must equal the trusted tenant")
        if binding.resource_type in resolved:
            raise ValueError(f"duplicate binding for resource {binding.resource_type.value}")
        profile = registry.resolve(binding.profile_id, binding.profile_revision)
        capability = BackendCapability(binding.resource_type.value)
        if capability not in profile.capabilities:
            raise ValueError(f"profile lacks required {binding.resource_type.value} capability")
        if production and worker_count > 1 and profile.kind is BackendKind.IN_MEMORY:
            raise ValueError("in-memory backends are forbidden in multi-worker production")
        resolved[binding.resource_type] = (binding, profile)

    if ResourceType.SUMMARY in resolved and ResourceType.SESSION in resolved:
        summary_binding, summary_profile = resolved[ResourceType.SUMMARY]
        session_binding, session_profile = resolved[ResourceType.SESSION]
        if (summary_binding.profile_id, summary_profile.kind) != (session_binding.profile_id, session_profile.kind):
            raise ValueError("summary must use the session profile and backend kind")
    return resolved
