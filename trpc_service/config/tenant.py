"""Immutable tenant configuration models using Pydantic 2."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic import StrictBool as _StrictBool
from pydantic import StrictInt as _StrictInt

from trpc_service.channels.models import validate_channel
from trpc_service.governance.content_policy import ContentPolicyConfig
from trpc_service.tenant.context import InvalidTenantIdError
from trpc_service.tenant.context import validate_tenant_id as _validate_tenant_id

ToolDecision = Literal["allow", "deny", "review"]
"""Per-tool governance verdict; ``review`` blocks pending human approval."""

StateBackendKind = Literal["redis", "sql"]
"""Session/Memory state backend classes selectable per tenant (R1A)."""


class TenantBackendProfile(BaseModel):
    """Typed per-tenant data-backend capability bundle (Stage R1A).

    All four fields are REQUIRED — a stored configuration must state the
    backend explicitly so runtime selection can never fall back to a
    different backend by omission.  ``artifact/knowledge/audit`` are pinned to
    their single production implementation for now (R1B/R1C widen the
    literals); ``state_backend`` selects between the Redis and PostgreSQL
    Session/Memory backends at runtime.  Frozen + extra-forbid keeps the
    profile inside the versioned config/history/rollback boundary.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    state_backend: StateBackendKind
    artifact_backend: Literal["s3"]
    knowledge_backend: Literal["sql"]
    audit_backend: Literal["sql"]


_INTERNAL_USER_ID_PATTERN: re.Pattern[str] = re.compile(r"^usr_v1_[0-9a-f]{48}$")

_FIXED_CHANNEL_ERROR = "allowed_channels must be unique non-blank valid channels"
_FIXED_USER_ERROR = "allowed_user_ids must be unique internal usr_v1 projected IDs"
_FIXED_DECISION_KEY_ERROR = "tool_decisions keys must be non-blank tool names"
_FIXED_DECISION_SCOPE_ERROR = "tool_decisions may only reference allowed_tools"


class TenantConfigError(ValueError):
    """Raised when tenant configuration is invalid."""


class AgentAppConfig(BaseModel):
    """Configuration for an agent application within a tenant."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    app_id: str
    instruction: str
    model_profile: str
    allowed_tools: tuple[str, ...]

    @field_validator("app_id", "instruction", "model_profile")
    @classmethod
    def _must_be_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must be a non-blank string")
        return v.strip()

    @field_validator("allowed_tools", mode="before")
    @classmethod
    def _tools_must_be_valid(cls, v: object) -> tuple[str, ...]:
        if not isinstance(v, (list, tuple)):
            raise ValueError("must be a list")
        result: list[str] = []
        seen: set[str] = set()
        for item in v:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("each tool must be a non-blank string")
            tool = item.strip()
            if tool in seen:
                raise ValueError("duplicate tool not allowed")
            seen.add(tool)
            result.append(tool)
        return tuple(result)


class _FrozenDict(dict):
    """dict subclass that rejects every in-place mutation.

    ``model_config=frozen`` only blocks field rebinding; cached TenantConfig
    objects are shared across requests, so the decision mapping itself must
    refuse ``__setitem__``/``update``/… or a request path could rewrite the
    policy behind the version/history/hot-reload boundary. Reads and JSON
    serialization behave exactly like a plain dict.
    """

    __slots__ = ()

    def _immutable(self, *args: object, **kwargs: object):
        raise TypeError("tool_decisions is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    pop = _immutable
    popitem = _immutable
    clear = _immutable
    update = _immutable
    setdefault = _immutable
    __ior__ = _immutable


class TenantLimitConfig(BaseModel):
    """Versioned per-tenant rate/budget limits (Stage 6C).

    ``requests_per_minute`` is mandatory once a limit policy is attached;
    the daily token/cost budgets are optional hard stops.  Booleans and ints
    are strict; there are no silent defaults that could enable a limit for a
    tenant that never configured one.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    requests_per_minute: _StrictInt = Field(ge=1)
    daily_total_tokens: _StrictInt | None = Field(default=None, ge=1)
    daily_cost_microunits: _StrictInt | None = Field(default=None, ge=1)


class TenantAuditPolicy(BaseModel):
    """Versioned retention and delivery-audit policy for one tenant.

    The policy intentionally has no defaults: a tenant write must choose both
    its retention window and whether successful IM delivery events are kept.
    It therefore stays inside the same version/history boundary as the rest
    of :class:`TenantConfig`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    retention_days: _StrictInt = Field(ge=1, le=3650)
    delivery_events: Literal["all", "failures"]


class TenantGovernanceConfig(BaseModel):
    """Tenant-level channel/user admission and per-tool decisions (Stage 6A1).

    ``allowed_user_ids`` stores only internal projected ``usr_v1_*`` IDs, never
    raw IM user IDs.  An empty user list allows every user of the admitted
    channels.  ``tool_decisions`` may only reference tools present in the same
    configuration's ``allowed_tools``; omitted tools default to ``allow``.

    ``limits`` is REQUIRED (Stage 6C): every new write must state it
    explicitly — ``null`` preserves the pre-6C unlimited behavior, and a
    ``TenantLimitConfig`` enables rate/budget enforcement.  Migration 0007
    backfills an explicit ``null`` over legacy head/history rows so no old
    configuration silently gains a limit at a new code version.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    allowed_channels: tuple[str, ...]
    allowed_user_ids: tuple[str, ...] = ()
    tool_decisions: dict[str, ToolDecision] = Field(default_factory=dict)
    content_policy: ContentPolicyConfig
    limits: TenantLimitConfig | None

    @field_validator("allowed_channels", mode="before")
    @classmethod
    def _validate_channels(cls, v: object) -> tuple[str, ...]:
        if not isinstance(v, (list, tuple)):
            raise ValueError(_FIXED_CHANNEL_ERROR)
        result: list[str] = []
        for item in v:
            if not isinstance(item, str):
                raise ValueError(_FIXED_CHANNEL_ERROR)
            try:
                channel = validate_channel(item)
            except ValueError:
                raise ValueError(_FIXED_CHANNEL_ERROR) from None
            result.append(channel)
        if not result:
            raise ValueError(_FIXED_CHANNEL_ERROR)
        if len(set(result)) != len(result):
            raise ValueError(_FIXED_CHANNEL_ERROR)
        return tuple(result)

    @field_validator("allowed_user_ids", mode="before")
    @classmethod
    def _validate_user_ids(cls, v: object) -> tuple[str, ...]:
        if not isinstance(v, (list, tuple)):
            raise ValueError(_FIXED_USER_ERROR)
        result: list[str] = []
        for item in v:
            if not isinstance(item, str):
                raise ValueError(_FIXED_USER_ERROR)
            candidate = item.strip()
            if _INTERNAL_USER_ID_PATTERN.fullmatch(candidate) is None:
                raise ValueError(_FIXED_USER_ERROR)
            result.append(candidate)
        if len(set(result)) != len(result):
            raise ValueError(_FIXED_USER_ERROR)
        return tuple(result)

    @field_validator("tool_decisions", mode="before")
    @classmethod
    def _validate_decision_keys(cls, v: object) -> dict[str, ToolDecision]:
        if not isinstance(v, dict):
            raise ValueError(_FIXED_DECISION_KEY_ERROR)
        result: dict[str, ToolDecision] = {}
        for key in v:
            if not isinstance(key, str) or not key.strip():
                raise ValueError(_FIXED_DECISION_KEY_ERROR)
            normalized = key.strip()
            if normalized in result:
                # two spellings collapsing to one tool must never silently
                # last-wins overwrite
                raise ValueError(_FIXED_DECISION_KEY_ERROR)
            result[normalized] = v[key]  # type: ignore[assignment]
        return result

    @field_validator("tool_decisions")
    @classmethod
    def _seal_decisions(cls, v: dict[str, ToolDecision]) -> dict[str, ToolDecision]:
        return _FrozenDict(v)


def _check_decision_scope(app: AgentAppConfig, governance: TenantGovernanceConfig) -> None:
    unknown = set(governance.tool_decisions) - set(app.allowed_tools)
    if unknown:
        raise ValueError(_FIXED_DECISION_SCOPE_ERROR)


class TenantConfig(BaseModel):
    """Configuration for a single tenant."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: str
    enabled: _StrictBool
    version: _StrictInt = Field(ge=1)
    app: AgentAppConfig
    governance: TenantGovernanceConfig
    backend_profile: TenantBackendProfile
    audit_policy: TenantAuditPolicy

    @field_validator("tenant_id")
    @classmethod
    def _validate_tenant_id(cls, v: str) -> str:
        try:
            _validate_tenant_id(v)
        except InvalidTenantIdError:
            raise ValueError("invalid tenant ID format") from None
        return v

    @model_validator(mode="after")
    def _decisions_within_allowed_tools(self) -> "TenantConfig":
        _check_decision_scope(self.app, self.governance)
        return self


class TenantConfigDraft(BaseModel):
    """Write-side payload for creating or updating a tenant configuration.

    Callers never specify the resulting version; version assignment belongs
    to the repository.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    enabled: _StrictBool
    app: AgentAppConfig
    governance: TenantGovernanceConfig
    backend_profile: TenantBackendProfile
    audit_policy: TenantAuditPolicy

    @model_validator(mode="after")
    def _decisions_within_allowed_tools(self) -> "TenantConfigDraft":
        _check_decision_scope(self.app, self.governance)
        return self


__all__ = [
    "AgentAppConfig",
    "StateBackendKind",
    "TenantBackendProfile",
    "TenantAuditPolicy",
    "TenantConfig",
    "TenantConfigDraft",
    "TenantConfigError",
    "TenantGovernanceConfig",
    "ToolDecision",
]
