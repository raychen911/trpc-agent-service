"""Non-sensitive demo configuration for two isolated tenants."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from os import environ as process_environ
from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator

from trpc_service.storage.models import NodeIdentity
from trpc_service.channels.contracts import Channel
from trpc_service.channels.identity import ChannelIdentity
from trpc_service.storage.contracts import SecretBytes, SecretUnavailable
from trpc_service.tenant.models import AgentApplication, ChannelBinding, ResourceStatus, Tenant


class ConfigurationError(RuntimeError):
    pass


class EnvironmentSecretProvider:
    """Resolve short-lived credentials without retaining printable values."""

    __slots__ = ("__source",)

    def __init__(self, source: Mapping[str, str] | None = None) -> None:
        self.__source = process_environ if source is None else source

    def resolve(self, secret_ref: str) -> SecretBytes:
        value = self.__source.get(secret_ref, "").strip()
        if not value:
            raise SecretUnavailable("Channel credential is unavailable.")
        return SecretBytes(value.encode("utf-8"))

    def __repr__(self) -> str:
        return "EnvironmentSecretProvider(<redacted>)"


class ChannelCredentialSettings(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    channel: Channel
    app_or_bot_id: SecretBytes = Field(repr=False)
    secret: SecretBytes = Field(repr=False)

    def __repr__(self) -> str:
        return f"ChannelCredentialSettings(channel={self.channel.value!r}, credentials=<redacted>)"


def load_channel_credentials(
    channel: Channel,
    provider: EnvironmentSecretProvider | None = None,
) -> ChannelCredentialSettings:
    resolver = provider or EnvironmentSecretProvider()
    references = {
        Channel.FEISHU: ("LARK_APP_ID", "LARK_APP_SECRET"),
        Channel.WECOM: ("WECOM_BOT_ID", "WECOM_BOT_SECRET"),
    }
    try:
        identifier_ref, secret_ref = references[channel]
        identifier = resolver.resolve(identifier_ref)
        secret = resolver.resolve(secret_ref)
    except (KeyError, SecretUnavailable):
        raise SecretUnavailable("Channel credential is unavailable.") from None
    return ChannelCredentialSettings(
        channel=channel,
        app_or_bot_id=identifier,
        secret=secret,
    )


class RuntimeProfile(StrEnum):
    LOCAL = "local"
    SHARED = "shared"


class LeaseSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_ms: int = Field(default=10_000, gt=0, le=300_000)
    heartbeat_ms: int = Field(default=3_000, gt=0)
    acquire_wait_ms: int = Field(default=2_000, ge=0, le=60_000)

    @model_validator(mode="after")
    def heartbeat_precedes_expiry(self) -> "LeaseSettings":
        if self.heartbeat_ms >= self.lease_ms:
            raise ValueError("heartbeat must be shorter than the lease")
        return self


class RuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    profile: RuntimeProfile
    node: NodeIdentity
    redis_url: SecretStr | None = None
    database_url: SecretStr | None = None
    lease: LeaseSettings = Field(default_factory=LeaseSettings)
    agent_timeout_seconds: float = Field(default=30, gt=0, le=300)

    @model_validator(mode="after")
    def shared_requires_external_state(self) -> "RuntimeSettings":
        if self.profile == RuntimeProfile.SHARED and (
            self.redis_url is None or self.database_url is None
        ):
            raise ValueError("shared runtime dependencies are required")
        return self


def load_runtime_settings(
    environ: Mapping[str, str] | None = None,
) -> RuntimeSettings:
    source = process_environ if environ is None else environ
    profile_value = source.get("TRPC_RUNTIME_PROFILE", RuntimeProfile.LOCAL.value)
    values: dict[str, object] = {
        "profile": profile_value,
        "node": {
            "node_id": source.get(
                "TRPC_NODE_ID",
                "local-worker" if profile_value == RuntimeProfile.LOCAL else "",
            )
        },
        "agent_timeout_seconds": source.get("TRPC_AGENT_TIMEOUT_SECONDS", "30"),
        "lease": {
            "lease_ms": source.get("TRPC_LEASE_MS", "10000"),
            "heartbeat_ms": source.get("TRPC_LEASE_HEARTBEAT_MS", "3000"),
            "acquire_wait_ms": source.get("TRPC_LEASE_ACQUIRE_WAIT_MS", "2000"),
        },
    }
    if profile_value == RuntimeProfile.SHARED:
        values["redis_url"] = source.get("TRPC_SHARED_REDIS_URL") or None
        values["database_url"] = source.get("TRPC_SHARED_DATABASE_URL") or None
    try:
        return RuntimeSettings.model_validate(values)
    except ValidationError:
        raise ConfigurationError("Runtime configuration is invalid.") from None


class ObservabilitySettings(BaseModel):
    """Bounded observability and release settings (T090, FR-008/009/018).

    Illegal values are REJECTED at load time, never silently corrected:
    a tenant sampling ceiling above 25%, a negative buffer capacity or an
    alert window below the dependency probe TTL fails configuration.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    sampling_default_rate: float = Field(default=0.10, gt=0.0, le=1.0)
    sampling_tenant_ceiling: float = Field(default=0.25, gt=0.0, le=0.25)
    buffer_capacity: int = Field(default=10_000, gt=0, le=1_000_000)
    buffer_critical_reserved_pct: float = Field(default=20.0, ge=0.0, le=100.0)
    alert_fire_window_observations: int = Field(default=3, ge=1, le=60)
    alert_resolve_window_seconds: int = Field(default=60, ge=30, le=3600)
    capacity_max_sessions: int = Field(default=100, gt=0, le=10_000)
    capacity_overhead_ceiling_pct: float = Field(default=10.0, gt=0.0, le=10.0)
    otlp_endpoint: str | None = None


def load_observability_settings(
    environ: Mapping[str, str] | None = None,
) -> ObservabilitySettings:
    source = process_environ if environ is None else environ
    values: dict[str, object] = {
        "sampling_default_rate": source.get("TRPC_OBS_SAMPLING_DEFAULT", "0.10"),
        "sampling_tenant_ceiling": source.get("TRPC_OBS_SAMPLING_CEILING", "0.25"),
        "buffer_capacity": source.get("TRPC_OBS_BUFFER_CAPACITY", "10000"),
        "buffer_critical_reserved_pct": source.get(
            "TRPC_OBS_BUFFER_CRITICAL_PCT", "20"
        ),
        "alert_fire_window_observations": source.get("TRPC_OBS_ALERT_FIRE_WINDOW", "3"),
        "alert_resolve_window_seconds": source.get("TRPC_OBS_ALERT_RESOLVE_S", "60"),
        "capacity_max_sessions": source.get("TRPC_OBS_CAPACITY_SESSIONS", "100"),
        "capacity_overhead_ceiling_pct": source.get("TRPC_OBS_CAPACITY_OVERHEAD", "10"),
    }
    endpoint = source.get("TRPC_OTLP_ENDPOINT", "").strip()
    if endpoint:
        values["otlp_endpoint"] = endpoint
    try:
        return ObservabilitySettings.model_validate(values)
    except ValidationError:
        raise ConfigurationError("Observability configuration is invalid.") from None


class PlatformSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenants: tuple[Tenant, ...]
    agents: tuple[AgentApplication, ...]
    bindings: tuple[ChannelBinding, ...]
    model_credentials_required: bool = False


def build_runtime_channel_binding(
    channel: Channel,
    environ: Mapping[str, str] | None = None,
) -> ChannelBinding:
    """Build a real-IM binding without persisting credential values."""

    source = process_environ if environ is None else environ
    references = {
        Channel.FEISHU: (
            "LARK_TENANT_KEY",
            "LARK_APP_ID",
            "LARK_APP_SECRET",
            "binding-feishu-real",
        ),
        Channel.WECOM: (
            "WECOM_CORP_ID",
            "WECOM_BOT_ID",
            "WECOM_BOT_SECRET",
            "binding-wecom-real",
        ),
    }
    try:
        tenant_key_ref, identifier_ref, secret_ref, binding_id = references[channel]
    except KeyError:
        raise ConfigurationError("A real IM channel is required.") from None
    tenant_key = source.get(tenant_key_ref, "").strip()
    identifier = source.get(identifier_ref, "").strip()
    secret_available = bool(source.get(secret_ref, "").strip())
    if not tenant_key or not identifier or not secret_available:
        raise ConfigurationError("Real channel configuration is unavailable.")
    identity = ChannelIdentity(
        channel=channel,
        provider_tenant_key=tenant_key,
        provider_app_or_bot_id=identifier,
    )
    return ChannelBinding(
        binding_id=binding_id,
        tenant_id="tenant-alpha",
        agent_id="agent-alpha",
        channel=channel,
        status=ResourceStatus.ACTIVE,
        secret_ref=secret_ref,
        signature_version="v1",
        provider_tenant_key=identity.provider_tenant_key,
        provider_app_or_bot_id=identity.provider_app_or_bot_id,
        channel_identity_digest=identity.identity_digest,
        created_at=datetime.now(timezone.utc),
    )


def build_demo_settings() -> PlatformSettings:
    created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tenant_specs = (
        ("tenant-alpha", "Alpha Tenant", "agent-alpha", "Alpha Agent", "binding-alpha", "TRPC_DEMO_ALPHA_SECRET"),
        ("tenant-beta", "Beta Tenant", "agent-beta", "Beta Agent", "binding-beta", "TRPC_DEMO_BETA_SECRET"),
    )
    tenants = tuple(
        Tenant(
            tenant_id=tenant_id,
            display_name=display_name,
            status=ResourceStatus.ACTIVE,
            created_at=created_at,
            config_version=1,
        )
        for tenant_id, display_name, *_ in tenant_specs
    )
    agents = tuple(
        AgentApplication(
            tenant_id=tenant_id,
            agent_id=agent_id,
            agent_name=agent_name,
            status=ResourceStatus.ACTIVE,
            model_profile="deterministic-offline",
            instruction="Run the deterministic validation conversation.",
            config_version=1,
        )
        for tenant_id, _, agent_id, agent_name, _, _ in tenant_specs
    )
    bindings = tuple(
        ChannelBinding(
            binding_id=binding_id,
            tenant_id=tenant_id,
            agent_id=agent_id,
            channel="local_http",
            status=ResourceStatus.ACTIVE,
            secret_ref=secret_ref,
            signature_version="v1",
            created_at=created_at,
        )
        for tenant_id, _, agent_id, _, binding_id, secret_ref in tenant_specs
    )
    return PlatformSettings(tenants=tenants, agents=agents, bindings=bindings)


def load_settings(environ: Mapping[str, str] | None = None) -> PlatformSettings:
    source = process_environ if environ is None else environ
    settings = build_demo_settings()
    if any(not source.get(binding.secret_ref, "").strip() for binding in settings.bindings):
        raise ConfigurationError("Required binding secret is unavailable.")
    return settings
