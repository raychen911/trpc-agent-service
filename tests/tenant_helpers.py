"""Test-only tenant configuration helpers. Product code must not import this module."""

from __future__ import annotations

from collections.abc import AsyncIterator

from trpc_agent_sdk.memory import InMemoryMemoryService, MemoryServiceConfig
from trpc_agent_sdk.models import LLMModel, LlmResponse
from trpc_agent_sdk.sessions import InMemorySessionService, SessionServiceConfig
from trpc_agent_sdk.types import Content, Part
from trpc_agent_sdk.types import Ttl

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.config.tenant import AgentAppConfig
from trpc_service.config.tenant import TenantAuditPolicy, TenantBackendProfile, TenantConfig, TenantGovernanceConfig
from trpc_service.governance.content_policy import ContentPolicyConfig


class InMemoryTestStateBackend:
    """In-memory SDK state services without background TTL cleanup tasks."""

    def __init__(self) -> None:
        # store_historical_events mirrors the production Redis/SQL backends
        # (R1B): Session Summary compression must retain the raw events.
        self.session_service = InMemorySessionService(session_config=SessionServiceConfig(
            ttl=Ttl(enable=False),
            store_historical_events=True,
        ), )
        self.memory_service = InMemoryMemoryService(memory_service_config=MemoryServiceConfig(
            enabled=True,
            ttl=Ttl(enable=False),
        ), )
        self._closed = False

    def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.session_service.close()
        await self.memory_service.close()


def make_in_memory_state_backend() -> InMemoryTestStateBackend:
    """Create a test backend that cannot leave SDK cleanup tasks pending."""
    return InMemoryTestStateBackend()


def make_app_config(
        app_id: str = "app_demo",
        instruction: str = "You are a helpful assistant.",
        model_profile: str = "default",
        allowed_tools: tuple[str, ...] = ("get_current_time", ),
) -> AgentAppConfig:
    return AgentAppConfig(
        app_id=app_id,
        instruction=instruction,
        model_profile=model_profile,
        allowed_tools=allowed_tools,
    )


def make_governance(
    allowed_channels: tuple[str, ...] = ("web", "web_console", "wecom", "feishu"),
    allowed_user_ids: tuple[str, ...] = (),
    tool_decisions: dict[str, str] | None = None,
    content_policy: ContentPolicyConfig | None = None,
    limits=None,
) -> TenantGovernanceConfig:
    # Stage 6C: ``limits`` is a REQUIRED governance key; the explicit None
    # default preserves pre-6C unlimited behavior (never a silent limit).
    return TenantGovernanceConfig(
        allowed_channels=allowed_channels,
        allowed_user_ids=allowed_user_ids,
        tool_decisions=dict(tool_decisions or {}),
        content_policy=content_policy or ContentPolicyConfig(),
        limits=limits,
    )


def make_backend_profile(state_backend: str = "redis") -> TenantBackendProfile:
    # R1A default mirrors the migration backfill: Redis state, SQL
    # knowledge/audit, S3 artifacts.  Callers flip ``state_backend`` to
    # exercise per-tenant runtime selection.
    return TenantBackendProfile(
        state_backend=state_backend,
        artifact_backend="s3",
        knowledge_backend="sql",
        audit_backend="sql",
    )


def make_audit_policy() -> TenantAuditPolicy:
    """R2A default matching the migration backfill for ordinary test tenants."""
    return TenantAuditPolicy(retention_days=365, delivery_events="all")


def make_tenant_config(
    tenant_id: str,
    enabled: bool = True,
    version: int = 1,
    app: AgentAppConfig | None = None,
    governance: TenantGovernanceConfig | None = None,
    backend_profile: TenantBackendProfile | None = None,
    audit_policy: TenantAuditPolicy | None = None,
) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        enabled=enabled,
        version=version,
        app=app or make_app_config(),
        governance=governance or make_governance(),
        backend_profile=backend_profile or make_backend_profile(),
        audit_policy=audit_policy or make_audit_policy(),
    )


class FakeTenantConfigRepository:
    """In-memory repository for tests. Tracks query counts."""

    def __init__(self, configs: dict[str, TenantConfig]) -> None:
        self._configs = dict(configs)
        self.query_count = 0
        self._closed = False

    async def get(self, tenant_id: str) -> TenantConfig | None:
        self.query_count += 1
        return self._configs.get(tenant_id)

    async def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        self._closed = False
        self._closed = True


def make_default_test_configs() -> dict[str, TenantConfig]:
    """Build the standard test tenant set."""
    return {
        "tenant_default":
        make_tenant_config("tenant_default"),
        "tenant_a":
        make_tenant_config("tenant_a", app=make_app_config(instruction="Tenant A assistant.")),
        "tenant_b":
        make_tenant_config(
            "tenant_b",
            app=make_app_config(instruction="Tenant B assistant.", allowed_tools=()),
        ),
        "tenant_disabled":
        make_tenant_config("tenant_disabled", enabled=False),
    }


class FakeLLMModel(LLMModel):
    """Deterministic LLMModel double for unit tests."""

    def __init__(self, model_name: str = "fake", *, responses: list[LlmResponse] | None = None) -> None:
        super().__init__(model_name=model_name)
        self._responses: list[LlmResponse] = list(responses or [])
        self._default = LlmResponse(content=Content(role="model", parts=[Part.from_text(text="OK")]))
        self.calls: list[list[Content]] = []

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r".*"]

    async def _generate_async_impl(
        self,
        request,
        stream: bool = False,
        ctx=None,
    ) -> AsyncIterator[LlmResponse]:
        self.calls.append(list(request.contents))
        if self._responses:
            yield self._responses.pop(0)
        else:
            yield self._default

    @property
    def call_count(self) -> int:
        return len(self.calls)


class FakeModelProvider:
    """Test model provider supporting multiple profiles with call tracking."""

    def __init__(self, models: dict[str, LLMModel] | None = None) -> None:
        self._models = dict(models or {})
        self.call_count = 0

    def get_model(self, profile: str) -> LLMModel:
        self.call_count += 1
        if profile not in self._models:
            raise TenantAgentConfigurationError()
        return self._models[profile]
