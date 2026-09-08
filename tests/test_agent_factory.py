import asyncio

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.runners import Runner

from trpc_service.agent import AgentFactory, AgentRuntimeConfigRepository
from trpc_service.config import EnvironmentSecretResolver
from trpc_service.storage import Database
from trpc_service.tenant.schemas import (
    AgentAppCreate,
    BackendConfigDraft,
    DraftConfigUpdate,
    ModelConfigDraft,
    TenantCreate,
)
from trpc_service.tenant.service import ControlPlaneService


def test_agent_factory_builds_active_trpc_runtime(monkeypatch) -> None:
    monkeypatch.setenv("TEST_MODEL_API_KEY", "test-key")
    database = Database("sqlite+pysqlite:///:memory:")
    database.create_schema()
    with database.session_factory() as session:
        service = ControlPlaneService(session)
        tenant = service.create_tenant(TenantCreate(slug="factory-tenant", name="Factory"))
        app = service.create_app(
            tenant.id,
            AgentAppCreate(
                slug="factory-agent",
                name="Factory Agent",
                instruction="Answer carefully.",
            ),
        )
        service.update_draft(
            tenant.id,
            app.id,
            DraftConfigUpdate(
                expected_lock_version=1,
                instruction="Answer carefully.",
                model=ModelConfigDraft(
                    provider="openai-compatible",
                    model_name="test-model",
                    api_key_secret_ref="env://TEST_MODEL_API_KEY",
                    parameters={"temperature": 0.1},
                ),
                backends=[
                    BackendConfigDraft(
                        backend_kind="session",
                        backend_type="inmemory",
                    ),
                    BackendConfigDraft(
                        backend_kind="memory",
                        backend_type="inmemory",
                    ),
                ],
            ),
        )
        service.publish(tenant.id, app.id, expected_lock_version=2)
        tenant_id, app_id = tenant.id, app.id

    async def scenario() -> None:
        factory = AgentFactory(
            AgentRuntimeConfigRepository(database.session_factory),
            EnvironmentSecretResolver(),
        )
        runtime = await factory.build(tenant_id, app_id)
        cached = await factory.build(tenant_id, app_id)
        assert runtime is cached
        assert runtime.config_version == 1
        assert isinstance(runtime.agent, LlmAgent)
        assert isinstance(runtime.runner, Runner)
        await factory.invalidate(tenant_id, app_id)
        rebuilt = await factory.build(tenant_id, app_id)
        assert rebuilt is not runtime
        await factory.close()

    try:
        asyncio.run(scenario())
    finally:
        database.dispose()
