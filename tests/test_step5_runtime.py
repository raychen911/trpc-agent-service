"""Step 5 model, tenant routing, Tool, Admin API, and chat tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.execution import AgentExecutionService, RunAgentCommand
from trpc_service.agent.runtime import ModelProvider, TenantRunnerFactory
from trpc_service.config.models import (
    AgentAppRecord,
    ChannelType,
    StorageBackend,
    TenantRecord,
    TenantStorageConfig,
)
from trpc_service.config.secrets import SecretResolutionError, SecretResolver
from trpc_service.config.settings import ServiceSettings
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import (
    AgentAppRepository,
    AuditLogRepository,
    MemoryRepository,
    SessionEventRepository,
    SessionRepository,
    SummaryRepository,
    TenantRepository,
)
from trpc_service.tenant.session_id import SessionIdFactory
from trpc_service.tool.calculator import calculator
from trpc_service.web.app import create_app
from trpc_service.web.schemas import AgentAppCreate, ChannelBindingCreate


class StaticTestModel(LLMModel):
    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"test-static"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx=None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del request, stream, ctx
        yield LlmResponse(content=Content(parts=[Part.from_text(text="test response")]))


class StaticModelProvider(ModelProvider):
    def create(self, app: AgentAppRecord) -> LLMModel:
        del app
        return StaticTestModel("test-static")


class DuplicateFinalRunner:
    async def run_async(self, **kwargs):
        del kwargs
        for _ in range(2):
            yield Event(
                invocation_id="duplicate-final-test",
                author="assistant",
                content=Content(parts=[Part.from_text(text="one reply")]),
            )


class DuplicateFinalRunnerProvider:
    async def get_runner(self, app: AgentAppRecord) -> DuplicateFinalRunner:
        del app
        return DuplicateFinalRunner()


class SlowRunner:
    async def run_async(self, **kwargs):
        del kwargs
        await asyncio.sleep(0.1)
        yield Event(invocation_id="slow", author="assistant")


class SlowRunnerProvider:
    async def get_runner(self, app: AgentAppRecord) -> SlowRunner:
        del app
        return SlowRunner()


def test_secret_resolver_and_session_ids_are_tenant_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STEP5_SECRET", "a-secure-test-secret")
    assert SecretResolver().resolve("env://STEP5_SECRET") == "a-secure-test-secret"
    with pytest.raises(SecretResolutionError, match="not set"):
        SecretResolver().resolve("env://MISSING_STEP5_SECRET")

    factory = SessionIdFactory("a-secure-test-secret")
    common = {
        "app_id": "assistant",
        "channel": ChannelType.HTTP,
        "account_id": "http-api",
        "principal_id": "same-user",
        "conversation_id": "same-session",
    }
    first = factory.create(tenant_id="tenant-a", **common)
    second = factory.create(tenant_id="tenant-b", **common)
    assert first != second
    assert first == factory.create(tenant_id="tenant-a", **common)
    assert "same-user" not in first


def test_calculator_accepts_arithmetic_and_rejects_code() -> None:
    assert calculator("(2 + 3) * 4") == 20
    assert calculator("7 / 2") == 3.5
    with pytest.raises(ValueError, match="only numeric"):
        calculator("__import__('os').getcwd()")
    with pytest.raises(ValueError, match="exponent"):
        calculator("2 ** 100")


def test_admin_payloads_reject_literal_or_invalid_secrets() -> None:
    with pytest.raises(ValidationError, match="api_key_ref"):
        AgentAppCreate(
            app_id="assistant",
            name="Assistant",
            system_prompt="Be helpful.",
            model_config_data={"api_key": "must-not-be-stored"},
        )
    with pytest.raises(ValidationError, match="secret reference"):
        ChannelBindingCreate(
            binding_id="telegram-main",
            channel_type="telegram",
            account_id="bot-1",
            token_ref="literal://must-not-be-stored",
        )
    with pytest.raises(ValidationError, match="redis_url_ref is required"):
        TenantStorageConfig(session_backend="redis")
    with pytest.raises(ValidationError, match="secret reference"):
        TenantStorageConfig(memory_backend="redis", redis_url_ref="redis://localhost:6379")


@pytest.mark.asyncio
async def test_runner_factory_builds_real_trpc_runner_with_allowed_tool() -> None:
    settings = ServiceSettings(_env_file=None, app_env="test")
    factory = TenantRunnerFactory(settings, model_provider=StaticModelProvider())
    app = AgentAppRecord(
        tenant_id="tenant-a",
        app_id="assistant",
        name="Assistant",
        system_prompt="Be helpful.",
        tool_policy={"allow": ["calculator"]},
    )
    try:
        runner = await factory.get_runner(app)
        assert runner.app_name == "tenant-a:assistant"
        assert runner.agent.name == "tenant_a_assistant"
        assert [tool.name for tool in runner.agent.tools] == ["calculator"]
    finally:
        await factory.close()


@pytest.mark.asyncio
async def test_execution_uses_latest_final_text_without_duplication() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    try:
        await TenantRepository(database).create(TenantRecord(tenant_id="tenant-a", name="A"))
        await AgentAppRepository(database).create(
            AgentAppRecord(
                tenant_id="tenant-a",
                app_id="assistant",
                name="Assistant",
                system_prompt="Be helpful.",
            )
        )
        execution = AgentExecutionService(database, DuplicateFinalRunnerProvider())
        reply = await execution.execute(
            RunAgentCommand(
                tenant_id="tenant-a",
                app_id="assistant",
                user_id="user-a",
                session_id="session-a",
                message="hello",
            )
        )
        assert reply.text == "one reply"
        memories = await MemoryRepository(database).list_for_principal("tenant-a", "user-a")
        summary = await SummaryRepository(database).latest("tenant-a", "session-a")
        assert len(memories) == 1
        assert memories[0].content == "User: hello\nAssistant: one reply"
        assert memories[0].metadata_data["visibility"] == "principal"
        assert summary is not None
        assert summary.content == memories[0].content
        assert summary.source_end_sequence == 2
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_execution_timeout_is_persisted_as_failure() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    try:
        await TenantRepository(database).create(TenantRecord(tenant_id="tenant-a", name="A"))
        await AgentAppRepository(database).create(
            AgentAppRecord(
                tenant_id="tenant-a",
                app_id="assistant",
                name="Assistant",
                system_prompt="Be helpful.",
            )
        )
        execution = AgentExecutionService(database, SlowRunnerProvider(), timeout_seconds=0.01)
        with pytest.raises(TimeoutError):
            await execution.execute(
                RunAgentCommand(
                    tenant_id="tenant-a",
                    app_id="assistant",
                    user_id="user-a",
                    session_id="session-timeout",
                    message="wait",
                )
            )
        events = await SessionEventRepository(database).list_for_session(
            "tenant-a", "session-timeout"
        )
        audits = await AuditLogRepository(database).list_for_tenant("tenant-a")
        assert [event.event_type.value for event in events] == ["user_message", "system"]
        assert audits[0].decision.value == "error"
        assert audits[0].error_type == "TimeoutError"
    finally:
        await database.dispose()


def test_admin_and_chat_keep_two_tenants_isolated(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STEP5_ADMIN_KEY", "admin-secret")
    monkeypatch.setenv("STEP5_SESSION_KEY", "session-secret-at-least-16")
    database_path = tmp_path / "step5.db"
    settings = ServiceSettings(
        _env_file=None,
        app_env="test",
        database_url=f"sqlite+aiosqlite:///{database_path.as_posix()}",
        admin_api_key_ref="env://STEP5_ADMIN_KEY",
        session_hmac_key_ref="env://STEP5_SESSION_KEY",
    )
    database = Database(settings.database_url)
    runners = TenantRunnerFactory(settings, model_provider=StaticModelProvider())
    application = create_app(settings=settings, database=database, runner_provider=runners)
    headers = {"X-Admin-API-Key": "admin-secret"}

    with TestClient(application) as client:
        assert client.get("/admin/tenants").status_code == 401
        for tenant_id in ("tenant-a", "tenant-b"):
            tenant = client.post(
                "/admin/tenants",
                headers=headers,
                json={"tenant_id": tenant_id, "name": tenant_id},
            )
            assert tenant.status_code == 201
            app = client.post(
                f"/admin/tenants/{tenant_id}/apps",
                headers=headers,
                json={
                    "app_id": "assistant",
                    "name": "Assistant",
                    "system_prompt": "Answer clearly.",
                    "tool_policy": {"allow": ["calculator"]},
                },
            )
            assert app.status_code == 201

        assert len(client.get("/admin/tenants", headers=headers).json()) == 2
        responses = []
        for tenant_id in ("tenant-a", "tenant-b"):
            response = client.post(
                "/v1/chat",
                headers={"X-Tenant-ID": tenant_id},
                json={
                    "app_id": "assistant",
                    "user_id": "same-user",
                    "session_id": "same-visible-session",
                    "message": "hello",
                },
            )
            assert response.status_code == 200, response.text
            assert response.json()["reply"] == "test response"
            responses.append(response.json())

        updated = client.put(
            "/admin/tenants/tenant-b/storage",
            headers=headers,
            json={
                "storage_config": {
                    "session_backend": "redis",
                    "memory_backend": "redis",
                    "redis_url_ref": "env://TEST_REDIS_URL",
                }
            },
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["storage_config"]["session_backend"] == "redis"
        missing = client.put(
            "/admin/tenants/missing/storage",
            headers=headers,
            json={"storage_config": {}},
        )
        assert missing.status_code == 404

    assert responses[0]["session_id"] != responses[1]["session_id"]
    assert responses[0]["trace_id"] != responses[1]["trace_id"]

    async def verify_persistence() -> None:
        reopened = Database(settings.database_url)
        try:
            tenant_a_session = await SessionRepository(reopened).get(
                "tenant-a", responses[0]["session_id"]
            )
            tenant_b_session = await SessionRepository(reopened).get(
                "tenant-b", responses[1]["session_id"]
            )
            assert tenant_a_session is not None and tenant_a_session.last_event_sequence == 2
            assert tenant_b_session is not None and tenant_b_session.last_event_sequence == 2
            assert (
                len(
                    await SessionEventRepository(reopened).list_for_session(
                        "tenant-a", responses[0]["session_id"]
                    )
                )
                == 2
            )
            assert len(await AuditLogRepository(reopened).list_for_tenant("tenant-a")) == 1
            memories = await MemoryRepository(reopened).list_for_principal("tenant-a", "same-user")
            assert len(memories) == 1
            assert (
                await SummaryRepository(reopened).latest("tenant-a", responses[0]["session_id"])
                is not None
            )
        finally:
            await reopened.dispose()

    asyncio.run(verify_persistence())


def test_tenant_storage_defaults_are_explicit() -> None:
    tenant = TenantRecord(tenant_id="tenant-a", name="A")
    assert tenant.storage_config.session_backend == StorageBackend.SQLITE
    assert tenant.storage_config.memory_backend == StorageBackend.SQLITE


def test_production_http_chat_requires_tenant_api_key(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROD_MODEL_KEY", "model-secret")
    monkeypatch.setenv("PROD_ADMIN_KEY", "admin-secret")
    monkeypatch.setenv("PROD_SESSION_KEY", "session-secret-at-least-16")
    monkeypatch.setenv("PROD_TENANT_KEY", "tenant-secret")
    settings = ServiceSettings(
        _env_file=None,
        app_env="production",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'production.db').as_posix()}",
        model_provider="openai",
        model_base_url="https://model.example.com/v1",
        model_api_key_ref="env://PROD_MODEL_KEY",
        admin_api_key_ref="env://PROD_ADMIN_KEY",
        session_hmac_key_ref="env://PROD_SESSION_KEY",
    )
    application = create_app(
        settings=settings,
        database=Database(settings.database_url),
        runner_provider=DuplicateFinalRunnerProvider(),
    )
    with TestClient(application) as client:
        admin = {"X-Admin-API-Key": "admin-secret"}
        assert (
            client.post(
                "/admin/tenants",
                headers=admin,
                json={
                    "tenant_id": "tenant-a",
                    "name": "A",
                    "audit_policy": {"http_api_key_ref": "env://PROD_TENANT_KEY"},
                },
            ).status_code
            == 201
        )
        assert (
            client.post(
                "/admin/tenants/tenant-a/apps",
                headers=admin,
                json={
                    "app_id": "assistant",
                    "name": "Assistant",
                    "system_prompt": "Be helpful.",
                },
            ).status_code
            == 201
        )
        body = {
            "app_id": "assistant",
            "user_id": "user-a",
            "session_id": "conversation-a",
            "message": "hello",
        }
        assert (
            client.post(
                "/v1/chat",
                headers={"X-Tenant-ID": "tenant-a", "X-Tenant-API-Key": "wrong"},
                json=body,
            ).status_code
            == 401
        )
        response = client.post(
            "/v1/chat",
            headers={"X-Tenant-ID": "tenant-a", "X-Tenant-API-Key": "tenant-secret"},
            json=body,
        )
        assert response.status_code == 200
        assert response.json()["reply"] == "one reply"
