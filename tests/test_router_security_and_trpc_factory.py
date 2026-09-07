from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from tenant_agent.agent.trpc import TrpcAgentEngine
from tenant_agent.container import ApplicationContainer
from tenant_agent.governance.policies import ConfirmationManager, GovernanceService
from tenant_agent.models import (
    AgentAppConfig,
    BackendKind,
    BackendRef,
    DataBackendConfig,
    ModelConfig,
    SecretRef,
)
from tenant_agent.security import (
    CompositeSecretResolver,
    Redactor,
    SecretResolutionError,
)
from tenant_agent.services.native_session import provision_native_sql_schema
from tenant_agent.settings import Settings
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.external import FilesystemArtifactRepository
from tenant_agent.storage.memory import InMemoryPlane
from tenant_agent.storage.router import StorageRouter
from tenant_agent.storage.sql import SqlPlane
from tests.helpers import make_tenant


@pytest.mark.asyncio
async def test_storage_router_builds_mixed_backends_and_reuses_pools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sql_url = f"sqlite+aiosqlite:///{(tmp_path / 'data.db').as_posix()}"
    monkeypatch.setenv("TENANT_ALPHA_SQL_URL", sql_url)
    monkeypatch.setenv("TENANT_ALPHA_SQL_URL_ALIAS", sql_url)
    control = InMemoryPlane()
    settings = Settings(
        control_database_url="inmemory://",
        bootstrap_config_path=None,
        session_hmac_key="a-long-enough-test-hmac-key",
        runner_cache_max_entries=1,
    )
    secrets = CompositeSecretResolver(file_root=tmp_path)
    router = StorageRouter(
        settings=settings,
        control=control,
        control_plane=control,
        secrets=secrets,
    )
    await router.initialize()
    sql = BackendRef(kind=BackendKind.SQL, dsn_ref=SecretRef(uri="env://TENANT_ALPHA_SQL_URL"))
    tenant = make_tenant().model_copy(
        update={
            "data_backends": DataBackendConfig(
                session=sql,
                memory=BackendRef(
                    kind=BackendKind.SQL,
                    namespace="another-logical-resource",
                    dsn_ref=SecretRef(uri="env://TENANT_ALPHA_SQL_URL_ALIAS"),
                ),
                summary=sql,
                artifact=BackendRef(
                    kind=BackendKind.FILESYSTEM,
                    namespace="artifacts",
                    options={"root": str(tmp_path)},
                ),
                knowledge=BackendRef(
                    kind=BackendKind.LOCAL_VECTOR,
                    namespace="vectors",
                    options={"path": str(tmp_path / "vectors.db")},
                ),
                audit=BackendRef(kind=BackendKind.INMEMORY),
            )
        }
    )
    initialize_started = asyncio.Event()
    allow_initialize = asyncio.Event()
    original_initialize = SqlPlane.initialize

    async def controlled_initialize(adapter: SqlPlane) -> None:
        initialize_started.set()
        await allow_initialize.wait()
        await original_initialize(adapter)

    monkeypatch.setattr(SqlPlane, "initialize", controlled_initialize)
    first_task = asyncio.create_task(router.for_tenant(tenant))
    await initialize_started.wait()
    second_task = asyncio.create_task(router.for_tenant(tenant))
    await asyncio.sleep(0)
    assert not second_task.done()
    allow_initialize.set()
    first, second = await asyncio.gather(first_task, second_task)
    assert isinstance(first.sessions, SqlPlane)
    assert first.sessions is first.memories is first.summaries
    assert first.sessions is second.sessions
    assert router._initialized[id(first.sessions)] is first.sessions  # type: ignore[attr-defined]
    assert isinstance(first.artifacts, FilesystemArtifactRepository)
    assert isinstance(first.knowledge, SqlPlane)
    await first.sessions.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )

    invalid = tenant.model_copy(
        update={
            "data_backends": tenant.data_backends.model_copy(
                update={
                    "artifact": BackendRef(
                        kind=BackendKind.REDIS,
                        dsn_ref=SecretRef(uri="env://TENANT_ALPHA_SQL_URL"),
                    )
                }
            )
        }
    )
    with pytest.raises(ValueError, match="cannot back"):
        await router.for_tenant(invalid)
    await router.close()

    production_router = StorageRouter(
        settings=settings.model_copy(update={"environment": "production"}),
        control=control,
        control_plane=control,
        secrets=secrets,
    )
    await production_router.initialize()
    with pytest.raises(ValueError, match="shared tenant backends"):
        await production_router.for_tenant(make_tenant())
    with pytest.raises(ValueError, match="shared tenant backends"):
        await production_router.preflight_tenant(make_tenant())

    role_checks: list[SqlPlane] = []

    async def check_runtime_role(adapter: SqlPlane) -> None:
        role_checks.append(adapter)

    monkeypatch.setattr(SqlPlane, "assert_runtime_role_unprivileged", check_runtime_role)
    session_sql = sql.model_copy(update={"native_dsn_ref": SecretRef(uri="env://TENANT_ALPHA_SQL_URL_ALIAS")})
    production_sql_tenant = make_tenant().model_copy(
        update={
            "data_backends": DataBackendConfig(
                session=session_sql,
                memory=sql,
                summary=sql,
                artifact=sql,
                knowledge=sql,
                audit=sql,
            )
        }
    )
    await production_router.preflight_tenant(production_sql_tenant)
    assert len(role_checks) == 6
    assert len({id(adapter) for adapter in role_checks}) == 1
    await production_router.close()

    bounded_router = StorageRouter(
        settings=settings.model_copy(update={"storage_adapter_cache_max_entries": 1}),
        control=control,
        control_plane=control,
        secrets=secrets,
    )
    await bounded_router.initialize()
    with pytest.raises(RuntimeError, match="cache limit"):
        await bounded_router.for_tenant(tenant)
    await bounded_router.close()


@pytest.mark.asyncio
async def test_activation_preflight_requires_secret_and_backend_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TENANT_ALPHA_WEBHOOK_TOKEN", raising=False)
    container = ApplicationContainer.build(
        Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=None,
            session_hmac_key="a-long-enough-test-hmac-key",
        )
    )
    await container.initialize()
    tenant = make_tenant()
    with pytest.raises(SecretResolutionError):
        await container.preflight_tenant(tenant)

    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "resolved")

    async def unhealthy() -> bool:
        return False

    container.storage.inmemory.healthcheck = unhealthy  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="not ready"):
        await container.preflight_tenant(tenant)
    await container.close()


@pytest.mark.asyncio
async def test_secret_resolver_vault_success_and_safe_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MISSING_SECRET", raising=False)
    resolver = CompositeSecretResolver(file_root=tmp_path)
    with pytest.raises(SecretResolutionError):
        await resolver.resolve(SecretRef(uri="env://MISSING_SECRET"))
    with pytest.raises(SecretResolutionError):
        await resolver.resolve(SecretRef(uri="file://missing"))
    with pytest.raises(SecretResolutionError):
        await resolver.resolve(SecretRef(uri="aws-secretsmanager://secret"))
    with pytest.raises(SecretResolutionError):
        await resolver.resolve(SecretRef(uri="vault://kv/data/app#key"))

    monkeypatch.setenv("VAULT_TOKEN", "vault-auth-token")
    real_client = httpx.AsyncClient

    vault_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal vault_requests
        vault_requests += 1
        assert request.headers["x-vault-token"] == "vault-auth-token"
        return httpx.Response(200, json={"data": {"data": {"key": "resolved-value"}}})

    import tenant_agent.security as security_module

    monkeypatch.setattr(
        security_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler)),
    )
    vault = CompositeSecretResolver(
        file_root=tmp_path,
        vault_address="https://vault.example",
        cache_ttl_seconds=60,
    )
    reference = SecretRef(uri="vault://kv/data/app#key")
    assert await asyncio.gather(*(vault.resolve(reference) for _ in range(5))) == ["resolved-value"] * 5
    assert await vault.resolve(reference) == "resolved-value"
    assert vault_requests == 1

    token_file = tmp_path / "vault-token"
    token_file.write_text("first-file-token", encoding="utf-8")
    observed_tokens: list[str] = []

    def rotating_handler(request: httpx.Request) -> httpx.Response:
        observed_tokens.append(request.headers["x-vault-token"])
        return httpx.Response(200, json={"data": {"data": {"key": "rotated-value"}}})

    monkeypatch.setattr(
        security_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(rotating_handler)),
    )
    rotating_vault = CompositeSecretResolver(
        file_root=tmp_path,
        vault_address="https://vault.example",
        vault_token_file=token_file,
    )
    assert await rotating_vault.resolve(reference) == "rotated-value"
    token_file.write_text("second-file-token", encoding="utf-8")
    assert await rotating_vault.resolve(reference) == "rotated-value"
    assert observed_tokens == ["first-file-token", "second-file-token"]


def remote_tenant(provider: str) -> object:
    tenant = make_tenant(tools=frozenset())
    model = ModelConfig(
        provider=provider,  # type: ignore[arg-type]
        model_name="provider/model" if provider == "litellm" else "model",
        api_key_ref=SecretRef(uri="env://TENANT_ALPHA_MODEL_KEY"),
        base_url="https://model.example/v1",
    )
    return tenant.model_copy(
        update={
            "models": {"remote": model},
            "apps": {
                "assistant": AgentAppConfig(
                    app_id="assistant",
                    agent_name="RemoteAssistant",
                    instruction="Help.",
                    model_profile="remote",
                    allowed_tools=frozenset(),
                )
            },
        }
    )


@pytest.mark.asyncio
async def test_trpc_model_factories_runner_cache_and_sql_session_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_MODEL_KEY", "model-key")
    monkeypatch.setenv(
        "TENANT_ALPHA_SESSION_SQL",
        f"sqlite+aiosqlite:///{(tmp_path / 'platform-session.db').as_posix()}",
    )
    monkeypatch.setenv(
        "TENANT_ALPHA_NATIVE_SESSION_SQL",
        f"sqlite+aiosqlite:///{(tmp_path / 'native-session.db').as_posix()}",
    )
    memory = InMemoryPlane()
    settings = Settings(
        control_database_url="inmemory://",
        bootstrap_config_path=None,
        session_hmac_key="a-long-enough-test-hmac-key",
        runner_cache_max_entries=1,
    )
    engine = TrpcAgentEngine(
        settings=settings,
        secrets=CompositeSecretResolver(file_root=tmp_path),
        governance=GovernanceService(Redactor()),
        confirmations=ConfirmationManager(b"a-long-enough-test-hmac-key", memory),
    )
    from trpc_agent_sdk.models import AnthropicModel, LiteLLMModel, OpenAIModel

    openai_tenant = remote_tenant("openai-compatible")
    anthropic_tenant = remote_tenant("anthropic")
    litellm_tenant = remote_tenant("litellm")
    assert isinstance(await engine._model(openai_tenant, "assistant"), OpenAIModel)  # type: ignore[arg-type]
    assert isinstance(await engine._model(anthropic_tenant, "assistant"), AnthropicModel)  # type: ignore[arg-type]
    assert isinstance(await engine._model(litellm_tenant, "assistant"), LiteLLMModel)  # type: ignore[arg-type]

    data = TenantDataPlane(
        sessions=memory,
        memories=memory,
        summaries=memory,
        artifacts=memory,
        knowledge=memory,
        audit=memory,
        receipts=memory,
        usage=memory,
        concurrency=memory,
        outbox=memory,
        leases=memory,
    )
    first = await engine._bundle(openai_tenant, "assistant", data)  # type: ignore[arg-type]
    second = await engine._bundle(openai_tenant, "assistant", data)  # type: ignore[arg-type]
    assert first is second
    await engine._release_bundle(first.cache_key)
    await engine._release_bundle(first.cache_key)
    revision_two = openai_tenant.model_copy(update={"revision": 2})  # type: ignore[union-attr]
    third = await engine._bundle(revision_two, "assistant", data)
    assert third is not first
    assert first.cache_key not in engine._runners

    sql_backend = BackendRef(
        kind=BackendKind.SQL,
        dsn_ref=SecretRef(uri="env://TENANT_ALPHA_SESSION_SQL"),
        native_dsn_ref=SecretRef(uri="env://TENANT_ALPHA_NATIVE_SESSION_SQL"),
    )
    platform_plane = SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'platform-session.db').as_posix()}")
    await platform_plane.initialize()
    await platform_plane.close()
    await provision_native_sql_schema(f"sqlite+aiosqlite:///{(tmp_path / 'native-session.db').as_posix()}")
    sql_tenant = openai_tenant.model_copy(  # type: ignore[union-attr]
        update={
            "data_backends": openai_tenant.data_backends.model_copy(  # type: ignore[union-attr]
                update={"session": sql_backend}
            )
        }
    )
    native_role_checks: list[str] = []

    async def check_native_role(dsn: str) -> None:
        native_role_checks.append(dsn)

    monkeypatch.setattr(
        "tenant_agent.agent.trpc.assert_native_sql_runtime_role_unprivileged",
        check_native_role,
    )
    engine.settings = settings.model_copy(update={"environment": "production"})
    service = await engine._session_service(sql_tenant)
    assert native_role_checks == [f"sqlite+aiosqlite:///{(tmp_path / 'native-session.db').as_posix()}"]
    session = await service.create_session(app_name="test", user_id="user", session_id="session")
    assert session.id == "session"
    loaded_session = await service.get_session(
        app_name="test",
        user_id="user",
        session_id="session",
    )
    assert loaded_session is not None
    assert loaded_session.id == "session"
    await service.close()
    shared_backend = sql_backend.model_copy(
        update={"native_dsn_ref": SecretRef(uri="env://TENANT_ALPHA_SESSION_SQL")}
    )
    shared_tenant = sql_tenant.model_copy(
        update={"data_backends": sql_tenant.data_backends.model_copy(update={"session": shared_backend})}
    )
    with pytest.raises(ValueError, match="must be different"):
        await engine._session_service(shared_tenant)
    await engine.close()
