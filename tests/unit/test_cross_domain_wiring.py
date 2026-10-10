from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.tools import FunctionTool

import trpc_service._cli as cli
from trpc_service.agent.registry import RevisionRegistry
from trpc_service.agent.runner import TenantRunner
from trpc_service.agent.wecom_manager import WeComConnectionManager
from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.config.secrets import (
    LocalSecretProvider,
    SecretRef,
    SecretResolutionError,
)
from trpc_service.config.settings import Environment, ServiceSettings
from trpc_service.storage.models import BindingRoute, SessionLease, SessionSnapshot
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ConversationKind,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
    TenantContext,
    ToolPolicy,
)
from trpc_service.tool.confirmation import (
    ConfirmationTokenService,
    InMemoryConfirmationLedger,
)
from trpc_service.tool.governance import (
    GovernancePipeline,
    InMemoryBudgetLedger,
    SdkToolSafetyScanner,
)
from trpc_service.tool.integration import GovernedTool
from trpc_service.workspace import WorkspaceManager


def _config(*, tenant_id: str = "tenant-a", version: int = 1) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        app_id="support",
        version=version,
        model=ModelPolicy(provider="offline", model="deterministic"),
        tools=ToolPolicy(allow=frozenset({"write"})),
        storage=StorageSelection(profile_id="default"),
    )


def _context(*, tenant_id: str = "tenant-a") -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id,
        app_id="support",
        config_version=1,
        channel_binding_id="binding-a",
        principal_id="user-a",
        session_id="session-a",
        request_id="request-a",
        trace_id="trace-a",
    )


def _wecom_binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id="wecom-binding",
        tenant_id="tenant-a",
        app_id="support",
        channel=Channel.WECOM_AI_BOT,
        account_id="wecom-bot",
    )


def _wecom_envelope() -> InboundEnvelope:
    return InboundEnvelope(
        channel=Channel.WECOM_AI_BOT,
        account_id="wecom-bot",
        external_message_id="message-a",
        external_user_id="user-a",
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text="offline test",
        occurred_at=datetime(2026, 8, 23, tzinfo=UTC),
    )


def test_settings_validate_rotation_root_and_production_stale_cache() -> None:
    current = ServiceSettings(
        _env_file=None,
        emergency_queue_key_version="v2",
        emergency_queue_previous_key_refs={"v1": SecretRef(uri="env://OLD_KEY")},
        tenant_secret_root=Path.cwd() / "synthetic-secrets",
    )
    assert current.emergency_queue_key_version == "v2"
    assert tuple(current.emergency_queue_previous_key_refs) == ("v1",)
    assert current.tenant_secret_root == Path.cwd() / "synthetic-secrets"

    with pytest.raises(ValueError, match="current emergency key version"):
        ServiceSettings(
            _env_file=None,
            emergency_queue_key_version="v1",
            emergency_queue_previous_key_refs={"v1": SecretRef(uri="env://OLD_KEY")},
        )
    with pytest.raises(ValueError, match="absolute"):
        ServiceSettings(_env_file=None, tenant_secret_root=Path("relative/secrets"))

    with pytest.raises(ValueError, match="stale Feishu"):
        ServiceSettings(
            _env_file=None,
            environment=Environment.PRODUCTION,
            allow_development_token=False,
            oidc_issuer="https://issuer.example",
            oidc_audience="trpc-service",
            feishu_allow_stale_binding_cache=True,
        )


def test_cli_secret_provider_wires_tenant_allowlist_and_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mounted = tmp_path / "tenant-secret"
    mounted.write_text("synthetic-mounted-secret\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_TENANT_SYNTHETIC", "synthetic-env-secret")
    settings = ServiceSettings(
        _env_file=None,
        tenant_secret_root=tmp_path,
        tenant_secret_env_names=("TRPC_TENANT_SYNTHETIC",),
    )
    provider = cli._secret_provider(settings)

    assert provider.resolve_tenant(SecretRef(uri="env://TRPC_TENANT_SYNTHETIC")) == (
        "synthetic-env-secret"
    )
    assert provider.resolve_tenant(SecretRef(uri=mounted.as_uri())) == ("synthetic-mounted-secret")
    with pytest.raises(SecretResolutionError, match="not registered"):
        provider.resolve_tenant(SecretRef(uri="env://TRPC_TENANT_UNREGISTERED"))
    with pytest.raises(SecretResolutionError, match="outside"):
        provider.resolve_tenant(SecretRef(uri=(tmp_path.parent / "outside").as_uri()))


def test_cli_emergency_queue_wires_current_and_previous_keys() -> None:
    class RedisStub:
        pass

    settings = ServiceSettings(
        _env_file=None,
        emergency_queue_key_version="current",
        emergency_queue_key_ref=SecretRef(uri="literal://" + "c" * 32),
        emergency_queue_previous_key_refs={
            "previous": SecretRef(uri="literal://" + "p" * 32),
        },
    )
    provider = LocalSecretProvider(allow_literal=True)
    queue = cli._emergency_queue(settings, provider, RedisStub())

    assert queue._key_version == "current"
    assert set(queue._keys) == {"current", "previous"}
    assert queue._keys["current"] == b"c" * 32
    assert queue._keys["previous"] == b"p" * 32


@pytest.mark.asyncio
async def test_database_head_check_matches_mismatches_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        def __init__(self, rows):
            self.rows = rows
            self.closed = False

        async def fetch(self, _query):
            return self.rows

        async def close(self):
            self.closed = True

    connections: list[Connection] = []

    class Script:
        @classmethod
        def from_config(cls, _config):
            return SimpleNamespace(get_heads=lambda: ["head-a"])

    monkeypatch.setattr("alembic.script.ScriptDirectory", Script)
    monkeypatch.setattr(cli, "_alembic_config_path", lambda: Path("alembic.ini"))

    async def connect(**_kwargs):
        connection = Connection([{"version_num": "head-a"}])
        connections.append(connection)
        return connection

    monkeypatch.setattr(asyncpg, "connect", connect)
    assert await cli._database_is_at_alembic_head("postgresql+psycopg://unit/db")
    assert connections[-1].closed

    async def mismatch_connect(**_kwargs):
        connection = Connection([{"version_num": "old-head"}])
        connections.append(connection)
        return connection

    monkeypatch.setattr(asyncpg, "connect", mismatch_connect)
    assert not await cli._database_is_at_alembic_head("postgresql://unit/db")

    async def error_connect(**_kwargs):
        raise OSError("synthetic database unavailable")

    monkeypatch.setattr(asyncpg, "connect", error_connect)
    assert not await cli._database_is_at_alembic_head("postgresql://unit/db")


@pytest.mark.asyncio
async def test_wecom_emergency_uses_only_authenticated_resolved_route() -> None:
    binding = _wecom_binding()
    route = BindingRoute(binding=binding, active_config_version=1)

    class Repository:
        def __init__(self, resolved):
            self.resolved = resolved
            self.resolved_ids: list[str] = []

        async def list_bindings(self, _channel):
            return [binding]

        async def resolve_binding(self, binding_id):
            self.resolved_ids.append(binding_id)
            return self.resolved

    class Connector:
        async def run(self, _binding, _sink, stop_event, _emergency):
            await stop_event.wait()

    delivered: list[tuple[BindingRoute, InboundEnvelope]] = []

    async def emergency_sink(authenticated_route, envelope):
        delivered.append((authenticated_route, envelope))

    repository = Repository(route)
    manager = WeComConnectionManager(
        repository, Connector(), lambda *_args: None, emergency_sink=emergency_sink
    )
    await manager.reconcile_once()
    await manager._emergency_for_binding(binding.binding_id, _wecom_envelope())
    assert repository.resolved_ids == [binding.binding_id]
    assert delivered == [(route, delivered[0][1])]

    manager._stop_event.set()
    for task in manager._tasks.values():
        task.cancel()
    await asyncio.gather(*manager._tasks.values(), return_exceptions=True)

    missing = WeComConnectionManager(
        Repository(None), Connector(), lambda *_args: None, emergency_sink=emergency_sink
    )
    await missing.reconcile_once()
    with pytest.raises(RuntimeError, match="authenticated WeCom route"):
        await missing._emergency_for_binding(binding.binding_id, _wecom_envelope())


@pytest.mark.asyncio
async def test_governed_tool_passes_lease_identity_and_rejects_invalid_identity() -> None:
    async def write(value: int) -> int:
        return value

    class Executor:
        def __init__(self):
            self.kwargs = None

        async def execute(self, _context, **kwargs):
            self.kwargs = kwargs
            return "synthetic-result"

    executor = Executor()
    governed = GovernedTool(
        FunctionTool(write),
        config=_config(),
        governance=GovernancePipeline(
            InMemoryBudgetLedger(),
            SdkToolSafetyScanner(),
            ConfirmationTokenService(b"t" * 32, InMemoryConfirmationLedger()),
        ),
        executor=executor,
    )
    metadata = {
        "tenant_id": "tenant-a",
        "app_id": "support",
        "config_version": 1,
        "binding_id": "binding-a",
        "principal_id": "user-a",
        "session_id": "session-a",
        "request_id": "request-a",
        "trace_id": "trace-a",
        "turn_id": "turn-a",
        "lease_owner": "worker-a",
        "lease_epoch": 7,
    }
    invocation = SimpleNamespace(
        agent_context=new_agent_context(metadata=metadata),
        agent=SimpleNamespace(
            before_tool_callback=None,
            after_tool_callback=None,
            parallel_tool_calls=False,
        ),
    )
    assert await governed._run_async_impl(tool_context=invocation, args={"value": 1}) == (
        "synthetic-result"
    )
    assert executor.kwargs["owner_id"] == "worker-a"
    assert executor.kwargs["fencing_token"] == 7

    for invalid in (
        {"lease_owner": "worker-a"},
        {"lease_epoch": 7},
        {"lease_owner": "worker-a", "lease_epoch": 0},
        {"lease_owner": "worker-a", "lease_epoch": "7"},
    ):
        bad_metadata = {
            key: value for key, value in metadata.items() if not key.startswith("lease_")
        }
        bad_metadata.update(invalid)
        bad_invocation = SimpleNamespace(
            agent_context=new_agent_context(metadata=bad_metadata),
            agent=invocation.agent,
        )
        with pytest.raises(ValueError, match="invalid lease identity"):
            await governed._run_async_impl(tool_context=bad_invocation, args={"value": 1})


@pytest.mark.asyncio
async def test_workspace_manager_injects_runner_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    context = _context()
    config = _config()
    snapshot = SessionSnapshot(
        tenant_id=context.tenant_id,
        app_id=context.app_id,
        session_id=context.session_id,
        principal_id=context.principal_id,
    )
    lease = SessionLease(
        tenant_id=context.tenant_id,
        session_id=context.session_id,
        turn_id="turn-a",
        inbound_id="inbound-a",
        worker_id="worker-a",
        fencing_token=3,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
        snapshot=snapshot,
    )
    workspace = WorkspaceManager(tmp_path, key=b"w" * 32).for_context(context)
    captured: dict[str, object] = {}

    class Runner:
        def __init__(self, **_kwargs):
            pass

        async def run_async(self, **kwargs):
            captured.update(kwargs)
            if False:
                yield None

    monkeypatch.setattr("trpc_service.agent.runner.Runner", Runner)

    async def load_agent(_config):
        return SimpleNamespace()

    runner = TenantRunner(
        config=config,
        lease=lease,
        registry=RevisionRegistry(),
        agent_loader=load_agent,
        workspace=workspace,
    )
    assert [event async for event in runner.run(context, _wecom_envelope())] == []
    metadata = captured["agent_context"].metadata
    assert metadata["workspace_root"] == str(workspace.path)
    assert metadata["lease_owner"] == "worker-a"
    assert metadata["lease_epoch"] == 3
