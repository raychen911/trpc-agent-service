"""Long-connection supervisor tests through reconciliation and callback seams."""

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.admin.models import ChannelAdapterType
from trpc_service.agent.models import AgentApp
from trpc_service.channels.adapters.feishu import (
    FeishuChannelAdapter,
    FeishuTransportRegistry,
)
from trpc_service.channels.adapters.wecom import WeComChannelAdapter, WeComTransportRegistry
from trpc_service.channels.feishu_runtime import FeishuBindingSupervisor
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.wecom_runtime import WeComBindingSupervisor
from trpc_service.metrics import PlatformTelemetry
from trpc_service.storage.orm import Base
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import Tenant


class _Messages:

    def __init__(self) -> None:
        self.submissions: list[tuple[object, object, TenantContext]] = []

    async def submit(self, frame: object, binding: object, tenant: TenantContext) -> str:
        self.submissions.append((frame, binding, tenant))
        if isinstance(frame, Mapping):
            return str(frame.get("message_id", "message"))
        return "message"


class _FeishuClient:

    def __init__(self, *, fail_connect: bool = False, resource: bytes | None = b"media") -> None:
        self.handlers: dict[str, Callable[..., object]] = {}
        self.fail_connect = fail_connect
        self.resource = resource
        self.connected = False
        self.disconnected = False

    def on(self, event: str, handler: Callable[..., object]) -> object:
        self.handlers[event] = handler
        return self

    async def connect_until_ready(self, *, timeout: float | None = 30.0) -> None:
        assert timeout == 30.0
        if self.fail_connect:
            raise ConnectionError("provider unavailable")
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def download_resource(
        self,
        file_key: str,
        resource_type: str = "image",
        message_id: str | None = None,
    ) -> bytes | None:
        del file_key, resource_type, message_id
        return self.resource


class _FeishuFactory:

    def __init__(self, clients: list[_FeishuClient]) -> None:
        self.clients = clients
        self.credentials: list[tuple[str, str]] = []

    def create(self, app_id: str, app_secret: str) -> _FeishuClient:
        self.credentials.append((app_id, app_secret))
        return self.clients.pop(0)


class _WeComClient:

    def __init__(self, *, fail_connect: bool = False) -> None:
        self.handlers: dict[str, Callable[..., object]] = {}
        self.fail_connect = fail_connect
        self.connected = False
        self.disconnected = False

    def on(self, event: str, handler: Callable[..., object]) -> "_WeComClient":
        self.handlers[event] = handler
        return self

    async def connect(self) -> "_WeComClient":
        if self.fail_connect:
            raise ConnectionError("provider unavailable")
        self.connected = True
        authenticated = self.handlers.get("authenticated")
        assert authenticated is not None
        authenticated()
        return self

    async def disconnect(self) -> None:
        self.disconnected = True

    async def reply_stream(
        self,
        frame: dict[str, object],
        stream_id: str,
        content: str,
        finish: bool = False,
    ) -> dict[str, object]:
        del frame, stream_id, content, finish
        return {"errcode": 0}


class _WeComFactory:

    def __init__(self, clients: list[_WeComClient]) -> None:
        self.clients = clients
        self.credentials: list[tuple[str, str]] = []

    def create(self, bot_id: str, secret: str) -> _WeComClient:
        self.credentials.append((bot_id, secret))
        return self.clients.pop(0)


def _telemetry() -> PlatformTelemetry:
    return PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="channel",
        otlp_endpoint=None,
    )


async def _database(tmp_path: Path, channel_type: str) -> tuple[Any, Any, ChannelBinding]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / f'{channel_type}.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    tenant_id = uuid4()
    agent_id = uuid4()
    secret_name = f"TRPC_TENANT_{str(tenant_id).replace('-', '_').upper()}_CHANNEL_TEST_SECRET"
    account_config = ({
        "app_id": "cli_test"
    } if channel_type == "feishu" else {
        "bot_id": "aibot-test"
    })
    secret_key = "app_secret" if channel_type == "feishu" else "bot_secret"
    binding = ChannelBinding(
        binding_id=uuid4(),
        binding_public_id=f"public-{channel_type}",
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        channel_type=channel_type,
        external_account_hash=f"sha256:{channel_type}",
        account_config=account_config,
        secret_ref_map={secret_key: f"env://{secret_name}"},
        capabilities={},
        status="active",
    )
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name=f"{channel_type} tenant"))
        await database.flush()
        database.add(
            AgentApp(
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                name=f"{channel_type} agent",
                stable_config_version=1,
            ))
        database.add(
            ChannelAdapterType(
                channel_type=channel_type,
                display_name=channel_type,
                adapter_version="test",
                status="active",
            ))
        await database.flush()
        database.add(binding)
    return engine, sessions, binding


def _feishu_message(*, resources: tuple[object, ...] = ()) -> object:
    return SimpleNamespace(
        message_id="om_test",
        create_time=1_700_000_000_000,
        conversation=SimpleNamespace(chat_id="oc_test", chat_type="p2p", thread_id=None),
        sender=SimpleNamespace(open_id="ou_test", display_name="Alice"),
        body_text="你好",
        content_text="",
        raw_content_type="text",
        mentioned_bot=True,
        resources=resources,
    )


async def _wait_for_submissions(messages: _Messages, count: int) -> None:
    for _ in range(100):
        if len(messages.submissions) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError("scheduled IM callback did not complete")


@pytest.mark.anyio
async def test_feishu_supervisor_reconciles_database_and_callback_lifecycle(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    engine, sessions, binding = await _database(tmp_path, "feishu")
    secret_name = binding.secret_ref_map["app_secret"].removeprefix("env://")
    monkeypatch.setenv(secret_name, "feishu-secret")
    client = _FeishuClient()
    factory = _FeishuFactory([client])
    transports = FeishuTransportRegistry()
    messages = _Messages()
    supervisor = FeishuBindingSupervisor(
        sessions,
        FeishuChannelAdapter(transports),
        transports,
        messages,  # type: ignore[arg-type]
        _telemetry(),
        client_factory=factory,
        poll_interval_seconds=0.01,
    )

    with pytest.raises(ValueError, match="interval"):
        FeishuBindingSupervisor(
            sessions,
            FeishuChannelAdapter(FeishuTransportRegistry()),
            FeishuTransportRegistry(),
            messages,  # type: ignore[arg-type]
            _telemetry(),
            client_factory=factory,
            poll_interval_seconds=0,
        )

    await supervisor.reconcile_once()
    assert client.connected
    assert transports.resolve(binding.binding_id) is client
    callback = client.handlers["message"]
    await callback(_feishu_message())  # type: ignore[misc]
    await _wait_for_submissions(messages, 1)
    assert messages.submissions[0][2].config_version == 1
    await client.handlers["error"](RuntimeError("provider event"))  # type: ignore[misc]

    # Reconciliation refreshes rollout pointers without reconnecting the IM.
    async with sessions.begin() as database:
        agent = await database.get(AgentApp, binding.agent_app_id)
        assert agent is not None
        agent.stable_config_version = 2
    await supervisor.reconcile_once()
    await callback(_feishu_message())  # type: ignore[misc]
    await _wait_for_submissions(messages, 2)
    assert messages.submissions[1][2].config_version == 2

    supervisor.stop_ingress()
    await callback(_feishu_message())  # type: ignore[misc]
    await asyncio.sleep(0)
    assert len(messages.submissions) == 2

    async with sessions.begin() as database:
        row = await database.get(ChannelBinding, binding.binding_id)
        assert row is not None
        row.status = "disabled"
    await supervisor.reconcile_once()
    assert client.disconnected
    with pytest.raises(ConnectionError):
        transports.resolve(binding.binding_id)
    await supervisor.close()
    await engine.dispose()


@pytest.mark.anyio
async def test_feishu_supervisor_handles_pending_secret_connect_failure_and_task_loop(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    engine, sessions, binding = await _database(tmp_path, "feishu")
    secret_name = binding.secret_ref_map["app_secret"].removeprefix("env://")
    monkeypatch.delenv(secret_name, raising=False)
    messages = _Messages()
    supervisor = FeishuBindingSupervisor(
        sessions,
        FeishuChannelAdapter(FeishuTransportRegistry()),
        FeishuTransportRegistry(),
        messages,  # type: ignore[arg-type]
        _telemetry(),
        client_factory=_FeishuFactory([_FeishuClient(fail_connect=True)]),
        poll_interval_seconds=0.001,
    )
    await supervisor.reconcile_once()
    await supervisor.reconcile_once()

    monkeypatch.setenv(secret_name, "now-configured")
    await supervisor.reconcile_once()
    # The failed connection is removed and the supervisor remains healthy.
    await supervisor.start()
    await supervisor.start()
    await asyncio.sleep(0.003)
    await supervisor.close()
    await engine.dispose()


def _wecom_frame() -> dict[str, object]:
    return {
        "headers": {
            "req_id": "request-1"
        },
        "body": {
            "msgid": "message-1",
            "aibotid": "aibot-test",
            "chattype": "single",
            "from": {
                "userid": "zhangsan"
            },
            "msgtype": "text",
            "create_time": 1_700_000_000,
            "text": {
                "content": "你好"
            },
        },
    }


@pytest.mark.anyio
async def test_wecom_supervisor_reconciles_database_and_callback_lifecycle(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    engine, sessions, binding = await _database(tmp_path, "wecom")
    secret_name = binding.secret_ref_map["bot_secret"].removeprefix("env://")
    monkeypatch.setenv(secret_name, "wecom-secret")
    client = _WeComClient()
    factory = _WeComFactory([client])
    transports = WeComTransportRegistry()
    messages = _Messages()
    supervisor = WeComBindingSupervisor(
        sessions,
        WeComChannelAdapter(transports),
        transports,
        messages,  # type: ignore[arg-type]
        _telemetry(),
        client_factory=factory,
        poll_interval_seconds=0.01,
    )

    with pytest.raises(ValueError, match="interval"):
        WeComBindingSupervisor(
            sessions,
            WeComChannelAdapter(WeComTransportRegistry()),
            WeComTransportRegistry(),
            messages,  # type: ignore[arg-type]
            _telemetry(),
            client_factory=factory,
            poll_interval_seconds=0,
        )

    await supervisor.reconcile_once()
    assert client.connected
    assert transports.resolve(binding.binding_id) is client
    callback = client.handlers["message.text"]
    await callback("invalid")  # type: ignore[misc]
    assert messages.submissions == []
    await callback(_wecom_frame())  # type: ignore[misc]
    assert len(messages.submissions) == 1
    assert messages.submissions[0][2].config_version == 1

    disconnected = client.handlers["disconnected"]
    disconnected("network")
    with pytest.raises(ConnectionError):
        transports.resolve(binding.binding_id)
    client.handlers["authenticated"]()
    assert transports.resolve(binding.binding_id) is client

    supervisor.stop_ingress()
    await callback(_wecom_frame())  # type: ignore[misc]
    assert len(messages.submissions) == 1

    async with sessions.begin() as database:
        row = await database.get(ChannelBinding, binding.binding_id)
        assert row is not None
        row.status = "disabled"
    await supervisor.reconcile_once()
    assert client.disconnected
    await supervisor.close()
    await engine.dispose()


@pytest.mark.anyio
async def test_wecom_supervisor_survives_connection_failure_and_task_loop(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    engine, sessions, binding = await _database(tmp_path, "wecom")
    secret_name = binding.secret_ref_map["bot_secret"].removeprefix("env://")
    monkeypatch.setenv(secret_name, "wecom-secret")
    supervisor = WeComBindingSupervisor(
        sessions,
        WeComChannelAdapter(WeComTransportRegistry()),
        WeComTransportRegistry(),
        _Messages(),  # type: ignore[arg-type]
        _telemetry(),
        client_factory=_WeComFactory([_WeComClient(fail_connect=True)]),
        poll_interval_seconds=0.001,
    )

    await supervisor.reconcile_once()
    await supervisor.start()
    await supervisor.start()
    await asyncio.sleep(0.003)
    await supervisor.close()
    await engine.dispose()
