from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tenant_agent.channels.base import (
    DeliveryError,
    PermanentDeliveryError,
    RateLimited,
    SignatureError,
    UnsupportedMessage,
    WebhookRequest,
)
from tenant_agent.channels.planning import plan_outbound
from tenant_agent.channels.wecom_bot import (
    WECOM_BOT_STREAM_BYTES,
    WeComBotAdapter,
    WeComBotConnection,
    bot_outbox_kind,
    validate_bot_credentials,
)
from tenant_agent.container import ApplicationContainer
from tenant_agent.models import ChannelBindingConfig, ChannelType, OutboundMessage, SecretRef
from tenant_agent.security import CompositeSecretResolver
from tenant_agent.services.broker import InlineBroker
from tenant_agent.services.config import TenantConfigService
from tenant_agent.services.dispatcher import GatewayRouter
from tenant_agent.services.wecom_bot import WeComBotManager
from tenant_agent.settings import Settings
from tenant_agent.storage.base import OutboxItem
from tenant_agent.storage.memory import InMemoryPlane
from tenant_agent.storage.sql import SqlPlane
from tests.helpers import make_tenant


def bot_binding() -> ChannelBindingConfig:
    return ChannelBindingConfig(
        binding_id="wecom-bot-001",
        channel=ChannelType.WECOM_BOT,
        app_id="assistant",
        external_account_id="bot-account",
        credential_refs={
            "bot_id": SecretRef(uri="env://TENANT_ALPHA_WECOM_BOT_ID"),
            "bot_secret": SecretRef(uri="env://TENANT_ALPHA_WECOM_BOT_SECRET"),
        },
    )


def test_wecom_bot_credentials_and_outbox_kind_are_bounded() -> None:
    validate_bot_credentials("aibot_12345678", "secret-value-1234567890")
    with pytest.raises(ValueError):
        validate_bot_credentials("short", "secret-value-1234567890")
    with pytest.raises(ValueError):
        validate_bot_credentials("aibot_12345678", "short")
    assert bot_outbox_kind("alpha", "wecom-bot-001").startswith("wb_")
    assert len(bot_outbox_kind("alpha", "wecom-bot-001")) == 32


def test_wecom_bot_frame_maps_direct_group_and_hides_media_capabilities() -> None:
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    adapter = WeComBotAdapter()
    direct = adapter.parse_frame(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "aibot_12345678",
                "chattype": "single",
                "from": {"userid": "alice"},
                "msgtype": "text",
                "text": {"content": "hello"},
            },
        },
        tenant=tenant,
        binding=tenant.channels[0],
        bot_id="aibot_12345678",
    )
    assert direct is not None
    assert direct.channel is ChannelType.WECOM_BOT
    assert direct.external_chat_id == "alice"
    assert direct.metadata["wecom_bot_req_id"] == "req-1"
    group = adapter.parse_frame(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-2"},
            "body": {
                "msgid": "msg-2",
                "aibotid": "aibot_12345678",
                "chattype": "group",
                "chatid": "room-1",
                "from": {"userid": "alice"},
                "msgtype": "image",
                "image": {"url": "https://capability.invalid", "aeskey": "secret"},
            },
        },
        tenant=tenant,
        binding=tenant.channels[0],
        bot_id="aibot_12345678",
    )
    assert group is not None
    assert group.external_chat_id == "room-1"
    assert group.attachments[0].external_id == "msg-2:0"
    assert "capability.invalid" not in group.model_dump_json()
    with pytest.raises(SignatureError):
        adapter.parse_frame(
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-3"},
                "body": {
                    "msgid": "msg-3",
                    "aibotid": "other-bot",
                    "chattype": "single",
                    "from": {"userid": "alice"},
                    "msgtype": "text",
                    "text": {"content": "hello"},
                },
            },
            tenant=tenant,
            binding=tenant.channels[0],
            bot_id="aibot_12345678",
        )


@pytest.mark.asyncio
async def test_wecom_bot_reply_is_atomic_correlated_and_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FakeSocket:
        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            pending = connection._pending[frame["headers"]["req_id"]]
            pending.set_result({"headers": frame["headers"], "errcode": 0, "errmsg": "ok"})

        async def close(self) -> None:
            return None

    connection = WeComBotConnection()
    connection.socket = FakeSocket()  # type: ignore[assignment]
    connection.bot_id = "aibot_12345678"
    adapter = WeComBotAdapter(connection)
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_ID", "aibot_12345678")
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_SECRET", "secret-value-1234567890")
    message = OutboundMessage(
        tenant_id="alpha",
        binding_id="wecom-bot-001",
        channel=ChannelType.WECOM_BOT,
        external_chat_id="alice",
        text="hello",
        stream_key="session:msg",
        metadata={"wecom_bot_req_id": "req-1"},
    )
    result = await adapter.deliver(
        message,
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=CompositeSecretResolver(file_root=tmp_path),
    )
    assert result.external_message_ids[0].startswith("s_")
    with pytest.raises(RateLimited):
        await adapter.deliver(
            message,
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=CompositeSecretResolver(file_root=tmp_path),
        )


def test_wecom_bot_reply_planning_caps_provider_stream_bytes() -> None:
    tenant_id = "alpha"
    message = OutboundMessage(
        tenant_id=tenant_id,
        binding_id="wecom-bot-001",
        channel=ChannelType.WECOM_BOT,
        external_chat_id="alice",
        text="x" * (WECOM_BOT_STREAM_BYTES + 100),
    )
    planned = plan_outbound(message)
    assert len(planned) == 1
    assert len(planned[0].text.encode()) <= WECOM_BOT_STREAM_BYTES
    assert "truncated" in planned[0].text


@pytest.mark.asyncio
async def test_wecom_bot_delivery_rejects_scope_media_and_missing_correlation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    secrets = CompositeSecretResolver(file_root=tmp_path)
    message = OutboundMessage(
        tenant_id="alpha",
        binding_id="wecom-bot-001",
        channel=ChannelType.WECOM_BOT,
        external_chat_id="alice",
        text="hello",
        metadata={"wecom_bot_req_id": "req-1"},
    )
    with pytest.raises(DeliveryError, match="owner"):
        await WeComBotAdapter().deliver(message, tenant=tenant, binding=tenant.channels[0], secrets=secrets)

    class FakeSocket:
        async def send(self, raw: str) -> None:
            del raw

        async def close(self) -> None:
            return None

    connection = WeComBotConnection()
    connection.socket = FakeSocket()  # type: ignore[assignment]
    connection.bot_id = "aibot_12345678"
    adapter = WeComBotAdapter(connection)
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_ID", connection.bot_id)
    with pytest.raises(PermanentDeliveryError, match="scope"):
        await adapter.deliver(
            message.model_copy(update={"tenant_id": "beta"}),
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=secrets,
        )
    with pytest.raises(PermanentDeliveryError, match="correlation"):
        await adapter.deliver(
            message.model_copy(update={"metadata": {}}),
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=secrets,
        )
    with pytest.raises(PermanentDeliveryError, match="media"):
        await adapter.deliver(
            message.model_copy(update={"attachments": (object(),)}),  # type: ignore[arg-type]
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=secrets,
        )


@pytest.mark.asyncio
async def test_wecom_bot_request_handles_provider_errors_and_timeouts() -> None:
    class ResponseSocket:
        def __init__(self, code: object) -> None:
            self.code = code

        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            pending = connection._pending[frame["headers"]["req_id"]]
            pending.set_result({"headers": frame["headers"], "errcode": self.code})

        async def close(self) -> None:
            return None

    connection = WeComBotConnection(timeout=0.01)
    connection.socket = ResponseSocket(45009)  # type: ignore[assignment]
    with pytest.raises(RateLimited):
        await connection.request("aibot_respond_msg")
    connection.socket = ResponseSocket(40001)  # type: ignore[assignment]
    with pytest.raises(DeliveryError, match="rejected"):
        await connection.request("aibot_respond_msg")

    class HangingSocket:
        async def send(self, raw: str) -> None:
            del raw

        async def close(self) -> None:
            return None

    connection.socket = HangingSocket()  # type: ignore[assignment]
    with pytest.raises(DeliveryError, match="timed out"):
        await connection.request("aibot_respond_msg")
    assert connection.closed.is_set()


@pytest.mark.asyncio
async def test_wecom_bot_rejects_http_callbacks_and_invalid_reader_frames(
    tmp_path: Path,
) -> None:
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    with pytest.raises(UnsupportedMessage):
        await WeComBotAdapter().parse(
            WebhookRequest(method="POST", headers={}, query={}, body=b""),
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=CompositeSecretResolver(file_root=tmp_path),
        )

    class InvalidSocket:
        def __aiter__(self) -> InvalidSocket:
            return self

        async def __anext__(self) -> str:
            if hasattr(self, "read"):
                raise StopAsyncIteration
            self.read = True
            return "[]"

    connection = WeComBotConnection()
    connection.socket = InvalidSocket()  # type: ignore[assignment]

    async def ignore(frame: dict[str, object]) -> None:
        del frame

    await connection._read(ignore)
    assert connection.closed.is_set()
    assert connection.failure is not None


def test_wecom_bot_ignores_events_and_rejects_unknown_payloads() -> None:
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    adapter = WeComBotAdapter()
    assert (
        adapter.parse_frame(
            {"cmd": "aibot_event_callback"},
            tenant=tenant,
            binding=tenant.channels[0],
            bot_id="aibot_12345678",
        )
        is None
    )
    base = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req"},
        "body": {
            "msgid": "msg",
            "aibotid": "aibot_12345678",
            "chattype": "single",
            "from": {"userid": "alice"},
            "msgtype": "unsupported",
        },
    }
    with pytest.raises(UnsupportedMessage):
        adapter.parse_frame(base, tenant=tenant, binding=tenant.channels[0], bot_id="aibot_12345678")


@pytest.mark.asyncio
async def test_wecom_bot_card_reply_uses_stream_with_template_card(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeSocket:
        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            connection._pending[frame["headers"]["req_id"]].set_result(
                {"headers": frame["headers"], "errcode": 0}
            )

        async def close(self) -> None:
            return None

    connection = WeComBotConnection()
    connection.socket = FakeSocket()  # type: ignore[assignment]
    connection.bot_id = "aibot_12345678"
    adapter = WeComBotAdapter(connection)
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_ID", connection.bot_id)
    message = OutboundMessage(
        tenant_id="alpha",
        binding_id="wecom-bot-001",
        channel=ChannelType.WECOM_BOT,
        external_chat_id="alice",
        text="hello",
        cards=({"card_type": "text_notice"},),
        metadata={"wecom_bot_req_id": "req-card"},
    )
    result = await adapter.deliver(
        message,
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=CompositeSecretResolver(file_root=tmp_path),
    )
    assert result.external_message_ids


def make_manager(repository: object) -> WeComBotManager:
    return WeComBotManager(
        settings=__import__("tenant_agent.settings", fromlist=["Settings"]).Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=None,
            session_hmac_key="a-long-enough-test-session-hmac-key",
        ),
        configs=TenantConfigService(repository),  # type: ignore[arg-type]
        repository=repository,  # type: ignore[arg-type]
        leases=InMemoryPlane(),
        broker=InlineBroker(),
        gateway=GatewayRouter(
            __import__("tenant_agent.ids", fromlist=["IdentityDeriver"]).IdentityDeriver(
                "a-long-enough-test-session-hmac-key"
            )
        ),
        storage=None,  # type: ignore[arg-type]
        outbox_repository=InMemoryPlane(),
        secrets=CompositeSecretResolver(file_root=Path(".")),
        redactor=__import__("tenant_agent.security", fromlist=["Redactor"]).Redactor(),
    )


@pytest.mark.asyncio
async def test_wecom_bot_manager_refreshes_empty_and_cancels_stale_lanes() -> None:
    class Repository:
        async def list_active_tenants(self) -> list[object]:
            return []

    manager = make_manager(Repository())
    stale = asyncio.create_task(asyncio.sleep(30))
    manager._tasks["stale"] = stale
    await manager._refresh()
    assert stale.cancelled()
    stop = asyncio.Event()
    stop.set()
    await manager.run_forever(stop)


@pytest.mark.asyncio
async def test_wecom_bot_manager_rejects_duplicate_active_bot_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_one = make_tenant(tenant_id="alpha", channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
    tenant_two = make_tenant(tenant_id="bravo", channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-002")
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_ID", "aibot_12345678")
    monkeypatch.setenv("TENANT_BRAVO_WECOM_BOT_ID", "aibot_12345678")

    class Repository:
        async def list_active_tenants(self) -> list[object]:
            return [tenant_one, tenant_two]

    manager = make_manager(Repository())
    with pytest.raises(RuntimeError, match="exactly one"):
        await manager._refresh()


@pytest.mark.parametrize("sql", [False, True])
async def test_outbox_claim_filters_before_incrementing_attempts(tmp_path: Path, sql: bool) -> None:
    repository = (
        SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'outbox.db').as_posix()}") if sql else InMemoryPlane()
    )
    await repository.initialize()
    try:
        now = datetime.now(UTC)
        for outbox_id, kind in (("ordinary", "im-delivery"), ("bot", "wb_binding"), ("other", "wb_other")):
            await repository.enqueue_outbox(
                OutboxItem(
                    outbox_id=outbox_id,
                    tenant_id="alpha",
                    kind=kind,
                    payload={},
                    status="pending",
                    attempts=0,
                    available_at=now,
                )
            )
        bot = await repository.claim_outbox("bot-owner", limit=10, now=now, kinds=("wb_binding",))
        assert [row.outbox_id for row in bot] == ["bot"]
        ordinary = await repository.claim_outbox("ordinary-owner", limit=10, now=now, kinds=("im-delivery",))
        assert [row.outbox_id for row in ordinary] == ["ordinary"]
        remaining = await repository.claim_outbox("other-owner", limit=10, now=now, kinds=("wb_other",))
        assert remaining[0].outbox_id == "other" and remaining[0].attempts == 1
    finally:
        await repository.close()


async def test_real_local_bot_socket_routes_through_worker_and_durable_outbox(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from websockets.asyncio.server import ServerConnection, serve

    bot_id, secret = "aibot_12345678", "local-test-secret-123456789"
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_ID", bot_id)
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_SECRET", secret)
    callbacks = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "callback-one"},
        "body": {
            "msgid": "message-one",
            "aibotid": bot_id,
            "chattype": "single",
            "from": {"userid": "alice"},
            "msgtype": "text",
            "text": {"content": "Hello from the local socket"},
        },
    }
    replies: list[dict[str, object]] = []
    delivered, heartbeat_seen = asyncio.Event(), asyncio.Event()

    async def server(socket: ServerConnection) -> None:
        subscribe = json.loads(await socket.recv())
        assert subscribe["cmd"] == "aibot_subscribe"
        assert subscribe["body"] == {"bot_id": bot_id, "secret": secret}
        await socket.send(json.dumps({"headers": subscribe["headers"], "errcode": 0}))
        await socket.send(json.dumps(callbacks))
        async for raw in socket:
            frame = json.loads(raw)
            await socket.send(json.dumps({"headers": frame["headers"], "errcode": 0}))
            if frame["cmd"] == "aibot_respond_msg":
                replies.append(frame)
                if frame["body"]["stream"]["finish"]:  # type: ignore[index]
                    delivered.set()
                    # A duplicate provider delivery must be absorbed by the
                    # normal receipt path and must not create a second final reply.
                    await socket.send(json.dumps(callbacks))
            elif frame["cmd"] == "ping":
                heartbeat_seen.set()

    async with serve(server, "127.0.0.1", 0) as server_instance:
        port = server_instance.sockets[0].getsockname()[1]
        connections: list[WeComBotConnection] = []

        def local_connection(**kwargs: object) -> WeComBotConnection:
            del kwargs
            connection = WeComBotConnection(url=f"ws://127.0.0.1:{port}", timeout=2, heartbeat_seconds=0.05)
            connections.append(connection)
            return connection

        monkeypatch.setattr("tenant_agent.services.wecom_bot.WeComBotConnection", local_connection)
        container = ApplicationContainer.build(
            Settings(
                control_database_url="inmemory://",
                bootstrap_config_path=None,
                secret_file_root=tmp_path,
                session_hmac_key="a-long-enough-test-session-hmac-key",
                worker_poll_ms=10,
                worker_concurrency=2,
            )
        )
        await container.initialize()
        tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")
        await container.preflight_tenant(tenant)
        await container.configs.create_version(tenant, actor="test", activate=True)
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(container.worker.run_forever(stop)),
            asyncio.create_task(container.wecom_bot.run_forever(stop)),
        ]
        try:
            await asyncio.wait_for(delivered.wait(), timeout=5)
            await asyncio.wait_for(heartbeat_seen.wait(), timeout=5)
            await asyncio.sleep(0.15)
            assert len(replies) == 2
            frame = replies[0]
            assert frame["headers"] == {"req_id": "callback-one"}
            assert frame["body"]["stream"]["finish"] is False  # type: ignore[index]
            assert replies[1]["body"]["stream"]["finish"] is True  # type: ignore[index]
            assert frame["body"]["stream"]["id"] == replies[1]["body"]["stream"]["id"]  # type: ignore[index]
            records = container.control._outbox  # type: ignore[attr-defined]
            assert len(records) == 1
            assert next(iter(records.values())).status == "completed"
            assert await container.outbox.run_once() == 0
            await container.wecom_bot._refresh()
            assert len(connections) == 1
        finally:
            stop.set()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            await container.close()
        assert connections[0].socket is None
        assert not connections[0]._pending
