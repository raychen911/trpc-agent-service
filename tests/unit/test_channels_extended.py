from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from trpc_service.channels.envelopes import DeliveryStatus, OutboundEnvelope, PayloadKind
from trpc_service.channels.wecom import WeComConnector, parse_wecom_frame, sdk_client_factory
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.storage.models import WeComBindingLeaseGrant
from trpc_service.tenant.models import Channel, ChannelBinding


def outbound(*, kind=PayloadKind.TEXT, text="reply") -> OutboundEnvelope:
    return OutboundEnvelope(
        outbound_id="out",
        tenant_id="tenant",
        binding_id="wecom",
        channel=Channel.WECOM_AI_BOT,
        target_id="user",
        session_id="session",
        payload_kind=kind,
        text=text,
    )


def test_parse_all_wecom_frame_variants() -> None:
    voice = parse_wecom_frame(
        {"body": {"from": {"userid": "u"}, "msgtype": "voice", "voice": {"content": "hi"}}},
        account_id="bot",
    )
    assert voice.text == "hi" and voice.external_message_id.startswith("frame_")
    for kind in ("image", "file", "video"):
        value = parse_wecom_frame(
            {
                "body": {
                    "from": {"userid": "u"},
                    "msgtype": kind,
                    kind: {"url": "https://media", "filename": "file"},
                }
            },
            account_id="bot",
        )
        assert value.media[0].filename == "file"
    event = parse_wecom_frame(
        {
            "body": {
                "from": {"userid": "u"},
                "msgtype": "unknown",
                "event": {"eventtype": "recall"},
            }
        },
        account_id="bot",
    )
    assert event.payload_kind == PayloadKind.EVENT and event.event_type == "recall"
    assert event.provider_metadata["target_id"] == "u"
    with pytest.raises(ValueError, match="body"):
        parse_wecom_frame(SimpleNamespace(body="bad"), account_id="bot")
    with pytest.raises(ValueError, match="sender"):
        parse_wecom_frame({"body": {"from": "bad"}}, account_id="bot")


class Lease:
    def __init__(self, allowed=True) -> None:
        self.allowed = allowed
        self.released = []
        self.provider_events = []
        self.authenticated = None
        self.disconnected = None

    async def acquire_binding(self, binding, owner_id):
        if not self.allowed:
            return None
        return WeComBindingLeaseGrant(
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            owner_hash="a" * 64,
            epoch=1,
            acquired_at=datetime.now(UTC),
        )

    async def mark_authenticated(self, grant):
        self.authenticated = grant
        return True

    async def record_provider_event(self, grant, provider_event_id):
        self.provider_events.append((grant, provider_event_id))
        return True

    async def mark_disconnected(self, grant):
        self.disconnected = grant
        return True

    async def release_binding(self, grant):
        self.released.append(grant)


class Client:
    def __init__(self, *, connected=True, async_disconnect=False) -> None:
        self.is_connected = connected
        self.is_authenticated = False
        self.handlers = {}
        self.async_disconnect = async_disconnect
        self.sent = []
        self.send_error = None
        self.response = {"req_id": "provider"}

    def on(self, event, handler):
        self.handlers[event] = handler

    async def connect_async(self):
        if not self.is_connected:
            return
        self.is_authenticated = True
        await self.handlers["authenticated"]()
        handler = self.handlers["message.text"]
        await handler(
            {"body": {"from": {"userid": "u"}, "msgtype": "text", "text": {"content": "x"}}}
        )
        if self.is_connected:
            await self.handlers["disconnected"]()

    def disconnect(self):
        self.is_connected = False
        self.is_authenticated = False
        if self.async_disconnect:

            async def done():
                return None

            return done()
        return None

    async def send_message(self, chat_id, body):
        if self.send_error:
            raise self.send_error
        self.sent.append((chat_id, body))
        return self.response


class AsyncLifecycleClient:
    def __init__(self) -> None:
        self.is_connected = True
        self.is_authenticated = False
        self.handlers = {}
        self.connect_returned = asyncio.Event()
        self.disconnect_calls = 0

    def on(self, event, handler):
        self.handlers[event] = handler

    async def connect_async(self):
        self.connect_returned.set()

    async def authenticate(self):
        self.is_authenticated = True
        await self.handlers["authenticated"]()

    async def provider_disconnect(self):
        self.is_connected = False
        self.is_authenticated = False
        await self.handlers["disconnected"]()

    def disconnect(self):
        self.disconnect_calls += 1
        self.is_connected = False
        self.is_authenticated = False

    async def send_message(self, _chat_id, _body):
        raise AssertionError("send is unavailable before fenced activation")


def wecom_binding(*, secret=True) -> ChannelBinding:
    refs = {"bot_secret": SecretRef(uri="literal://bot-secret")} if secret else {}
    return ChannelBinding(
        binding_id="wecom",
        tenant_id="tenant",
        app_id="app",
        channel=Channel.WECOM_AI_BOT,
        account_id="bot",
        secret_refs=refs,
    )


@pytest.mark.asyncio
async def test_wecom_connector_lifecycle_and_send_contract() -> None:
    denied = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        Lease(False),
        owner_id="owner",
        client_factory=lambda *args: Client(),
    )
    with pytest.raises(RuntimeError, match="owns"):
        await denied.run(wecom_binding(), lambda *args: None)

    lease = Lease()
    missing = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="owner",
        client_factory=lambda *args: Client(),
    )
    with pytest.raises(RuntimeError, match="not configured"):
        await missing.run(wecom_binding(secret=False), lambda *args: None)
    assert lease.released

    client = Client(async_disconnect=True)
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="owner",
        client_factory=lambda *args: client,
    )
    accepted = []

    async def sink(binding_id, inbound):
        accepted.append((binding_id, inbound))

    await connector.run(wecom_binding(), sink)
    assert len(accepted) == 1
    assert accepted[0][1].payload_kind == PayloadKind.TEXT
    assert "event" not in client.handlers
    assert lease.released[-1].binding_id == "wecom"
    assert lease.authenticated == lease.released[-1]
    assert lease.disconnected == lease.released[-1]
    assert lease.provider_events[-1][1] == accepted[0][1].external_message_id

    unavailable = await connector.send(outbound(), wecom_binding())
    assert unavailable.provider_code == "connector_unavailable"
    connector._clients["wecom"] = client
    connector._fenced_bindings.add("wecom")
    client.is_connected = True
    client.is_authenticated = True
    unsupported = await connector.send(outbound(kind=PayloadKind.IMAGE, text=None), wecom_binding())
    assert unsupported.provider_code == "unsupported_payload"
    delivered = await connector.send(outbound(), wecom_binding())
    assert delivered.status == DeliveryStatus.DELIVERED
    client.response = SimpleNamespace(req_id="attribute-id")
    assert (await connector.send(outbound(), wecom_binding())).provider_message_id == "attribute-id"
    client.send_error = TimeoutError()
    assert (await connector.send(outbound(), wecom_binding())).status == DeliveryStatus.AMBIGUOUS


@pytest.mark.asyncio
async def test_wecom_connector_waits_for_provider_ack_and_fencing_before_ready() -> None:
    lease = Lease()
    client = AsyncLifecycleClient()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="owner",
        client_factory=lambda *_: client,
        authentication_timeout_seconds=1,
    )
    task = asyncio.create_task(connector.run(wecom_binding(), lambda *_: None))
    await client.connect_returned.wait()
    await asyncio.sleep(0)
    assert lease.authenticated is None
    assert not connector.ready_for_delivery(wecom_binding())
    assert (await connector.send(outbound(), wecom_binding())).provider_code == (
        "connector_unavailable"
    )

    await client.authenticate()
    assert lease.authenticated is not None
    assert connector.ready_for_delivery(wecom_binding())

    await client.provider_disconnect()
    await asyncio.wait_for(task, timeout=1)
    assert not connector.ready_for_delivery(wecom_binding())
    assert len(lease.released) == 1


@pytest.mark.asyncio
async def test_wecom_connector_authentication_timeout_is_fail_closed() -> None:
    lease = Lease()
    client = AsyncLifecycleClient()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="owner",
        client_factory=lambda *_: client,
        authentication_timeout_seconds=0.01,
    )
    with pytest.raises(TimeoutError, match="authentication timed out"):
        await connector.run(wecom_binding(), lambda *_: None)
    assert lease.authenticated is None
    assert len(lease.released) == 1
    assert client.disconnect_calls == 1


@pytest.mark.asyncio
async def test_wecom_connector_disconnect_or_stop_before_authentication_cannot_reactivate() -> None:
    for stop_first in (False, True):
        lease = Lease()
        client = AsyncLifecycleClient()
        connector = WeComConnector(
            LocalSecretProvider(allow_literal=True),
            lease,
            owner_id="owner",
            client_factory=lambda *_, current=client: current,
            authentication_timeout_seconds=1,
        )
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            connector.run(wecom_binding(), lambda *_: None, stop_event=stop_event)
        )
        await client.connect_returned.wait()
        if stop_first:
            stop_event.set()
        else:
            await client.provider_disconnect()
        await asyncio.wait_for(task, timeout=1)
        await client.authenticate()
        assert lease.authenticated is None
        assert not connector.ready_for_delivery(wecom_binding())
        assert len(lease.released) == 1


@pytest.mark.asyncio
async def test_wecom_connector_fencing_rejection_never_becomes_ready() -> None:
    class RejectingLease(Lease):
        async def mark_authenticated(self, grant):
            self.authenticated = grant
            return False

    lease = RejectingLease()
    client = AsyncLifecycleClient()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="owner",
        client_factory=lambda *_: client,
        authentication_timeout_seconds=1,
    )
    task = asyncio.create_task(connector.run(wecom_binding(), lambda *_: None))
    await client.connect_returned.wait()
    await client.authenticate()
    with pytest.raises(RuntimeError, match="fencing rejected"):
        await asyncio.wait_for(task, timeout=1)
    assert not connector.ready_for_delivery(wecom_binding())
    assert len(lease.released) == 1


def test_sdk_wecom_client_factory_constructs_public_client() -> None:
    client = sdk_client_factory("bot", "secret")
    assert client is not None
