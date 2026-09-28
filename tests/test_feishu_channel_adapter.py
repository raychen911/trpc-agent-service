import asyncio
from dataclasses import replace
import json
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from trpc_service.channels import (
    ChannelBindingConfig,
    IncomingEnvelope,
    IncomingMessage,
    MessageKind,
    OutgoingMessage,
)
from trpc_service.channels.adapters.feishu import (
    FeishuChannelAdapter,
    FeishuTransportRegistry,
)
from trpc_service.agent.models import AgentApp
from trpc_service.channels.feishu import FeishuMessageService
from trpc_service.channels.feishu_runtime import FeishuBindingSupervisor
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.identity import ResolvedChannelContext
from trpc_service.metrics import PlatformTelemetry


class RecordingFeishuClient:

    def __init__(self) -> None:
        self.sent = []
        self.streamed = []
        self.uploaded = []

    async def send(self, to, message, opts=None):  # type: ignore[no-untyped-def]
        self.sent.append((to, message, opts))
        return SimpleNamespace(success=True, message_id="om_reply_1", error=None)

    async def upload_media(self, source, *, kind, file_name=None):  # type: ignore[no-untyped-def]
        self.uploaded.append((source, kind, file_name))
        return "file_uploaded_1"

    async def stream(self, to, spec, opts=None):  # type: ignore[no-untyped-def]
        chunks = []

        class Controller:

            async def append(self, content):  # type: ignore[no-untyped-def]
                chunks.append(content)

        await spec["markdown"](Controller())
        self.streamed.append((to, chunks, opts))
        return SimpleNamespace(success=True, message_id="om_stream_1", error=None)


class RecordingMediaStore:

    def __init__(self) -> None:
        self.reads = []
        self.puts = []

    async def read(self, binding, artifact_id):  # type: ignore[no-untyped-def]
        self.reads.append((binding.binding_id, artifact_id))
        return b"local-artifact"

    async def put(self, binding, **values):  # type: ignore[no-untyped-def]
        self.puts.append((binding.binding_id, values))
        return "stored-artifact-id"


class LifecycleFeishuClient(RecordingFeishuClient):

    def __init__(self) -> None:
        super().__init__()
        self.handlers = {}
        self.connected = False
        self.disconnected = False

    def on(self, event, handler):  # type: ignore[no-untyped-def]
        self.handlers[event] = handler

    async def connect_until_ready(self, *, timeout=30.0):  # type: ignore[no-untyped-def]
        del timeout
        self.connected = True

    async def disconnect(self):  # type: ignore[no-untyped-def]
        self.disconnected = True

    async def download_resource(self,
                                file_key,
                                resource_type="image",
                                message_id=None):  # type: ignore[no-untyped-def]
        del file_key, resource_type, message_id
        return b"resource"


class RecordingFeishuFactory:

    def __init__(self, client: LifecycleFeishuClient) -> None:
        self.client = client
        self.credentials = []

    def create(self, app_id, app_secret):  # type: ignore[no-untyped-def]
        self.credentials.append((app_id, app_secret))
        return self.client


class RecordingTaskQueue:

    def __init__(self) -> None:
        self.requests = []

    async def enqueue(self, request):  # type: ignore[no-untyped-def]
        self.requests.append(request)
        return "task-feishu"


class PassThroughApprovalCommands:

    async def process(
        self,
        incoming: IncomingMessage,
        binding: ChannelBindingConfig,
        session_id: str,
    ) -> IncomingMessage:
        del binding, session_id
        return incoming


class DeterministicIdentityService:

    async def resolve(
        self,
        binding: ChannelBindingConfig,
        incoming: IncomingMessage,
    ) -> ResolvedChannelContext:
        return ResolvedChannelContext(
            principal_id=uuid5(NAMESPACE_URL, incoming.principal_id),
            conversation_id=uuid5(NAMESPACE_URL, incoming.conversation_id),
            session_id=f"{binding.binding_id}:{incoming.conversation_id}",
        )


def _message_service(adapter, queue, telemetry):  # type: ignore[no-untyped-def]
    return FeishuMessageService(
        adapter,
        queue,
        telemetry,
        PassThroughApprovalCommands(),
        DeterministicIdentityService(),
    )


def _binding() -> ChannelBindingConfig:
    return ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        channel_type="feishu",
        account_config={"app_id": "cli_test"},
    )


@pytest.mark.anyio
async def test_feishu_adapter_normalizes_text_identity_and_thread() -> None:
    adapter = FeishuChannelAdapter(FeishuTransportRegistry())
    binding = _binding()
    payload = {
        "message_id": "om_1",
        "create_time": 1_700_000_000_000,
        "chat_id": "oc_group",
        "chat_type": "group",
        "thread_id": "omt_thread",
        "sender_id": "ou_user",
        "sender_name": "Alice",
        "content_text": "hello",
        "raw_content_type": "text",
        "mentioned_bot": True,
        "resources": [],
    }

    message = await adapter.decode(
        IncomingEnvelope(str(binding.binding_id),
                         json.dumps(payload).encode()),
        binding,
    )

    assert message.principal_id == "ou_user"
    assert message.conversation_id == "thread:omt_thread"
    assert message.kind is MessageKind.TEXT
    assert message.occurred_at.tzinfo is timezone.utc
    assert message.attributes["conversation_kind"] == "thread"


@pytest.mark.anyio
async def test_feishu_adapter_preserves_media_descriptors_for_ingestion() -> None:
    adapter = FeishuChannelAdapter(FeishuTransportRegistry())
    binding = _binding()
    payload = {
        "message_id": "om_file",
        "create_time": 1_700_000_000_000,
        "chat_id": "oc_group",
        "chat_type": "group",
        "sender_id": "ou_user",
        "content_text": "<file name=report.pdf>",
        "raw_content_type": "file",
        "resources": [{
            "type": "file",
            "file_key": "file_1",
            "file_name": "report.pdf"
        }],
    }

    message = await adapter.decode(
        IncomingEnvelope(str(binding.binding_id),
                         json.dumps(payload).encode()),
        binding,
    )

    assert message.kind is MessageKind.FILE
    assert message.attributes["provider_media"][0]["file_key"] == "file_1"


@pytest.mark.anyio
async def test_feishu_adapter_delivers_text_and_provider_media() -> None:
    registry = FeishuTransportRegistry()
    adapter = FeishuChannelAdapter(registry)
    binding = _binding()
    client = RecordingFeishuClient()
    registry.register(binding.binding_id, client)

    await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-1",
            conversation_id="group:oc_group",
            kind=MessageKind.TEXT,
            text="answer",
            attributes={"reply_context": {
                "chat_id": "oc_group",
                "message_id": "om_1"
            }},
        ),
        binding,
    )
    await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-2",
            conversation_id="group:oc_group",
            kind=MessageKind.IMAGE,
            artifact_refs=("img_key_1", ),
            attributes={"reply_context": {
                "chat_id": "oc_group"
            }},
        ),
        binding,
    )

    assert client.sent[0] == ("oc_group", {"markdown": "answer"}, {"reply_to": "om_1"})
    assert client.sent[1][1] == {"image": {"source": "img_key_1"}}


@pytest.mark.anyio
async def test_feishu_adapter_streams_enabled_text_in_multiple_chunks() -> None:
    """A streaming binding must use the provider stream API, not one final send."""

    registry = FeishuTransportRegistry()
    adapter = FeishuChannelAdapter(registry)
    binding = replace(_binding(), capabilities={"streaming": True})
    client = RecordingFeishuClient()
    registry.register(binding.binding_id, client)
    answer = "这是一个需要分多次更新到飞书消息中的较长回答。" * 10

    await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-stream",
            conversation_id="group:oc_group",
            kind=MessageKind.TEXT,
            text=answer,
            attributes={"reply_context": {
                "chat_id": "oc_group",
                "message_id": "om_1"
            }},
        ),
        binding,
    )

    assert client.sent == []
    assert len(client.streamed) == 1
    target, chunks, opts = client.streamed[0]
    assert target == "oc_group"
    assert len(chunks) > 1
    assert "".join(chunks) == answer
    assert opts == {"reply_to": "om_1"}


@pytest.mark.anyio
async def test_feishu_adapter_delivers_card_and_uploads_local_artifact() -> None:
    registry = FeishuTransportRegistry()
    media = RecordingMediaStore()
    adapter = FeishuChannelAdapter(registry, media)  # type: ignore[arg-type]
    binding = _binding()
    client = RecordingFeishuClient()
    registry.register(binding.binding_id, client)

    await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-card",
            conversation_id="group:oc_group",
            kind=MessageKind.CARD,
            attributes={
                "reply_context": {
                    "chat_id": "oc_group"
                },
                "card": {
                    "header": {
                        "title": "Approval"
                    }
                },
            },
        ),
        binding,
    )
    await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-file",
            conversation_id="group:oc_group",
            kind=MessageKind.FILE,
            artifact_refs=("local-artifact-id", ),
            attributes={
                "reply_context": {
                    "chat_id": "oc_group"
                },
                "filename": "report.pdf",
            },
        ),
        binding,
    )

    assert client.sent[0][1] == {"card": {"header": {"title": "Approval"}}}
    assert media.reads == [(binding.binding_id, "local-artifact-id")]
    assert client.uploaded[0][1:] == ("file", "report.pdf")
    assert client.sent[1][1] == {"file": {"source": "file_uploaded_1"}}


@pytest.mark.anyio
async def test_feishu_adapter_acknowledges_and_rejects_invalid_delivery() -> None:
    registry = FeishuTransportRegistry()
    adapter = FeishuChannelAdapter(registry)
    binding = _binding()
    response = await adapter.acknowledge(IncomingEnvelope(str(binding.binding_id), b"{}"), binding)

    assert response.status_code == 202
    with pytest.raises(ValueError, match="reply_context"):
        await adapter.deliver(
            OutgoingMessage(
                delivery_id="delivery-invalid",
                conversation_id="group:oc_group",
                kind=MessageKind.TEXT,
                text="answer",
            ),
            binding,
        )


@pytest.mark.anyio
async def test_feishu_adapter_maps_direct_interactive_message_to_card() -> None:
    adapter = FeishuChannelAdapter(FeishuTransportRegistry())
    binding = _binding()
    payload = {
        "message_id": "om_card",
        "create_time": 1_700_000_000_000,
        "chat_id": "oc_direct",
        "chat_type": "p2p",
        "sender_id": "ou_user",
        "content_text": "confirm",
        "raw_content_type": "interactive",
        "resources": [],
    }

    message = await adapter.decode(
        IncomingEnvelope(str(binding.binding_id),
                         json.dumps(payload).encode()),
        binding,
    )

    assert message.conversation_id == "single:oc_direct"
    assert message.kind is MessageKind.CARD


@pytest.mark.anyio
async def test_feishu_supervisor_connects_binding_and_routes_normalized_message(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    binding = _binding()
    secret_name = (
        f"TRPC_TENANT_{str(binding.tenant_id).replace('-', '_').upper()}_CHANNEL_FEISHU_SECRET")
    monkeypatch.setenv(secret_name, "feishu-secret-value")
    row = ChannelBinding(
        binding_id=binding.binding_id,
        binding_public_id="binding-public-id",
        tenant_id=binding.tenant_id,
        agent_app_id=binding.agent_app_id,
        channel_type="feishu",
        external_account_hash="feishu-account",
        account_config=dict(binding.account_config),
        secret_ref_map={"app_secret": f"env://{secret_name}"},
        capabilities={},
        status="active",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    agent = AgentApp(
        tenant_id=binding.tenant_id,
        agent_app_id=binding.agent_app_id,
        name="Feishu Agent",
        stable_config_version=4,
        status="active",
    )
    transports = FeishuTransportRegistry()
    adapter = FeishuChannelAdapter(transports)
    queue = RecordingTaskQueue()
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="channel",
        otlp_endpoint=None,
    )
    client = LifecycleFeishuClient()
    factory = RecordingFeishuFactory(client)
    media = RecordingMediaStore()
    supervisor = FeishuBindingSupervisor(
        None,  # type: ignore[arg-type]
        adapter,
        transports,
        _message_service(adapter, queue, telemetry),
        telemetry,
        client_factory=factory,
        media_store=media,  # type: ignore[arg-type]
    )

    async def active_bindings():  # type: ignore[no-untyped-def]
        return [(row, agent)]

    monkeypatch.setattr(supervisor, "_active_bindings", active_bindings)
    await supervisor.reconcile_once()
    message = SimpleNamespace(
        message_id="om_inbound",
        create_time=1_700_000_000_000,
        conversation=SimpleNamespace(chat_id="oc_group", chat_type="group", thread_id=None),
        sender=SimpleNamespace(open_id="ou_user", display_name="Alice"),
        body_text="<file name=report.pdf>",
        raw_content_type="file",
        mentioned_bot=True,
        resources=(SimpleNamespace(
            type="file",
            file_key="file_1",
            file_name="report.pdf",
            duration_ms=None,
            cover_image_key=None,
        ), ),
    )
    # The official SDK dispatches callbacks on its own background loop. This
    # verifies project database/queue work is bridged back to the host loop.
    sdk_thread = threading.Thread(
        target=lambda: asyncio.run(client.handlers["message"](message)),
        daemon=True,
    )
    sdk_thread.start()
    for _ in range(100):
        if queue.requests and not sdk_thread.is_alive():
            break
        await asyncio.sleep(0.01)
    sdk_thread.join(timeout=1)
    assert not sdk_thread.is_alive()

    assert factory.credentials == [("cli_test", "feishu-secret-value")]
    assert client.connected
    assert len(queue.requests) == 1
    assert client.sent[0] == (
        "oc_group",
        {
            "markdown": "正在思考..."
        },
        {
            "reply_to": "om_inbound"
        },
    )
    assert queue.requests[0].tenant.config_version == 4
    assert queue.requests[0].session_id.endswith(":group:oc_group")
    assert queue.requests[0].incoming.artifact_refs == ("stored-artifact-id", )
    assert media.puts[0][1]["filename"] == "report.pdf"

    async def no_bindings():  # type: ignore[no-untyped-def]
        return []

    monkeypatch.setattr(supervisor, "_active_bindings", no_bindings)
    await supervisor.reconcile_once()
    assert client.disconnected
