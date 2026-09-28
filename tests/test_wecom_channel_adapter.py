import json
from dataclasses import replace
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from trpc_service.channels import (
    ChannelBindingConfig,
    IncomingEnvelope,
    IncomingMessage,
    MessageKind,
)
from trpc_service.channels.adapters.wecom import (
    WeComChannelAdapter,
    WeComTransportRegistry,
)
from trpc_service.channels.contracts import OutgoingMessage
from tests.test_agent_task_queue import _request
from tests.test_storage_router import StubSessionStore
from trpc_service.agent import (
    AgentExecutionClaim,
    AgentExecutionContext,
    AgentReply,
    AgentRuntimeConfig,
    AgentRunResult,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.channels.wecom import WeComMessageService
from trpc_service.channels.wecom_runtime import WeComBindingSupervisor
from trpc_service.metrics import PlatformTelemetry
from trpc_service.agent.models import AgentApp
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.identity import ResolvedChannelContext
from trpc_service.agent.runtime import StorageResultCommitter
from trpc_service.agent.recovery import ProviderOutcomeUnknown
from trpc_service.storage import BackendProfile, ExecutionCommit, ResolvedStorage
from trpc_service.storage.types import SessionSnapshot


def _binding() -> ChannelBindingConfig:
    tenant_id = uuid4()
    return ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=tenant_id,
        agent_app_id=uuid4(),
        channel_type="wecom",
        account_config={
            "bot_id": "aibot-test",
            "thinking_message": "正在思考...",
        },
        secret_ref_map={
            "bot_secret":
            (f"env://TRPC_TENANT_{str(tenant_id).replace('-', '_').upper()}_CHANNEL_WECOM_SECRET")
        },
    )


def _frame(*, chat_type: str, user_id: str, chat_id: str | None = None) -> dict[str, object]:
    body: dict[str, object] = {
        "msgid": "message-001",
        "aibotid": "aibot-test",
        "chattype": chat_type,
        "from": {
            "userid": user_id
        },
        "msgtype": "text",
        "create_time": 1_788_163_200,
        "text": {
            "content": "你好"
        },
    }
    if chat_id is not None:
        body["chatid"] = chat_id
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": "request-frame-001"
        },
        "body": body,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("chat_type", "user_id", "chat_id", "expected_conversation"),
    [
        ("single", "zhangsan", None, "single:zhangsan"),
        ("group", "lisi", "group-001", "group:group-001"),
    ],
)
async def test_wecom_adapter_normalizes_private_and_group_text(
    chat_type: str,
    user_id: str,
    chat_id: str | None,
    expected_conversation: str,
) -> None:
    """WeCom protocol details stop at the provider-neutral Channel seam."""

    binding = _binding()
    frame = _frame(chat_type=chat_type, user_id=user_id, chat_id=chat_id)
    adapter = WeComChannelAdapter(WeComTransportRegistry())

    incoming = await adapter.decode(
        IncomingEnvelope(
            binding_public_id="binding-public-id",
            body=json.dumps(frame).encode(),
        ),
        binding,
    )

    assert incoming.external_message_id == "message-001"
    assert incoming.principal_id == f"{binding.tenant_id}:wecom:{user_id}"
    assert incoming.conversation_id == expected_conversation
    assert incoming.kind is MessageKind.TEXT
    assert incoming.text == "你好"
    assert incoming.occurred_at == datetime.fromtimestamp(1_788_163_200, timezone.utc)
    reply_context = incoming.attributes["reply_context"]
    assert reply_context == {
        "req_id": "request-frame-001",
        "stream_id": reply_context["stream_id"],
    }
    assert isinstance(reply_context["stream_id"], str)
    assert reply_context["stream_id"].startswith("trpc-")


class _RecordingWeComClient:

    def __init__(self) -> None:
        self.replies: list[dict[str, object]] = []

    async def reply_stream(
        self,
        frame: dict[str, object],
        stream_id: str,
        content: str,
        finish: bool = False,
    ) -> dict[str, object]:
        self.replies.append({
            "frame": frame,
            "stream_id": stream_id,
            "content": content,
            "finish": finish,
        })
        return {
            "headers": {
                "req_id": "request-frame-001"
            },
            "errcode": 0,
        }


class _MediaStore:

    def __init__(self) -> None:
        self.saved: list[tuple[str, bytes, str]] = []

    async def put(self,
                  binding,
                  *,
                  principal_id,
                  message_id,
                  content,
                  filename,
                  media_type,
                  context=None):  # type: ignore[no-untyped-def]
        del binding, principal_id, media_type, context
        self.saved.append((message_id, content, filename))
        return "artifact-001"

    async def read(self, binding, artifact_id):  # type: ignore[no-untyped-def]
        del binding, artifact_id
        return b"outgoing-image"


class _MediaWeComClient(_RecordingWeComClient):

    def __init__(self) -> None:
        super().__init__()
        self.media_replies: list[tuple[str, str]] = []

    async def download_file(self, url, aes_key=None):  # type: ignore[no-untyped-def]
        del url, aes_key
        return {"buffer": b"incoming-image", "filename": "download.png"}

    async def upload_media(self, file_data, *, type, filename):  # type: ignore[no-untyped-def]
        assert file_data == b"outgoing-image"
        assert type == "image"
        assert filename == "answer.png"
        return {"media_id": "media-uploaded"}

    async def reply_media(self, frame, media_type, media_id):  # type: ignore[no-untyped-def]
        del frame
        self.media_replies.append((media_type, media_id))
        return {"errcode": 0}


class _LifecycleWeComClient(_RecordingWeComClient):

    def __init__(self) -> None:
        super().__init__()
        self.handlers: dict[str, object] = {}
        self.connected = False
        self.disconnected = False

    def on(self, event: str, handler: object) -> "_LifecycleWeComClient":
        self.handlers[event] = handler
        return self

    async def connect(self) -> "_LifecycleWeComClient":
        self.connected = True
        self.handlers["authenticated"]()
        return self

    async def disconnect(self) -> None:
        self.disconnected = True


class _UnknownOutcomeWeComClient(_RecordingWeComClient):

    async def reply_stream(
        self,
        frame: dict[str, object],
        stream_id: str,
        content: str,
        finish: bool = False,
    ) -> dict[str, object]:
        del frame, stream_id, content, finish
        raise Exception("Reply ACK timeout for request")


class _RecordingWeComFactory:

    def __init__(self, client: _LifecycleWeComClient) -> None:
        self.client = client
        self.credentials: list[tuple[str, str]] = []

    def create(self, bot_id: str, secret: str) -> _LifecycleWeComClient:
        self.credentials.append((bot_id, secret))
        return self.client


@pytest.mark.anyio
async def test_wecom_adapter_delivers_outbox_reply_through_registered_transport() -> None:
    """Delivery uses durable correlation data instead of process-local frames."""

    binding = _binding()
    client = _RecordingWeComClient()
    transports = WeComTransportRegistry()
    transports.register(binding.binding_id, client)
    adapter = WeComChannelAdapter(transports)

    receipt = await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-001",
            conversation_id="single:zhangsan",
            kind=MessageKind.TEXT,
            text="你好，我是企业助手。",
            attributes={
                "reply_context": {
                    "req_id": "request-frame-001",
                    "stream_id": "trpc-stream-001",
                }
            },
        ),
        binding,
    )

    assert client.replies == [{
        "frame": {
            "headers": {
                "req_id": "request-frame-001"
            }
        },
        "stream_id": "trpc-stream-001",
        "content": "你好，我是企业助手。",
        "finish": True,
    }]
    assert receipt.delivery_id == "delivery-001"
    assert receipt.external_delivery_id == "request-frame-001"


@pytest.mark.anyio
async def test_wecom_ack_timeout_stops_blind_duplicate_delivery() -> None:
    binding = _binding()
    transports = WeComTransportRegistry()
    transports.register(binding.binding_id, _UnknownOutcomeWeComClient())
    adapter = WeComChannelAdapter(transports)

    with pytest.raises(ProviderOutcomeUnknown):
        await adapter.deliver(
            OutgoingMessage(
                delivery_id="delivery-unknown",
                conversation_id="single:zhangsan",
                kind=MessageKind.TEXT,
                text="reply",
                attributes={
                    "reply_context": {
                        "req_id": "request-frame-001",
                        "stream_id": "trpc-stream-001",
                    }
                },
            ),
            binding,
        )


@pytest.mark.anyio
async def test_wecom_media_is_downloaded_to_tenant_store_and_reuploaded() -> None:
    binding = replace(_binding(), capabilities={"max_inbound_media_bytes": 1024})
    frame = _frame(chat_type="single", user_id="zhangsan")
    body = frame["body"]
    assert isinstance(body, dict)
    body.pop("text")
    body["msgtype"] = "image"
    body["image"] = {
        "url": "https://provider.invalid/media",
        "aeskey": "opaque-key",
        "filename": "photo.png",
    }
    transports = WeComTransportRegistry()
    client = _MediaWeComClient()
    transports.register(binding.binding_id, client)
    media_store = _MediaStore()
    adapter = WeComChannelAdapter(transports, media_store)  # type: ignore[arg-type]

    incoming = await adapter.decode(
        IncomingEnvelope("binding-public-id",
                         json.dumps(frame).encode()),
        binding,
    )
    assert incoming.kind is MessageKind.IMAGE
    assert incoming.artifact_refs == ("artifact-001", )
    assert media_store.saved == [("message-001", b"incoming-image", "photo.png")]

    await adapter.deliver(
        OutgoingMessage(
            delivery_id="delivery-media",
            conversation_id=incoming.conversation_id,
            kind=MessageKind.IMAGE,
            artifact_refs=("artifact-001", ),
            attributes={
                "filename": "answer.png",
                "reply_context": incoming.attributes["reply_context"],
            },
        ),
        binding,
    )
    assert client.media_replies == [("image", "media-uploaded")]


@pytest.mark.anyio
async def test_wecom_mixed_message_keeps_caption_and_all_images() -> None:
    """The provider's image-with-caption event remains one multimodal turn."""

    binding = replace(_binding(), capabilities={"max_inbound_media_bytes": 1024})
    frame = _frame(chat_type="single", user_id="zhangsan")
    body = frame["body"]
    assert isinstance(body, dict)
    body.pop("text")
    body["msgtype"] = "mixed"
    body["mixed"] = {
        "msg_item": [
            {
                "msgtype": "text",
                "text": {
                    "content": "请识别这两张图片"
                }
            },
            {
                "msgtype": "image",
                "image": {
                    "url": "https://provider.invalid/one",
                    "aeskey": "key-one"
                },
            },
            {
                "msgtype": "image",
                "image": {
                    "url": "https://provider.invalid/two",
                    "aeskey": "key-two"
                },
            },
        ]
    }
    transports = WeComTransportRegistry()
    client = _MediaWeComClient()
    transports.register(binding.binding_id, client)
    media_store = _MediaStore()

    incoming = await WeComChannelAdapter(
        transports,
        media_store,  # type: ignore[arg-type]
    ).decode(
        IncomingEnvelope("binding-public-id",
                         json.dumps(frame).encode()),
        binding,
    )

    assert incoming.kind is MessageKind.IMAGE
    assert incoming.text == "请识别这两张图片"
    assert incoming.artifact_refs == ("artifact-001", "artifact-001")
    assert len(incoming.attributes["provider_media"]) == 2
    assert len(media_store.saved) == 2


class _RecordingTaskQueue:

    def __init__(self) -> None:
        self.requests: list[object] = []

    async def enqueue(self, request: object) -> str:
        self.requests.append(request)
        return "task-001"


class _PassThroughApprovalCommands:

    async def process(
        self,
        incoming: IncomingMessage,
        binding: ChannelBindingConfig,
        session_id: str,
    ) -> IncomingMessage:
        del binding, session_id
        return incoming


class _DeterministicIdentityService:

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
    return WeComMessageService(
        adapter,
        queue,
        telemetry,
        _PassThroughApprovalCommands(),
        _DeterministicIdentityService(),
    )


@pytest.mark.anyio
async def test_wecom_message_service_durably_enqueues_before_progress_reply() -> None:
    """The SDK callback returns only after ownership moved to the SQL queue."""

    binding = _binding()
    client = _RecordingWeComClient()
    transports = WeComTransportRegistry()
    transports.register(binding.binding_id, client)
    adapter = WeComChannelAdapter(transports)
    queue = _RecordingTaskQueue()
    service = _message_service(
        adapter,
        queue,
        PlatformTelemetry(
            service_name="test",
            environment="test",
            node_role="channel",
            otlp_endpoint=None,
        ),
    )
    tenant = _request().tenant.model_copy(update={
        "tenant_id": binding.tenant_id,
        "agent_app_id": binding.agent_app_id,
    })

    message_id = await service.submit(
        _frame(chat_type="single", user_id="zhangsan"),
        binding,
        tenant,
    )

    assert message_id == "message-001"
    assert len(queue.requests) == 1
    queued = queue.requests[0]
    assert queued.session_id == f"{binding.binding_id}:single:zhangsan"
    assert queued.channel.channel_type == "wecom"
    assert client.replies[0]["content"] == "正在思考..."
    assert client.replies[0]["finish"] is False


@pytest.mark.anyio
async def test_wecom_supervisor_connects_active_binding_and_routes_callback(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    """Binding CRUD is reflected without coupling the SDK to Agent Workers."""

    binding_config = _binding()
    secret_ref = binding_config.secret_ref_map["bot_secret"]
    secret_name = secret_ref.removeprefix("env://")
    monkeypatch.setenv(secret_name, "wecom-secret-value")
    row = ChannelBinding(
        binding_id=binding_config.binding_id,
        binding_public_id="binding-public-id",
        tenant_id=binding_config.tenant_id,
        agent_app_id=binding_config.agent_app_id,
        channel_type="wecom",
        external_account_hash="wecom-account",
        account_config=dict(binding_config.account_config),
        secret_ref_map=dict(binding_config.secret_ref_map),
        capabilities={},
        status="active",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    agent = AgentApp(
        tenant_id=binding_config.tenant_id,
        agent_app_id=binding_config.agent_app_id,
        name="WeCom Agent",
        stable_config_version=3,
        status="active",
    )
    transports = WeComTransportRegistry()
    adapter = WeComChannelAdapter(transports)
    queue = _RecordingTaskQueue()
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="channel",
        otlp_endpoint=None,
    )
    client = _LifecycleWeComClient()
    factory = _RecordingWeComFactory(client)
    supervisor = WeComBindingSupervisor(
        None,  # type: ignore[arg-type]
        adapter,
        transports,
        _message_service(adapter, queue, telemetry),
        telemetry,
        client_factory=factory,
    )

    async def active_bindings() -> list[tuple[ChannelBinding, AgentApp]]:
        return [(row, agent)]

    monkeypatch.setattr(supervisor, "_active_bindings", active_bindings)
    await supervisor.reconcile_once()
    assert "message.mixed" in client.handlers
    await client.handlers["message.text"](_frame(chat_type="group",
                                                 user_id="lisi",
                                                 chat_id="group-001"))

    assert factory.credentials == [("aibot-test", "wecom-secret-value")]
    assert client.connected
    assert len(queue.requests) == 1
    assert queue.requests[0].tenant.config_version == 3

    refreshed_agent = AgentApp(
        tenant_id=binding_config.tenant_id,
        agent_app_id=binding_config.agent_app_id,
        name="WeCom Agent",
        stable_config_version=4,
        status="active",
    )

    async def refreshed_bindings() -> list[tuple[ChannelBinding, AgentApp]]:
        return [(row, refreshed_agent)]

    monkeypatch.setattr(supervisor, "_active_bindings", refreshed_bindings)
    await supervisor.reconcile_once()
    await client.handlers["message.text"](_frame(chat_type="single", user_id="wangwu"))

    # Agent releases are picked up by reconciliation without reconnecting the
    # provider socket or retaining the previous rollout pointer in a closure.
    assert factory.credentials == [("aibot-test", "wecom-secret-value")]
    assert len(queue.requests) == 2
    assert queue.requests[1].tenant.config_version == 4

    supervisor.stop_ingress()
    await client.handlers["message.text"](_frame(chat_type="single", user_id="zhaoliu"))
    assert len(queue.requests) == 2

    async def no_bindings() -> list[tuple[ChannelBinding, AgentApp]]:
        return []

    monkeypatch.setattr(supervisor, "_active_bindings", no_bindings)
    await supervisor.reconcile_once()
    assert client.disconnected


class _CapturingSessionStore(StubSessionStore):
    """Capture the atomic commit at the concrete Session storage seam."""

    def __init__(self) -> None:
        self.commit: ExecutionCommit | None = None

    async def commit_execution(
        self,
        context: object,
        commit: ExecutionCommit,
    ) -> SessionSnapshot:
        del context
        self.commit = commit
        return SessionSnapshot(session_id=commit.session_id, version=1)


@pytest.mark.anyio
async def test_agent_commit_carries_provider_reply_context_into_durable_outbox() -> None:
    """A later delivery process can reply without retaining the callback frame."""

    request = _request()
    request = replace(
        request,
        incoming=replace(
            request.incoming,
            attributes={
                "reply_context": {
                    "req_id": "request-frame-001",
                    "stream_id": "trpc-stream-001",
                }
            },
        ),
        channel=replace(request.channel, channel_type="wecom"),
    )
    store = _CapturingSessionStore()
    storage = ResolvedStorage(
        profile=BackendProfile(session="test"),
        session=store,
        outbox=store,
        memory=None,
        summary=None,
        knowledge=None,
        artifact=None,
        audit=None,
    )
    context = AgentExecutionContext(
        request=request,
        config=AgentRuntimeConfig(config_version=3, runner_name="test"),
        policy=PolicyDecision(action=PolicyAction.ALLOW),
        claim=AgentExecutionClaim(claim_id="claim-001", fencing_token=1),
    )

    await StorageResultCommitter(storage).commit(
        context,
        AgentRunResult(replies=(AgentReply(kind=MessageKind.TEXT, text="reply"), )),
    )

    assert store.commit is not None
    assert store.commit.outbox[0].payload["attributes"] == {
        "in_reply_to": "message-1",
        "reply_context": {
            "req_id": "request-frame-001",
            "stream_id": "trpc-stream-001",
        },
    }
