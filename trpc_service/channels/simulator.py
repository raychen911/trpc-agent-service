"""Development-only, protocol-faithful IM simulator used by the visual demo."""

from __future__ import annotations

import base64
import hashlib
import time
import uuid
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from trpc_service.channels.customer_service import CustomerServiceAdapter, FakeCustomerServiceClient
from trpc_service.channels.customer_store import InMemoryCustomerStore
from trpc_service.channels.telegram import TelegramChannelAdapter
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.channels.wecom_runtime import FakeWeComClient
from trpc_service.config import ChannelType
from trpc_service.gateway.outbox import OutboxState
from trpc_service.gateway.models import TraceContext
from trpc_service.channels.base import ChannelTransportError

CHANNEL_DISPLAY_ORDER = (ChannelType.WECOM, ChannelType.WECOM_KF, ChannelType.TELEGRAM)


class ImSimulationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel: ChannelType
    model_mode: str = Field(default="fake", pattern="^(fake|configured)$")
    external_user_id: str = Field(default="student-001", min_length=1, max_length=128)
    external_conversation_id: str = Field(default="conversation-001", min_length=1, max_length=128)
    chat_type: str = Field(default="single", pattern="^(single|group)$")
    text: str = Field(default="你好，请介绍一下你自己。", max_length=32768)
    message_id: str = Field(default="", max_length=128)
    duplicate_count: int = Field(default=1, ge=1, le=2)
    attachment_name: str = Field(default="", max_length=128)
    attachment_mime_type: str = Field(default="application/octet-stream", max_length=128)
    attachment_base64: str = Field(default="", max_length=1_500_000)
    attachments: list["ImSimulationAttachment"] = Field(default_factory=list, max_length=10)

    @field_validator("attachment_mime_type")
    @classmethod
    def safe_demo_mime(cls, value: str) -> str:
        allowed = {"application/octet-stream", "application/pdf", "text/plain", "image/jpeg", "image/png"}
        if value.lower() not in allowed:
            raise ValueError("demo attachment MIME type is not allowed")
        return value.lower()


class ImSimulationAttachment(BaseModel):
    """One browser-selected file; the simulator converts it to a real IM message."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    mime_type: str = Field(default="application/octet-stream", max_length=128)
    content_base64: str = Field(min_length=1, max_length=1_500_000)

    @field_validator("mime_type")
    @classmethod
    def safe_demo_mime(cls, value: str) -> str:
        return ImSimulationRequest.safe_demo_mime(value)


ImSimulationRequest.model_rebuild()


class ImFaultRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel: ChannelType
    fault: str = Field(pattern="^(none|rate_limit|delivery_timeout|human_handoff|disconnect)$")


class RecordingTelegramTransport:

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.files: dict[str, bytes] = {}
        self.fault = "none"
        self._message_id = 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append({"method": request.method, "path": path, "body": request.content.decode(errors="replace")})
        if self.fault == "rate_limit" and path.endswith(("sendMessage", "sendPhoto", "sendDocument")):
            self.fault = "none"
            return httpx.Response(429,
                                  json={
                                      "ok": False,
                                      "error_code": 429,
                                      "description": "Too Many Requests",
                                      "parameters": {
                                          "retry_after": 1
                                      }
                                  })
        if self.fault == "delivery_timeout" and path.endswith(("sendMessage", "sendPhoto", "sendDocument")):
            self.fault = "none"
            raise httpx.ReadTimeout("simulated response loss", request=request)
        if path.endswith("getFile"):
            file_id = request.url.params.get("file_id", "file")
            return httpx.Response(200, json={"ok": True, "result": {"file_path": f"documents/{file_id}"}})
        if "/file/bot" in path:
            return httpx.Response(200, content=self.files.get(path.rsplit("/", 1)[-1], b"simulated attachment"))
        self._message_id += 1
        return httpx.Response(200, json={"ok": True, "result": {"message_id": self._message_id}})


class LocalImSimulator:
    """Build official-shaped inbound payloads and feed the production adapters."""

    def __init__(self, container, bindings: dict[tuple[ChannelType, str], Any], clients: dict[str, Any]) -> None:
        self.container = container
        self.bindings = bindings
        self.clients = clients
        self.last_protocol: dict[str, dict[str, Any]] = {}
        self.pending_faults: dict[ChannelType, str] = {}
        self.blocked_channels: set[ChannelType] = set()

    @classmethod
    async def attach(cls, container) -> "LocalImSimulator":
        bindings: dict[tuple[ChannelType, str], Any] = {}
        clients: dict[str, Any] = {}
        customer_store = InMemoryCustomerStore()
        container.customer_store = customer_store
        for tenant in await container.registry.list_active():
            for binding in tenant.channels:
                mode = str(binding.options.get("simulator_mode", ""))
                if not mode or binding.channel not in CHANNEL_DISPLAY_ORDER:
                    continue
                bindings[(binding.channel, mode)] = binding
                if binding.channel == ChannelType.WECOM:
                    client = FakeWeComClient()

                    async def sender(conversation_id, text, reply_to, *, _client=client):
                        del reply_to
                        return await _client.send_message(conversation_id, text)

                    adapter = WeComChannelAdapter(sender,
                                                  client.reply_stream,
                                                  expected_bot_id=binding.external_account_id,
                                                  downloader=client.download_file)
                elif binding.channel == ChannelType.WECOM_KF:
                    client = FakeCustomerServiceClient()
                    adapter = CustomerServiceAdapter(binding, client, customer_store, artifacts=container.artifacts)
                else:
                    transport = RecordingTelegramTransport()
                    client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
                    adapter = TelegramChannelAdapter("demo-token",
                                                     "demo-webhook-secret",
                                                     client,
                                                     artifacts=container.artifacts)
                    clients[f"{binding.binding_id}:transport"] = transport
                clients[binding.binding_id] = client
                container.channel_adapters[binding.binding_id] = adapter
        simulator = cls(container, bindings, clients)
        for (channel, _), binding in bindings.items():
            if channel == ChannelType.WECOM:
                container.channel_adapters[binding.binding_id].available = (
                    lambda _channel=channel: _channel not in simulator.blocked_channels)
        container.attachment_ingestor._adapters = container.channel_adapters
        return simulator

    def bootstrap(self) -> dict[str, Any]:
        return {
            "channels": [{
                "value": channel.value,
                "label": {
                    ChannelType.WECOM: "企业微信智能机器人",
                    ChannelType.WECOM_KF: "微信客服",
                    ChannelType.TELEGRAM: "Telegram",
                }[channel]
            } for channel in CHANNEL_DISPLAY_ORDER],
            "model_modes": [{
                "value": "fake",
                "label": "Fake Model（默认、免费）"
            }, {
                "value": "configured",
                "label": "当前配置的真实模型"
            }],
            "limits": {
                "attachment_bytes": 1024 * 1024,
                "duplicate_count": 2
            },
        }

    async def send(self, request: ImSimulationRequest) -> dict[str, Any]:
        if request.channel == ChannelType.WEB:
            raise ValueError("web is not an IM simulation channel")
        binding = self.bindings.get((request.channel, request.model_mode))
        if binding is None:
            raise ValueError("simulator binding is not configured")
        if request.attachments:
            if request.attachment_base64:
                raise ValueError("use either attachments or the legacy single attachment fields")
            return await self._send_batch(request, binding)
        message_id = request.message_id or uuid.uuid4().hex
        self._apply_fault(request)
        content = self._attachment(request)
        adapter = self.container.channel_adapters[binding.binding_id]
        payload, headers = self._payload(binding, request, message_id, content)
        normalized = await adapter.normalize(binding.binding_id, payload, headers)
        trace = TraceContext(traceparent=f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-01")
        normalized.trace = trace
        protocol_normalized = normalized.model_dump(mode="json")
        if request.channel == ChannelType.WECOM_KF:

            def remember_customer(state):
                state["customers"][request.external_user_id] = time.time()

            await adapter.store.mutate(binding.binding_id, remember_customer)
        request_ids = []
        for index in range(request.duplicate_count):
            # Materialize the transport reference only once. A redelivered IM
            # message must reuse the same Artifact metadata; generating a new
            # object ID would turn an exact duplicate into a payload conflict.
            current = normalized if index == 0 else normalized.model_copy(deep=True)
            request_ids.append(await self.container.admit_channel(current))
        self.last_protocol[request_ids[0]] = {
            "raw": self._redact(payload),
            "normalized": protocol_normalized,
        }
        return {
            "request_id": request_ids[0],
            "duplicate_reused": request.duplicate_count > 1 and len(set(request_ids)) == 1,
            "raw": self._redact(payload),
            "normalized": protocol_normalized,
        }

    async def _send_batch(self, request: ImSimulationRequest, binding) -> dict[str, Any]:
        """Normalize real per-file messages, then aggregate one user action before enqueue."""
        self._apply_fault(request)
        adapter = self.container.channel_adapters[binding.binding_id]
        trace = TraceContext(traceparent=f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-01")
        batch_id = request.message_id or uuid.uuid4().hex
        raw_messages: list[dict[str, Any]] = []
        source_messages: list[dict[str, Any]] = []
        materialized = []
        aggregate = None
        source_ids: list[str] = []
        tenant, _ = await self.container.registry.resolve_binding(binding.binding_id)
        if request.text:
            text_request = request.model_copy(update={
                "message_id": f"{batch_id}-text",
                "attachment_name": "",
                "attachment_base64": "",
                "attachments": [],
            })
            payload, headers = self._payload(binding, text_request, text_request.message_id, b"")
            normalized = await adapter.normalize(binding.binding_id, payload, headers)
            normalized.trace = trace
            raw_messages.append(self._redact(payload))
            source_messages.append(normalized.model_dump(mode="json"))
            source_ids.append(normalized.message_id)
            aggregate = normalized
        for index, attachment in enumerate(request.attachments):
            content = self._decode_attachment(attachment.content_base64)
            single = request.model_copy(
                update={
                    "text": "",
                    "message_id": f"{batch_id}-attachment-{index + 1}",
                    "attachment_name": attachment.name,
                    "attachment_mime_type": attachment.mime_type,
                    "attachment_base64": attachment.content_base64,
                    "attachments": [],
                })
            payload, headers = self._payload(binding, single, single.message_id, content)
            normalized = await adapter.normalize(binding.binding_id, payload, headers)
            normalized.trace = trace
            raw_messages.append(self._redact(payload))
            source_messages.append(normalized.model_dump(mode="json"))
            source_ids.append(normalized.message_id)
            normalized = await self.container.attachment_ingestor.materialize_inbound(
                tenant.tenant_id, binding.app_id, normalized)
            if aggregate is None:
                aggregate = normalized
            materialized.extend(normalized.attachments)
        if aggregate is None:  # guarded by the request schema, kept explicit for type safety
            raise ValueError("attachment batch is empty")
        aggregate.message_id = f"batch:{batch_id}"
        aggregate.text = request.text
        aggregate.attachments = materialized
        aggregate.trace = trace
        aggregate.metadata.update({"source_message_ids": source_ids, "batch_size": len(source_ids)})
        if request.channel == ChannelType.WECOM_KF:

            def remember_customer(state):
                state["customers"][request.external_user_id] = time.time()

            await adapter.store.mutate(binding.binding_id, remember_customer)
        request_ids = []
        for index in range(request.duplicate_count):
            current = aggregate if index == 0 else aggregate.model_copy(deep=True)
            request_ids.append(await self.container.admit_channel(current))
        protocol = {
            "raw": raw_messages,
            "normalized": aggregate.model_dump(mode="json"),
            "source_normalized": source_messages,
        }
        self.last_protocol[request_ids[0]] = protocol
        return {
            "request_id": request_ids[0],
            "duplicate_reused": request.duplicate_count > 1 and len(set(request_ids)) == 1,
            "batch_size": len(source_ids),
            "raw": raw_messages,
            "normalized": source_messages,
            "aggregated": protocol["normalized"],
        }

    @staticmethod
    def _attachment(request: ImSimulationRequest) -> bytes:
        if not request.attachment_base64:
            return b""
        return LocalImSimulator._decode_attachment(request.attachment_base64)

    @staticmethod
    def _decode_attachment(content_base64: str) -> bytes:
        try:
            content = base64.b64decode(content_base64, validate=True)
        except ValueError as error:
            raise ValueError("attachment_base64 is invalid") from error
        if len(content) > 1024 * 1024:
            raise ValueError("simulated attachment exceeds 1 MiB")
        return content

    def _payload(self, binding, request, message_id, content):
        if request.channel == ChannelType.WECOM:
            body = {
                "msgid": message_id,
                "aibotid": binding.external_account_id,
                "chattype": request.chat_type,
                "from": {
                    "userid": request.external_user_id
                },
                "create_time": int(time.time()),
                "msgtype": "text",
                "text": {
                    "content": request.text
                },
            }
            if request.chat_type == "group":
                body["chatid"] = request.external_conversation_id
            if content:
                kind = "image" if request.attachment_mime_type.startswith("image/") else "file"
                url = f"https://simulator.invalid/media/{message_id}"
                media = {"url": url, "aeskey": "simulated-aes-key", "filename": request.attachment_name}
                if kind == "image" and request.text:
                    body.pop("text", None)
                    body.update(msgtype="mixed",
                                mixed={
                                    "msg_item": [
                                        {
                                            "msgtype": "text",
                                            "text": {
                                                "content": request.text
                                            }
                                        },
                                        {
                                            "msgtype": "image",
                                            "image": media
                                        },
                                    ]
                                })
                else:
                    body.pop("text", None)
                    body.update(msgtype=kind, **{kind: media})
                client = self.clients[binding.binding_id]
                client.files[url] = (content, request.attachment_name or f"attachment-{message_id}")
            return {"cmd": "aibot_msg_callback", "headers": {"req_id": f"req-{message_id}"}, "body": body}, {}
        if request.channel == ChannelType.WECOM_KF:
            kind = "text"
            part = {"content": request.text}
            if content:
                kind = "image" if request.attachment_mime_type.startswith("image/") else "file"
                media_id = f"media-{message_id}"
                part = {"media_id": media_id, "name": request.attachment_name}
                self.clients[binding.binding_id].files[media_id] = (content, request.attachment_mime_type)
            return {
                "msgid": message_id,
                "open_kfid": binding.open_kfid,
                "external_userid": request.external_user_id,
                "origin": 3,
                "send_time": int(time.time()),
                "msgtype": kind,
                kind: part,
            }, {}
        update_id = int(hashlib.sha256(message_id.encode()).hexdigest()[:12], 16)
        user_number = self._telegram_number(request.external_user_id)
        conversation_number = self._telegram_number(request.external_conversation_id)
        message = {
            "message_id": update_id % 2_000_000_000,
            "date": int(time.time()),
            "from": {
                "id": user_number,
                "is_bot": False,
                "first_name": "Local user"
            },
            "chat": {
                "id": conversation_number,
                "type": "group" if request.chat_type == "group" else "private"
            },
            "text": request.text,
        }
        if content:
            file_id = f"file-{message_id}"
            message.pop("text", None)
            message["caption"] = request.text
            if request.attachment_mime_type.startswith("image/"):
                message["photo"] = [{"file_id": file_id, "file_unique_id": message_id, "file_size": len(content)}]
            else:
                message["document"] = {
                    "file_id": file_id,
                    "file_unique_id": message_id,
                    "file_name": request.attachment_name,
                    "mime_type": request.attachment_mime_type,
                    "file_size": len(content)
                }
            self.clients[f"{binding.binding_id}:transport"].files[file_id] = content
        return {"update_id": update_id, "message": message}, {"x-telegram-bot-api-secret-token": "demo-webhook-secret"}

    async def status(self, request_id: str) -> dict[str, Any]:
        protocol = self.last_protocol.get(request_id)
        if protocol is None:
            raise KeyError(request_id)
        tenant_id = protocol["normalized"]["metadata"].get("tenant_id", "")
        if not tenant_id:
            binding_id = protocol["normalized"]["binding_id"]
            tenant, _ = await self.container.registry.resolve_binding(binding_id)
            tenant_id = tenant.tenant_id
        record = await self.container.gateway.request_status(tenant_id, request_id)
        outbound_id = hashlib.sha256(f"{request_id}:reply:0".encode()).hexdigest()
        outbox = await self.container.outbox.get(outbound_id)
        return {
            "request": record.model_dump(mode="json"),
            "outbox": outbox.model_dump(mode="json") if outbox else None,
            "protocol": protocol,
            "delivered": bool(outbox and outbox.state == OutboxState.DELIVERED),
            "stages": self._stages(record, outbox),
        }

    @staticmethod
    def _stages(record, outbox) -> list[dict[str, str]]:
        state = record.state.value
        accepted = state not in {"reserved"}
        completed = state == "succeeded"
        delivered = bool(outbox and outbox.state == OutboxState.DELIVERED)
        return [
            {
                "name": "Channel Adapter",
                "state": "done"
            },
            {
                "name": "Redis Streams / Queue",
                "state": "done" if accepted else "waiting"
            },
            {
                "name": "Agent Worker",
                "state": "done" if completed else state
            },
            {
                "name": "Runner + Model",
                "state": "done" if completed else state
            },
            {
                "name": "Session / Memory",
                "state": "done" if completed else "waiting"
            },
            {
                "name": "PostgreSQL / InMemory Outbox",
                "state": "done" if outbox else "waiting"
            },
            {
                "name": "Channel Delivery",
                "state": "done" if delivered else (outbox.state.value if outbox else "waiting")
            },
        ]

    def set_fault(self, request: ImFaultRequest) -> dict[str, str]:
        supported = {
            ChannelType.WECOM: {"none", "rate_limit", "delivery_timeout", "disconnect"},
            ChannelType.WECOM_KF: {"none", "rate_limit", "delivery_timeout", "human_handoff"},
            ChannelType.TELEGRAM: {"none", "rate_limit", "delivery_timeout"},
        }
        if request.channel not in supported or request.fault not in supported[request.channel]:
            raise ValueError(f"{request.fault} is not supported for {request.channel.value}")
        self.pending_faults[request.channel] = request.fault
        for (channel, _), binding in self.bindings.items():
            if channel != request.channel:
                continue
            client = self.clients[binding.binding_id]
            if channel == ChannelType.WECOM_KF:
                client.states.clear()
                client.send_error = None
            elif channel == ChannelType.TELEGRAM:
                self.clients[f"{binding.binding_id}:transport"].fault = request.fault
            elif channel == ChannelType.WECOM:
                if request.fault == "disconnect":
                    self.blocked_channels.add(channel)
                else:
                    self.blocked_channels.discard(channel)
                client.send_error = None
        return {"channel": request.channel.value, "fault": request.fault}

    def _apply_fault(self, request: ImSimulationRequest) -> None:
        fault = self.pending_faults.pop(request.channel, "none")
        for (channel, _), binding in self.bindings.items():
            if channel != request.channel:
                continue
            client = self.clients[binding.binding_id]
            if channel == ChannelType.WECOM_KF:
                if fault == "human_handoff":
                    client.states[request.external_user_id] = 3
                elif fault == "rate_limit":
                    from trpc_service.channels.customer_service import CustomerServiceError
                    client.send_error = CustomerServiceError("kf_rate_limited", retryable=True, retry_after=1)
                elif fault == "delivery_timeout":
                    from trpc_service.channels.customer_service import CustomerServiceError
                    client.send_error = CustomerServiceError("kf_delivery_unknown", uncertain=True)
            elif channel == ChannelType.WECOM:
                if fault == "rate_limit":
                    client.send_error = ChannelTransportError("wecom_rate_limited",
                                                              retryable=True,
                                                              retry_after_seconds=1)
                elif fault == "delivery_timeout":
                    client.send_error = ChannelTransportError("wecom_delivery_unknown", uncertain=True)

    @staticmethod
    def _telegram_number(value: str) -> int:
        try:
            return int(value)
        except ValueError:
            return int(hashlib.sha256(value.encode()).hexdigest()[:12], 16)

    @staticmethod
    def _redact(payload: dict[str, Any]) -> dict[str, Any]:
        clone = __import__("copy").deepcopy(payload)
        body = clone.get("body") if isinstance(clone, dict) else None
        if isinstance(body, dict):
            for kind in ("image", "file"):
                if isinstance(body.get(kind), dict) and body[kind].get("aeskey"):
                    body[kind]["aeskey"] = "***"
            mixed = body.get("mixed")
            if isinstance(mixed, dict):
                for item in mixed.get("msg_item", []):
                    image = item.get("image") if isinstance(item, dict) else None
                    if isinstance(image, dict) and image.get("aeskey"):
                        image["aeskey"] = "***"
        return clone

    async def close(self) -> None:
        closed: set[int] = set()
        for client in self.clients.values():
            close = getattr(client, "aclose", None)
            if close and id(client) not in closed:
                closed.add(id(client))
                await close()


async def build_im_demo_container(settings, configs):
    """Compose the development-only UI with offline and configured runtimes."""
    if settings.environment != "development":
        raise ValueError("im-demo is available only when TRPC_SERVICE_ENV=development")
    from trpc_service.agent import TenantRuntimeFactory
    from trpc_service.config import SecretProviderRegistry, ServiceRole
    from trpc_service.offline import DemoRuntimeFactory, OfflineRuntimeFactory
    from trpc_service.storage import StorageProviderFactory
    from trpc_service.tool import ToolRegistry
    from trpc_service.web.container import build_container

    def runtime_factory(artifacts, knowledge, metrics, audit):
        configured = TenantRuntimeFactory(storage_factory=StorageProviderFactory(metrics=metrics),
                                          secret_resolver=SecretProviderRegistry(),
                                          tool_registry=ToolRegistry(knowledge_provider=knowledge,
                                                                     metrics=metrics,
                                                                     audit=audit),
                                          artifact_store=artifacts)
        offline = OfflineRuntimeFactory(artifact_store=artifacts)
        return DemoRuntimeFactory(configured, offline)

    # The visual demo is deliberately a self-contained process.  Do not let a
    # TRPC_SERVICE_ROLES value left over from a production/Compose test disable
    # its local queue consumer or Outbox delivery loop.
    settings.roles.update({ServiceRole.GATEWAY, ServiceRole.WORKER, ServiceRole.DELIVERY})
    container = build_container(settings, configs, runtime_factory_override=runtime_factory)
    container.im_simulator = await LocalImSimulator.attach(container)
    return container
