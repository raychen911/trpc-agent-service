"""Long-connection supervisor for active tenant Feishu bindings."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import json
import logging
from typing import Protocol, cast
from uuid import UUID, uuid4
import warnings

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.admin.models import ChannelAdapterType
from trpc_service.admin.secret_store import TenantSecretStore
from trpc_service.agent.configuration import select_agent_config_version
from trpc_service.agent.models import AgentApp
from trpc_service.channels.adapters.feishu import (
    FeishuChannelAdapter,
    FeishuClient,
    FeishuTransportRegistry,
)
from trpc_service.channels.contracts import ChannelBindingConfig
from trpc_service.channels.feishu import FeishuMessageService
from trpc_service.channels.media import ChannelMediaStore
from trpc_service.channels.models import ChannelBinding
from trpc_service.config.secret_scope import validate_tenant_channel_secret_ref
from trpc_service.config.storage import LocalSecretNotReadyError, resolve_local_secret
from trpc_service.metrics import PlatformTelemetry
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import Tenant

logger = logging.getLogger(__name__)

# lark-channel-sdk 1.4.0 captures an event loop in a module-level variable when
# it is imported. Import it while this runtime module is loaded, before the CLI
# enters ``asyncio.run``; a lazy import from ``create`` would capture the
# already-running application loop and the SDK's worker thread would fail with
# "This event loop is already running".
with warnings.catch_warnings():
    # Python 3.12 also warns when the SDK creates its module-level transport
    # loop. This import is intentionally the sole compatibility boundary.
    warnings.filterwarnings(
        "ignore",
        message="There is no current event loop",
        category=DeprecationWarning,
    )
    warnings.filterwarnings(
        "ignore",
        message=r"datetime\.datetime\.utcfromtimestamp\(\) is deprecated.*",
        category=DeprecationWarning,
        module=r"lark_channel\.ws\.pb\.google\.protobuf.*",
    )
    from lark_channel import FeishuChannel, PolicyConfig, SecurityConfig
    from lark_channel.core.enum import LogLevel


class FeishuSDKChannel(FeishuClient, Protocol):
    """Official SDK lifecycle and event surface used by the supervisor."""

    def on(self, event: str, handler: Callable[..., object]) -> object:
        ...

    async def connect_until_ready(self, *, timeout: float | None = 30.0) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    async def download_resource(
        self,
        file_key: str,
        resource_type: str = "image",
        message_id: str | None = None,
    ) -> bytes | None:
        ...


class FeishuClientFactory(Protocol):

    def create(self, app_id: str, app_secret: str) -> FeishuSDKChannel:
        ...


class SDKFeishuClientFactory:
    """Construct the supported standalone Feishu Channel SDK."""

    def create(self, app_id: str, app_secret: str) -> FeishuSDKChannel:
        return cast(
            FeishuSDKChannel,
            FeishuChannel(
                app_id=app_id,
                app_secret=app_secret,
                transport="ws",
                # Provider logs may contain transport details. Project logs
                # retain only binding IDs and exception classes.
                log_level=LogLevel.ERROR,
                policy=PolicyConfig(require_mention=True),
                security=SecurityConfig(mode="strict"),
            ),
        )


@dataclass(slots=True)
class _Connection:
    client: FeishuSDKChannel
    fingerprint: str
    agent: AgentApp


class FeishuBindingSupervisor:
    """Reconcile database bindings into authenticated Feishu connections."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        adapter: FeishuChannelAdapter,
        transports: FeishuTransportRegistry,
        messages: FeishuMessageService,
        telemetry: PlatformTelemetry,
        *,
        client_factory: FeishuClientFactory,
        poll_interval_seconds: float = 5.0,
        media_store: ChannelMediaStore | None = None,
        secret_store: TenantSecretStore | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("Feishu reconciliation interval must be positive")
        self._sessions = sessions
        self._adapter = adapter
        self._transports = transports
        self._messages = messages
        self._telemetry = telemetry
        self._client_factory = client_factory
        self._poll_interval_seconds = poll_interval_seconds
        self._media_store = media_store
        self._secret_store = secret_store
        self._connections: dict[UUID, _Connection] = {}
        # Empty placeholder files are a supported provisioning state. Remember
        # them so reconciliation does not emit the same warning every 5 seconds.
        self._pending_secrets: set[UUID] = set()
        # The SDK dispatches handlers on its private loop, while SQLAlchemy and
        # the durable queues belong to the Channel Runtime loop.
        self._application_loop: asyncio.AbstractEventLoop | None = None
        self._message_tasks: set[asyncio.Task[None]] = set()
        self._accepting_messages = True
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @staticmethod
    def _fingerprint(row: ChannelBinding) -> str:
        return json.dumps(
            {
                "account": row.account_config,
                "secrets": row.secret_ref_map,
                "updated": row.updated_at.isoformat()
            },
            sort_keys=True,
            default=str,
        )

    async def _active_bindings(self) -> list[tuple[ChannelBinding, AgentApp]]:
        async with self._sessions() as database:
            result = await database.execute(
                select(ChannelBinding,
                       AgentApp).join(Tenant, Tenant.tenant_id == ChannelBinding.tenant_id).join(
                           AgentApp,
                           (AgentApp.tenant_id == ChannelBinding.tenant_id)
                           & (AgentApp.agent_app_id == ChannelBinding.agent_app_id),
                       ).join(
                           ChannelAdapterType,
                           ChannelAdapterType.channel_type == ChannelBinding.channel_type,
                       ).where(
                           ChannelBinding.channel_type == "feishu",
                           ChannelBinding.status == "active",
                           Tenant.status == "active",
                           AgentApp.status == "active",
                           ChannelAdapterType.status == "active",
                       ))
            return [(binding, agent) for binding, agent in result.all()]

    @staticmethod
    def _normalized_message(message: object) -> dict[str, object]:
        resources = []
        for resource in getattr(message, "resources", ()):
            resources.append({
                "type": getattr(resource, "type", "file"),
                "file_key": getattr(resource, "file_key", ""),
                "file_name": getattr(resource, "file_name", None),
                "duration_ms": getattr(resource, "duration_ms", None),
                "cover_image_key": getattr(resource, "cover_image_key", None),
            })
        conversation = getattr(message, "conversation")
        sender = getattr(message, "sender")
        return {
            "message_id": getattr(message, "message_id"),
            "create_time": int(getattr(message, "create_time")),
            "chat_id": getattr(conversation, "chat_id"),
            "chat_type": getattr(conversation, "chat_type"),
            "thread_id": getattr(conversation, "thread_id", None),
            "sender_id": getattr(sender, "open_id"),
            "sender_name": getattr(sender, "display_name", None),
            "content_text": getattr(message, "body_text", "")
            or getattr(message, "content_text", ""),
            "raw_content_type": getattr(message, "raw_content_type", "unknown"),
            "mentioned_bot": bool(getattr(message, "mentioned_bot", False)),
            "resources": resources,
        }

    async def _handle_message(
        self,
        binding: ChannelBindingConfig,
        connection: _Connection,
        message: object,
    ) -> None:
        if not self._accepting_messages:
            return
        frame = self._normalized_message(message)
        tenant = TenantContext(
            tenant_id=binding.tenant_id,
            agent_app_id=binding.agent_app_id,
            config_version=select_agent_config_version(
                connection.agent,
                str(frame["sender_id"]),
            ),
            request_id=str(uuid4()),
            trace_id=self._telemetry.current_trace_id(),
        )
        if self._media_store is not None and frame["resources"]:
            max_bytes = binding.capabilities.get("max_inbound_media_bytes", 50 * 1024 * 1024)
            if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
                raise ValueError("Feishu media size limit must be a positive integer")
            stored: list[str] = []
            total_bytes = 0
            resources = cast(list[dict[str, object]], frame["resources"])
            for resource in resources:
                resource_type = str(resource["type"])
                file_key = str(resource["file_key"])
                content = await connection.client.download_resource(
                    file_key,
                    "image" if resource_type == "sticker" else resource_type,
                    str(frame["message_id"]),
                )
                if content is None:
                    raise RuntimeError("Feishu media download returned no content")
                total_bytes += len(content)
                if total_bytes > max_bytes:
                    raise ValueError("Feishu inbound media exceeds the configured size limit")
                filename_value = resource.get("file_name")
                filename = (filename_value
                            if isinstance(filename_value, str) else f"{file_key}-{resource_type}")
                stored.append(await self._media_store.put(
                    binding,
                    principal_id=str(frame["sender_id"]),
                    message_id=str(frame["message_id"]),
                    content=content,
                    filename=filename,
                    media_type=f"application/x-feishu-{resource_type}",
                    context=tenant,
                ))
            frame["artifact_refs"] = stored
        with self._telemetry.start_span("channel.receive", attributes={"channel.type": "feishu"}):
            await self._messages.submit(frame, binding, tenant)

    def _schedule_message(
        self,
        binding: ChannelBindingConfig,
        connection: _Connection,
        value: object,
    ) -> None:
        """Start one ingress task on the Channel Runtime event loop."""

        if not self._accepting_messages:
            return
        task = asyncio.create_task(
            self._handle_message(binding, connection, value),
            name=f"feishu-message-{binding.binding_id}",
        )
        self._message_tasks.add(task)

        def completed(done: asyncio.Task[None]) -> None:
            self._message_tasks.discard(done)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                # Never include the SDK message or provider exception text.
                logger.error(
                    "Feishu message handling failed binding_id=%s error_type=%s",
                    binding.binding_id,
                    type(error).__name__,
                )

        task.add_done_callback(completed)

    async def _connect(self, row: ChannelBinding, agent: AgentApp, fingerprint: str) -> None:
        binding = row.to_config()
        app_id = self._adapter.app_id(binding)
        secret_ref = binding.secret_ref_map.get("app_secret")
        if not isinstance(secret_ref, str):
            raise ValueError("Feishu binding requires app_secret SecretRef")
        validate_tenant_channel_secret_ref(secret_ref, binding.tenant_id)
        secret = (resolve_local_secret(secret_ref) if self._secret_store is None else await
                  self._secret_store.resolve(secret_ref, binding.tenant_id))
        client = self._client_factory.create(app_id, secret)
        connection = _Connection(client, fingerprint, agent)
        self._connections[binding.binding_id] = connection

        async def message(value: object) -> None:
            application_loop = self._application_loop
            if application_loop is None:
                raise RuntimeError("Feishu application event loop is not initialized")
            if asyncio.get_running_loop() is application_loop:
                self._schedule_message(binding, connection, value)
                return
            # The official SDK owns a background thread and event loop. Route
            # all project storage/queue work back to the process main loop. The
            # callback returns quickly; shutdown explicitly drains these tasks.
            application_loop.call_soon_threadsafe(
                self._schedule_message,
                binding,
                connection,
                value,
            )

        async def error(value: object) -> None:
            logger.error(
                "Feishu channel error binding_id=%s error_type=%s",
                binding.binding_id,
                type(value).__name__,
            )

        client.on("message", message)
        client.on("error", error)
        try:
            await client.connect_until_ready(timeout=30.0)
            self._transports.register(binding.binding_id, client)
            logger.info("Feishu binding authenticated binding_id=%s", binding.binding_id)
        except Exception:
            self._connections.pop(binding.binding_id, None)
            raise

    async def _disconnect(self, binding_id: UUID) -> None:
        connection = self._connections.pop(binding_id, None)
        if connection is None:
            return
        self._transports.unregister(binding_id, connection.client)
        await connection.client.disconnect()

    async def reconcile_once(self) -> None:
        current_loop = asyncio.get_running_loop()
        if self._application_loop is None:
            self._application_loop = current_loop
        elif self._application_loop is not current_loop:
            raise RuntimeError("Feishu supervisor cannot move between event loops")
        rows = await self._active_bindings()
        desired = {binding.binding_id: (binding, agent) for binding, agent in rows}
        self._pending_secrets.intersection_update(desired)
        for binding_id in tuple(self._connections):
            target = desired.get(binding_id)
            if (target is None
                    or self._connections[binding_id].fingerprint != self._fingerprint(target[0])):
                await self._disconnect(binding_id)
        for binding_id, (binding, agent) in desired.items():
            if binding_id in self._connections:
                self._connections[binding_id].agent = agent
                continue
            try:
                await self._connect(binding, agent, self._fingerprint(binding))
                self._pending_secrets.discard(binding_id)
            except LocalSecretNotReadyError:
                if binding_id not in self._pending_secrets:
                    logger.warning(
                        "Feishu binding is waiting for its App Secret binding_id=%s",
                        binding_id,
                    )
                self._pending_secrets.add(binding_id)
            except Exception as error:
                self._pending_secrets.discard(binding_id)
                logger.error(
                    "Feishu binding connection failed binding_id=%s error_type=%s",
                    binding_id,
                    type(error).__name__,
                )

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.reconcile_once()
            except Exception as error:
                # A temporary SQL or provider outage must not terminate the
                # process that owns every other tenant connection.
                logger.error("Feishu reconciliation failed error_type=%s", type(error).__name__)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval_seconds)
            except TimeoutError:
                pass

    async def start(self) -> None:
        if self._task is not None:
            return
        self._accepting_messages = True
        self._stop.clear()
        await self.reconcile_once()
        self._task = asyncio.create_task(self._run(), name="feishu-binding-supervisor")

    def stop_ingress(self) -> None:
        self._accepting_messages = False

    async def close(self) -> None:
        self.stop_ingress()
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self._message_tasks:
            _, pending = await asyncio.wait(tuple(self._message_tasks), timeout=30.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        for binding_id in tuple(self._connections):
            await self._disconnect(binding_id)
