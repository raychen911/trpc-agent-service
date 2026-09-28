"""Long-connection runtime for active enterprise WeCom bindings."""

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import json
import logging
from typing import Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.admin.models import ChannelAdapterType
from trpc_service.admin.secret_store import TenantSecretStore
from trpc_service.agent.configuration import select_agent_config_version
from trpc_service.agent.models import AgentApp
from trpc_service.channels.adapters.wecom import (
    WeComChannelAdapter,
    WeComReplyClient,
    WeComTransportRegistry,
)
from trpc_service.channels.contracts import ChannelBindingConfig
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.wecom import WeComMessageService
from trpc_service.config.secret_scope import validate_tenant_channel_secret_ref
from trpc_service.config.storage import resolve_local_secret
from trpc_service.metrics import PlatformTelemetry
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import Tenant

logger = logging.getLogger(__name__)


class WeComClient(WeComReplyClient, Protocol):
    """Typed subset of the SDK needed by the connection supervisor."""

    def on(self, event: str, handler: Callable[..., object]) -> "WeComClient":
        ...

    async def connect(self) -> "WeComClient":
        ...

    async def disconnect(self) -> None:
        ...


class WeComClientFactory(Protocol):
    """Factory seam allowing protocol tests without a real WeCom account."""

    def create(self, bot_id: str, secret: str) -> WeComClient:
        ...


class _SafeSDKLogger:
    """Drop SDK arguments because received frames may contain user content."""

    @staticmethod
    def debug(message: str, *args: object) -> None:
        del message, args

    @staticmethod
    def info(message: str, *args: object) -> None:
        del message, args
        logger.debug("WeCom SDK lifecycle event")

    @staticmethod
    def warn(message: str, *args: object) -> None:
        del message, args
        logger.warning("WeCom SDK warning")

    @staticmethod
    def error(message: str, *args: object) -> None:
        del message, args
        logger.error("WeCom SDK error")


class SDKWeComClientFactory:
    """Construct the pinned SDK behind a project-owned typed boundary."""

    def create(self, bot_id: str, secret: str) -> WeComClient:
        from wecom_aibot_sdk import WSClient

        # Unlimited reconnects keep a configured connector alive through short
        # provider or network outages; process supervision handles hard faults.
        return cast(
            WeComClient,
            WSClient(
                bot_id,
                secret,
                max_reconnect_attempts=-1,
                logger=_SafeSDKLogger(),
            ),
        )


@dataclass(slots=True)
class _Connection:
    client: WeComClient
    fingerprint: str
    # Agent rollout pointers can change without changing the channel binding.
    # Keep the latest database snapshot here so callbacks do not retain stale
    # configuration through the closure created when the socket connected.
    agent: AgentApp


class WeComBindingSupervisor:
    """Reconcile active tenant bindings into one connection per Bot ID."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        adapter: WeComChannelAdapter,
        transports: WeComTransportRegistry,
        messages: WeComMessageService,
        telemetry: PlatformTelemetry,
        *,
        client_factory: WeComClientFactory,
        poll_interval_seconds: float = 5.0,
        secret_store: TenantSecretStore | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("WeCom reconciliation interval must be positive")
        self._sessions = sessions
        self._adapter = adapter
        self._transports = transports
        self._messages = messages
        self._telemetry = telemetry
        self._client_factory = client_factory
        self._secret_store = secret_store
        self._poll_interval_seconds = poll_interval_seconds
        self._connections: dict[UUID, _Connection] = {}
        self._accepting_messages = True
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @staticmethod
    def _fingerprint(row: ChannelBinding) -> str:
        return json.dumps(
            {
                "account": row.account_config,
                "secrets": row.secret_ref_map,
                "updated": row.updated_at.isoformat(),
            },
            sort_keys=True,
            default=str,
        )

    async def _active_bindings(self) -> list[tuple[ChannelBinding, AgentApp]]:
        async with self._sessions() as database:
            result = await database.execute(
                select(ChannelBinding, AgentApp).join(
                    Tenant,
                    Tenant.tenant_id == ChannelBinding.tenant_id,
                ).join(
                    AgentApp,
                    (AgentApp.tenant_id == ChannelBinding.tenant_id)
                    & (AgentApp.agent_app_id == ChannelBinding.agent_app_id),
                ).join(
                    ChannelAdapterType,
                    ChannelAdapterType.channel_type == ChannelBinding.channel_type,
                ).where(
                    ChannelBinding.channel_type == "wecom",
                    ChannelBinding.status == "active",
                    Tenant.status == "active",
                    AgentApp.status == "active",
                    ChannelAdapterType.status == "active",
                ))
            return [(binding, agent) for binding, agent in result.all()]

    @staticmethod
    def _rollout_key(frame: Mapping[str, object]) -> str | None:
        body = frame.get("body")
        if not isinstance(body, Mapping):
            return None
        sender = body.get("from")
        if not isinstance(sender, Mapping):
            return None
        user_id = sender.get("userid")
        return user_id if isinstance(user_id, str) else None

    async def _handle_message(
        self,
        binding: ChannelBindingConfig,
        agent: AgentApp,
        frame: object,
    ) -> None:
        if not self._accepting_messages:
            # During shutdown the provider receives no progress response and
            # can redeliver later; accepting work after Workers stop would
            # create an avoidable backlog.
            return
        if not isinstance(frame, Mapping):
            logger.warning("WeCom SDK returned a non-object message frame")
            return
        with self._telemetry.start_span(
                "channel.receive",
                attributes={"channel.type": "wecom"},
        ):
            tenant = TenantContext(
                tenant_id=binding.tenant_id,
                agent_app_id=binding.agent_app_id,
                config_version=select_agent_config_version(
                    agent,
                    self._rollout_key(frame),
                ),
                request_id=str(uuid4()),
                trace_id=self._telemetry.current_trace_id(),
            )
            await self._messages.submit(frame, binding, tenant)

    async def _connect(self, row: ChannelBinding, agent: AgentApp, fingerprint: str) -> None:
        binding = row.to_config()
        bot_id = self._adapter.bot_id(binding)
        secret_ref = binding.secret_ref_map.get("bot_secret")
        if not isinstance(secret_ref, str):
            raise ValueError("WeCom binding requires bot_secret SecretRef")
        validate_tenant_channel_secret_ref(secret_ref, binding.tenant_id)
        secret = (resolve_local_secret(secret_ref) if self._secret_store is None else await
                  self._secret_store.resolve(secret_ref, binding.tenant_id))
        client = self._client_factory.create(bot_id, secret)
        connection = _Connection(
            client=client,
            fingerprint=fingerprint,
            agent=agent,
        )
        self._connections[binding.binding_id] = connection

        def authenticated() -> None:
            self._transports.register(binding.binding_id, client)
            logger.info("WeCom binding authenticated binding_id=%s", binding.binding_id)

        def disconnected(reason: object) -> None:
            del reason
            self._transports.unregister(binding.binding_id, client)
            logger.warning("WeCom binding disconnected binding_id=%s", binding.binding_id)

        async def message(frame: object) -> None:
            try:
                await self._handle_message(binding, connection.agent, frame)
            except Exception as error:
                # Record only the exception class and binding correlation ID.
                # Provider frames can contain user content and encrypted URLs.
                logger.error(
                    "WeCom message handling failed binding_id=%s error_type=%s",
                    binding.binding_id,
                    type(error).__name__,
                )
                raise

        client.on("authenticated", authenticated)
        client.on("disconnected", disconnected)
        for event_name in ("message.text", "message.image", "message.mixed", "message.file",
                           "message.voice", "message.video"):
            client.on(event_name, message)
        try:
            await client.connect()
        except Exception:
            self._connections.pop(binding.binding_id, None)
            self._transports.unregister(binding.binding_id, client)
            raise

    async def _disconnect(self, binding_id: UUID) -> None:
        connection = self._connections.pop(binding_id, None)
        if connection is None:
            return
        self._transports.unregister(binding_id, connection.client)
        await connection.client.disconnect()

    async def reconcile_once(self) -> None:
        """Apply binding additions, configuration changes, and removals once."""

        rows = await self._active_bindings()
        desired = {binding.binding_id: (binding, agent) for binding, agent in rows}
        for binding_id in tuple(self._connections):
            current = self._connections[binding_id]
            target = desired.get(binding_id)
            if target is None or current.fingerprint != self._fingerprint(target[0]):
                await self._disconnect(binding_id)
        for binding_id, (binding, agent) in desired.items():
            if binding_id in self._connections:
                # Reconciliation also refreshes Agent rollout state. A model or
                # config release must not require reconnecting the provider.
                self._connections[binding_id].agent = agent
                continue
            try:
                await self._connect(binding, agent, self._fingerprint(binding))
            except Exception as error:
                # Do not expose provider errors or resolved credentials in logs.
                logger.error(
                    "WeCom binding connection failed binding_id=%s error_type=%s",
                    binding_id,
                    type(error).__name__,
                )

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.reconcile_once()
            except Exception as error:
                logger.error("WeCom reconciliation failed error_type=%s", type(error).__name__)
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._poll_interval_seconds,
                )
            except TimeoutError:
                continue

    async def start(self) -> None:
        if self._task is not None:
            return
        self._accepting_messages = True
        self._stop.clear()
        await self.reconcile_once()
        self._task = asyncio.create_task(self._run(), name="wecom-binding-supervisor")

    def stop_ingress(self) -> None:
        """Reject new callbacks while keeping transports alive for reply drain."""

        self._accepting_messages = False

    async def close(self) -> None:
        self.stop_ingress()
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
        for binding_id in tuple(self._connections):
            await self._disconnect(binding_id)
