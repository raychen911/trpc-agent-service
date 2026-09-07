"""Durable tenant routing for WeCom intelligent-bot WebSocket connections."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from tenant_agent.channels.base import SignatureError, UnsupportedMessage
from tenant_agent.channels.registry import ChannelRegistry
from tenant_agent.channels.wecom_bot import (
    BotDisconnected,
    WeComBotAdapter,
    WeComBotConnection,
    bot_outbox_kind,
)
from tenant_agent.ids import stable_checksum
from tenant_agent.models import ChannelBindingConfig, ChannelType, TenantConfig
from tenant_agent.observability import ERRORS, REQUESTS, inject_trace_context, traced
from tenant_agent.security import CompositeSecretResolver, Redactor
from tenant_agent.services.broker import JobBroker
from tenant_agent.services.config import TenantConfigService
from tenant_agent.services.dispatcher import GatewayRouter
from tenant_agent.services.outbox import OutboxWorker
from tenant_agent.settings import Settings
from tenant_agent.storage.base import ConfigRepository, LeaseProvider, OutboxRepository, SessionLeaseTimeout
from tenant_agent.storage.router import StorageRouter

logger = logging.getLogger(__name__)
_LOCK_TENANT = "__wecom_bot_transport__"


class WeComBotManager:
    """Runs one leased socket and one scoped outbox lane per active bot binding."""

    def __init__(
        self,
        *,
        settings: Settings,
        configs: TenantConfigService,
        repository: ConfigRepository,
        leases: LeaseProvider,
        broker: JobBroker,
        gateway: GatewayRouter,
        storage: StorageRouter,
        outbox_repository: OutboxRepository,
        secrets: CompositeSecretResolver,
        redactor: Redactor,
    ) -> None:
        self.settings = settings
        self.configs = configs
        self.repository = repository
        self.leases = leases
        self.broker = broker
        self.gateway = gateway
        self.storage = storage
        self.outbox_repository = outbox_repository
        self.secrets = secrets
        self.redactor = redactor
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._blocked: set[str] = set()
        self._failures: dict[str, int] = {}
        self._retry_after: dict[str, float] = {}

    async def run_forever(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    await self._refresh()
                except Exception as exc:
                    logger.warning("WeCom bot configuration refresh failed error=%s", exc.__class__.__name__)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=10)
                except TimeoutError:
                    pass
        finally:
            tasks = tuple(self._tasks.values())
            self._tasks.clear()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _refresh(self) -> None:
        tenants = await self.repository.list_active_tenants()
        seen: set[str] = set()
        bindings: list[tuple[TenantConfig, ChannelBindingConfig, str, str]] = []
        bot_ids: dict[str, str] = {}
        for tenant in tenants:
            for binding in tenant.channels:
                if not binding.enabled or binding.channel is not ChannelType.WECOM_BOT:
                    continue
                bot_id = await self.secrets.resolve(binding.credential_refs["bot_id"])
                if bot_id in bot_ids:
                    duplicate_tasks = tuple(self._tasks.values())
                    self._tasks.clear()
                    for duplicate_task in duplicate_tasks:
                        duplicate_task.cancel()
                    await asyncio.gather(*duplicate_tasks, return_exceptions=True)
                    raise RuntimeError("one WeCom Bot ID must have exactly one active binding")
                bot_ids[bot_id] = tenant.tenant_id
                key = f"{tenant.tenant_id}:{binding.binding_id}:{tenant.revision}"
                seen.add(key)
                bindings.append((tenant, binding, bot_id, key))
        stale = [self._tasks.pop(key) for key in set(self._tasks) - seen]
        self._blocked.intersection_update(seen)
        for stale_task in stale:
            stale_task.cancel()
        await asyncio.gather(*stale, return_exceptions=True)
        for tenant, binding, bot_id, key in bindings:
            existing_task = self._tasks.get(key)
            if (
                key not in self._blocked
                and (existing_task is None or existing_task.done())
                and asyncio.get_running_loop().time() >= self._retry_after.get(key, 0)
            ):
                self._tasks[key] = asyncio.create_task(
                    self._run_binding(tenant, binding, bot_id, key),
                    name=f"wecom-bot:{binding.binding_id}",
                )

    async def _run_binding(
        self, tenant: TenantConfig, binding: ChannelBindingConfig, bot_id: str, key: str
    ) -> None:
        lock_id = "bot_" + stable_checksum(bot_id)[:48]
        owner = f"wecom-bot:{self.settings.node_id}:{uuid.uuid4().hex[:8]}"
        lease_seconds = max(90, int(self.settings.model_timeout_seconds) + 60)
        try:
            async with self.leases.acquire_session(
                tenant_id=_LOCK_TENANT,
                session_id=lock_id,
                owner=owner,
                wait_timeout=10,
                lease_seconds=lease_seconds,
            ):
                secret = await self.secrets.resolve(binding.credential_refs["bot_secret"])
                connection = WeComBotConnection(timeout=self.settings.delivery_timeout_seconds)
                adapter = WeComBotAdapter(connection)
                worker = OutboxWorker(
                    settings=self.settings,
                    repository=self.outbox_repository,
                    storage=self.storage,
                    configs=self.configs,
                    channels=ChannelRegistry(wecom_bot=adapter),
                    secrets=self.secrets,
                    redactor=self.redactor,
                    kinds=(bot_outbox_kind(tenant.tenant_id, binding.binding_id),),
                )
                progress_sent: set[str] = set()

                async def handle(frame: dict[str, Any]) -> None:
                    with traced(
                        "im.callback",
                        {"tenant.id": tenant.tenant_id, "messaging.system": "wecom_bot"},
                        redactor=self.redactor,
                    ):
                        try:
                            envelope = adapter.parse_frame(
                                frame, tenant=tenant, binding=binding, bot_id=bot_id
                            )
                        except (UnsupportedMessage, SignatureError) as exc:
                            ERRORS.labels(tenant.tenant_id, "wecom_bot_input", exc.__class__.__name__).inc()
                            return
                        if envelope is None:
                            return
                        envelope = envelope.model_copy(update={"trace_context": inject_trace_context()})
                        routed = self.gateway.route(envelope, tenant)
                        req_id = envelope.metadata.get("wecom_bot_req_id")
                        stream_id = envelope.metadata.get("wecom_bot_stream_id")
                        if not isinstance(req_id, str) or not isinstance(stream_id, str):
                            raise RuntimeError("WeCom bot callback correlation is missing")
                        if envelope.message_id not in progress_sent:
                            await connection.request(
                                "aibot_respond_msg",
                                {
                                    "msgtype": "stream",
                                    "stream": {
                                        "id": stream_id,
                                        "finish": False,
                                        "content": "Working on it...",
                                    },
                                },
                                req_id=req_id,
                            )
                            progress_sent.add(envelope.message_id)
                            if len(progress_sent) > 2_048:
                                progress_sent.pop()
                        async with asyncio.timeout(10):
                            try:
                                await self.broker.publish(routed)
                            except Exception:
                                await connection.request(
                                    "aibot_respond_msg",
                                    {
                                        "msgtype": "stream",
                                        "stream": {
                                            "id": stream_id,
                                            "finish": True,
                                            "content": (
                                                "The assistant is temporarily unavailable. "
                                                "Please retry shortly."
                                            ),
                                        },
                                    },
                                    req_id=req_id,
                                )
                                raise
                        REQUESTS.labels(tenant.tenant_id, "wecom_bot", "accepted").inc()

                async with connection.connected(bot_id, secret, handle):
                    self._failures.pop(key, None)
                    delivery = asyncio.create_task(
                        self._delivery_loop(worker, connection),
                        name=f"wecom-bot-outbox:{binding.binding_id}",
                    )
                    closed = asyncio.create_task(connection.closed.wait())
                    try:
                        await asyncio.wait((delivery, closed), return_when=asyncio.FIRST_COMPLETED)
                        if delivery.done():
                            await delivery
                    finally:
                        delivery.cancel()
                        closed.cancel()
                        await asyncio.gather(delivery, closed, return_exceptions=True)
                    if connection.failure is not None:
                        raise connection.failure
                    failures = self._failures.get(key, 0) + 1
                    self._failures[key] = failures
                    self._retry_after[key] = asyncio.get_running_loop().time() + min(
                        60, 2 ** min(failures, 6)
                    )
        except asyncio.CancelledError:
            raise
        except SessionLeaseTimeout:
            return
        except (SignatureError, BotDisconnected) as exc:
            self._blocked.add(key)
            ERRORS.labels(tenant.tenant_id, "wecom_bot_connection", exc.__class__.__name__).inc()
            logger.warning("WeCom bot requires credential/owner review binding=%s", binding.binding_id)
        except Exception as exc:
            logger.warning(
                "WeCom bot connection stopped binding=%s error=%s",
                binding.binding_id,
                exc.__class__.__name__,
            )
            failures = self._failures.get(key, 0) + 1
            self._failures[key] = failures
            self._retry_after[key] = asyncio.get_running_loop().time() + min(60, 2 ** min(failures, 6))

    async def _delivery_loop(self, worker: OutboxWorker, connection: WeComBotConnection) -> None:
        while not connection.closed.is_set():
            await worker.run_once(limit=16)
            try:
                await asyncio.wait_for(connection.closed.wait(), timeout=0.2)
            except TimeoutError:
                continue
