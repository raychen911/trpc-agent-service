"""Owned IM connections with cancellation, SQL leases and durable poll acknowledgements."""

import asyncio
from contextlib import suppress
import hashlib
import logging
import os
import random
import uuid

from trpc_service.telemetry.runtime import operation, count
from trpc_service.tenant import ChannelBindingRegistry, MessageRouter, SessionIdentityFactory
from trpc_service.storage.runtime_resources import resolve_env
from .events import safe_media_filename
from .models import AttachmentRef, NormalizedInboundMessage
from .persistence import ContextCipher, IMAccounts, IMIngress, AccountOwnershipError
from .delivery import IMDeliveryWorker


def quiet_transport_logging():
    """Protocol libraries can log token-bearing URLs and raw callback bodies at debug level."""
    for name in ("telegram", "httpx", "httpcore", "websockets", "wecom_aibot_sdk", "Lark"):
        logging.getLogger(name).setLevel(logging.CRITICAL + 1)
        for child in list(logging.Logger.manager.loggerDict):
            if child.startswith(name + "."):
                logging.getLogger(child).setLevel(logging.CRITICAL + 1)


class ChannelRuntime:

    def __init__(self, database, store, *, identity_key: bytes, encryption_keys: list[bytes], owner_id=None):
        if database.engine.dialect.name != "postgresql":
            raise ValueError("independent IM processes require PostgreSQL row locks")
        if len(identity_key) < 32:
            raise ValueError("identity key must contain at least 32 bytes")
        self.database, self.store = database, store
        self.identities = SessionIdentityFactory(lambda _: identity_key)
        self.cipher = ContextCipher(encryption_keys)
        self.accounts = IMAccounts(database)
        self.ingress = IMIngress(database, self.cipher, self.identities)
        self.owner_id = owner_id or "channel-" + uuid.uuid4().hex
        self._artifact_services = {}
        self._artifact_lock = asyncio.Lock()
        quiet_transport_logging()

    @staticmethod
    def credential(binding, name):
        reference = next((value.secret_ref.uri for value in binding.credential_refs if value.name == name), None)
        if reference is None:
            raise ValueError("required channel credential reference is missing")
        return resolve_env(reference)

    async def run(self, stop):
        tasks = {}
        try:
            while not stop.is_set():
                tenants = await asyncio.to_thread(self.store.active)
                desired = {
                    (tenant.tenant_id, binding.binding_id): (tenant, binding)
                    for tenant in tenants if tenant.status.value == "active" for binding in tenant.channel_bindings
                    if binding.enabled and binding.transport in {"wecom_ws", "telegram_polling", "feishu_ws"}
                }
                for key, (snapshot, task) in list(tasks.items()):
                    if key not in desired or snapshot != desired[key]:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        del tasks[key]
                for key, snapshot in desired.items():
                    if key not in tasks:
                        tasks[key] = (snapshot, asyncio.create_task(self.consume_account(*snapshot, stop)))
                try:
                    await asyncio.wait_for(stop.wait(), 3)
                except asyncio.TimeoutError:
                    pass
        finally:
            for _, task in tasks.values():
                task.cancel()
            await asyncio.gather(*(task for _, task in tasks.values()), return_exceptions=True)
            await asyncio.gather(*(service.close() for service in self._artifact_services.values()),
                                 return_exceptions=True)
            self._artifact_services.clear()

    async def consume_account(self, tenant, binding, stop):
        while not stop.is_set():
            lease = None
            try:
                lease = await asyncio.to_thread(self.accounts.acquire, binding, self.owner_id)
                if lease:
                    router = MessageRouter(ChannelBindingRegistry([tenant]), self.identities)
                    await self.run_owned(tenant, binding, lease, router, stop)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Never export the original dependency exception or configured URL.
                self.connection_failed(binding, error)
            finally:
                if lease:
                    with suppress(AccountOwnershipError):
                        await asyncio.to_thread(self.accounts.release, lease)
            try:
                await asyncio.wait_for(stop.wait(), random.uniform(3, 6))
            except asyncio.TimeoutError:
                pass

    async def run_owned(self, tenant, binding, lease, router, stop):
        tasks, adapter = [], None
        try:
            if binding.transport == "telegram_polling":
                from telegram import Bot
                from .telegram import TelegramAdapter
                adapter = TelegramAdapter(Bot(self.credential(binding, "bot_token")))
                await adapter.bot.initialize()
                if str(adapter.bot.id) != binding.external_account_id:
                    raise AccountOwnershipError("Telegram token belongs to a different account")
                info = await adapter.bot.get_webhook_info()
                if info.url:
                    raise ValueError("Telegram webhook must be explicitly removed before selecting polling")
                receive = self.poll_telegram(adapter, binding, lease, router)
            elif binding.transport == "feishu_ws":
                from .feishu import FeishuAdapter
                adapter = FeishuAdapter(binding.external_account_id, self.credential(binding, "app_secret"))
                await adapter.initialize()
                receive = adapter.receive(binding,
                                          lambda event: self.record(event, binding, lease, router),
                                          transform=lambda event: self.materialize_media(
                                              event, adapter, tenant, binding, router),
                                          on_ready=lambda: self.connection_ready(binding))
            else:
                from .wecom import WecomAdapter
                adapter = WecomAdapter.create(binding.external_account_id, self.credential(binding, "bot_secret"))
                receive = self.receive_wecom(adapter, tenant, binding, lease, router)
            delivery = IMDeliveryWorker(self.database, lease, adapter, self.cipher)
            from trpc_service.governance.action_delivery import ActionNoticeWorker
            notices = ActionNoticeWorker(self.database, lease, adapter, self.cipher)
            tasks = [
                asyncio.create_task(receive),
                asyncio.create_task(self.renew(lease)),
                asyncio.create_task(self.deliver(delivery, notices)),
                asyncio.create_task(stop.wait())
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if adapter:
                with suppress(Exception):
                    if binding.transport == "telegram_polling":
                        await adapter.bot.shutdown()
                    else:
                        await adapter.close()

    async def renew(self, lease):
        while True:
            await asyncio.sleep(15)
            await asyncio.to_thread(self.accounts.renew, lease)

    def connection_ready(self, binding):
        count("im.connection_ready", channel=binding.channel.value)

    def connection_failed(self, binding, error):
        count("im.connection_failure", channel=binding.channel.value)
        with operation("im.connection_failure",
                       attributes={
                           "tenant.id": binding.tenant_id,
                           "channel": binding.channel.value,
                           "error.type": type(error).__name__
                       }):
            pass

    async def deliver(self, worker, notices=None):
        while True:
            delivered = await worker.run_once()
            notified = await notices.run_once() if notices is not None else False
            if not delivered and not notified:
                await asyncio.sleep(0.5)

    async def record(self, event, binding, lease, router, *, next_offset=None):
        route = None
        if event.message:
            # Re-resolve on every callback to honor canary selection and immediate disable.
            route = await asyncio.to_thread(self.store.route_message, event.message)
            expected = router.route_message(event.message, expected_binding=binding)
            if route.model_copy(update={"config_version": expected.config_version}) != expected:
                raise AccountOwnershipError("binding identity changed during connection lifetime")
        with operation("im.callback", attributes={"tenant.id": binding.tenant_id, "channel": binding.channel.value}):
            receipt = await asyncio.to_thread(self.ingress.record,
                                              event,
                                              binding,
                                              lease,
                                              route=route,
                                              next_offset=next_offset)
            count("im.ingress", channel=binding.channel.value, disposition=receipt.disposition)
            return receipt

    async def artifact_service(self, tenant):
        key = (tenant.tenant_id, tenant.config_version)
        async with self._artifact_lock:
            if key not in self._artifact_services:
                from trpc_service.storage.runtime_resources import build_artifact_resource
                self._artifact_services[key] = await build_artifact_resource(self.store, tenant, self.database)
            return self._artifact_services[key]

    async def materialize_media(self, event, adapter, tenant, binding, router):
        if not event.pending_media:
            return event
        if not await asyncio.to_thread(self.ingress.authorized, event, binding):
            return event.model_copy(update={"pending_media": ()})
        route = await asyncio.to_thread(self.store.route_message, event.message)
        expected = router.route_message(event.message, expected_binding=binding)
        if route.model_copy(update={"config_version": expected.config_version}) != expected:
            raise AccountOwnershipError("binding identity changed during media staging")
        service = await self.artifact_service(tenant)
        from trpc_agent_sdk.abc import ArtifactId
        from trpc_agent_sdk.artifacts import create_artifact_uri
        from trpc_agent_sdk.types import Part
        attachments = []
        for pending in event.pending_media:
            payload, filename, mime_type = await adapter.download_media(
                pending, event.reply_context.get("message_id"))
            digest = hashlib.sha256(payload).hexdigest()
            filename = hashlib.sha256(event.event_id.encode()).hexdigest()[:12] + "-" + safe_media_filename(
                filename, pending.kind)
            artifact_id = ArtifactId(app_name=tenant.tenant_id + ":" + binding.agent_app_id,
                                     user_id=route.internal_user_id,
                                     session_id=route.session_id,
                                     filename=filename)
            existing = await service.load_artifact(artifact_id=artifact_id)
            if existing is not None:
                metadata = existing.version.custom_metadata or {}
                if metadata.get("sha256") != digest:
                    raise ValueError("media event was reused with different content")
                version = existing.version.version
            else:
                version = await service.save_artifact(
                    artifact_id=artifact_id,
                    artifact=Part.from_bytes(data=payload, mime_type=mime_type),
                    metadata={"source": binding.channel.value, "sha256": digest})
            attachments.append(AttachmentRef(artifact_id=create_artifact_uri(artifact_id, version),
                                             filename=filename,
                                             mime_type=mime_type,
                                             size_bytes=len(payload),
                                             sha256=digest))
        message_data = event.message.model_dump(mode="json")
        message_data.update(text="", attachments=[item.model_dump(mode="json") for item in attachments])
        message = NormalizedInboundMessage.model_validate(message_data)
        return event.model_copy(update={"message": message, "pending_media": ()})

    async def poll_telegram(self, adapter, binding, lease, router):
        while True:
            offset = await asyncio.to_thread(self.accounts.offset, lease)
            updates = await adapter.poll(offset)
            for update in sorted(updates, key=lambda value: value.update_id):
                await self.record(adapter.normalize(update.to_dict(), binding),
                                  binding,
                                  lease,
                                  router,
                                  next_offset=update.update_id + 1)

    async def receive_wecom(self, adapter, tenant, binding, lease, router):
        lost, authenticated = asyncio.Event(), asyncio.Event()

        async def disconnected(_):
            lost.set()

        async def ready(_):
            authenticated.set()

        async def receive(frame):
            try:
                event = adapter.normalize(frame, binding)
                event = await self.materialize_media(event, adapter, tenant, binding, router)
                await self.record(event, binding, lease, router)
            except Exception as error:
                # The public client swallows handler exceptions; force connection teardown.
                self.connection_failed(binding, error)
                lost.set()

        adapter.client.on("authenticated", ready)
        adapter.client.on("disconnected", disconnected)
        adapter.client.on("error", disconnected)
        adapter.client.on("message", receive)
        adapter.client.on("event", receive)
        await asyncio.wait_for(adapter.client.connect_async(), 15)
        await asyncio.wait_for(authenticated.wait(), 15)
        self.connection_ready(binding)
        # Closing the public client cancels its own reconnect task before account release.
        await lost.wait()


async def run_channel_process(database, store, stop):
    runtime = ChannelRuntime(
        database,
        store,
        identity_key=os.environ["TRPC_IDENTITY_KEY"].encode(),
        encryption_keys=[value.encode() for value in os.environ["TRPC_IM_CONTEXT_KEYS"].split(",")])
    await runtime.run(stop)
