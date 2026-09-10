"""Outbound long-lived channel workers that do not require public webhooks."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, Protocol

from wecom_aibot_sdk import DefaultLogger, WSClient, WSClientOptions, WsFrame, generate_req_id

from trpc_service.agent.execution import AgentExecutionService
from trpc_service.agent.runtime import TenantRunnerFactory
from trpc_service.bus import ExecutionBus, InlineExecutionBus, RedisExecutionBus
from trpc_service.channels.models import ChannelMessage
from trpc_service.channels.processing import ChannelProcessor
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.config.models import ChannelBindingRecord, ChannelMode, ChannelType
from trpc_service.config.secrets import SecretResolver
from trpc_service.config.settings import (
    AppEnvironment,
    ExecutionBackend,
    ServiceSettings,
    get_settings,
)
from trpc_service.metrics import ServiceMetrics
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import ChannelBindingRepository
from trpc_service.storage.router import TenantStorageRouter
from trpc_service.tenant.session_id import SessionIdFactory
from trpc_service.worker import WorkerService

logger = logging.getLogger(__name__)


class TelegramPollingClient(Protocol):
    async def get_updates(
        self, token: str, offset: int | None, timeout: int = 30
    ) -> list[dict[str, Any]]: ...

    async def send(self, token: str, message: ChannelMessage, text: str) -> None: ...

    @staticmethod
    def parse(account_id: str, payload: dict[str, Any]) -> ChannelMessage | None: ...

    async def close(self) -> None: ...


class TelegramPollingWorker:
    def __init__(
        self,
        binding: ChannelBindingRecord,
        token: str,
        processor: ChannelProcessor,
        adapter: TelegramPollingClient | None = None,
    ) -> None:
        self.binding = binding
        self._token = token
        self._processor = processor
        self._adapter = adapter or TelegramAdapter()

    async def run(self) -> None:
        offset: int | None = None
        try:
            while True:
                try:
                    offset = await self.poll_once(offset)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Telegram polling failed for binding %s: %s",
                        self.binding.binding_id,
                        type(exc).__name__,
                    )
                    await asyncio.sleep(2)
        finally:
            await self._adapter.close()

    async def poll_once(self, offset: int | None) -> int | None:
        updates = await self._adapter.get_updates(self._token, offset)
        next_offset = offset
        for update in updates:
            update_id = update.get("update_id")
            if not isinstance(update_id, int):
                continue
            next_offset = max(next_offset or 0, update_id + 1)
            message = self._adapter.parse(self.binding.account_id, update)
            if message is None:
                continue
            result = await self._processor.process(self.binding, message)
            if result.duplicate:
                continue
            assert result.reply is not None and result.inbound is not None
            try:
                await self._adapter.send(self._token, message, result.reply.text)
                await self._processor.mark_delivery(result.inbound.inbound_id, "telegram", True)
            except Exception:
                await self._processor.mark_delivery(result.inbound.inbound_id, "telegram", False)
                raise
        return next_offset


class WeComAIBotWorker:
    def __init__(
        self,
        binding: ChannelBindingRecord,
        secret: str,
        processor: ChannelProcessor,
        client_factory: Callable[[WSClientOptions], WSClient] = WSClient,
    ) -> None:
        self.binding = binding
        self._processor = processor
        self._client = client_factory(
            WSClientOptions(
                bot_id=binding.account_id,
                secret=secret,
                max_reconnect_attempts=-1,
                logger=DefaultLogger(logging.WARNING),
            )
        )
        self._client.on("message.text", self._on_text)

    async def run(self) -> None:
        await self._client.connect_async()
        try:
            for _ in range(100):
                if self._client.is_authenticated:
                    break
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError("WeCom AIBot authentication timed out")
            await asyncio.Event().wait()
        finally:
            await self._client.disconnect()

    async def _on_text(self, frame: WsFrame) -> None:
        message = self.parse(self.binding.account_id, frame.body)
        if message is None:
            return
        result = await self._processor.process(self.binding, message)
        if result.duplicate:
            return
        assert result.reply is not None and result.inbound is not None
        try:
            await self._client.reply_stream(
                frame,
                generate_req_id("stream"),
                result.reply.text,
                finish=True,
            )
            await self._processor.mark_delivery(result.inbound.inbound_id, "wecom", True)
        except Exception:
            await self._processor.mark_delivery(result.inbound.inbound_id, "wecom", False)
            raise

    @staticmethod
    def parse(account_id: str, body: Any) -> ChannelMessage | None:
        if not isinstance(body, dict) or body.get("msgtype") != "text":
            return None
        text_data = body.get("text")
        sender_data = body.get("from")
        content = text_data.get("content") if isinstance(text_data, dict) else None
        sender = sender_data.get("userid") if isinstance(sender_data, dict) else None
        sender = sender or body.get("from_userid")
        message_id = body.get("msgid")
        if not all(isinstance(value, str) and value for value in (content, sender, message_id)):
            return None
        chat_type = str(body.get("chattype", "single"))
        chat_id = str(body.get("chatid") or sender)
        return ChannelMessage(
            external_message_id=message_id,
            channel=ChannelType.WECOM,
            account_id=account_id,
            chat_type=chat_type,
            chat_id=chat_id,
            sender_id=sender,
            text=content,
            metadata={"aibot_id": body.get("aibotid")},
        )


async def run_pull_channels(settings: ServiceSettings | None = None) -> None:
    service_settings = settings or get_settings()
    database = Database(service_settings.database_url)
    secrets = SecretResolver()
    storage_router = TenantStorageRouter(
        database, secrets, artifact_root=service_settings.artifact_root
    )
    runners = TenantRunnerFactory(
        service_settings,
        database=database,
        secrets=secrets,
        storage_router=storage_router,
    )
    metrics = ServiceMetrics()
    telegram_workers: list[TelegramPollingWorker] = []
    wecom_workers: list[WeComAIBotWorker] = []
    await database.initialize()
    bus: ExecutionBus | None = None
    try:
        session_secret = _resolve_session_secret(service_settings, secrets)
        if service_settings.execution_backend == ExecutionBackend.REDIS:
            assert service_settings.queue_redis_url_ref is not None
            bus = RedisExecutionBus(
                database,
                secrets.resolve(service_settings.queue_redis_url_ref),
                stream=service_settings.queue_stream,
                result_timeout_seconds=service_settings.queue_result_timeout_seconds,
            )
        else:
            bus = InlineExecutionBus(
                WorkerService(
                    AgentExecutionService(
                        database,
                        runners,
                        storage_router=storage_router,
                        metrics=metrics,
                        timeout_seconds=service_settings.agent_timeout_seconds,
                    )
                )
            )
        processor = ChannelProcessor(
            database,
            bus,
            SessionIdFactory(session_secret),
            metrics,
        )
        bindings = await ChannelBindingRepository(database).list_active(ChannelMode.PULL)
        for binding in bindings:
            if binding.channel_type == ChannelType.TELEGRAM:
                if not binding.token_ref:
                    raise RuntimeError(f"binding {binding.binding_id} requires token_ref")
                telegram_workers.append(
                    TelegramPollingWorker(binding, secrets.resolve(binding.token_ref), processor)
                )
            elif binding.channel_type == ChannelType.WECOM:
                if not binding.secret_ref:
                    raise RuntimeError(f"binding {binding.binding_id} requires secret_ref")
                wecom_workers.append(
                    WeComAIBotWorker(binding, secrets.resolve(binding.secret_ref), processor)
                )

        workers = [*telegram_workers, *wecom_workers]
        if not workers:
            raise RuntimeError("no active pull-mode channel bindings found")
        logger.info("Starting %d pull-mode channel worker(s)", len(workers))
        await asyncio.gather(*(worker.run() for worker in workers))
    finally:
        if isinstance(bus, RedisExecutionBus):
            await bus.close()
        await runners.close()
        await storage_router.close()
        await database.dispose()


def _resolve_session_secret(settings: ServiceSettings, secrets: SecretResolver) -> str:
    if settings.session_hmac_key_ref:
        return secrets.resolve(settings.session_hmac_key_ref)
    if settings.app_env == AppEnvironment.TEST:
        return "test-only-session-hmac-key"
    raise RuntimeError("session_hmac_key_ref is required")


__all__ = ["TelegramPollingWorker", "WeComAIBotWorker", "run_pull_channels"]
