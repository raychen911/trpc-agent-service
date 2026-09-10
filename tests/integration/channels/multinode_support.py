"""Credential-free two-node harness for real-IM shared-state tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from tests.support_channels import (
    FakeProviderClient,
    dual_im_settings,
    feishu_text_event,
    wecom_text_event,
)
from trpc_service.channels.contracts import Channel
from trpc_service.channels.delivery import DeliveryService, InMemoryDeliveryRepository
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.gateway.service import GatewayService
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.storage.contracts import SecretBytes
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.locks import SessionLockManager
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.worker.service import AgentExecutor


NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)


class SharedSessionFactoryView:
    """Give each Worker its own Runner cache over one shared Session backend."""

    def __init__(self, shared: SessionBackendFactory) -> None:
        self.shared = shared

    def get_backend(self, tenant_id: str, agent_id: str):
        return self.shared.get_backend(tenant_id, agent_id)

    async def close(self) -> None:
        # The harness owns and closes the shared backend after both Workers.
        return None


@dataclass
class TwoNodeIMHarness:
    channel: Channel
    platform: InMemoryPlatformAdapters
    delivery_repository: InMemoryDeliveryRepository
    delivery_service: DeliveryService
    session_factory: SessionBackendFactory
    workers: list[Any]
    adapters: list[Any]
    providers: list[FakeProviderClient]
    identity: Any

    @classmethod
    async def create(
        cls,
        channel: Channel,
        *,
        workers: list[Any] | None = None,
        delivery_repository: InMemoryDeliveryRepository | None = None,
    ) -> "TwoNodeIMHarness":
        settings, identities = dual_im_settings()
        identity = identities[channel]
        platform = InMemoryPlatformAdapters(settings)
        shared_sessions = SessionBackendFactory()
        worker_list = workers or [
            AgentExecutor(SharedSessionFactoryView(shared_sessions)),
            AgentExecutor(SharedSessionFactoryView(shared_sessions)),
        ]
        locks = SessionLockManager()
        repository = delivery_repository or InMemoryDeliveryRepository(now=lambda: NOW)
        delivery = DeliveryService(repository, now=lambda: NOW)
        adapter_type = (
            FeishuChannelAdapter if channel == Channel.FEISHU else WeComChannelAdapter
        )
        adapters: list[Any] = []
        providers: list[FakeProviderClient] = []
        for index, worker in enumerate(worker_list):
            provider = FakeProviderClient(
                RuntimeBotIdentity(
                    channel=channel,
                    sender_type="bot",
                    sender_id=f"runtime-{channel.value}-bot",
                    channel_identity_digest=identity.identity_digest,
                    authenticated_at=NOW,
                )
            )
            gateway = GatewayService(
                platform,
                InMemoryMetricsRecorder(),
                worker,
                locks=locks,
                now=lambda: NOW,
            )
            service = ChannelMessageService(
                platform,
                gateway,
                delivery,
                now=lambda: NOW,
            )
            adapter = adapter_type(
                provider=provider,
                credential_secret=SecretBytes(b"credential-placeholder"),
                message_service=service,
                now=lambda: NOW,
            )
            await adapter.start(identity, NodeIdentity(node_id=f"{channel.value}-node-{index + 1}"))
            adapters.append(adapter)
            providers.append(provider)
        return cls(
            channel=channel,
            platform=platform,
            delivery_repository=repository,
            delivery_service=delivery,
            session_factory=shared_sessions,
            workers=worker_list,
            adapters=adapters,
            providers=providers,
            identity=identity,
        )

    @property
    def agent_calls(self) -> int:
        return sum(getattr(worker, "call_count", 0) for worker in self.workers)

    def event(
        self,
        *,
        message_id: str,
        conversation_id: str,
        sender_id: str,
        text: str,
        group: bool = False,
    ) -> dict[str, Any]:
        bot_id = f"runtime-{self.channel.value}-bot"
        if self.channel == Channel.FEISHU:
            return feishu_text_event(
                message_id=message_id,
                chat_id=conversation_id,
                chat_type="group" if group else "p2p",
                sender={
                    "sender_type": "user",
                    "sender_id": {"open_id": sender_id},
                },
                text=f"@_bot {text}" if group else text,
                mentions=(
                    [{"id": {"open_id": bot_id}, "key": "@_bot"}] if group else []
                ),
            )
        base = wecom_text_event()
        return wecom_text_event(
            headers={"req_id": f"request-{message_id}"},
            body={
                **base["body"],
                "msgid": message_id,
                "chatid": conversation_id,
                "chattype": "group" if group else "single",
                "from": {"userid": sender_id, "type": "user"},
                "text": {"content": f"@bot {text}" if group else text},
                "mentions": (
                    [{"userid": bot_id, "key": "@bot"}] if group else []
                ),
            },
        )

    async def close(self) -> None:
        for adapter in self.adapters:
            await adapter.stop("test_complete")
        for worker in self.workers:
            closer = getattr(worker, "close", None)
            if closer is not None:
                await closer()
        await self.session_factory.close()
