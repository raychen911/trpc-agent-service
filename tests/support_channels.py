"""Credential-free SDK doubles and deterministic timing helpers for channel tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from trpc_service.channels.base import ConnectionState, ProviderSendAck
from trpc_service.channels.identity import ProviderReplyContext, RuntimeBotIdentity
from trpc_service.storage.contracts import SecretBytes
from trpc_service.channels.contracts import Channel
from trpc_service.channels.identity import ChannelIdentity
from trpc_service.config.settings import PlatformSettings
from trpc_service.tenant.models import AgentApplication, ChannelBinding, ResourceStatus, Tenant


@dataclass(slots=True)
class FakeProviderClient:
    bot_identity: RuntimeBotIdentity
    state: ConnectionState = ConnectionState.DISCONNECTED
    send_results: list[ProviderSendAck | Exception] = field(default_factory=list)
    sent: list[tuple[ProviderReplyContext, str]] = field(default_factory=list)
    close_count: int = 0
    _on_event: Callable[[Any], Awaitable[None]] | None = field(default=None, repr=False)

    async def authenticate(self, secret: SecretBytes) -> RuntimeBotIdentity:
        del secret
        self.state = ConnectionState.AUTHENTICATED
        return self.bot_identity

    async def connect(self, on_event, on_disconnect, on_error) -> None:
        del on_disconnect, on_error
        self._on_event = on_event
        self.state = ConnectionState.READY

    async def close(self) -> None:
        self.close_count += 1
        self.state = ConnectionState.DISCONNECTED

    async def send_text(
        self, reply_context: ProviderReplyContext, text: str
    ) -> ProviderSendAck:
        self.sent.append((reply_context, text))
        result = self.send_results.pop(0) if self.send_results else ProviderSendAck(acknowledged=True)
        if isinstance(result, Exception):
            raise result
        return result

    def connection_state(self) -> ConnectionState:
        return self.state

    async def emit(self, event: Any) -> None:
        if self._on_event is None:
            raise RuntimeError("fake provider is not connected")
        await self._on_event(event)


class FeishuSDKStub(FakeProviderClient):
    """Named fake used to prove the Feishu SDK is replaceable."""


class WeComSDKStub(FakeProviderClient):
    """Named fake used to prove the WeCom SDK is replaceable."""


@dataclass(slots=True)
class VirtualClock:
    current: datetime = field(
        default_factory=lambda: datetime(2026, 9, 8, tzinfo=timezone.utc)
    )
    sleeps: list[float] = field(default_factory=list)

    def now(self) -> datetime:
        return self.current

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)


@dataclass(slots=True)
class AgentCallCounter:
    calls: int = 0
    inputs: list[Any] = field(default_factory=list)

    async def __call__(self, value: Any) -> Any:
        self.calls += 1
        self.inputs.append(value)
        return value


def feishu_text_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "message_id": "feishu-message-001",
        "chat_id": "feishu-chat-001",
        "chat_type": "p2p",
        "sender": {"sender_type": "user", "sender_id": {"open_id": "feishu-user-001"}},
        "message_type": "text",
        "text": "你好",
        "mentions": [],
    }
    event.update(overrides)
    return event


def wecom_text_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "headers": {"req_id": "protocol-request-001"},
        "body": {
            "msgid": "wecom-message-001",
            "chatid": "wecom-chat-001",
            "chattype": "single",
            "from": {"userid": "wecom-user-001", "type": "user"},
            "msgtype": "text",
            "text": {"content": "你好"},
            "mentions": [],
        },
    }
    event.update(overrides)
    return event


def dual_im_settings(
    *,
    feishu_binding_status: ResourceStatus = ResourceStatus.ACTIVE,
    feishu_tenant_status: ResourceStatus = ResourceStatus.ACTIVE,
    feishu_agent_status: ResourceStatus = ResourceStatus.ACTIVE,
) -> tuple[PlatformSettings, dict[Channel, ChannelIdentity]]:
    identities = {
        Channel.FEISHU: ChannelIdentity(
            channel=Channel.FEISHU,
            provider_tenant_key="feishu-tenant-alpha",
            provider_app_or_bot_id="feishu-bot-alpha",
        ),
        Channel.WECOM: ChannelIdentity(
            channel=Channel.WECOM,
            provider_tenant_key="wecom-corp-beta",
            provider_app_or_bot_id="wecom-bot-beta",
        ),
    }
    tenants = (
        Tenant(
            tenant_id="tenant-alpha", display_name="Alpha",
            status=feishu_tenant_status, created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            config_version=2,
        ),
        Tenant(
            tenant_id="tenant-beta", display_name="Beta",
            status=ResourceStatus.ACTIVE, created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            config_version=2,
        ),
    )
    agents = (
        AgentApplication(
            tenant_id="tenant-alpha", agent_id="agent-alpha", agent_name="Alpha Agent",
            status=feishu_agent_status, model_profile="deterministic-offline",
            instruction="deterministic", config_version=3,
        ),
        AgentApplication(
            tenant_id="tenant-beta", agent_id="agent-beta", agent_name="Beta Agent",
            status=ResourceStatus.ACTIVE, model_profile="deterministic-offline",
            instruction="deterministic", config_version=3,
        ),
    )
    bindings = (
        ChannelBinding(
            binding_id="binding-feishu-alpha", tenant_id="tenant-alpha",
            agent_id="agent-alpha", channel=Channel.FEISHU,
            status=feishu_binding_status, secret_ref="LARK_APP_SECRET",
            signature_version="v1",
            provider_tenant_key=identities[Channel.FEISHU].provider_tenant_key,
            provider_app_or_bot_id=identities[Channel.FEISHU].provider_app_or_bot_id,
            channel_identity_digest=identities[Channel.FEISHU].identity_digest,
            config_version=4, created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        ChannelBinding(
            binding_id="binding-wecom-beta", tenant_id="tenant-beta",
            agent_id="agent-beta", channel=Channel.WECOM,
            status=ResourceStatus.ACTIVE, secret_ref="WECOM_BOT_SECRET",
            signature_version="v1",
            provider_tenant_key=identities[Channel.WECOM].provider_tenant_key,
            provider_app_or_bot_id=identities[Channel.WECOM].provider_app_or_bot_id,
            channel_identity_digest=identities[Channel.WECOM].identity_digest,
            config_version=4, created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
    )
    return PlatformSettings(tenants=tenants, agents=agents, bindings=bindings), identities


__all__ = [
    "AgentCallCounter",
    "FakeProviderClient",
    "FeishuSDKStub",
    "VirtualClock",
    "WeComSDKStub",
    "feishu_text_event",
    "dual_im_settings",
    "wecom_text_event",
]
