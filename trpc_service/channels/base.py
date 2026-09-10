"""Provider-independent channel adapter ports, readiness and stable errors."""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Awaitable, Callable, Protocol, runtime_checkable
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.channels.identity import (
    AuthenticatedSender,
    ChannelIdentity,
    ProviderReplyContext,
    RuntimeBotIdentity,
)
from trpc_service.storage.contracts import SecretBytes
from trpc_service.storage.models import AdapterFence, NodeIdentity
from trpc_service.channels.contracts import Channel, ConversationType


class ConnectionState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHENTICATED = "authenticated"
    READY = "ready"


class AdapterReadiness(StrEnum):
    READY = "ready"
    STANDBY = "standby"
    NOT_READY = "not_ready"


class AdapterEventDisposition(StrEnum):
    ACCEPTED = "accepted"
    IGNORED = "ignored"
    REJECTED = "rejected"


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProviderSendAck(_Frozen):
    acknowledged: bool
    provider_request_digest: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )


class AdapterEventResult(_Frozen):
    disposition: AdapterEventDisposition
    safe_code: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    trace_id: str | None = None


class ParsedProviderEvent(_Frozen):
    """SDK-free event produced at the provider boundary."""

    channel: Channel
    channel_identity_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    external_message_id: str = Field(min_length=1, max_length=128)
    external_conversation_id: str = Field(min_length=1, max_length=128)
    conversation_type: ConversationType
    sender: AuthenticatedSender | None = None
    message_type: str = Field(min_length=1, max_length=64)
    text: str = Field(default="", max_length=4000)
    bot_mentioned: bool
    reply_context: ProviderReplyContext
    received_at: datetime
    trace_id: UUID

    @classmethod
    def utc_now(cls) -> datetime:
        return datetime.now(timezone.utc)


class DeliveryResult(_Frozen):
    status: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    attempt_no: int | None = Field(default=None, ge=1, le=4)
    delivery_id: UUID | None = None
    execution_trace_id: UUID | None = None
    delivery_status: str | None = Field(
        default=None,
        pattern=r"^(pending|sending|retry_wait|delivered|delivery_failed|delivery_unknown)$",
    )


class ProviderError(RuntimeError):
    safe_message = "Provider operation failed."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class ProviderTransientError(ProviderError):
    safe_message = "Provider operation is temporarily unavailable."


class ProviderPermanentError(ProviderError):
    safe_message = "Provider operation was rejected."


class ProviderOutcomeUnknown(ProviderError):
    safe_message = "Provider delivery outcome is unknown."


class ProviderAuthenticationError(ProviderError):
    safe_message = "Provider authentication failed."


class ProviderConnectionLost(ProviderError):
    safe_message = "Provider connection was lost."


class ProviderProtocolError(ProviderError):
    safe_message = "Provider protocol data is invalid."


def classify_provider_error(error: BaseException) -> ProviderError:
    """Map only explicit safe provider failures; unknown errors stay unknown."""
    if isinstance(error, ProviderError):
        return error
    return ProviderOutcomeUnknown()


EventCallback = Callable[[Any], Awaitable[None]]
SignalCallback = Callable[[], Awaitable[None]]
ErrorCallback = Callable[[ProviderError], Awaitable[None]]


@runtime_checkable
class ProviderClientPort(Protocol):
    async def authenticate(self, secret: SecretBytes) -> RuntimeBotIdentity: ...
    async def connect(
        self,
        on_event: EventCallback,
        on_disconnect: SignalCallback,
        on_error: ErrorCallback,
    ) -> None: ...
    async def close(self) -> None: ...
    async def send_text(
        self, reply_context: ProviderReplyContext, text: str
    ) -> ProviderSendAck: ...
    def connection_state(self) -> ConnectionState: ...


@runtime_checkable
class ChannelAdapterPort(Protocol):
    async def start(self, channel_identity: ChannelIdentity, node_identity: Any) -> AdapterReadiness: ...
    async def stop(self, reason: str) -> None: ...
    async def handle_provider_event(self, provider_event: Any) -> AdapterEventResult: ...
    async def deliver(self, delivery_intent: Any, adapter_fence: Any) -> DeliveryResult: ...
    def readiness(self) -> AdapterReadiness: ...


class BaseChannelAdapter:
    """Shared lifecycle and SDK-free event entry point for real IM adapters."""

    channel: Channel

    def __init__(
        self,
        *,
        provider: ProviderClientPort,
        credential_secret: SecretBytes,
        message_service: Any,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.provider = provider
        self._credential_secret = credential_secret
        self.message_service = message_service
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._readiness = AdapterReadiness.NOT_READY
        self._channel_identity: ChannelIdentity | None = None
        self._runtime_bot_identity: RuntimeBotIdentity | None = None
        self._adapter_fence: AdapterFence | None = None

    def readiness(self) -> AdapterReadiness:
        return self._readiness

    @property
    def runtime_bot_identity(self) -> RuntimeBotIdentity | None:
        return self._runtime_bot_identity

    @property
    def adapter_fence(self) -> AdapterFence | None:
        return self._adapter_fence

    def replace_adapter_fence(self, fence: AdapterFence) -> None:
        if (
            self._channel_identity is None
            or fence.identity_digest != self._channel_identity.identity_digest
        ):
            raise ProviderAuthenticationError()
        self._adapter_fence = fence

    async def start(
        self, channel_identity: ChannelIdentity, node_identity: NodeIdentity
    ) -> AdapterReadiness:
        if channel_identity.channel != self.channel:
            raise ProviderAuthenticationError()
        if self._readiness == AdapterReadiness.READY:
            return self._readiness
        bot = await self.provider.authenticate(self._credential_secret)
        if (
            bot.channel != self.channel
            or bot.channel_identity_digest != channel_identity.identity_digest
        ):
            raise ProviderAuthenticationError()
        self._channel_identity = channel_identity
        self._runtime_bot_identity = bot
        self._adapter_fence = AdapterFence(
            identity_digest=channel_identity.identity_digest,
            node_id=node_identity.node_id,
            generation=1,
            owner_token=secrets.token_urlsafe(24),
            expires_at=self._now() + timedelta(hours=1),
        )
        await self.provider.connect(
            self.handle_provider_event,
            self._on_disconnect,
            self._on_error,
        )
        self._readiness = AdapterReadiness.READY
        return self._readiness

    async def _on_disconnect(self) -> None:
        self._readiness = AdapterReadiness.NOT_READY

    async def _on_error(self, error: ProviderError) -> None:
        del error
        self._readiness = AdapterReadiness.NOT_READY

    async def stop(self, reason: str) -> None:
        del reason
        if self._readiness == AdapterReadiness.NOT_READY and self.provider.connection_state() == ConnectionState.DISCONNECTED:
            return
        self._readiness = AdapterReadiness.NOT_READY
        await self.provider.close()

    def parse_provider_event(self, provider_event: Any) -> ParsedProviderEvent:
        raise NotImplementedError

    async def handle_provider_event(self, provider_event: Any) -> AdapterEventResult:
        if (
            self._readiness != AdapterReadiness.READY
            or self._channel_identity is None
            or self._runtime_bot_identity is None
            or self._adapter_fence is None
        ):
            return AdapterEventResult(
                disposition=AdapterEventDisposition.REJECTED,
                safe_code="adapter_not_ready",
            )
        try:
            parsed = self.parse_provider_event(provider_event)
        except ProviderProtocolError:
            return AdapterEventResult(
                disposition=AdapterEventDisposition.REJECTED,
                safe_code="provider_protocol_invalid",
            )
        return await self.message_service.handle(
            event=parsed,
            channel_identity=self._channel_identity,
            runtime_bot_identity=self._runtime_bot_identity,
            provider=self.provider,
            adapter_fence=self._adapter_fence,
        )

    async def deliver(self, delivery_intent: Any, adapter_fence: Any) -> DeliveryResult:
        if self._adapter_fence is None or adapter_fence != self._adapter_fence:
            return DeliveryResult(status="stale_fence")
        return await self.message_service.delivery_service.deliver_reply(
            **delivery_intent,
            provider=self.provider,
            adapter_fence=adapter_fence,
        )


__all__ = [name for name in globals() if not name.startswith("_")]
