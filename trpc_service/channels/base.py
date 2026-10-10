"""Channel contracts shared by webhook and streaming providers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol

from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    InboundEnvelope,
    OutboundEnvelope,
    PayloadKind,
    RecallEnvelope,
    VerifiedCallback,
)
from trpc_service.tenant.models import ChannelBinding


@dataclass(frozen=True, slots=True)
class WebhookRequest:
    method: str
    query: Mapping[str, str] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""


class WebhookChannelAdapter(Protocol):
    def verify_url(self, request: WebhookRequest, binding: ChannelBinding) -> str: ...

    def verify_and_parse(
        self, request: WebhookRequest, binding: ChannelBinding
    ) -> VerifiedCallback: ...


@dataclass(frozen=True, slots=True)
class ChannelCapabilities:
    """Implemented outbound behavior, not the provider's theoretical surface."""

    outbound_payloads: frozenset[PayloadKind]
    stream: bool
    card: bool
    media: bool
    recall: bool
    proactive: bool
    text_split: bool
    max_text_bytes: int | None


class ChannelAdapter(Protocol):
    capabilities: ChannelCapabilities

    async def send(
        self, envelope: OutboundEnvelope, binding: ChannelBinding
    ) -> DeliveryReceipt: ...

    async def recall(
        self, envelope: RecallEnvelope, binding: ChannelBinding
    ) -> DeliveryReceipt: ...


InboundSink = Callable[[str, InboundEnvelope], Awaitable[None]]


class StreamingChannelConnector(Protocol):
    async def run(self, binding: ChannelBinding, sink: InboundSink) -> None: ...


__all__ = [
    "ChannelAdapter",
    "ChannelCapabilities",
    "InboundSink",
    "StreamingChannelConnector",
    "WebhookChannelAdapter",
    "WebhookRequest",
]
