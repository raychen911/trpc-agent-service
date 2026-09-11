"""Outbound channel dispatcher with a testable delivery sink."""

from __future__ import annotations

from collections.abc import Callable

from ..metrics.prometheus import record_outbound
from ..tenant.models import ChannelBinding, InboundMessage, OutboundMessage
from .base import ChannelAdapter


class ChannelDispatcher:
    def __init__(
        self,
        adapters: dict[str, ChannelAdapter],
        sink: Callable[[OutboundMessage], None] | None = None,
        binding_resolver: Callable[[str, str, str], ChannelBinding] | None = None,
        delivery_store: object | None = None,
    ) -> None:
        self.adapters = adapters
        self.sink = sink
        self.binding_resolver = binding_resolver
        self.deliveries: list[OutboundMessage] = []
        self.delivery_store = delivery_store

    async def reply(
        self,
        inbound: InboundMessage,
        text: str,
        binding: ChannelBinding | None = None,
    ) -> list[OutboundMessage]:
        adapter = self.adapters[inbound.channel]
        messages = adapter.to_outbound(inbound, text)
        for message in messages:
            if self.delivery_store is not None:
                state = await self.delivery_store.begin_outbound(message)
                if state == "delivered":
                    continue
                if state in {"ambiguous", "terminal_failed"}:
                    raise RuntimeError(f"{inbound.channel} delivery requires review: {state}")
            selected_binding = binding
            if selected_binding is None and self.binding_resolver is not None:
                selected_binding = self.binding_resolver(message.tenant_id, message.channel, message.account_id)
            receipt = await adapter.send(message, selected_binding)
            if self.delivery_store is not None:
                await self.delivery_store.finish_outbound(message, receipt)
            if not receipt.get("ok"):
                record_outbound(message.tenant_id, message.channel, "failed")
                raise RuntimeError(f"{inbound.channel} delivery failed: {receipt.get('code', 'unknown')}")
            record_outbound(message.tenant_id, message.channel, "delivered")
            self.deliveries.append(message)
            if self.sink:
                self.sink(message)
        return messages
