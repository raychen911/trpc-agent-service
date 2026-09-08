"""Channel-backed implementation of the reliable delivery transport port."""

from __future__ import annotations

from typing import Any

from trpc_service.messaging._models import OutboxMessage
from trpc_service.messaging._relay import DeliveryTransportABC
from ._models import InboundMessage
from ._models import SendResult


class ChannelDeliveryTransport(DeliveryTransportABC):
    """Resolve the recorded tenant revision and send one outbound part."""

    def __init__(self, *, manager: Any, registry: Any) -> None:
        self._manager = manager
        self._registry = registry

    async def send_part(self, message: OutboxMessage, part: str, part_index: int) -> SendResult:
        tenant = (self._manager.get_version(message.tenant_id, message.config_revision)
                  if message.config_revision is not None else self._manager.get(message.tenant_id))
        if tenant is None:
            raise RuntimeError("tenant revision is unavailable for outbox delivery")
        adapter = self._registry.get(tenant, message.channel)
        if adapter is None:
            raise RuntimeError("channel binding is unavailable for outbox delivery")
        inbound = InboundMessage.model_validate(message.inbound)
        inbound.metadata = {
            **inbound.metadata,
            "outbox_event_id": message.event_id,
            "outbox_part_index": part_index,
            "turn_id": message.turn_id,
        }
        result = await adapter.reply_text(inbound, part)
        return result or SendResult(ok=True)
