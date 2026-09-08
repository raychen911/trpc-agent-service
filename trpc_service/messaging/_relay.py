"""Independent Outbox delivery service."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC
from abc import abstractmethod
from contextlib import suppress
from typing import Optional

from trpc_service.channels._models import SendResult
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from ._models import OutboxMessage
from ._repository import MessageStoreABC

logger = logging.getLogger(__name__)


class DeliveryTransportABC(ABC):
    """Minimal provider boundary consumed by :class:`OutboxRelay`."""

    @abstractmethod
    async def send_part(self, message: OutboxMessage, part: str, part_index: int) -> SendResult:
        """Deliver exactly one checkpointed part."""


class OutboxRelay:
    """Lease and deliver reply intents without invoking an Agent turn."""

    def __init__(
        self,
        *,
        store: MessageStoreABC,
        transport: DeliveryTransportABC,
        owner: str,
        max_attempts: int = 8,
        lease_seconds: float = 30,
        metrics: Optional[EnterpriseMetrics] = None,
    ) -> None:
        self._store = store
        self._transport = transport
        self._owner = owner
        self._max_attempts = max_attempts
        self._lease_seconds = lease_seconds
        self._metrics = metrics or get_enterprise_metrics()

    async def run_once(self, limit: int = 20) -> int:
        messages = await self._store.claim_outbox(
            owner=self._owner,
            limit=limit,
            lease_seconds=self._lease_seconds,
        )
        for message in messages:
            await self._deliver(message)
        return len(messages)

    async def _deliver(self, message: OutboxMessage) -> None:
        part_index = message.next_part
        started = time.perf_counter()
        outcome = "error"
        error_type = None
        try:
            result = await self._transport.send_part(message, message.parts[part_index], part_index)
            if not result.ok:
                raise RuntimeError(result.error or "channel delivery failed")
            await self._store.checkpoint_part(
                message.event_id,
                owner=self._owner,
                part_index=part_index,
                provider_message_id=result.message_id,
            )
            outcome = "success"
        except Exception as exc:  # noqa: BLE001 - failure becomes durable retry state
            error_type = type(exc).__name__
            with suppress(Exception):
                await self._store.fail_delivery(
                    message.event_id,
                    owner=self._owner,
                    part_index=part_index,
                    error=str(exc),
                    max_attempts=self._max_attempts,
                )
            logger.exception("outbox delivery failed for event=%s", message.event_id)
        finally:
            attributes = {
                "tenant_id": message.tenant_id,
                "channel": message.channel,
                "outcome": outcome,
                "error_type": error_type,
            }
            self._metrics.increment("agent_im_delivery_total", **attributes)
            self._metrics.observe(
                "agent_im_delivery_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )
            self._metrics.observe("agent_im_delivery_parts", 1, **attributes)

    async def run(self, poll_interval_seconds: float = 1.0) -> None:
        while True:
            try:
                processed = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - transient store failures must not kill the relay
                logger.exception("outbox polling failed")
                processed = 0
            if processed == 0:
                await asyncio.sleep(poll_interval_seconds)
