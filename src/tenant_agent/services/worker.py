"""Stateless Agent Worker loop."""

from __future__ import annotations

import asyncio
import logging
import random

from tenant_agent.observability import ERRORS, extracted_trace_context
from tenant_agent.services.broker import BrokerMessage, JobBroker
from tenant_agent.services.config import TenantConfigService
from tenant_agent.services.dispatcher import TurnDispatcher
from tenant_agent.settings import Settings

logger = logging.getLogger(__name__)


class AgentWorker:
    def __init__(
        self,
        *,
        settings: Settings,
        broker: JobBroker,
        configs: TenantConfigService,
        dispatcher: TurnDispatcher,
    ) -> None:
        self.settings = settings
        self.broker = broker
        self.configs = configs
        self.dispatcher = dispatcher

    async def _handle_message(self, message: BrokerMessage) -> None:
        try:
            with extracted_trace_context(message.routed.inbound.trace_context):
                current = await self.configs.repository.get_active_tenant(message.routed.inbound.tenant_id)
                if current is None:
                    raise ValueError("tenant is no longer active")
                if not any(
                    binding.enabled
                    and binding.channel == message.routed.inbound.channel
                    and binding.binding_id == message.routed.inbound.binding_id
                    for binding in current.channels
                ):
                    raise ValueError("channel binding is no longer active")
                tenant = await self.configs.exact_revision(
                    message.routed.inbound.tenant_id, message.routed.config_revision
                )
                result = await self.dispatcher.process(tenant=tenant, routed=message.routed)
            if result.status == "duplicate_processing":
                await self.broker.defer(
                    message,
                    delay_seconds=self.settings.worker_duplicate_defer_seconds,
                    count_attempt=False,
                )
            else:
                await self.broker.ack(message)
        except Exception as exc:
            terminal = isinstance(exc, (ValueError, KeyError))
            logger.warning("worker job failed with %s (terminal=%s)", exc.__class__.__name__, terminal)
            if terminal:
                await self.broker.fail(message, terminal=True)
            else:
                cap = min(
                    self.settings.worker_retry_max_seconds,
                    self.settings.worker_retry_initial_seconds * (2 ** min(message.attempts, 8)),
                )
                delay = random.uniform(cap / 2, cap) if cap else 0  # noqa: S311 - retry jitter only
                await self._defer_or_dead(message, delay_seconds=delay)

    async def _defer_or_dead(self, message: BrokerMessage, *, delay_seconds: float) -> None:
        if message.attempts + 1 >= self.settings.worker_max_attempts:
            await self.broker.fail(message, terminal=True)
        else:
            await self.broker.defer(message, delay_seconds=delay_seconds)

    async def run_once(self) -> bool:
        message = await self.broker.receive(timeout_ms=self.settings.worker_poll_ms)
        if message is None:
            return False
        await self._handle_message(message)
        return True

    async def run_forever(self, stop: asyncio.Event) -> None:
        in_flight: set[asyncio.Task[None]] = set()

        def observe(tasks: set[asyncio.Task[None]]) -> None:
            for task in tasks:
                if task.cancelled():
                    continue
                try:
                    error = task.exception()
                except asyncio.CancelledError:
                    continue
                if error is not None:
                    error_type = error.__class__.__name__
                    ERRORS.labels("_system", "worker_task", error_type).inc()
                    logger.warning("Worker task failed with %s", error_type)

        try:
            while not stop.is_set():
                if len(in_flight) >= self.settings.worker_concurrency:
                    done, in_flight = await asyncio.wait(
                        in_flight,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    observe(done)
                    continue
                try:
                    message = await self.broker.receive(timeout_ms=self.settings.worker_poll_ms)
                except Exception as exc:
                    error_type = exc.__class__.__name__
                    ERRORS.labels("_system", "worker_loop", error_type).inc()
                    logger.warning("Worker receive loop failed with %s", error_type)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=1.0)
                    except TimeoutError:
                        pass
                    continue
                if message is None:
                    continue
                task = asyncio.create_task(
                    self._handle_message(message),
                    name=f"agent-turn:{message.broker_id}",
                )
                in_flight.add(task)
                completed = {item for item in in_flight if item.done()}
                in_flight.difference_update(completed)
                observe(completed)
        finally:
            if in_flight:
                results = await asyncio.gather(*in_flight, return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                        error_type = result.__class__.__name__
                        ERRORS.labels("_system", "worker_task", error_type).inc()
                        logger.warning("Worker task failed during drain with %s", error_type)
