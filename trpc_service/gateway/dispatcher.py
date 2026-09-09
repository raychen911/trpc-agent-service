# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Worker queue consumption and Outbox delivery roles."""

from __future__ import annotations

import asyncio
import hashlib
import time

from trpc_service.channels import ChannelAdapter
from trpc_service.gateway.models import OutboundMessage
from trpc_service.gateway.models import RequestState
from trpc_service.gateway.outbox import OutboxStore
from trpc_service.gateway.queue import AgentTaskQueue
from trpc_service.gateway.queue import QueueDelivery
from trpc_service.gateway.service import GatewayService
from trpc_service.metrics import current_trace_context
from trpc_service.metrics import platform_span
from trpc_service.metrics import MetricsRegistry
from trpc_service.agent.runtime import IncompleteRunError
from trpc_service.resources.attachments import UnsupportedAttachmentError


class AgentTaskProcessor:
    """Consume one at-least-once task and commit its reply to the Outbox."""

    def __init__(self,
                 queue: AgentTaskQueue,
                 gateway: GatewayService,
                 outbox: OutboxStore,
                 *,
                 consumer: str,
                 max_attempts: int = 5,
                 metrics: MetricsRegistry | None = None,
                 usage_ledger: object | None = None,
                 registry: object | None = None,
                 budget: object | None = None,
                 attachments: object | None = None) -> None:
        self._attachments = attachments
        self._queue = queue
        self._gateway = gateway
        self._outbox = outbox
        self._consumer = consumer
        self._max_attempts = max_attempts
        self._metrics = metrics
        self._usage_ledger = usage_ledger
        self._registry = registry
        self._budget = budget

    @property
    def consumer(self) -> str:
        """Return the Redis Streams Consumer identity owned by this processor."""
        return self._consumer

    async def process_one(self, timeout_seconds: float = 5.0) -> bool:
        delivery = await self._queue.receive(self._consumer, timeout_seconds=timeout_seconds)
        if delivery is None:
            return False
        return await self.process_delivery(delivery)

    async def reclaim_stale(self, *, idle_ms: int = 60000, limit: int = 100) -> int:
        """Re-enter claimed pending messages through the normal Processor path."""
        processed = 0
        for delivery in await self._queue.reclaim_stale(self._consumer, idle_ms=idle_ms, count=limit):
            await self._gateway.record_recovery(delivery.envelope.request)
            if await self.process_delivery(delivery):
                processed += 1
        return processed

    async def process_delivery(self, delivery: QueueDelivery) -> bool:
        """Process a new or reclaimed delivery and ACK only after Outbox commit."""
        request = delivery.envelope.request
        with platform_span("queue.agent_task.process", request.trace):
            request.trace = current_trace_context()
            return await self._process_claimed(delivery)

    async def _process_claimed(self, delivery: QueueDelivery) -> bool:
        request = delivery.envelope.request
        record = await self._gateway.ensure_request(request, delivery.envelope.idempotency_key)
        if record.state == RequestState.FAILED:
            await self._queue.dead_letter(delivery, record.error_code or "request_failed")
            return False
        try:
            previous = await self._gateway.request_status(request.tenant_id, request.request_id)
            if previous.state == RequestState.SUCCEEDED:
                await self._queue.ack(delivery)
                return True
            else:
                if request.attachments:
                    if self._attachments is None:
                        raise UnsupportedAttachmentError("attachment ingestor is not configured")
                    request = await self._attachments.materialize(previous.request or request)
                    await self._gateway.prepare(request)
                await self._gateway.chat(
                    request,
                    delivery.envelope.idempotency_key,
                    finalize_idempotency=False,
                    abandon_on_error=False,
                    commit_callback=lambda result: self._commit_processed(request, result),
                )
            # Compatibility cache only; durable success is already committed.
            await self._gateway.complete_reservation(delivery.envelope.idempotency_key, request.request_id)
            await self._queue.ack(delivery)
            if self._metrics:
                self._metrics.increment("trpc_service_agent_tasks_total",
                                        result="succeeded",
                                        channel=request.channel.value)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._metrics:
                self._metrics.increment("trpc_service_agent_tasks_total",
                                        result="failed",
                                        channel=request.channel.value)
            # Gateway.stream records the concrete model/runner failure.  On a
            # recovery pass, IncompleteRunError is intentionally raised to
            # stop an unsafe replay; do not let that internal guard obscure
            # the original cause exposed by the request status API.
            current = await self._gateway.request_status(request.tenant_id, request.request_id)
            error_code = current.error_code or type(error).__name__
            if isinstance(error, (IncompleteRunError, UnsupportedAttachmentError)) or (delivery.envelope.attempts + 1
                                                                                       >= self._max_attempts):
                await self._gateway.mark_failed(request, error_code)
                await self._queue.dead_letter(delivery, error_code)
            else:
                await self._gateway.mark_retryable(request, error_code)
                await self._queue.retry(delivery)
                await self._gateway.record_recovery(request)
            return False

    async def _commit_processed(self, request, result):
        if self._registry is not None:
            tenant = await self._registry.get(request.tenant_id)
            app = await self._registry.get_app(request.tenant_id, request.app_id, request.config_version)
            if self._usage_ledger is not None:
                await self._usage_ledger.record(tenant_id=request.tenant_id,
                                                app_id=request.app_id,
                                                request_id=request.request_id,
                                                model_name=app.model.model_name,
                                                input_tokens=result.usage.input_tokens,
                                                output_tokens=result.usage.output_tokens,
                                                cost_usd=result.usage.cost_usd)
            settle = getattr(self._budget, "settle_actual", None)
            if settle:
                reserved = request.metadata.get("budget_reservation", {})
                await settle(tenant,
                             request_id=request.request_id,
                             budget_day=str(reserved.get("day", "")),
                             input_tokens=result.usage.input_tokens - int(reserved.get("input_tokens", 0)),
                             output_tokens=result.usage.output_tokens - int(reserved.get("output_tokens", 0)),
                             cost_usd=result.usage.cost_usd - float(reserved.get("cost_usd", 0)))
            if self._metrics:
                self._metrics.increment("trpc_service_model_input_tokens_total",
                                        result.usage.input_tokens,
                                        model=app.model.model_name)
                self._metrics.increment("trpc_service_model_output_tokens_total",
                                        result.usage.output_tokens,
                                        model=app.model.model_name)
                self._metrics.increment("trpc_service_model_cost_usd_total",
                                        result.usage.cost_usd,
                                        model=app.model.model_name)
        messages = []
        if request.binding_id:
            chunks = ([result.text[index:index + 4096] for index in range(0, len(result.text), 4096)]
                      if request.channel.value == "telegram" else [result.text]) or [""]
            if request.channel.value == "wecom_kf":
                chunks, chunk = [], ""
                for char in result.text:
                    if len((chunk + char).encode("utf-8")) > 2048:
                        chunks.append(chunk)
                        chunk = ""
                    chunk += char
                chunks.append(chunk)
            for index, chunk in enumerate(chunks):
                outbound_id = hashlib.sha256(f"{request.request_id}:reply:{index}".encode()).hexdigest()
                messages.append(
                    OutboundMessage(outbound_id=outbound_id,
                                    request_id=request.request_id,
                                    tenant_id=request.tenant_id,
                                    binding_id=request.binding_id,
                                    channel=request.channel,
                                    external_conversation_id=str(request.metadata["external_conversation_id"]),
                                    reply_to_message_id=str(request.metadata.get("reply_to_message_id", "")),
                                    text=chunk,
                                    trace=request.trace))
        await self._gateway.commit_result(request, result, messages, self._outbox)


class DeliveryWorker:
    """Claim Outbox rows and deliver them through the active channel binding."""

    def __init__(self,
                 outbox: OutboxStore,
                 adapters: dict[str, ChannelAdapter],
                 metrics: MetricsRegistry | None = None) -> None:
        self._outbox = outbox
        self._adapters = adapters
        self._metrics = metrics

    async def deliver_due(self, limit: int = 100) -> int:
        delivered = 0
        binding_ids = [key for key, adapter in self._adapters.items() if getattr(adapter, "available", lambda: True)()]
        for record in await self._outbox.claim(limit, binding_ids=binding_ids):
            started = time.perf_counter()
            adapter = self._adapters.get(record.message.binding_id)
            if adapter is None:
                await self._outbox.failed(record.message.outbound_id, "channel adapter is unavailable", retryable=True)
                self._record(record.message.channel.value, "unavailable", started)
                continue
            try:
                with platform_span("channel.outbound.deliver", record.message.trace,
                                   {"trpc_service.channel": record.message.channel.value}):
                    result = await adapter.deliver(record.message)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._outbox.failed(record.message.outbound_id, type(error).__name__, retryable=True)
                self._record(record.message.channel.value, "retry", started)
                continue
            if result.uncertain:
                await self._outbox.unknown(record.message.outbound_id, result.error_code)
                self._record(record.message.channel.value, "unknown", started)
                continue
            if result.delivered:
                await self._outbox.delivered(record.message.outbound_id, result.external_message_id)
                delivered += 1
                self._record(record.message.channel.value, "delivered", started)
            else:
                await self._outbox.failed(
                    record.message.outbound_id,
                    result.error_code,
                    retryable=result.retryable,
                    retry_after_seconds=result.retry_after_seconds,
                )
                self._record(record.message.channel.value, "retry" if result.retryable else "dead", started)
        return delivered

    def _record(self, channel: str, result: str, started: float) -> None:
        if self._metrics is None:
            return
        self._metrics.increment("trpc_service_delivery_total", channel=channel, result=result)
        self._metrics.observe("trpc_service_delivery_duration_seconds",
                              max(0.0,
                                  time.perf_counter() - started),
                              channel=channel,
                              result=result)
