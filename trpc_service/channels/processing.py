"""Channel-neutral inbound processing shared by webhook and pull workers."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from uuid import uuid4

from trpc_service.agent.execution import AgentReply, RunAgentCommand
from trpc_service.bus import ExecutionBus
from trpc_service.channels.models import ChannelMessage
from trpc_service.config.models import ChannelBindingRecord, InboundMessageRecord
from trpc_service.governance import InputPolicy, InputRejectedError, RequestRateLimiter
from trpc_service.metrics import ServiceMetrics
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import InboundMessageRepository, TenantRepository
from trpc_service.telemetry import tracer
from trpc_service.tenant.session_id import SessionIdFactory


@dataclass(frozen=True, slots=True)
class ChannelProcessResult:
    reply: AgentReply | None
    inbound: InboundMessageRecord | None

    @property
    def duplicate(self) -> bool:
        return self.inbound is None


class ChannelProcessor:
    def __init__(
        self,
        database: Database,
        bus: ExecutionBus,
        session_ids: SessionIdFactory,
        metrics: ServiceMetrics,
        input_policy: InputPolicy | None = None,
        rate_limiter: RequestRateLimiter | None = None,
    ) -> None:
        self._database = database
        self._bus = bus
        self._session_ids = session_ids
        self._metrics = metrics
        self._input_policy = input_policy or InputPolicy()
        self._rate_limiter = rate_limiter or RequestRateLimiter()

    async def process(
        self, binding: ChannelBindingRecord, message: ChannelMessage
    ) -> ChannelProcessResult:
        tenant = await TenantRepository(self._database).get(binding.tenant_id)
        if tenant is None:
            raise InputRejectedError("tenant does not exist")
        try:
            self._input_policy.authorize(message.sender_id, tenant.audit_policy)
            configured_limit = tenant.audit_policy.get("max_message_chars")
            max_chars = int(configured_limit) if configured_limit is not None else None
            normalized_text = self._input_policy.validate(message.text, max_chars=max_chars)
            configured_rate = tenant.audit_policy.get("requests_per_minute")
            rate = int(configured_rate) if configured_rate is not None else None
            await self._rate_limiter.check(f"{binding.tenant_id}:{message.sender_id}", rate)
        except ValueError:
            self._metrics.requests.labels(message.channel.value, "rejected").inc()
            raise

        trace_id = uuid4().hex
        session_id = self._session_ids.create(
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            channel=message.channel,
            account_id=message.account_id,
            principal_id=message.sender_id,
            conversation_id=message.conversation_id,
        )
        repository = InboundMessageRepository(self._database)
        inbound, created = await repository.register(
            InboundMessageRecord(
                inbound_id=uuid4().hex,
                tenant_id=binding.tenant_id,
                binding_id=binding.binding_id,
                external_message_id=message.external_message_id,
                session_id=session_id,
                trace_id=trace_id,
                payload={
                    "channel": message.channel.value,
                    "chat_type": message.chat_type,
                    "chat_id": message.chat_id,
                    "sender_id": message.sender_id,
                },
            )
        )
        if not created:
            self._metrics.requests.labels(message.channel.value, "duplicate").inc()
            return ChannelProcessResult(reply=None, inbound=None)

        command = RunAgentCommand(
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            user_id=message.sender_id,
            session_id=session_id,
            message=normalized_text,
            channel=message.channel,
            trace_id=trace_id,
        )
        started = perf_counter()
        try:
            with tracer.start_as_current_span(
                "channel.process",
                attributes={
                    "tenant.id": binding.tenant_id,
                    "messaging.system": message.channel.value,
                    "trpc.trace_id": trace_id,
                },
            ):
                reply = await self._bus.submit(command)
            await repository.update_status(inbound.inbound_id, "agent_completed")
            self._metrics.requests.labels(message.channel.value, "completed").inc()
            return ChannelProcessResult(reply=reply, inbound=inbound)
        except Exception:
            await repository.update_status(inbound.inbound_id, "failed")
            self._metrics.requests.labels(message.channel.value, "failed").inc()
            raise
        finally:
            self._metrics.agent_latency.labels(message.channel.value).observe(
                perf_counter() - started
            )

    async def mark_delivery(self, inbound_id: str, channel: str, delivered: bool) -> None:
        status = "delivered" if delivered else "send_failed"
        await InboundMessageRepository(self._database).update_status(inbound_id, status)
        metric_status = "completed" if delivered else "failed"
        self._metrics.channel_sends.labels(channel, metric_status).inc()


__all__ = ["ChannelProcessResult", "ChannelProcessor"]
