"""Channel reply delivery orchestration."""

from __future__ import annotations

import json
import asyncio
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Callable
from uuid import UUID, uuid4

from trpc_service.channels.base import (
    DeliveryResult,
    ProviderOutcomeUnknown,
    ProviderPermanentError,
    ProviderTransientError,
)
from trpc_service.channels.contracts import DeliveryAction, OutboundReply
from trpc_service.channels.identity import ProviderReplyContext, length_prefixed_digest
from trpc_service.storage.contracts import ConditionalWriteFailed
from trpc_service.storage.models import (
    AdapterFence,
    DeliveryAttempt,
    DeliveryOutcome,
    DeliveryRecord,
    DeliveryStatus,
    ExecutionResult,
    ExecutionStatus,
)


class InMemoryDeliveryRepository:
    """Deterministic repository used by adapter contract and local integration tests."""

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
        fence_validator: Any | None = None,
    ) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._records: dict[tuple[str, str, str, str], DeliveryRecord] = {}
        self._attempts: dict[UUID, DeliveryAttempt] = {}
        self.fence_validator = fence_validator

    async def _require_fence(self, fence: AdapterFence) -> None:
        if self.fence_validator is None:
            return
        valid = self.fence_validator(fence)
        if hasattr(valid, "__await__"):
            valid = await valid
        if not valid:
            from trpc_service.storage.contracts import StaleFence

            raise StaleFence("Stale adapter fence was rejected.")

    @staticmethod
    def _tenant_id(scope: Any) -> str:
        return scope.tenant_id if hasattr(scope, "tenant_id") else str(scope)

    async def create_or_get(self, tenant_scope, binding_scope, execution_result, reply_context, adapter_fence):
        await self._require_fence(adapter_fence)
        tenant_id = self._tenant_id(tenant_scope)
        key = (tenant_id, binding_scope.channel.value, binding_scope.binding_id, reply_context.provider_message_id)
        if key in self._records:
            return self._records[key]
        payload = execution_result.model_dump(mode="json")
        record = DeliveryRecord(
            delivery_id=uuid4(),
            tenant_id=tenant_id,
            binding_id=binding_scope.binding_id,
            channel=binding_scope.channel,
            idempotency_key_digest=length_prefixed_digest(*key),
            execution_trace_id=execution_result.original_trace_id,
            reply_context=reply_context,
            result_digest=sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            status=DeliveryStatus.PENDING,
            adapter_generation=adapter_fence.generation,
            created_at=self._now(),
            updated_at=self._now(),
        )
        self._records[key] = record
        return record

    async def begin_attempt(self, tenant_scope, delivery_id, expected_status, adapter_fence, trace_id):
        await self._require_fence(adapter_fence)
        record = await self.get(tenant_scope, delivery_id)
        if (
            record.status != expected_status
            or record.status not in {
                DeliveryStatus.PENDING,
                DeliveryStatus.RETRY_WAIT,
            }
        ):
            raise ConditionalWriteFailed("Delivery state changed.")
        attempt_no = 1 + sum(item.delivery_id == delivery_id for item in self._attempts.values())
        if attempt_no > 4:
            raise ConditionalWriteFailed("Delivery attempt limit reached.")
        attempt = DeliveryAttempt(
            attempt_id=uuid4(), delivery_id=delivery_id, attempt_no=attempt_no,
            trace_id=trace_id, adapter_node_id=adapter_fence.node_id,
            adapter_generation=adapter_fence.generation, started_at=self._now(),
        )
        self._attempts[attempt.attempt_id] = attempt
        self._replace(record.transition(DeliveryStatus.SENDING, self._now(), adapter_generation=adapter_fence.generation))
        return attempt

    async def finish_attempt(self, tenant_scope, attempt_id, outcome, safe_error_code, retry_delay_seconds, adapter_fence):
        await self._require_fence(adapter_fence)
        attempt = self._attempts[attempt_id]
        record = await self.get(tenant_scope, attempt.delivery_id)
        if record.status != DeliveryStatus.SENDING:
            raise ConditionalWriteFailed("Delivery state changed.")
        self._attempts[attempt_id] = attempt.model_copy(update={
            "finished_at": self._now(), "outcome": outcome,
            "safe_error_code": safe_error_code, "retry_delay_seconds": retry_delay_seconds,
        })
        normalized = DeliveryOutcome(outcome)
        next_attempt_at = None
        if normalized == DeliveryOutcome.SUCCEEDED:
            target = DeliveryStatus.DELIVERED
        elif normalized == DeliveryOutcome.TRANSIENT and attempt.attempt_no < 4:
            from datetime import timedelta

            target = DeliveryStatus.RETRY_WAIT
            next_attempt_at = self._now() + timedelta(
                seconds=int(retry_delay_seconds or 0)
            )
        elif normalized in {DeliveryOutcome.TRANSIENT, DeliveryOutcome.PERMANENT}:
            target = DeliveryStatus.DELIVERY_FAILED
        else:
            target = DeliveryStatus.DELIVERY_UNKNOWN
        updated = record.transition(
            target,
            self._now(),
            next_attempt_at=next_attempt_at,
            adapter_generation=adapter_fence.generation,
        )
        self._replace(updated)
        return updated

    def _replace(self, record: DeliveryRecord) -> None:
        for key, current in self._records.items():
            if current.delivery_id == record.delivery_id:
                self._records[key] = record
                return

    async def get(self, tenant_scope, delivery_id):
        tenant_id = self._tenant_id(tenant_scope)
        for record in self._records.values():
            if record.tenant_id == tenant_id and record.delivery_id == delivery_id:
                return record
        raise KeyError("delivery not found")

    async def list_due(self, tenant_scope, now, limit):
        tenant_id = self._tenant_id(tenant_scope)
        return [
            record for record in self._records.values()
            if record.tenant_id == tenant_id and record.status == DeliveryStatus.RETRY_WAIT
            and record.next_attempt_at is not None and record.next_attempt_at <= now
        ][:limit]


class DeliveryService:
    """Persist one delivery intent and send it without re-entering the Runner."""

    _ADAPTER_ROLES = {
        "feishu": "feishu_adapter",
        "wecom": "wecom_adapter",
        "local_http": "gateway",
    }

    def __init__(
        self,
        repository: Any,
        *,
        telemetry: Any | None = None,
        node_id: str = "delivery-local",
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Any] | None = None,
    ) -> None:
        self.repository = repository
        self.telemetry = telemetry
        self._node_id = node_id
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._sleep = sleep or asyncio.sleep

    def _correlation(self, trace_id: Any, tenant_scope: Any, binding_scope: Any) -> Any | None:
        if self.telemetry is None:
            return None
        try:
            from trpc_service.observability.context import bind_tenant_scope, build_correlation

            channel = getattr(binding_scope, "channel", None)
            channel_value = channel.value if hasattr(channel, "value") else str(channel or "local_http")
            tenant_id = getattr(tenant_scope, "tenant_id", "platform")
            context = build_correlation(
                channel=channel_value,
                external_message_digest="",
                trace_id=str(trace_id),
                role=self._ADAPTER_ROLES.get(channel_value, "gateway"),
                node_id=self._node_id,
            )
            return bind_tenant_scope(context, tenant_id)
        except Exception:
            return None

    def _trace(
        self,
        correlation: Any | None,
        stage: str,
        outcome: str,
        *,
        error_type: str | None = None,
        retryable: bool = False,
    ) -> None:
        if self.telemetry is None or correlation is None:
            return
        try:
            self.telemetry.record_stage_now(
                correlation, stage, outcome,
                error_type=error_type, retryable=retryable,
            )
        except Exception:
            return

    async def deliver_reply(
        self,
        *,
        reply: OutboundReply,
        execution_result: ExecutionResult | None = None,
        tenant_scope: Any,
        binding_scope: Any,
        reply_context: ProviderReplyContext,
        provider: Any,
        adapter_fence: AdapterFence,
    ) -> DeliveryResult:
        if reply.delivery_action == DeliveryAction.SUPPRESS:
            return DeliveryResult(status="suppressed")
        if reply.delivery_action != DeliveryAction.DELIVER or reply.text is None:
            return DeliveryResult(status="not_deliverable")
        execution = execution_result
        if execution is None:
            # Compatibility for SDK/contract doubles that do not expose a
            # Gateway repository. Real ChannelMessageService paths always
            # supply the durable terminal result.
            now = self._now()
            execution = ExecutionResult(
                status=ExecutionStatus.SUCCEEDED,
                response_text=reply.text,
                original_trace_id=reply.original_trace_id or reply.trace_id,
                platform_session_id=reply.platform_session_id or ("sess_" + "0" * 64),
                started_at=now,
                finished_at=now,
                agent_event_count=0,
                final_response_count=1,
                delivery_action=DeliveryAction.DELIVER,
            )
        elif (
            execution.status != ExecutionStatus.SUCCEEDED
            or execution.response_text != reply.text
            or execution.platform_session_id != reply.platform_session_id
        ):
            raise ValueError("execution result does not match reply")
        return await self._deliver_execution_result(
            execution_result=execution,
            trace_id=reply.trace_id,
            tenant_scope=tenant_scope,
            binding_scope=binding_scope,
            reply_context=reply_context,
            provider=provider,
            adapter_fence=adapter_fence,
        )

    async def recover_execution_result(
        self,
        *,
        execution_result: ExecutionResult,
        tenant_scope: Any,
        binding_scope: Any,
        reply_context: ProviderReplyContext,
        provider: Any,
        adapter_fence: AdapterFence,
    ) -> DeliveryResult:
        """Resume delivery from durable state without any Runner dependency."""
        if (
            execution_result.status != ExecutionStatus.SUCCEEDED
            or execution_result.response_text is None
        ):
            return DeliveryResult(status="not_deliverable")
        return await self._deliver_execution_result(
            execution_result=execution_result,
            trace_id=execution_result.original_trace_id,
            tenant_scope=tenant_scope,
            binding_scope=binding_scope,
            reply_context=reply_context,
            provider=provider,
            adapter_fence=adapter_fence,
        )

    async def _deliver_execution_result(
        self,
        *,
        execution_result: ExecutionResult,
        trace_id: UUID,
        tenant_scope: Any,
        binding_scope: Any,
        reply_context: ProviderReplyContext,
        provider: Any,
        adapter_fence: AdapterFence,
    ) -> DeliveryResult:
        record = await self.repository.create_or_get(
            tenant_scope,
            binding_scope,
            execution_result,
            reply_context,
            adapter_fence,
        )
        correlation = self._correlation(trace_id, tenant_scope, binding_scope)
        self._trace(correlation, "delivery.queue", "success")
        if record.status == DeliveryStatus.DELIVERED:
            self._trace(correlation, "delivery.result", "success")
            return DeliveryResult(
                status="already_delivered",
                delivery_id=record.delivery_id,
                execution_trace_id=record.execution_trace_id,
                delivery_status=record.status.value,
            )
        if record.status.terminal:
            self._trace(
                correlation, "delivery.result", "failed",
                error_type=record.status.value,
            )
            return DeliveryResult(
                status=record.status.value,
                delivery_id=record.delivery_id,
                execution_trace_id=record.execution_trace_id,
                delivery_status=record.status.value,
            )
        while True:
            attempt = await self.repository.begin_attempt(
                tenant_scope, record.delivery_id, record.status, adapter_fence, trace_id
            )
            try:
                ack = await provider.send_text(
                    reply_context, execution_result.response_text
                )
                if not ack.acknowledged:
                    raise ProviderOutcomeUnknown()
            except ProviderTransientError:
                retry_delay = {1: 1, 2: 2, 3: 4, 4: 4}[attempt.attempt_no]
                updated = await self.repository.finish_attempt(
                    tenant_scope, attempt.attempt_id, DeliveryOutcome.TRANSIENT,
                    "provider_unavailable", retry_delay, adapter_fence
                )
                self._trace(
                    correlation, "delivery.attempt", "failed",
                    error_type="provider_unavailable", retryable=True,
                )
            except ProviderPermanentError:
                updated = await self.repository.finish_attempt(
                    tenant_scope, attempt.attempt_id, DeliveryOutcome.PERMANENT,
                    "provider_rejected", None, adapter_fence
                )
                self._trace(
                    correlation, "delivery.attempt", "failed",
                    error_type="provider_rejected", retryable=False,
                )
            except Exception:
                updated = await self.repository.finish_attempt(
                    tenant_scope, attempt.attempt_id, DeliveryOutcome.UNKNOWN,
                    "delivery_outcome_unknown", None, adapter_fence
                )
                self._trace(
                    correlation, "delivery.attempt", "unknown",
                    error_type="delivery_outcome_unknown", retryable=False,
                )
            else:
                updated = await self.repository.finish_attempt(
                    tenant_scope, attempt.attempt_id, DeliveryOutcome.SUCCEEDED,
                    None, None, adapter_fence
                )
                self._trace(correlation, "delivery.attempt", "success")
            if updated.status != DeliveryStatus.RETRY_WAIT:
                result_outcome = {
                    "delivered": "success",
                    "delivery_failed": "failed",
                    "delivery_unknown": "unknown",
                }.get(updated.status.value)
                if result_outcome is not None:
                    self._trace(correlation, "delivery.result", result_outcome)
                return DeliveryResult(
                    status=updated.status.value,
                    attempt_no=attempt.attempt_no,
                    delivery_id=updated.delivery_id,
                    execution_trace_id=updated.execution_trace_id,
                    delivery_status=updated.status.value,
                )
            await self._sleep(retry_delay)
            record = updated


__all__ = ["DeliveryService", "InMemoryDeliveryRepository"]
