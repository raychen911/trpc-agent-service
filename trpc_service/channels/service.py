"""Provider-neutral channel message orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Any, Callable
from uuid import uuid4

from trpc_service.audit.models import AuditDecision, AuditRecord, PreAuthScope, TenantScope
from trpc_service.channels.base import (
    AdapterEventDisposition,
    AdapterEventResult,
    DeliveryResult,
    ParsedProviderEvent,
)
from trpc_service.channels.contracts import (
    ConversationType,
    DeliveryAction,
    UnifiedInboundMessage,
    VerifiedBindingScope,
)
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.storage.contracts import ResolvedChannelBinding, StaleFence
from trpc_service.storage.models import ExecutionResult, ExecutionStatus, IdempotencyKey


class ChannelMessageService:
    """Apply common filters, resolve the trusted binding, then invoke Gateway."""

    _ADAPTER_ROLES = {
        "feishu": "feishu_adapter",
        "wecom": "wecom_adapter",
        "local_http": "gateway",
    }

    def __init__(
        self,
        binding_repository: Any,
        gateway: Any,
        delivery_service: Any,
        *,
        preauth_audit: Any | None = None,
        telemetry: Any | None = None,
        node_id: str = "channel-local",
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.binding_repository = binding_repository
        self.gateway = gateway
        self.delivery_service = delivery_service
        self.preauth_audit = preauth_audit or getattr(binding_repository, "audit", None)
        self.telemetry = telemetry
        self._node_id = node_id
        self._now = now or (lambda: datetime.now(timezone.utc))

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

    def _platform_correlation(self, event: ParsedProviderEvent) -> Any | None:
        if self.telemetry is None:
            return None
        from trpc_service.observability.context import build_correlation

        channel = event.channel.value if hasattr(event.channel, "value") else str(event.channel)
        try:
            return build_correlation(
                channel=channel,
                external_message_digest=self._digest(event.external_message_id),
                trace_id=str(event.trace_id),
                role=self._ADAPTER_ROLES.get(channel, "gateway"),
                node_id=self._node_id,
            )
        except Exception:
            return None

    @staticmethod
    def _result(event: ParsedProviderEvent, disposition: AdapterEventDisposition, code: str) -> AdapterEventResult:
        return AdapterEventResult(disposition=disposition, safe_code=code, trace_id=str(event.trace_id))

    @staticmethod
    def _digest(value: str) -> str:
        return "sha256:" + sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _scoped_digest(scope: str, value: str) -> str:
        digest = sha256()
        for part in (scope, value):
            encoded = part.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
        return "sha256:" + digest.hexdigest()

    async def _audit_tenant_event(
        self,
        *,
        tenant_scope: TenantScope,
        event: ParsedProviderEvent,
        resolved: ResolvedChannelBinding,
        decision: AuditDecision,
        adapter_fence: Any,
        session_id: str | None = None,
        execution_result: ExecutionResult | None = None,
        delivery: Any | None = None,
        diagnostic: bool = False,
        rejected_generation: int | None = None,
    ) -> None:
        if self.preauth_audit is None:
            return
        execution_trace_id = (
            execution_result.original_trace_id if execution_result is not None else None
        )
        record = AuditRecord(
            audit_id=uuid4(),
            trace_id=event.trace_id,
            original_trace_id=(
                execution_trace_id if execution_trace_id != event.trace_id else None
            ),
            first_claim_trace_id=execution_trace_id,
            owner_trace_id=execution_trace_id,
            execution_trace_id=execution_trace_id,
            rejected_generation=rejected_generation,
            tenant_id=tenant_scope.tenant_id,
            channel=event.channel,
            binding_id_digest=self._digest(resolved.scope.binding_id),
            user_id=self._scoped_digest(
                tenant_scope.tenant_id,
                event.sender.sender_id if event.sender is not None else "unknown",
            ),
            session_id=session_id,
            agent_id=resolved.context.agent_id,
            agent_name=resolved.context.agent_name,
            decision=decision,
            latency_ms=0,
            cost=Decimal("0"),
            external_message_digest=self._digest(event.external_message_id),
            adapter_node_id=getattr(adapter_fence, "node_id", None),
            adapter_generation=getattr(adapter_fence, "generation", None),
            channel_identity_digest=event.channel_identity_digest,
            provider_message_digest=self._digest(event.external_message_id),
            delivery_id=getattr(delivery, "delivery_id", None),
            delivery_attempt_no=getattr(delivery, "attempt_no", None),
            delivery_status=getattr(delivery, "delivery_status", None),
            audit_kind="diagnostic" if diagnostic else "business",
            created_at=self._now(),
        )
        try:
            if diagnostic:
                await self.preauth_audit.append_diagnostic(tenant_scope, record)
            else:
                await self.preauth_audit.append(tenant_scope, record, adapter_fence)
        except Exception:
            # Gateway authorization audits remain fail-closed. These boundary
            # correlation records are best-effort and never trigger re-execution.
            return

    def _observe_channel(
        self,
        tenant_scope: TenantScope,
        *,
        channel: Any,
        stage: str,
        outcome: str,
        adapter_fence: Any,
        attempt_no: int | None = None,
    ) -> None:
        observe = getattr(getattr(self.gateway, "metrics", None), "observe_channel", None)
        if observe is None:
            return
        try:
            observe(
                tenant_scope,
                channel=channel,
                stage=stage,
                outcome=outcome,
                duration_ms=0,
                attempt_no=attempt_no,
                generation=getattr(adapter_fence, "generation", None),
            )
        except Exception:
            return

    async def _audit_rejection(
        self,
        event: ParsedProviderEvent,
        identity: ChannelIdentity,
        decision: AuditDecision,
        adapter_fence: Any,
    ) -> None:
        if self.preauth_audit is None:
            return
        sender_digest = None
        if event.sender is not None:
            sender_digest = self._digest(
                identity.identity_digest + ":" + event.sender.sender_id
            )
        record = AuditRecord(
            audit_id=uuid4(),
            trace_id=event.trace_id,
            tenant_id=None,
            channel=event.channel,
            binding_id_digest=self._digest(identity.identity_digest),
            user_id=sender_digest,
            decision=decision,
            latency_ms=0,
            error_type=decision.value,
            cost=Decimal("0"),
            external_message_digest=self._digest(event.external_message_id),
            adapter_node_id=getattr(adapter_fence, "node_id", None),
            adapter_generation=getattr(adapter_fence, "generation", None),
            channel_identity_digest=identity.identity_digest,
            provider_message_digest=self._digest(event.external_message_id),
            audit_kind="diagnostic",
            created_at=self._now(),
        )
        try:
            await self.preauth_audit.append_diagnostic(PreAuthScope(), record)
        except Exception:
            pass

    async def _durable_execution_result(
        self,
        *,
        tenant_id: str,
        message: UnifiedInboundMessage,
        reply: Any,
    ) -> ExecutionResult | None:
        """Read the exact terminal result produced by the Gateway.

        Lightweight Gateway doubles used by contract tests do not expose
        repositories, so they retain the delivery service's compatibility
        fallback. Production/local/shared Gateways always expose adapters.
        """
        idempotency = getattr(
            getattr(self.gateway, "adapters", None), "idempotency", None
        )
        if idempotency is None:
            return None
        record = await idempotency.get(
            IdempotencyKey(
                tenant_id=tenant_id,
                channel=message.channel,
                binding_id=message.binding_id,
                external_message_id=message.external_message_id,
            )
        )
        result = record.result
        if (
            result is None
            or result.status != ExecutionStatus.SUCCEEDED
            or result.response_text != reply.text
            or result.platform_session_id != reply.platform_session_id
        ):
            raise RuntimeError("durable execution result does not match reply")
        return result

    async def handle(
        self,
        *,
        event: ParsedProviderEvent,
        channel_identity: ChannelIdentity,
        runtime_bot_identity: RuntimeBotIdentity,
        provider: Any,
        adapter_fence: Any,
    ) -> AdapterEventResult:
        correlation = self._platform_correlation(event)
        result = await self._handle_impl(
            event=event,
            channel_identity=channel_identity,
            runtime_bot_identity=runtime_bot_identity,
            provider=provider,
            adapter_fence=adapter_fence,
            correlation=correlation,
        )
        outcome = {
            AdapterEventDisposition.ACCEPTED: "success",
            AdapterEventDisposition.REJECTED: "rejected",
            AdapterEventDisposition.IGNORED: "not_applicable",
        }.get(result.disposition, "unknown")
        self._trace(correlation, "adapter.receive", outcome)
        return result

    async def _handle_impl(
        self,
        *,
        event: ParsedProviderEvent,
        channel_identity: ChannelIdentity,
        runtime_bot_identity: RuntimeBotIdentity,
        provider: Any,
        adapter_fence: Any,
        correlation: Any | None,
    ) -> AdapterEventResult:
        if event.message_type != "text":
            return self._result(event, AdapterEventDisposition.IGNORED, "unsupported_event")
        if event.sender is None or event.sender.is_bot is None:
            await self._audit_rejection(
                event,
                channel_identity,
                AuditDecision.SENDER_IDENTITY_UNVERIFIED,
                adapter_fence,
            )
            return self._result(event, AdapterEventDisposition.REJECTED, "sender_identity_unverified")
        if event.sender.is_same_bot(runtime_bot_identity):
            return self._result(event, AdapterEventDisposition.IGNORED, "self_message")
        if event.conversation_type == ConversationType.GROUP and not event.bot_mentioned:
            return self._result(event, AdapterEventDisposition.IGNORED, "group_bot_not_mentioned")
        if not event.text.strip():
            return self._result(event, AdapterEventDisposition.REJECTED, "empty_text")
        if (
            event.channel != channel_identity.channel
            or event.channel_identity_digest != channel_identity.identity_digest
        ):
            await self._audit_rejection(
                event, channel_identity, AuditDecision.BINDING_REJECTED, adapter_fence
            )
            return self._result(event, AdapterEventDisposition.REJECTED, "binding_rejected")
        try:
            resolved = await self.binding_repository.resolve_by_channel_identity(
                channel_identity,
                external_user_id=event.sender.sender_id,
                trace_id=event.trace_id,
            )
        except Exception:
            self._trace(correlation, "binding.resolve", "rejected")
            await self._audit_rejection(
                event, channel_identity, AuditDecision.BINDING_REJECTED, adapter_fence
            )
            return self._result(event, AdapterEventDisposition.REJECTED, "binding_rejected")
        if (
            not isinstance(resolved, ResolvedChannelBinding)
            or resolved.context.channel != channel_identity.channel
            or resolved.context.binding_id != resolved.scope.binding_id
        ):
            self._trace(correlation, "binding.resolve", "rejected")
            await self._audit_rejection(
                event, channel_identity, AuditDecision.BINDING_REJECTED, adapter_fence
            )
            return self._result(event, AdapterEventDisposition.REJECTED, "binding_rejected")
        from trpc_service.observability.context import bind_tenant_scope

        self._trace(
            bind_tenant_scope(correlation, resolved.context.tenant_id) if correlation is not None else None,
            "binding.resolve",
            "success",
        )
        scope = VerifiedBindingScope._issue(
            binding_id=resolved.context.binding_id,
            channel=resolved.context.channel,
        )
        tenant_scope = TenantScope.from_context(resolved.context)
        message = UnifiedInboundMessage(
            channel=event.channel,
            binding_id=scope.binding_id,
            external_message_id=event.external_message_id,
            external_user_id=event.sender.sender_id,
            conversation_type=event.conversation_type,
            external_conversation_id=event.external_conversation_id,
            channel_identity_digest=event.channel_identity_digest,
            group_sender_id=event.sender.sender_id if event.conversation_type == ConversationType.GROUP else None,
            message_type="text",
            text=event.text,
            received_at=event.received_at,
            trace_id=event.trace_id,
        )
        await self._audit_tenant_event(
            tenant_scope=tenant_scope,
            event=event,
            resolved=resolved,
            decision=AuditDecision.RECEIVED,
            adapter_fence=adapter_fence,
        )
        self._observe_channel(
            tenant_scope,
            channel=event.channel,
            stage="filter",
            outcome="accepted",
            adapter_fence=adapter_fence,
        )
        reply = await self.gateway.handle_verified_message(scope, message)
        if reply.delivery_action != DeliveryAction.DELIVER:
            code = "duplicate_suppressed" if reply.delivery_action == DeliveryAction.SUPPRESS else "reply_not_deliverable"
            return self._result(event, AdapterEventDisposition.ACCEPTED, code)
        execution_result = await self._durable_execution_result(
            tenant_id=resolved.context.tenant_id,
            message=message,
            reply=reply,
        )
        try:
            delivery = await self.delivery_service.deliver_reply(
                reply=reply,
                execution_result=execution_result,
                tenant_scope=tenant_scope,
                binding_scope=scope,
                reply_context=event.reply_context,
                provider=provider,
                adapter_fence=adapter_fence,
            )
        except StaleFence:
            await self._audit_tenant_event(
                tenant_scope=tenant_scope,
                event=event,
                resolved=resolved,
                decision=AuditDecision.STALE_ADAPTER_REJECTED,
                adapter_fence=adapter_fence,
                session_id=reply.platform_session_id,
                execution_result=execution_result,
                diagnostic=True,
                rejected_generation=adapter_fence.generation,
            )
            delivery = DeliveryResult(
                status="stale_fence",
                execution_trace_id=execution_result.original_trace_id,
            )
        status = getattr(delivery, "status", str(delivery))
        decision = {
            "delivered": AuditDecision.DELIVERED,
            "already_delivered": AuditDecision.DELIVERED,
            "delivery_failed": AuditDecision.DELIVERY_FAILED,
            "delivery_unknown": AuditDecision.DELIVERY_UNKNOWN,
        }.get(status)
        if decision is not None:
            await self._audit_tenant_event(
                tenant_scope=tenant_scope,
                event=event,
                resolved=resolved,
                decision=decision,
                adapter_fence=adapter_fence,
                session_id=reply.platform_session_id,
                execution_result=execution_result,
                delivery=delivery,
            )
        self._observe_channel(
            tenant_scope,
            channel=event.channel,
            stage="delivery",
            outcome={
                "delivered": "success",
                "already_delivered": "success",
                "delivery_unknown": "unknown",
            }.get(status, "failed"),
            adapter_fence=adapter_fence,
            attempt_no=getattr(delivery, "attempt_no", None),
        )
        code = "reply_delivered" if status in {"delivered", "already_delivered"} else f"reply_{status}"
        return self._result(event, AdapterEventDisposition.ACCEPTED, code)


__all__ = ["ChannelMessageService"]
