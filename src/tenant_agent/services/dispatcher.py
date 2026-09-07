"""Tenant routing and exactly-once turn orchestration."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from tenant_agent.agent.trpc import EngineSelector
from tenant_agent.channels.planning import plan_outbound
from tenant_agent.channels.wecom_bot import bot_outbox_kind
from tenant_agent.governance.policies import GovernanceService, PolicyDenied
from tenant_agent.ids import IdentityDeriver, stable_checksum
from tenant_agent.models import (
    AgentEvent,
    AgentEventType,
    AuditRecord,
    ChannelType,
    InboundEnvelope,
    MemoryRecord,
    OutboundMessage,
    ReceiptStatus,
    RoutedEnvelope,
    SessionEvent,
    SummaryRecord,
    TenantConfig,
    UsageDelta,
)
from tenant_agent.observability import (
    ACTIVE_SESSIONS,
    COST,
    ERRORS,
    MODEL_LATENCY,
    REQUESTS,
    TOKENS,
    backend_timing,
    inject_trace_context,
    trace_id,
    traced,
)
from tenant_agent.security import Redactor
from tenant_agent.settings import Settings
from tenant_agent.storage.base import OutboxItem, TenantDataPlane
from tenant_agent.storage.router import StorageRouter

logger = logging.getLogger(__name__)
EventSink = Callable[[AgentEvent], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class DispatchResult:
    status: str
    responses: tuple[OutboundMessage, ...]
    events: tuple[AgentEvent, ...] = ()


class GatewayRouter:
    """Route from trusted channel binding, never from a caller-supplied tenant header."""

    def __init__(self, identities: IdentityDeriver) -> None:
        self.identities = identities

    def route(self, envelope: InboundEnvelope, tenant: TenantConfig) -> RoutedEnvelope:
        binding = next(
            (
                item
                for item in tenant.channels
                if item.binding_id == envelope.binding_id
                and item.channel == envelope.channel
                and item.enabled
            ),
            None,
        )
        if (
            binding is None
            or binding.app_id != envelope.app_id
            or envelope.tenant_id != tenant.tenant_id
            or binding.external_account_id != envelope.external_account_id
        ):
            raise ValueError("message route does not match the active tenant binding")
        group_scope = tenant.governance.users.group_session_scope
        return RoutedEnvelope(
            inbound=envelope,
            internal_user_id=self.identities.user_id(envelope),
            session_id=self.identities.session_id(envelope, group_scope=group_scope),
            config_revision=tenant.revision,
        )


class TurnDispatcher:
    def __init__(
        self,
        *,
        settings: Settings,
        identities: IdentityDeriver,
        storage: StorageRouter,
        governance: GovernanceService,
        engines: EngineSelector,
        redactor: Redactor,
    ) -> None:
        self.settings = settings
        self.identities = identities
        self.storage = storage
        self.governance = governance
        self.engines = engines
        self.redactor = redactor

    async def process(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        event_sink: EventSink | None = None,
    ) -> DispatchResult:
        if tenant.tenant_id != routed.inbound.tenant_id or tenant.revision != routed.config_revision:
            raise ValueError("worker configuration does not match the routed message")
        plane = await self.storage.for_tenant(tenant)
        dedupe_key = self.identities.dedupe_key(routed.inbound)
        owner = f"{self.settings.node_id}:{uuid.uuid4().hex}"
        with backend_timing(tenant.tenant_id, "receipt", "claim", plane.receipts):
            receipt_lease_seconds = max(
                self.settings.processing_lease_seconds,
                int(self.settings.session_lock_timeout_seconds + self.settings.model_timeout_seconds + 60),
            )
            claim = await plane.receipts.claim_receipt(
                tenant_id=tenant.tenant_id,
                dedupe_key=dedupe_key,
                owner=owner,
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=receipt_lease_seconds),
            )
        if not claim.acquired:
            duplicate_status = (
                "duplicate_completed"
                if claim.receipt.status is ReceiptStatus.COMPLETED
                else "duplicate_processing"
            )
            REQUESTS.labels(tenant.tenant_id, routed.inbound.channel.value, duplicate_status).inc()
            return DispatchResult(duplicate_status, tuple(claim.receipt.response))

        started = time.perf_counter()
        try:
            dispatch_result = await self._run_claimed(
                tenant=tenant,
                routed=routed,
                plane=plane,
                dedupe_key=dedupe_key,
                owner=owner,
                reservation_expires_at=datetime.now(UTC) + timedelta(seconds=receipt_lease_seconds),
                event_sink=event_sink,
            )
            REQUESTS.labels(tenant.tenant_id, routed.inbound.channel.value, dispatch_result.status).inc()
            return dispatch_result
        except asyncio.CancelledError:
            ERRORS.labels(tenant.tenant_id, "dispatcher", "CancelledError").inc()
            try:
                await plane.receipts.fail_receipt(
                    tenant_id=tenant.tenant_id,
                    dedupe_key=dedupe_key,
                    owner=owner,
                    error_type="CancelledError",
                    usage_reservation_id=dedupe_key,
                )
            except Exception:
                logger.exception("failed to mark a cancelled inbound receipt as failed")
            raise
        except Exception as exc:
            error_type = exc.__class__.__name__
            ERRORS.labels(tenant.tenant_id, "dispatcher", error_type).inc()
            try:
                await plane.receipts.fail_receipt(
                    tenant_id=tenant.tenant_id,
                    dedupe_key=dedupe_key,
                    owner=owner,
                    error_type=error_type,
                    usage_reservation_id=dedupe_key,
                )
            except Exception:
                logger.exception("failed to mark the inbound receipt as failed")
            raise
        finally:
            elapsed = (time.perf_counter() - started) * 1_000
            logger.info(
                "turn finished tenant=%s channel=%s latency_ms=%.2f trace_id=%s",
                tenant.tenant_id,
                routed.inbound.channel.value,
                elapsed,
                trace_id(),
            )

    async def _run_claimed(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        dedupe_key: str,
        owner: str,
        reservation_expires_at: datetime,
        event_sink: EventSink | None,
    ) -> DispatchResult:
        envelope = routed.inbound
        usage_period = datetime.now(UTC).strftime("%Y-%m")
        recovered = await self._recover_committed(
            tenant=tenant,
            routed=routed,
            plane=plane,
            dedupe_key=dedupe_key,
            owner=owner,
            usage_reservation_id=dedupe_key,
        )
        if recovered is not None:
            return recovered
        try:
            effective_text = await self.governance.authorize_input(
                tenant,
                envelope,
                plane,
                usage_period=usage_period,
                reservation_id=dedupe_key,
                reservation_expires_at=reservation_expires_at,
            )
        except PolicyDenied as denied:
            recovered = await self._recover_committed(
                tenant=tenant,
                routed=routed,
                plane=plane,
                dedupe_key=dedupe_key,
                owner=owner,
                usage_reservation_id=dedupe_key,
            )
            if recovered is not None:
                return recovered
            response = self._outbound(tenant, routed, denied.public_message)
            await self._complete(
                tenant=tenant,
                routed=routed,
                plane=plane,
                dedupe_key=dedupe_key,
                owner=owner,
                responses=(response,),
                usage_period=usage_period,
                usage_delta=UsageDelta(),
                usage_reservation_id=dedupe_key,
            )
            await self._audit(
                tenant,
                routed,
                plane,
                decision=denied.decision,
                latency_ms=0.0,
                error_type=denied.__class__.__name__,
            )
            return DispatchResult("policy_denied", (response,))

        slot_owner = f"slot:{owner}"
        slot_lease_seconds = max(
            self.settings.processing_lease_seconds,
            int(self.settings.session_lock_timeout_seconds + self.settings.model_timeout_seconds + 60),
        )
        slot_acquired = await plane.concurrency.acquire_tenant_slot(
            tenant_id=tenant.tenant_id,
            owner=slot_owner,
            limit=tenant.governance.budget.max_concurrent_sessions,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=slot_lease_seconds),
        )
        if not slot_acquired:
            response = self._outbound(
                tenant,
                routed,
                "This tenant is at its concurrent-session limit. Please retry shortly.",
            )
            await self._complete(
                tenant=tenant,
                routed=routed,
                plane=plane,
                dedupe_key=dedupe_key,
                owner=owner,
                responses=(response,),
                usage_period=usage_period,
                usage_delta=UsageDelta(),
                usage_reservation_id=dedupe_key,
            )
            await self._audit(
                tenant,
                routed,
                plane,
                decision="concurrency_limited",
                latency_ms=0.0,
                error_type="ConcurrencyLimit",
            )
            return DispatchResult("capacity_limited", (response,))

        try:
            lock_owner = f"turn:{owner}"
            with traced(
                "session.serialized_turn",
                {"tenant.id": tenant.tenant_id, "session.id": routed.session_id},
                redactor=self.redactor,
            ):
                async with plane.leases.acquire_session(
                    tenant_id=tenant.tenant_id,
                    session_id=routed.session_id,
                    owner=lock_owner,
                    wait_timeout=self.settings.session_lock_timeout_seconds,
                    lease_seconds=max(
                        self.settings.processing_lease_seconds,
                        self.settings.model_timeout_seconds + 30,
                    ),
                ):
                    ACTIVE_SESSIONS.labels(tenant.tenant_id).inc()
                    try:
                        return await self._run_locked(
                            tenant=tenant,
                            routed=routed,
                            plane=plane,
                            dedupe_key=dedupe_key,
                            owner=owner,
                            effective_text=effective_text,
                            usage_period=usage_period,
                            event_sink=event_sink,
                        )
                    finally:
                        ACTIVE_SESSIONS.labels(tenant.tenant_id).dec()
        finally:
            await plane.concurrency.release_tenant_slot(tenant_id=tenant.tenant_id, owner=slot_owner)

    async def _recover_committed(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        dedupe_key: str,
        owner: str,
        usage_reservation_id: str | None,
        lease_held: bool = False,
    ) -> DispatchResult | None:
        event_scope = stable_checksum(
            tenant.tenant_id,
            routed.inbound.channel.value,
            routed.inbound.binding_id,
            routed.inbound.external_account_id,
            routed.session_id,
            routed.inbound.message_id,
        )[:48]
        inbound_event_id = f"in_{event_scope}"
        outbound_event_id = f"out_{event_scope}"
        if not lease_held:
            # The common new-turn path stays read-only and lock-free. Once a
            # persisted output is observed, acquire the normal turn lease and
            # re-read beneath it before repairing projections or settling usage.
            if (
                await plane.sessions.get_event(
                    tenant.tenant_id,
                    routed.session_id,
                    outbound_event_id,
                )
                is None
            ):
                return None
            async with plane.leases.acquire_session(
                tenant_id=tenant.tenant_id,
                session_id=routed.session_id,
                owner=f"recovery:{owner}",
                wait_timeout=self.settings.session_lock_timeout_seconds,
                lease_seconds=max(
                    self.settings.processing_lease_seconds,
                    self.settings.model_timeout_seconds + 30,
                ),
            ):
                return await self._recover_committed(
                    tenant=tenant,
                    routed=routed,
                    plane=plane,
                    dedupe_key=dedupe_key,
                    owner=owner,
                    usage_reservation_id=usage_reservation_id,
                    lease_held=True,
                )
        committed_outbound = await plane.sessions.get_event(
            tenant.tenant_id,
            routed.session_id,
            outbound_event_id,
        )
        if committed_outbound is None:
            return None
        snapshot = await plane.sessions.get_session(tenant.tenant_id, routed.session_id)
        if snapshot is None:
            raise RuntimeError("committed outbound event has no session snapshot")
        recovery_snapshot = snapshot.model_copy(update={"last_event_sequence": committed_outbound.sequence})
        inbound = await plane.sessions.get_event(
            tenant.tenant_id,
            routed.session_id,
            inbound_event_id,
        )
        effective_text = ""
        if inbound is not None:
            effective_text = str(inbound.payload.get("effective_text", inbound.payload.get("text", "")))
            if "effective_text" not in inbound.payload and tenant.governance.redaction.redact_before_model:
                effective_text = self.governance.redact_output(tenant, effective_text)
        final_text = str(committed_outbound.payload.get("text", ""))
        error_type_value = committed_outbound.payload.get("error_type")
        recovered_error_type = str(error_type_value) if error_type_value else None
        token_input = int(committed_outbound.payload.get("token_input", 0))
        token_output = int(committed_outbound.payload.get("token_output", 0))
        cost = float(committed_outbound.payload.get("cost_usd", 0.0))
        committed_usage_period = str(
            committed_outbound.payload.get("usage_period", datetime.now(UTC).strftime("%Y-%m"))
        )
        await self._post_turn(
            tenant,
            routed,
            plane,
            recovery_snapshot,
            effective_text,
            final_text,
        )
        response = self._outbound(tenant, routed, final_text)
        await self._complete(
            tenant=tenant,
            routed=routed,
            plane=plane,
            dedupe_key=dedupe_key,
            owner=owner,
            responses=(response,),
            usage_period=committed_usage_period,
            usage_delta=UsageDelta(
                input_tokens=token_input,
                output_tokens=token_output,
                cost_usd=cost,
            ),
            usage_reservation_id=usage_reservation_id,
        )
        TOKENS.labels(tenant.tenant_id, "input").inc(token_input)
        TOKENS.labels(tenant.tenant_id, "output").inc(token_output)
        COST.labels(tenant.tenant_id).inc(cost)
        await self._audit(
            tenant,
            routed,
            plane,
            decision="recovered_after_commit",
            latency_ms=0.0,
            error_type=recovered_error_type,
            cost=cost,
            token_input=token_input,
            token_output=token_output,
        )
        return DispatchResult("recovered", (response,))

    async def _run_locked(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        dedupe_key: str,
        owner: str,
        effective_text: str,
        usage_period: str,
        event_sink: EventSink | None,
    ) -> DispatchResult:
        envelope = routed.inbound
        with traced(
            "session.read",
            {"tenant.id": tenant.tenant_id, "session.id": routed.session_id},
            redactor=self.redactor,
        ):
            with backend_timing(tenant.tenant_id, "session", "read", plane.sessions):
                snapshot = await plane.sessions.get_or_create_session(
                    tenant_id=tenant.tenant_id,
                    app_id=envelope.app_id,
                    session_id=routed.session_id,
                    user_id=routed.internal_user_id,
                    channel=envelope.channel.value,
                )
        event_scope = stable_checksum(
            tenant.tenant_id,
            envelope.channel.value,
            envelope.binding_id,
            envelope.external_account_id,
            routed.session_id,
            envelope.message_id,
        )[:48]
        inbound_event_id = f"in_{event_scope}"
        with traced(
            "session.write",
            {"tenant.id": tenant.tenant_id, "session.id": routed.session_id, "event.kind": "user"},
            redactor=self.redactor,
        ):
            with backend_timing(tenant.tenant_id, "session", "append_user", plane.sessions):
                snapshot, _ = await plane.sessions.append_event(
                    snapshot=snapshot,
                    event_id=inbound_event_id,
                    kind="user_message",
                    actor_id=routed.internal_user_id,
                    payload={
                        "text": envelope.text,
                        "effective_text": effective_text,
                        "attachments": [item.model_dump(mode="json") for item in envelope.attachments],
                        "external_message_id": envelope.message_id,
                    },
                    state_delta={
                        "last_message_id": envelope.message_id,
                        "last_actor_id": routed.internal_user_id,
                        "turn_status": "running",
                    },
                    trace_id=trace_id(),
                )

        outbound_event_id = f"out_{event_scope}"
        with backend_timing(tenant.tenant_id, "session", "read_event", plane.sessions):
            recovered = await self._recover_committed(
                tenant=tenant,
                routed=routed,
                plane=plane,
                dedupe_key=dedupe_key,
                owner=owner,
                usage_reservation_id=dedupe_key,
                lease_held=True,
            )
        if recovered is not None:
            return recovered

        engine = self.engines.for_tenant(tenant, envelope.app_id)
        events: list[AgentEvent] = []
        partial_text = ""
        final_text = ""
        token_input = 0
        token_output = 0
        error_type: str | None = None
        profile = tenant.models[tenant.apps[envelope.app_id].model_profile]
        model_started = time.perf_counter()
        redaction_policy = tenant.governance.redaction
        partial_streaming_is_safe = not any(
            (
                redaction_policy.redact_email,
                redaction_policy.redact_phone,
                redaction_policy.redact_credentials,
            )
        )
        with traced(
            "runner.execute",
            {
                "tenant.id": tenant.tenant_id,
                "session.id": routed.session_id,
                "agent.name": tenant.apps[envelope.app_id].agent_name,
                "model.name": profile.model_name,
            },
            redactor=self.redactor,
        ):
            try:
                async for agent_event in engine.stream(
                    tenant=tenant,
                    routed=routed,
                    effective_text=effective_text,
                    plane=plane,
                ):
                    safe_event = agent_event
                    if agent_event.text:
                        safe_event = agent_event.model_copy(
                            update={"text": self.governance.redact_output(tenant, agent_event.text)}
                        )
                    events.append(safe_event)
                    token_input += agent_event.token_input
                    token_output += agent_event.token_output
                    if agent_event.event_type is AgentEventType.TEXT_DELTA:
                        partial_text += agent_event.text or ""
                    elif agent_event.event_type is AgentEventType.TEXT_FINAL:
                        final_text = agent_event.text or partial_text
                    elif agent_event.event_type is AgentEventType.ERROR:
                        error_type = str(agent_event.payload.get("error_type", "model_error"))
                        final_text = "The assistant is temporarily unavailable. Please retry shortly."
                    elif agent_event.event_type in {AgentEventType.TOOL_START, AgentEventType.TOOL_RESULT}:
                        snapshot, _ = await plane.sessions.append_event(
                            snapshot=snapshot,
                            event_id=f"agent_{agent_event.event_id}_{agent_event.event_type.value}",
                            kind=agent_event.event_type.value,
                            actor_id=tenant.apps[envelope.app_id].agent_name,
                            payload={
                                "tool_name": agent_event.tool_name,
                                **self.redactor.value(agent_event.payload),
                            },
                            state_delta={},
                            trace_id=trace_id(),
                        )
                    if event_sink and (not safe_event.partial or partial_streaming_is_safe):
                        await event_sink(safe_event)
            except TimeoutError:
                error_type = "model_timeout"
                final_text = "The model timed out. Please retry in a moment."
            except Exception as exc:
                logger.warning("agent execution failed with %s", exc.__class__.__name__)
                error_type = "model_unavailable"
                final_text = "The assistant is temporarily unavailable. Please retry shortly."
        model_seconds = time.perf_counter() - model_started
        MODEL_LATENCY.labels(
            tenant.tenant_id, profile.model_name, "error" if error_type else "success"
        ).observe(model_seconds)
        if error_type:
            ERRORS.labels(tenant.tenant_id, "model", error_type).inc()
            fallback_event = AgentEvent(
                event_id=uuid.uuid4().hex,
                event_type=AgentEventType.TEXT_FINAL,
                text=final_text,
                payload={"degraded": True, "error_type": error_type},
            )
            events.append(fallback_event)
            if event_sink:
                await event_sink(fallback_event)
        if not final_text:
            final_text = partial_text or "I could not produce a response."
        final_text = self.governance.redact_output(tenant, final_text)
        cost = (
            token_input * profile.input_cost_per_million + token_output * profile.output_cost_per_million
        ) / 1_000_000
        usage_delta = UsageDelta(
            input_tokens=token_input,
            output_tokens=token_output,
            cost_usd=cost,
        )
        with traced(
            "session.write",
            {
                "tenant.id": tenant.tenant_id,
                "session.id": routed.session_id,
                "event.kind": "assistant",
            },
            redactor=self.redactor,
        ):
            with backend_timing(tenant.tenant_id, "session", "append_assistant", plane.sessions):
                snapshot, _ = await plane.sessions.append_event(
                    snapshot=snapshot,
                    event_id=outbound_event_id,
                    kind="assistant_message",
                    actor_id=tenant.apps[envelope.app_id].agent_name,
                    payload={
                        "text": final_text,
                        "error_type": error_type,
                        "token_input": token_input,
                        "token_output": token_output,
                        "cost_usd": cost,
                        "usage_period": usage_period,
                    },
                    state_delta={"turn_status": "completed" if not error_type else "degraded"},
                    trace_id=trace_id(),
                )
        await self._post_turn(tenant, routed, plane, snapshot, effective_text, final_text)

        response = self._outbound(tenant, routed, final_text)
        await self._complete(
            tenant=tenant,
            routed=routed,
            plane=plane,
            dedupe_key=dedupe_key,
            owner=owner,
            responses=(response,),
            usage_period=usage_period,
            usage_delta=usage_delta,
            usage_reservation_id=dedupe_key,
        )
        TOKENS.labels(tenant.tenant_id, "input").inc(token_input)
        TOKENS.labels(tenant.tenant_id, "output").inc(token_output)
        COST.labels(tenant.tenant_id).inc(cost)
        await self._audit(
            tenant,
            routed,
            plane,
            decision="degraded" if error_type else "allowed",
            latency_ms=model_seconds * 1_000,
            error_type=error_type,
            cost=cost,
            token_input=token_input,
            token_output=token_output,
        )
        return DispatchResult("degraded" if error_type else "processed", (response,), tuple(events))

    async def _post_turn(
        self,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        snapshot: object,
        user_text: str,
        assistant_text: str,
    ) -> None:
        from tenant_agent.models import SessionSnapshot

        assert isinstance(snapshot, SessionSnapshot)
        event_scope = stable_checksum(
            tenant.tenant_id,
            routed.inbound.channel.value,
            routed.inbound.binding_id,
            routed.inbound.external_account_id,
            routed.session_id,
            routed.inbound.message_id,
        )[:48]
        inbound_event_id = f"in_{event_scope}"
        outbound_event_id = f"out_{event_scope}"
        memory_id = f"m_{stable_checksum(tenant.tenant_id, routed.inbound.message_id)[:48]}"
        try:
            threshold = max(2, int(tenant.metadata.get("summary_every_events", "20")))
            previous = await plane.summaries.get_summary(tenant.tenant_id, routed.session_id)
            through = previous.through_event_sequence if previous else 0
            if snapshot.last_event_sequence - through >= threshold:
                events = await plane.sessions.list_events(
                    tenant.tenant_id, routed.session_id, after_sequence=through
                )
                events = tuple(event for event in events if event.sequence <= snapshot.last_event_sequence)
                lines = [
                    f"{event.kind}: " + self._event_effective_text(tenant, event)[:800]
                    for event in events
                    if event.kind in {"user_message", "assistant_message"}
                ]
                summary = SummaryRecord(
                    tenant_id=tenant.tenant_id,
                    session_id=routed.session_id,
                    version=(previous.version + 1) if previous else 1,
                    through_event_sequence=snapshot.last_event_sequence,
                    content=(previous.content + "\n" if previous else "") + "\n".join(lines),
                )
                with traced(
                    "summary.write",
                    {"tenant.id": tenant.tenant_id, "session.id": routed.session_id},
                    redactor=self.redactor,
                ):
                    with backend_timing(
                        tenant.tenant_id,
                        "summary",
                        "write",
                        plane.summaries,
                    ):
                        await plane.summaries.put_summary(
                            summary.model_copy(update={"content": summary.content[-8_000:]})
                        )
        except Exception as exc:
            ERRORS.labels(tenant.tenant_id, "summary", exc.__class__.__name__).inc()
            logger.warning("summary update failed with %s", exc.__class__.__name__)
            await self._enqueue_auxiliary_repair(
                tenant=tenant,
                routed=routed,
                plane=plane,
                resource="summary",
                memory_id=memory_id,
                inbound_event_id=inbound_event_id,
                outbound_event_id=outbound_event_id,
                error_type=exc.__class__.__name__,
            )
        try:
            with traced(
                "memory.write",
                {"tenant.id": tenant.tenant_id, "session.id": routed.session_id},
                redactor=self.redactor,
            ):
                with backend_timing(tenant.tenant_id, "memory", "write", plane.memories):
                    await plane.memories.put_memory(
                        MemoryRecord(
                            memory_id=memory_id,
                            tenant_id=tenant.tenant_id,
                            user_id=routed.internal_user_id,
                            content=f"User: {user_text}\nAssistant: {assistant_text}",
                            metadata={
                                "session_id": routed.session_id,
                                "message_id": routed.inbound.message_id,
                                "through_event_sequence": snapshot.last_event_sequence,
                            },
                        )
                    )
        except Exception as exc:
            ERRORS.labels(tenant.tenant_id, "memory", exc.__class__.__name__).inc()
            logger.warning("memory write failed with %s", exc.__class__.__name__)
            await self._enqueue_auxiliary_repair(
                tenant=tenant,
                routed=routed,
                plane=plane,
                resource="memory",
                memory_id=memory_id,
                inbound_event_id=inbound_event_id,
                outbound_event_id=outbound_event_id,
                error_type=exc.__class__.__name__,
            )

    def _event_effective_text(self, tenant: TenantConfig, event: SessionEvent) -> str:
        if "effective_text" in event.payload:
            return str(event.payload["effective_text"])
        if event.kind == "user_message":
            return self.governance.effective_input(
                tenant,
                str(event.payload.get("text", "")),
                event.payload.get("attachments", ()),
            )
        return str(event.payload.get("text", ""))

    async def _enqueue_auxiliary_repair(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        resource: str,
        memory_id: str,
        inbound_event_id: str,
        outbound_event_id: str,
        error_type: str,
    ) -> None:
        repair_id = (
            "r_"
            + stable_checksum(
                tenant.tenant_id,
                routed.session_id,
                routed.inbound.message_id,
                resource,
            )[:48]
        )
        await plane.outbox.enqueue_outbox(
            OutboxItem(
                outbox_id=repair_id,
                tenant_id=tenant.tenant_id,
                kind="auxiliary-repair",
                payload={
                    "resource": resource,
                    "config_revision": tenant.revision,
                    "session_id": routed.session_id,
                    "user_id": routed.internal_user_id,
                    "message_id": routed.inbound.message_id,
                    "memory_id": memory_id,
                    "inbound_event_id": inbound_event_id,
                    "outbound_event_id": outbound_event_id,
                    "summary_every_events": tenant.metadata.get("summary_every_events", "20"),
                    "source_error_type": error_type,
                    "trace_context": inject_trace_context(),
                },
                status="pending",
                attempts=0,
                available_at=datetime.now(UTC),
            )
        )

    def _outbound(self, tenant: TenantConfig, routed: RoutedEnvelope, text: str) -> OutboundMessage:
        return OutboundMessage(
            tenant_id=tenant.tenant_id,
            binding_id=routed.inbound.binding_id,
            channel=routed.inbound.channel,
            external_chat_id=routed.inbound.external_chat_id,
            reply_to_message_id=routed.inbound.metadata.get("platform_message_id"),
            text=text,
            stream_key=f"{routed.session_id}:{routed.inbound.message_id}",
            is_final=True,
            metadata={
                "chat_type": routed.inbound.chat_type.value,
                "internal_session_id": routed.session_id,
                "internal_user_id": routed.internal_user_id,
                "app_id": routed.inbound.app_id,
                **(
                    {
                        "wecom_bot_req_id": routed.inbound.metadata.get("wecom_bot_req_id"),
                        "wecom_bot_stream_id": routed.inbound.metadata.get("wecom_bot_stream_id"),
                    }
                    if routed.inbound.channel is ChannelType.WECOM_BOT
                    else {}
                ),
            },
        )

    async def _complete(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        dedupe_key: str,
        owner: str,
        responses: Sequence[OutboundMessage],
        usage_period: str | None = None,
        usage_delta: UsageDelta | None = None,
        usage_reservation_id: str | None = None,
    ) -> None:
        outbox_items: tuple[OutboxItem, ...] = ()
        if routed.inbound.channel is not ChannelType.WEB:
            outbox_items = tuple(
                OutboxItem(
                    outbox_id=f"o_{stable_checksum(dedupe_key, str(index))[:48]}",
                    tenant_id=tenant.tenant_id,
                    kind=(
                        bot_outbox_kind(tenant.tenant_id, routed.inbound.binding_id)
                        if routed.inbound.channel is ChannelType.WECOM_BOT
                        else "im-delivery"
                    ),
                    payload={
                        "message": response.model_dump(mode="json"),
                        "segments": [segment.model_dump(mode="json") for segment in plan_outbound(response)],
                        "next_segment": 0,
                        "config_revision": tenant.revision,
                        "app_id": routed.inbound.app_id,
                        "trace_context": inject_trace_context(),
                    },
                    status="pending",
                    attempts=0,
                    available_at=datetime.now(UTC),
                )
                for index, response in enumerate(responses)
            )
        with traced(
            "outbox.commit",
            {"tenant.id": tenant.tenant_id, "channel": routed.inbound.channel.value},
            redactor=self.redactor,
        ):
            with backend_timing(tenant.tenant_id, "outbox", "commit", plane.receipts):
                await plane.receipts.complete_receipt_with_outbox(
                    tenant_id=tenant.tenant_id,
                    dedupe_key=dedupe_key,
                    owner=owner,
                    response=responses,
                    items=outbox_items,
                    usage_period=usage_period,
                    usage_delta=usage_delta,
                    usage_reservation_id=usage_reservation_id,
                )

    async def _audit(
        self,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        plane: TenantDataPlane,
        *,
        decision: str,
        latency_ms: float,
        error_type: str | None = None,
        cost: float = 0.0,
        token_input: int = 0,
        token_output: int = 0,
    ) -> None:
        if not tenant.audit.enabled:
            return
        try:
            details: dict[str, object] = {"config_revision": tenant.revision}
            if tenant.audit.include_prompt_hash:
                details["prompt_hmac_sha256"] = self.identities.content_fingerprint(
                    tenant.tenant_id, routed.inbound.text
                )
            if tenant.audit.include_content:
                details["prompt"] = self.redactor.text(routed.inbound.text)
            await plane.audit.append_audit(
                AuditRecord(
                    audit_id=f"turn-{uuid.uuid4().hex}",
                    tenant_id=tenant.tenant_id,
                    channel=routed.inbound.channel.value,
                    user_id=routed.internal_user_id,
                    session_id=routed.session_id,
                    agent_name=tenant.apps[routed.inbound.app_id].agent_name,
                    decision=decision,
                    latency_ms=latency_ms,
                    error_type=error_type,
                    cost_usd=cost,
                    token_input=token_input,
                    token_output=token_output,
                    trace_id=trace_id(),
                    message_id=routed.inbound.message_id,
                    details=details,
                )
            )
        except Exception as exc:
            error_name = exc.__class__.__name__
            ERRORS.labels(tenant.tenant_id, "turn_audit", error_name).inc()
            logger.warning("Turn audit failed after receipt completion with %s", error_name)
