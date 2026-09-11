"""Durable vertical slice around the existing trusted route and Runner bridge."""

from __future__ import annotations

import hashlib
import asyncio

from trpc_agent_sdk.context import AgentContext

from trpc_service.agent import AgentChannelEvent
from trpc_service.agent import AgentChannelEventType
from trpc_service.agent import RunnerRegistry
from trpc_service.agent import run_normalized_message
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.storage import ConflictError
from trpc_service.storage import execution_scope
from trpc_service.tenant import ResolvedRoute

from .inbox import InboxRepository
from .outbox import OutboxRepository
from .post_turn import PostTurnRepository
from trpc_service.telemetry.runtime import operation, carrier, count


class DurableExecutionPipeline:
    """Guarantee durable input before execution and durable output before ACK."""

    def __init__(self,
                 inbox: InboxRepository,
                 outbox: OutboxRepository,
                 post_turn: PostTurnRepository,
                 coordinator,
                 recover_native=None,
                 prepare_route=None) -> None:
        self._inbox = inbox
        self._outbox = outbox
        self._post_turn = post_turn
        self._coordinator = coordinator
        self._recover_native = recover_native
        self._prepare_route = prepare_route
        from .completion import ExecutionCompletion
        self._completion = ExecutionCompletion(inbox.database)

    def ingest(self, message, route, *, trace_id: str):
        with operation("gateway.inbox"):
            result = self._ingest(message, route, trace_id=trace_id)
            count("gateway.received", disposition=result.disposition.value)
            return result

    def _ingest(self, message, route, *, trace_id: str):
        payload = {
            "message": message.model_dump(mode="json"),
            "route": route.model_dump(mode="json"),
            "trace_context": carrier(),
        }
        return self._inbox.accept(
            tenant_id=route.tenant_id,
            channel_binding_id=route.channel_binding_id,
            external_message_id=message.external_message_id,
            payload=payload,
            identity_payload=message.model_dump(mode="json", exclude={"request_id", "received_at"}),
            request_id=message.request_id,
            trace_id=trace_id,
            partition_key=route.partition_key,
            config_version=route.config_version,
            storage_revision=route.storage_revision,
        )

    async def execute(self, work, runner_registry: RunnerRegistry, *, worker_id: str, lease_seconds: float = 120):
        if work is None:
            return ()
        with operation("worker.execute",
                       parent=work.payload.get("trace_context"),
                       attributes={
                           "execution.id": work.execution_id,
                           "tenant.id": work.tenant_id
                       }):
            await asyncio.to_thread(self._inbox.require_active, work, worker_id)
            route = ResolvedRoute.model_validate(work.payload["route"])
            if route.tenant_id != work.tenant_id or route.partition_key != work.partition_key:
                raise ConflictError("durable work item does not match its trusted route")
            if "authz_epoch" in work.payload:
                from trpc_service.channels.authorization import require_im_execution
                await asyncio.to_thread(require_im_execution, self._inbox.database, route, work.payload["authz_epoch"])
            if self._prepare_route is not None:
                await self._prepare_route(route)
            # Native storage is the write authority. Acquire it before platform
            # execution ownership, and keep renewal/cancellation scoped to the
            # complete execution, including native recovery.
            protected = runner_registry.protected_sessions.get(
                (route.tenant_id, route.agent_app_id, route.config_version))
            if protected is not None:
                async with protected.execution(user_id=route.internal_user_id,
                                               session_id=route.session_id,
                                               seconds=lease_seconds):
                    return await self._execute(work, runner_registry, worker_id=worker_id, lease_seconds=lease_seconds)
            return await self._execute(work, runner_registry, worker_id=worker_id, lease_seconds=lease_seconds)

    async def _execute(self, work, runner_registry, *, worker_id, lease_seconds):
        if work is None:
            return ()
        message = NormalizedInboundMessage.model_validate(work.payload["message"])
        await asyncio.to_thread(self._inbox.require_active, work, worker_id)
        route = ResolvedRoute.model_validate(work.payload["route"])
        if route.tenant_id != work.tenant_id or route.partition_key != work.partition_key:
            raise ConflictError("durable work item does not match its trusted route")
        if "authz_epoch" in work.payload:
            from trpc_service.channels.authorization import require_im_execution
            await asyncio.to_thread(require_im_execution, self._inbox.database, route, work.payload["authz_epoch"])
        existing = self._outbox.list_for_inbound(route.tenant_id, work.inbound_message_id)
        if self._recover_native is not None and not (existing and existing[-1].payload.get("final")):
            native_outputs = await self._recover_native(work, route)
            if native_outputs and native_outputs[-1].final:
                lease = await self._coordinator.acquire(route.tenant_id,
                                                        route.agent_app_id,
                                                        route.internal_user_id,
                                                        route.session_id,
                                                        worker_id,
                                                        lease_seconds,
                                                        execution_id=work.execution_id)
                try:
                    revision = self._coordinator.snapshot(route.tenant_id, route.agent_app_id, route.internal_user_id,
                                                          route.session_id).revision
                    for part_no, event in enumerate(native_outputs):
                        payload = event.model_dump(mode="json")
                        revision = await self._coordinator.commit(
                            lease,
                            expected_revision=revision,
                            event_id=hashlib.sha256(f"{work.execution_id}:{part_no}".encode()).hexdigest(),
                            event_payload=payload,
                            event_type="channel_event",
                            author="agent")
                        self._outbox.enqueue(route.tenant_id, work.inbound_message_id, part_no, payload)
                    self._finish_inbound(work, worker_id, native_outputs)
                    return native_outputs
                finally:
                    await self._coordinator.release(lease)
        if existing and existing[-1].payload.get("final"):
            if not existing[-1].payload.get("final"):
                raise ConflictError("partial prior execution requires tool-ledger reconciliation")
            outputs = tuple(AgentChannelEvent.model_validate(item.payload) for item in existing)
            self._finish_inbound(work, worker_id, outputs)
            return outputs

        committed = self._coordinator.events_for_execution(route.tenant_id, route.session_id, work.execution_id)
        if committed:
            if not committed[-1].payload.get("final"):
                raise ConflictError("partial prior execution requires tool-ledger reconciliation")
            for part_no, event in enumerate(committed):
                self._outbox.enqueue(route.tenant_id, work.inbound_message_id, part_no, event.payload)
            outputs = tuple(AgentChannelEvent.model_validate(item.payload) for item in committed)
            self._finish_inbound(work, worker_id, outputs)
            return outputs

        if existing:
            raise ConflictError("partial prior execution requires tool-ledger reconciliation")
        if self._coordinator.has_attempt(work.execution_id):
            raise ConflictError("prior execution outcome requires reconciliation")

        lease = await self._coordinator.acquire(
            route.tenant_id,
            route.agent_app_id,
            route.internal_user_id,
            route.session_id,
            worker_id,
            lease_seconds,
            execution_id=work.execution_id,
        )
        snapshot = self._coordinator.snapshot(route.tenant_id, route.agent_app_id, route.internal_user_id,
                                              route.session_id)
        attempt_id = self._coordinator.start_attempt(
            work.execution_id,
            worker_id,
            work.config_version,
            work.storage_revision,
        )
        revision = snapshot.revision
        outputs = []
        current = asyncio.current_task()

        async def renew():
            try:
                while True:
                    await asyncio.sleep(lease_seconds / 3)
                    await self._coordinator.renew(lease, lease_seconds)
            except Exception:
                current.cancel()
                raise

        heartbeat = asyncio.create_task(renew())
        try:
            with execution_scope(route.tenant_id, work.execution_id, lease.fencing_token):
                agent_context = AgentContext()
                agent_context.with_metadata("trace_id", work.trace_id)
                agent_context.with_metadata("execution_id", work.execution_id)
                agent_context.with_metadata("fencing_token", lease.fencing_token)
                if "authz_epoch" in work.payload:
                    agent_context.with_metadata("authz_epoch", work.payload["authz_epoch"])
                async for event in run_normalized_message(
                        runner_registry=runner_registry,
                        route=route,
                        message=message,
                        agent_context=agent_context,
                        attachment_uri_resolver=lambda attachment: attachment.artifact_id,
                ):
                    await asyncio.to_thread(self._inbox.require_active, work, worker_id)
                    payload = event.model_dump(mode="json")
                    part_no = len(outputs)
                    event_id = hashlib.sha256(f"{work.execution_id}:{part_no}".encode()).hexdigest()
                    revision = await self._coordinator.commit(
                        lease,
                        expected_revision=revision,
                        event_id=event_id,
                        event_payload=payload,
                        event_type="channel_event",
                        author="agent",
                    )
                    self._outbox.enqueue(route.tenant_id, work.inbound_message_id, part_no, payload)
                    outputs.append(event)
            if not outputs or not outputs[-1].final:
                raise ConflictError("runner completed without a final channel event")
            self._finish_inbound(work, worker_id, outputs)
            return tuple(outputs)
        except Exception as error:
            status = "unknown_outcome" if outputs else "failed_retryable"
            try:
                self._coordinator.finish_attempt(attempt_id, status, type(error).__name__)
            except ConflictError:
                # A terminal attempt may already be durable when the Inbox write fails.
                # Its persisted output will be classified again on recovery.
                pass
            raise
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await self._coordinator.release(lease)

    @staticmethod
    def _outcome(outputs):
        if any(event.type is AgentChannelEventType.ERROR or event.error_type for event in outputs):
            status = ("unknown_outcome" if any(event.type is AgentChannelEventType.TOOL_CALL
                                               for event in outputs) else "failed_final")
            return status, "agent_error"
        return "succeeded", None

    def _finish_inbound(self, work, worker_id, outputs):
        status, error_type = self._outcome(outputs)
        route = ResolvedRoute.model_validate(work.payload["route"])
        self._completion.finish(work, worker_id, outputs, route, status, error_type)
