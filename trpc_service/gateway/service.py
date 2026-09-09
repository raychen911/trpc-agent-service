# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Gateway orchestration shared by HTTP and IM adapters."""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import aclosing
from collections.abc import AsyncIterator

from trpc_service.agent import AgentWorker
from trpc_service.gateway.identity import internal_session_id
from trpc_service.gateway.identity import internal_user_id
from trpc_service.gateway.identity import session_owner_id
from trpc_service.gateway.errors import MigrationTransitionError
from trpc_service.gateway.idempotency import IdempotencyState
from trpc_service.gateway.idempotency import IdempotencyStore
from trpc_service.gateway.idempotency import AdmissionInDoubtError, IdempotencyConflictError
from trpc_service.gateway.requests import RequestNotFoundError
from trpc_service.gateway.models import AgentRequest
from trpc_service.gateway.models import AgentStreamEvent
from trpc_service.gateway.models import ChatResult
from trpc_service.gateway.models import NormalizedInboundMessage
from trpc_service.gateway.models import StreamEventType
from trpc_service.gateway.models import TraceContext
from trpc_service.gateway.models import RequestRecord
from trpc_service.gateway.models import RequestState
from trpc_service.gateway.models import UsageSummary
from trpc_service.gateway.requests import InMemoryRequestStore
from trpc_service.gateway.requests import RequestStore
from trpc_service.gateway.ordering import InMemoryOrderingStore
from trpc_service.tenant import TenantRegistry
from trpc_service.metrics import current_trace_context
from trpc_service.metrics import platform_span


class DuplicateRequestError(RuntimeError):
    """Raised when a duplicate is already processing or has completed."""

    def __init__(self, request_id: str, state: IdempotencyState) -> None:
        super().__init__(f"request already {state}: {request_id}")
        self.request_id = request_id
        self.state = state


class AgentExecutionError(RuntimeError):
    """Public, payload-free model/runner failure classification."""

    def __init__(self, code: str = "agent_execution_failed") -> None:
        super().__init__(code)
        self.code = code


def _web_session_id(tenant_id: str, app_id: str, user_id: str, session_id: str) -> str:
    value = f"{tenant_id}:{app_id}:{user_id}:{session_id}"
    return f"t:{tenant_id}:a:{app_id}:s:{hashlib.sha256(value.encode()).hexdigest()[:32]}"


class GatewayService:
    """Resolve trusted tenant context, enforce idempotency and invoke a Worker."""

    def __init__(self,
                 registry: TenantRegistry,
                 worker: AgentWorker,
                 idempotency: IdempotencyStore,
                 requests: RequestStore | None = None,
                 ordering: object | None = None,
                 migration_routes: object | None = None) -> None:
        self._registry = registry
        self._worker = worker
        self._idempotency = idempotency
        self._requests = requests or InMemoryRequestStore()
        self._ordering = ordering or InMemoryOrderingStore()
        self._migration_routes = migration_routes
        attach_store = getattr(worker, "set_request_store", None)
        if attach_store:
            attach_store(self._requests)

    @staticmethod
    def _payload_hash(value: dict[str, object]) -> str:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    async def _check_legacy(self, tenant_id, key, payload_hash):
        lookup = getattr(self._idempotency, "lookup", None)
        if not key or not lookup:
            return
        legacy = await lookup(key)
        if legacy:
            if legacy.payload_hash and legacy.payload_hash != payload_hash:
                raise IdempotencyConflictError("legacy idempotency payload mismatch")
            try:
                await self._requests.get(tenant_id, legacy.request_id)
            except RequestNotFoundError:
                raise AdmissionInDoubtError("legacy_idempotency_requires_review") from None
            raise DuplicateRequestError(legacy.request_id, legacy.state)

    async def _storage_route_version(self, tenant_id: str) -> int:
        if self._migration_routes is None:
            return 0
        route = await self._migration_routes.active_route(tenant_id)
        if route is None:
            return 0
        if route.admission_paused:
            raise MigrationTransitionError("migration_transition")
        return route.route_version

    async def web_request(self,
                          *,
                          tenant_id: str,
                          app_id: str,
                          external_user_id: str,
                          session_id: str,
                          text: str,
                          idempotency_key: str = "",
                          trace: TraceContext | None = None) -> tuple[AgentRequest, str]:
        tenant = await self._registry.get(tenant_id)
        await self._registry.get_app(tenant_id, app_id, tenant.version)
        request_id = uuid.uuid4().hex
        key = f"web:{tenant_id}:{app_id}:{idempotency_key}" if idempotency_key else ""
        payload_hash = self._payload_hash({
            "tenant_id": tenant_id,
            "app_id": app_id,
            "user_id": external_user_id,
            "session_id": session_id,
            "text": text,
        })
        await self._check_legacy(tenant_id, key, payload_hash)
        storage_route_version = await self._storage_route_version(tenant_id)
        with platform_span("gateway.web.normalize", trace):
            propagated_trace = current_trace_context()
        request = AgentRequest(
            request_id=request_id,
            tenant_id=tenant_id,
            config_version=tenant.version,
            storage_route_version=storage_route_version,
            app_id=app_id,
            user_id=internal_user_id(tenant_id, "web", external_user_id),
            session_id=_web_session_id(tenant_id, app_id, external_user_id, session_id),
            text=text,
            source_message_id=idempotency_key,
            trace=propagated_trace,
        )
        stored, created = await self._requests.reserve_and_create_request(
            RequestRecord(request_id=request_id,
                          tenant_id=tenant_id,
                          state=RequestState.RESERVED,
                          payload_hash=payload_hash,
                          config_version=tenant.version,
                          storage_route_version=storage_route_version,
                          request=request,
                          idempotency_key=key))
        if not created:
            raise DuplicateRequestError(
                stored.request_id,
                IdempotencyState.COMPLETED if stored.state == RequestState.SUCCEEDED else IdempotencyState.PROCESSING)
        return request, key

    async def inbound_request(self, message: NormalizedInboundMessage) -> tuple[AgentRequest, str]:
        tenant, binding = await self._registry.resolve_binding(message.binding_id)
        app = await self._registry.get_app(tenant.tenant_id, binding.app_id, tenant.version)
        request_id = uuid.uuid4().hex
        key = f"{message.channel}:{message.binding_id}:{message.message_id}"
        # Reception timestamps, traces and ordering observations change on redelivery.
        canonical = message.model_dump(mode="json", exclude={"occurred_at", "trace", "metadata"})
        payload_hash = self._payload_hash(canonical)
        await self._check_legacy(tenant.tenant_id, key, payload_hash)
        storage_route_version = await self._storage_route_version(tenant.tenant_id)
        if message.message_id.isdigit():
            message.metadata["out_of_order"] = await self._ordering.observe(f"{message.channel}:{message.binding_id}",
                                                                            int(message.message_id))
        with platform_span("gateway.channel.normalize", message.trace):
            propagated_trace = current_trace_context()
            if not propagated_trace.traceparent:
                # The API-only OTel package has no active SDK context in local
                # demos. Preserve the validated incoming carrier instead of
                # silently replacing it with an empty TraceContext.
                propagated_trace = message.trace.model_copy(deep=True)
        request = AgentRequest(
            request_id=request_id,
            tenant_id=tenant.tenant_id,
            config_version=tenant.version,
            storage_route_version=storage_route_version,
            app_id=app.app_id,
            user_id=session_owner_id(tenant.tenant_id, message, app.runtime.group_session_mode),
            session_id=internal_session_id(tenant.tenant_id, app.app_id, message, app.runtime.group_session_mode),
            text=message.text,
            channel=message.channel,
            source_message_id=message.message_id,
            binding_id=message.binding_id,
            attachments=message.attachments,
            metadata={
                **message.metadata,
                "external_conversation_id":
                message.external_conversation_id,
                "reply_to_message_id":
                message.reply_to_message_id,
                "actor_user_id":
                internal_user_id(tenant.tenant_id, message.channel, message.external_user_id, message.binding_id),
            },
            trace=propagated_trace,
        )
        stored, created = await self._requests.reserve_and_create_request(
            RequestRecord(request_id=request_id,
                          tenant_id=tenant.tenant_id,
                          state=RequestState.RESERVED,
                          payload_hash=payload_hash,
                          config_version=tenant.version,
                          storage_route_version=storage_route_version,
                          request=request,
                          idempotency_key=key))
        if not created:
            raise DuplicateRequestError(
                stored.request_id,
                IdempotencyState.COMPLETED if stored.state == RequestState.SUCCEEDED else IdempotencyState.PROCESSING)
        return request, key

    async def commit_result(self, request, result, messages=(), outbox=None):
        finalize = getattr(self._requests, "finalize", None)
        if finalize:
            await finalize(request, result, messages)
        else:
            for message in messages:
                await outbox.add(message)
            await self.record_replayed_result(request, result)

    async def complete_reservation(self, idempotency_key: str, request_id: str) -> None:
        if idempotency_key:
            try:
                await self._idempotency.complete(idempotency_key, request_id, ttl_seconds=604800)
            except Exception:
                # Cache is not the durable outcome authority anymore.
                pass

    async def request_status(self, tenant_id: str, request_id: str) -> RequestRecord:
        return await self._requests.get(tenant_id, request_id)

    async def ensure_request(self, request: AgentRequest, idempotency_key: str = "") -> RequestRecord:
        """Import a durable/legacy queue envelope before state transitions."""
        return await self._requests.create(
            RequestRecord(
                request_id=request.request_id,
                tenant_id=request.tenant_id,
                state=RequestState.QUEUED,
                config_version=request.config_version,
                storage_route_version=request.storage_route_version,
                request=request,
                idempotency_key=idempotency_key,
            ))

    async def mark_queued(self, request: AgentRequest) -> RequestRecord:
        return await self._requests.transition(request.tenant_id, request.request_id, RequestState.QUEUED)

    async def prepare(self, request: AgentRequest) -> None:
        request.metadata["admission_complete"] = True
        await self._requests.save_payload(request)

    async def mark_failed(self, request: AgentRequest, error_code: str) -> RequestRecord:
        return await self._requests.transition(request.tenant_id,
                                               request.request_id,
                                               RequestState.FAILED,
                                               error_code=error_code,
                                               retryable=False)

    async def mark_retryable(self, request: AgentRequest, error_code: str) -> RequestRecord:
        return await self._requests.transition(request.tenant_id,
                                               request.request_id,
                                               RequestState.RETRYABLE_FAILED,
                                               error_code=error_code,
                                               retryable=True)

    async def record_replayed_result(self, request: AgentRequest, result: ChatResult) -> RequestRecord:
        return await self._requests.transition(request.tenant_id,
                                               request.request_id,
                                               RequestState.SUCCEEDED,
                                               result=result)

    async def record_recovery(self, request: AgentRequest) -> RequestRecord:
        """Increment the durable retry/reclaim counter without changing state."""
        return await self._requests.increment_recovery_count(request.tenant_id, request.request_id)

    async def abandon_reservation(self, idempotency_key: str, request_id: str) -> None:
        if idempotency_key:
            await self._idempotency.abandon(idempotency_key, request_id)

    async def stream(self,
                     request: AgentRequest,
                     idempotency_key: str = "",
                     *,
                     finalize_idempotency: bool = True,
                     abandon_on_error: bool = True,
                     commit_callback=None) -> AsyncIterator[AgentStreamEvent]:
        try:
            events: list[AgentStreamEvent] = []
            completed = None
            previous = await self._requests.get(request.tenant_id, request.request_id)
            await self._requests.transition(request.tenant_id,
                                            request.request_id,
                                            RequestState.RUNNING,
                                            error_code=previous.error_code,
                                            retryable=previous.retryable,
                                            increment_attempts=True)
            async with aclosing(self._worker.stream(request)) as stream:
                async for event in stream:
                    events.append(event)
                    if event.type == StreamEventType.ERROR:
                        raise AgentExecutionError(str(event.data.get("error_code") or "agent_execution_failed"))
                    if event.type == StreamEventType.COMPLETED:
                        completed = event
                        result = await self._result(request, events)
                        if commit_callback:
                            await commit_callback(result)
                        elif finalize_idempotency:
                            await self.commit_result(request, result)
                        else:
                            await self._requests.transition(request.tenant_id,
                                                            request.request_id,
                                                            RequestState.RUNNING,
                                                            result=result)
                    else:
                        yield event
            if completed is None:
                raise RuntimeError("agent_stream_incomplete")
            if finalize_idempotency:
                await self.complete_reservation(idempotency_key, request.request_id)
            yield completed
        except BaseException as error:
            # Keep the provider/runner failure that caused the interrupted
            # turn.  A later recovery may deliberately raise
            # IncompleteRunError to prevent a blind re-execution, but that is
            # a recovery decision rather than the original external failure.
            current = await self._requests.get(request.tenant_id, request.request_id)
            error_code = error.code if isinstance(error, AgentExecutionError) else type(error).__name__
            if type(error).__name__ == "IncompleteRunError" and current.error_code:
                error_code = current.error_code
            await self._requests.transition(request.tenant_id,
                                            request.request_id,
                                            RequestState.RETRYABLE_FAILED,
                                            error_code=error_code,
                                            retryable=True)
            # Keep the original key: releasing it would allow a new request ID to
            # repeat a model/tool effect after a partial Session commit.
            raise

    async def chat(self,
                   request: AgentRequest,
                   idempotency_key: str = "",
                   *,
                   finalize_idempotency: bool = True,
                   abandon_on_error: bool = True,
                   commit_callback=None) -> ChatResult:
        async for _ in self.stream(request,
                                   idempotency_key,
                                   finalize_idempotency=finalize_idempotency,
                                   abandon_on_error=abandon_on_error,
                                   commit_callback=commit_callback):
            pass
        record = await self.request_status(request.tenant_id, request.request_id)
        assert record.result is not None
        return record.result

    async def _result(self, request: AgentRequest, events: list[AgentStreamEvent]) -> ChatResult:
        final_text = ""
        partial_text = ""
        input_tokens = output_tokens = total_tokens = 0
        usage_events: set[str] = set()
        for event in events:
            if event.type == StreamEventType.DELTA:
                if event.partial:
                    partial_text += event.text
                else:
                    final_text = event.text
            usage = event.data.get("usage")
            usage_key = event.event_id or str(event.sequence)
            if isinstance(usage, dict) and not event.partial and usage_key not in usage_events:
                usage_events.add(usage_key)
                input_tokens += int(usage.get("input_tokens", 0))
                output_tokens += int(usage.get("output_tokens", 0))
                total_tokens += int(usage.get("total_tokens", 0))
        result = ChatResult(
            request_id=request.request_id,
            tenant_id=request.tenant_id,
            app_id=request.app_id,
            user_id=request.user_id,
            session_id=request.session_id,
            text=final_text or partial_text,
            events=events,
            usage=UsageSummary(input_tokens=input_tokens,
                               output_tokens=output_tokens,
                               total_tokens=total_tokens or input_tokens + output_tokens),
        )
        app = await self._registry.get_app(request.tenant_id, request.app_id, request.config_version)
        result.usage.cost_usd = (result.usage.input_tokens * app.model.input_cost_per_million_usd +
                                 result.usage.output_tokens * app.model.output_cost_per_million_usd) / 1_000_000
        return result
