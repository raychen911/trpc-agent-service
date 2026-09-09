# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Stateless Worker executing normalized Agent requests."""

from __future__ import annotations

import asyncio
import contextlib
import re
from collections.abc import AsyncIterator

from trpc_agent_sdk.context import new_agent_context

from trpc_service.agent.runtime import RuntimeProvider
from trpc_service.gateway.models import AgentRequest
from trpc_service.gateway.models import AgentStreamEvent
from trpc_service.gateway.models import StreamEventType
from trpc_service.metrics import platform_span
from trpc_service.metrics import install_sdk_async_generator_safe_tracing
from trpc_service.storage import SessionExecutionGuard
from trpc_service.storage.guard import lease_scope
from trpc_service.storage.keys import session_execution_key
from trpc_service.tenant import TenantContext
from trpc_service.tenant import tenant_scope

_GENERIC_SDK_ERROR_CODES = {"STREAMING_ERROR", "API_ERROR", "LLM_CALL_ERROR"}


def classify_sdk_error(error_code: str | None, error_message: str | None) -> str:
    """Map generic SDK model errors to stable, payload-free platform codes."""
    code = error_code or "agent_execution_failed"
    if code not in _GENERIC_SDK_ERROR_CODES:
        return code
    message = (error_message or "").lower()
    if "timed out" in message or "timeout" in message:
        return "APITimeoutError"
    if "rate limit" in message or "rate_limit" in message or re.search(r"\b429\b", message):
        return "RateLimitError"
    if "connection" in message or "connecterror" in message or "connect error" in message:
        return "APIConnectionError"
    if re.search(r"\b5\d\d\b", message):
        return "UpstreamServerError"
    return code


class AgentWorker:
    """Run one request under a tenant scope and per-session execution guard."""

    def __init__(self, runtimes: RuntimeProvider, guard: SessionExecutionGuard) -> None:
        install_sdk_async_generator_safe_tracing()
        self._runtimes = runtimes
        self._guard = guard
        self._request_store = None

    def set_request_store(self, request_store) -> None:
        """Attach durable accounting after Gateway and Worker are composed."""
        self._request_store = request_store

    async def stream(self, request: AgentRequest) -> AsyncIterator[AgentStreamEvent]:
        borrow_request = getattr(self._runtimes, "borrow_request", None)
        if borrow_request:
            async with borrow_request(request) as runtime:
                async with contextlib.aclosing(self._stream(request, runtime)) as stream:
                    async for event in stream:
                        yield event
            return
        borrow = getattr(self._runtimes, "borrow", None)
        if borrow:
            async with borrow(request.tenant_id, request.app_id, request.config_version) as runtime:
                async with contextlib.aclosing(self._stream(request, runtime)) as stream:
                    async for event in stream:
                        yield event
        else:
            runtime = await self._runtimes.get(request.tenant_id, request.app_id, request.config_version)
            async with contextlib.aclosing(self._stream(request, runtime)) as stream:
                async for event in stream:
                    yield event

    async def _stream(self, request: AgentRequest, runtime: object) -> AsyncIterator[AgentStreamEvent]:
        context = TenantContext(
            tenant_id=request.tenant_id,
            app_id=request.app_id,
            config_version=request.config_version,
            request_id=request.request_id,
            channel=request.channel,
            binding_id=request.binding_id,
        )

        async def model_call_started() -> None:
            if self._request_store is not None:
                await self._request_store.increment_model_attempts(request.tenant_id, request.request_id)

        async def model_call_succeeded() -> None:
            if self._request_store is not None:
                await self._request_store.increment_successful_model_calls(request.tenant_id, request.request_id)

        agent_context = new_agent_context(
            timeout=max(1, int(runtime.app.runtime.max_run_seconds * 1000)),
            metadata={
                "tenant_id": request.tenant_id,
                "app_id": request.app_id,
                "request_id": request.request_id,
                "channel": request.channel,
                "user_id": request.user_id,
                "session_id": request.session_id,
                "approved_arguments": request.metadata.get("approved_arguments", {}),
                "actor_user_id": request.metadata.get("actor_user_id", request.user_id),
                "trpc_service_model_call_started": model_call_started,
                "trpc_service_model_call_succeeded": model_call_succeeded,
            },
        )
        lock_key = session_execution_key(request.tenant_id, request.app_id, request.session_id)
        sequence = 0
        yield AgentStreamEvent(request_id=request.request_id, sequence=sequence, type=StreamEventType.STARTED)
        sequence += 1
        span_attributes = {
            "trpc_service.tenant_id": request.tenant_id,
            "trpc_service.app_id": request.app_id,
            "trpc_service.config_version": request.config_version,
            "trpc_service.channel": request.channel,
        }
        # Do not keep an OpenTelemetry context manager open across a yield to
        # another application layer. The SDK execution task below owns the
        # platform span and every nested SDK span from start through cleanup.
        with tenant_scope(context):
            # A fixed ten-second wait makes valid same-session bursts exhaust
            # their queue retries while an earlier model call is still
            # running. Wait long enough for one bounded Agent run to finish,
            # plus a small scheduling margin. This remains finite, and a
            # genuinely stuck/expired owner is still handled by the lease TTL.
            lock_wait_seconds = max(10.0, float(runtime.app.runtime.max_run_seconds) + 10.0)
            async with self._guard.hold(lock_key, wait_timeout=lock_wait_seconds, lease_seconds=30.0) as lease:
                producer: asyncio.Task[None] | None = None
                event_consumed = asyncio.Event()
                producer_at_yield = asyncio.Event()

                async def cancel_if_lost() -> None:
                    await lease.lost.wait()
                    # Wait until the producer is between model chunks, then
                    # cancel its own task. The SDK tracing compatibility
                    # adapter keeps no Context token attached across this
                    # yield boundary.
                    await producer_at_yield.wait()
                    if producer is not None and not producer.done():
                        producer.cancel()

                lease_monitor = asyncio.create_task(cancel_if_lost())
                try:
                    async with contextlib.AsyncExitStack() as stack:
                        stack.enter_context(lease_scope(lease))
                        scope = getattr(runtime, "execution_scope", None)
                        if scope:
                            await stack.enter_async_context(scope(lock_key))
                        replay_result = getattr(runtime, "replay_result", None)
                        recovered = (await replay_result(request_id=request.request_id,
                                                         user_id=request.user_id,
                                                         session_id=request.session_id) if replay_result else None)
                        if recovered is not None:
                            yield AgentStreamEvent(request_id=request.request_id,
                                                   sequence=sequence,
                                                   type=StreamEventType.DELTA,
                                                   text=recovered[0],
                                                   data={
                                                       "replayed_from_session": True,
                                                       "usage": recovered[1]
                                                   })
                            sequence += 1
                        else:
                            attachment_args = {"attachments": request.attachments} if request.attachments else {}
                            # SDK tracing spans remain active while its async
                            # generators yield streaming events. Keep every
                            # __anext__ and aclose operation in one dedicated
                            # task so OpenTelemetry ContextVar tokens are also
                            # attached and detached by that same task. The
                            # Worker consumes a queue instead of directly
                            # advancing/closing the SDK generator.
                            event_queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()

                            async def pump_runtime_events() -> None:
                                try:
                                    with platform_span("agent.worker.execute", request.trace, span_attributes):
                                        async with contextlib.aclosing(
                                                runtime.run(
                                                    user_id=request.user_id,
                                                    session_id=request.session_id,
                                                    text=request.text,
                                                    agent_context=agent_context,
                                                    **attachment_args,
                                                )) as events:
                                            async for sdk_event in events:
                                                event_queue.put_nowait(("event", sdk_event))
                                                producer_at_yield.set()
                                                try:
                                                    await event_consumed.wait()
                                                finally:
                                                    event_consumed.clear()
                                                    producer_at_yield.clear()
                                except asyncio.CancelledError:
                                    raise
                                except BaseException as exc:  # propagate in the consumer task
                                    event_queue.put_nowait(("error", exc))
                                finally:
                                    event_queue.put_nowait(("done", None))

                            producer = asyncio.create_task(
                                pump_runtime_events(),
                                name=f"sdk-run-{request.request_id}",
                            )
                            try:
                                while True:
                                    kind, value = await event_queue.get()
                                    if kind == "done":
                                        break
                                    if kind == "error":
                                        raise value  # type: ignore[misc]
                                    await lease.verify()
                                    async for platform_event in self._translate_event(request, value, sequence):
                                        yield platform_event
                                        sequence = platform_event.sequence + 1
                                    event_consumed.set()
                            finally:
                                if not producer.done():
                                    producer.cancel()
                                await asyncio.gather(producer, return_exceptions=True)
                        await lease.verify()
                        # Consumer commits Request/Outbox while this generator is
                        # suspended here, before any lease/context is released.
                        yield AgentStreamEvent(request_id=request.request_id,
                                               sequence=sequence,
                                               type=StreamEventType.COMPLETED)
                finally:
                    lease_monitor.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await lease_monitor

    async def _translate_event(self, request: AgentRequest, event: object,
                               sequence: int) -> AsyncIterator[AgentStreamEvent]:
        """Translate one SDK Event without allowing a success commit outside the lease."""
        usage_metadata = getattr(event, "usage_metadata", None)
        usage = {}
        if usage_metadata is not None:
            input_tokens = int(getattr(usage_metadata, "prompt_token_count", 0) or 0)
            output_tokens = int(getattr(usage_metadata, "candidates_token_count", 0) or 0)
            usage = {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": int(getattr(usage_metadata, "total_token_count", 0) or input_tokens + output_tokens),
            }
        calls = event.get_function_calls()
        responses = event.get_function_responses()
        if calls:
            for call in calls:
                yield AgentStreamEvent(
                    request_id=request.request_id,
                    sequence=sequence,
                    type=StreamEventType.TOOL_CALL,
                    event_id=event.id,
                    author=event.author,
                    partial=bool(event.partial),
                    tool_name=call.name or "",
                    data={
                        "arguments": call.args or {},
                        "usage": usage
                    },
                )
                sequence += 1
        elif responses:
            for response in responses:
                yield AgentStreamEvent(
                    request_id=request.request_id,
                    sequence=sequence,
                    type=StreamEventType.TOOL_RESULT,
                    event_id=event.id,
                    author=event.author,
                    partial=bool(event.partial),
                    tool_name=response.name or "",
                    data={
                        "response": response.response,
                        "usage": usage
                    },
                )
                sequence += 1
        else:
            text = event.get_text()
            if text or event.is_error():
                yield AgentStreamEvent(
                    request_id=request.request_id,
                    sequence=sequence,
                    type=StreamEventType.ERROR if event.is_error() else StreamEventType.DELTA,
                    text=text or event.error_message or "Agent execution failed",
                    event_id=event.id,
                    author=event.author,
                    partial=bool(event.partial),
                    data=({
                        "error_code": classify_sdk_error(event.error_code, event.error_message),
                        "usage": usage
                    } if event.is_error() else {
                        "usage": usage
                    }),
                )
            elif usage:
                yield AgentStreamEvent(
                    request_id=request.request_id,
                    sequence=sequence,
                    type=StreamEventType.DELTA,
                    event_id=event.id,
                    author=event.author,
                    data={"usage": usage},
                )
