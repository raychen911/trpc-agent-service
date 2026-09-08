import asyncio
import time
from collections.abc import Mapping
from typing import Any

from google.genai.types import Content, Part
from trpc_agent_sdk.runners import RunConfig

from trpc_service.agent import AgentFactory
from trpc_service.gateway.contracts import (
    AgentExecutor,
    AgentReply,
    GatewayResponse,
    NormalizedMessage,
)
from trpc_service.gateway.queue import ExecutionLedger, ExecutionUncertainError
from trpc_service.governance import GovernanceService
from trpc_service.metrics import PlatformMetrics, tracer
from trpc_service.storage.contracts import EventInput, SessionIdentity, TurnCommit
from trpc_service.storage.coordinator import TurnCoordinator
from trpc_service.storage.tenant_context import tenant_database_scope


class TrpcAgentExecutor:
    def __init__(
        self,
        factory: AgentFactory,
        execution_timeout_seconds: float = 120,
        max_llm_calls: int = 20,
        max_tool_calls: int = 20,
    ) -> None:
        self._factory = factory
        self._execution_timeout = execution_timeout_seconds
        self._max_llm_calls = max_llm_calls
        self._max_tool_calls = max_tool_calls

    async def execute(self, message: NormalizedMessage) -> AgentReply:
        runtime = await self._factory.build(message.tenant_id, message.agent_app_id)
        final_text = ""
        partial_parts: list[str] = []
        input_tokens = 0
        output_tokens = 0

        async def consume_events() -> None:
            nonlocal final_text, input_tokens, output_tokens
            async for event in runtime.runner.run_async(
                user_id=message.session_user_id,
                session_id=message.session_id,
                new_message=Content(role="user", parts=[Part(text=self._input_text(message))]),
                run_config=RunConfig(
                    max_llm_calls=self._max_llm_calls,
                    max_tool_calls=self._max_tool_calls,
                    custom_data={
                        "tenant_id": message.tenant_id,
                        "agent_app_id": message.agent_app_id,
                        "channel": message.channel,
                        "sender_user_id": message.sender_user_id,
                        "session_id": message.session_id,
                        "trace_id": message.trace_id,
                        "external_message_id": message.external_message_id,
                        "confirmed_tools": list(message.metadata.get("confirmed_tools", [])),
                        "attachments": list(message.metadata.get("attachments", [])),
                    },
                ),
            ):
                if event.is_error():
                    raise RuntimeError(
                        event.error_message or event.error_code or "agent run failed"
                    )
                text = event.get_text() or ""
                if event.partial:
                    partial_parts.append(text)
                elif text and event.is_final_response():
                    final_text = text
                usage = event.usage_metadata
                if usage is not None and not event.partial:
                    input_tokens += int(getattr(usage, "prompt_token_count", 0) or 0)
                    output_tokens += int(getattr(usage, "candidates_token_count", 0) or 0)

        try:
            await asyncio.wait_for(consume_events(), timeout=self._execution_timeout)
        except asyncio.TimeoutError:
            raise RuntimeError("Runner execution timed out; recovery review required") from None
        if not final_text:
            final_text = "".join(partial_parts)
        stream_updates = [
            "".join(partial_parts[:index]) for index in range(1, len(partial_parts) + 1)
        ]
        if final_text and stream_updates and stream_updates[-1] != final_text:
            stream_updates.append(final_text)
        cost = (
            input_tokens * runtime.input_cost_per_million
            + output_tokens * runtime.output_cost_per_million
        ) / 1_000_000
        return AgentReply(
            text=final_text,
            summary=final_text or None,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost,
            stream_updates=tuple(stream_updates),
        )

    @staticmethod
    def _input_text(message: NormalizedMessage) -> str:
        attachments = message.metadata.get("attachments", [])
        if not isinstance(attachments, list) or not attachments:
            return message.text
        descriptions: list[str] = []
        for item in attachments:
            if not isinstance(item, Mapping):
                continue
            kind = str(item.get("type", "file"))
            name = item.get("file_name")
            mime_type = item.get("mime_type")
            detail = ", ".join(str(value) for value in (name, mime_type) if value)
            descriptions.append(f"- {kind}" + (f" ({detail})" if detail else ""))
        if not descriptions:
            return message.text
        return message.text + "\n\nAttached IM media:\n" + "\n".join(descriptions)


class AgentMessageService:
    def __init__(
        self,
        executor: AgentExecutor,
        coordinator: TurnCoordinator,
        governance: GovernanceService | None = None,
        metrics: PlatformMetrics | None = None,
        execution_ledger: ExecutionLedger | None = None,
        max_concurrent_sessions: int | None = None,
    ) -> None:
        self._executor = executor
        self._coordinator = coordinator
        self._governance = governance
        self._metrics = metrics
        self._ledger = execution_ledger
        self._capacity = (
            asyncio.Semaphore(max_concurrent_sessions) if max_concurrent_sessions else None
        )

    async def handle(self, message: NormalizedMessage, node_id: str) -> GatewayResponse:
        if self._capacity is None:
            return await self._handle_scoped(message, node_id)
        async with self._capacity:
            if self._metrics is not None:
                self._metrics.active_sessions.inc()
            try:
                return await self._handle_scoped(message, node_id)
            finally:
                if self._metrics is not None:
                    self._metrics.active_sessions.dec()

    async def _handle_scoped(self, message: NormalizedMessage, node_id: str) -> GatewayResponse:
        with tenant_database_scope(message.tenant_id):
            return await self._handle_tenant_message(message, node_id)

    async def _handle_tenant_message(
        self, message: NormalizedMessage, node_id: str
    ) -> GatewayResponse:
        identity = SessionIdentity(
            message.tenant_id,
            message.agent_app_id,
            message.session_user_id,
            message.session_id,
        )
        reply_holder: dict[str, AgentReply] = {}
        execution = await self._ledger.prepare(message) if self._ledger else None

        async def build_turn(current_state: Mapping[str, Any], current_version: int) -> TurnCommit:
            if execution and execution.status == "uncertain":
                raise ExecutionUncertainError(
                    "previous Runner attempt has an uncertain commit boundary; "
                    "manual recovery required"
                )
            if execution and execution.status == "runner_completed" and execution.reply:
                reply = execution.reply
            else:
                governed = await self._governance.authorize(message) if self._governance else None
                effective_message = governed.message if governed else message
                started = time.perf_counter()
                if execution:
                    await self._ledger.mark_runner_started(execution.id)
                try:
                    with tracer.start_as_current_span("agent.execute") as span:
                        span.set_attribute("trpc.tenant_id", message.tenant_id)
                        span.set_attribute("trpc.agent_app_id", message.agent_app_id)
                        span.set_attribute("trpc.session_id", message.session_id)
                        reply = await self._executor.execute(effective_message)
                except Exception as error:
                    if self._metrics is not None:
                        self._metrics.agent_latency.labels(
                            message.tenant_id, message.agent_app_id, "error"
                        ).observe(time.perf_counter() - started)
                        self._metrics.model_calls.labels(
                            message.tenant_id, message.agent_app_id, "error"
                        ).inc()
                    if governed:
                        await self._governance.release(governed.reservation)
                    if execution:
                        await self._ledger.mark_uncertain(execution.id, str(error))
                    raise ExecutionUncertainError(
                        "Runner may have mutated its Session; automatic replay is disabled"
                    ) from error
                if self._metrics is not None:
                    self._metrics.agent_latency.labels(
                        message.tenant_id, message.agent_app_id, "ok"
                    ).observe(time.perf_counter() - started)
                    self._metrics.model_calls.labels(
                        message.tenant_id, message.agent_app_id, "ok"
                    ).inc()
                if governed:
                    reply = await self._governance.settle(governed, reply)
                if self._metrics is not None:
                    self._metrics.model_tokens.labels(
                        message.tenant_id, message.agent_app_id, "input"
                    ).inc(max(0, reply.input_tokens))
                    self._metrics.model_tokens.labels(
                        message.tenant_id, message.agent_app_id, "output"
                    ).inc(max(0, reply.output_tokens))
                    self._metrics.tenant_cost.labels(message.tenant_id, message.agent_app_id).inc(
                        max(0, reply.cost)
                    )
                if execution:
                    await self._ledger.mark_runner_completed(execution.id, reply)
            reply_holder["reply"] = reply
            next_state = dict(current_state)
            next_state.update(reply.state_delta)
            next_state.update(
                {
                    "last_channel": message.channel,
                    "last_external_message_id": message.external_message_id,
                    "last_sender_user_id": message.sender_user_id,
                    "last_reply": reply.text,
                }
            )
            event_metadata = {
                key: value
                for key, value in message.metadata.items()
                if key not in {"_trace_context", "response_url"}
            }
            return TurnCommit(
                identity=identity,
                expected_version=current_version,
                event=EventInput(
                    event_type="turn_completed",
                    role="user",
                    payload={
                        "text": message.text,
                        "response_text": reply.text,
                        "sender_user_id": message.sender_user_id,
                        "conversation_id": message.conversation_id,
                        "conversation_type": message.conversation_type,
                        "metadata": event_metadata,
                    },
                    trace_id=message.trace_id,
                    channel=message.channel,
                    external_message_id=message.external_message_id,
                ),
                next_state=next_state,
                summary_content=reply.summary,
                execution_id=execution.id if execution else None,
            )

        result = await self._coordinator.execute(
            identity,
            message.channel,
            message.external_message_id,
            build_turn,
        )
        reply = reply_holder["reply"]
        return GatewayResponse(
            status="processed",
            node_id=node_id,
            session_id=message.session_id,
            trace_id=message.trace_id,
            reply_text=reply.text,
            session_version=result.session.version,
            delivery={
                "attachments": list(reply.attachments),
                "card": dict(reply.card) if reply.card else None,
                "stream_updates": list(reply.stream_updates),
            },
        )
