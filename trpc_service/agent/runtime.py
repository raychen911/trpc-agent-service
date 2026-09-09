# mypy: disable-error-code="import-untyped"
"""Bounded execution of a complete tRPC-Agent event stream."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.context import AgentContext, new_agent_context
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.events import (
    framework_event_to_event_data,
    framework_event_to_reply_intent,
)
from trpc_service.agent.factory import AgentBuild, AgentFactory
from trpc_service.agent.governance import govern_agent_input
from trpc_service.channels.contracts import ReplyIntent
from trpc_service.metrics import METRICS
from trpc_service.reliability.types import EventData
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import AgentAppSpec
from trpc_service.tool import TENANT_CONTEXT_METADATA_KEY


class AgentExecutionError(RuntimeError):
    """Base class for controlled execution-layer failures."""


class AgentTurnTimeoutError(AgentExecutionError):
    """The wall-clock deadline expired while consuming the SDK event stream."""


class MissingFinalResponseError(AgentExecutionError):
    """The SDK stream ended without a safe final reply or error event."""


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    """Finite SDK loop guards independent of model/provider timeouts."""

    max_llm_calls: int = 8
    max_iterations: int = 16

    def __post_init__(self) -> None:
        if self.max_llm_calls < 1 or self.max_iterations < 1:
            raise ValueError("execution limits must be positive and finite")


@dataclass(frozen=True, slots=True)
class TurnResult:
    """Fully consumed framework stream and its pure platform projections."""

    build: AgentBuild
    framework_events: tuple[Event, ...]
    platform_events: tuple[EventData, ...]
    reply_intent: ReplyIntent
    sdk_error: bool


class TenantAgentRunner:
    """Execute one verified tenant turn through the real public Runner API."""

    def __init__(
        self,
        *,
        agent_factory: AgentFactory,
        session_service: BaseSessionService,
        memory_service: BaseMemoryService | None = None,
        limits: ExecutionLimits | None = None,
    ) -> None:
        self._agent_factory = agent_factory
        self._session_service = session_service
        self._memory_service = memory_service
        self._limits = limits or ExecutionLimits()

    async def run_turn(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        new_message: str | Content | list[Content],
        run_id: str,
        in_reply_to_delivery_id: str,
        attempt_no: int = 1,
        approved_tools: frozenset[str] = frozenset(),
        timeout_seconds: float | None = None,
    ) -> TurnResult:
        """Consume the Runner generator to exhaustion under one wall-clock timeout."""

        if not run_id or not in_reply_to_delivery_id:
            raise ValueError("run_id and in_reply_to_delivery_id must not be empty")
        if attempt_no < 1:
            raise ValueError("attempt_no must be positive")
        started = time.monotonic()
        outcome = "error"
        try:
            result = await self._execute_turn(
                tenant_context=tenant_context,
                app=app,
                new_message=new_message,
                run_id=run_id,
                in_reply_to_delivery_id=in_reply_to_delivery_id,
                attempt_no=attempt_no,
                approved_tools=approved_tools,
                timeout_seconds=timeout_seconds,
            )
            outcome = "sdk_error" if result.sdk_error else "success"
            _record_token_metrics(tenant_context, result.framework_events)
            return result
        except AgentTurnTimeoutError:
            outcome = "timeout"
            raise
        finally:
            METRICS.agent_duration_seconds.labels(
                tenant_context.tenant_id,
                tenant_context.app_id,
                outcome,
            ).observe(max(0.0, time.monotonic() - started))

    async def _execute_turn(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        new_message: str | Content | list[Content],
        run_id: str,
        in_reply_to_delivery_id: str,
        attempt_no: int,
        approved_tools: frozenset[str],
        timeout_seconds: float | None,
    ) -> TurnResult:
        effective_timeout = _effective_timeout(app, timeout_seconds)
        sdk_message = _as_sdk_input(govern_agent_input(new_message, app.governance))
        build = self._agent_factory.build_for_context(
            tenant_context=tenant_context,
            app=app,
            approved_tools=approved_tools,
        )

        runner = Runner(
            app_name=build.app_name,
            agent=build.agent,
            session_service=self._session_service,
            memory_service=self._memory_service,
            enable_post_turn_processing=False,
            close_session_service_on_close=False,
            close_memory_service_on_close=False,
        )
        agent_context = _new_agent_context(tenant_context, effective_timeout)
        run_config = RunConfig(
            max_llm_calls=self._limits.max_llm_calls,
            max_iterations=self._limits.max_iterations,
            # Zero means unlimited in SDK 1.1.19, so use one as a finite guard
            # when the policy disables tools; the scoped ToolSet remains empty.
            max_tool_calls=max(1, app.tools.max_calls_per_turn),
            streaming=True,
            custom_data={
                "tenant_id": tenant_context.tenant_id,
                "app_id": tenant_context.app_id,
                "app_revision": tenant_context.app_revision,
                "request_id": tenant_context.request_id,
                "trace_id": tenant_context.trace_id,
            },
        )

        framework_events: list[Event] = []
        platform_events: list[EventData] = []
        reply_intent: ReplyIntent | None = None
        saw_sdk_error = False
        stream = runner.run_async(
            user_id=tenant_context.principal_id,
            session_id=tenant_context.session_id,
            new_message=sdk_message,
            run_config=run_config,
            agent_context=agent_context,
        )
        try:
            try:
                async with asyncio.timeout(effective_timeout):
                    async for event in stream:
                        framework_events.append(event)
                        if event.is_error():
                            saw_sdk_error = True
                        platform_event = framework_event_to_event_data(
                            event,
                            run_id=run_id,
                            sequence=len(platform_events) + 1,
                            attempt_no=attempt_no,
                        )
                        if platform_event is not None:
                            platform_events.append(platform_event)
                        projected_reply = framework_event_to_reply_intent(
                            event,
                            tenant_context=tenant_context,
                            run_id=run_id,
                            in_reply_to_delivery_id=in_reply_to_delivery_id,
                        )
                        if projected_reply is not None:
                            reply_intent = projected_reply
            except TimeoutError as exc:
                raise AgentTurnTimeoutError(
                    f"Agent turn exceeded {effective_timeout:g} seconds"
                ) from exc
        finally:
            await stream.aclose()
            await runner.close()

        if reply_intent is None:
            raise MissingFinalResponseError(
                "tRPC-Agent stream ended without a user-visible final response"
            )
        return TurnResult(
            build=build,
            framework_events=tuple(framework_events),
            platform_events=tuple(platform_events),
            reply_intent=reply_intent,
            sdk_error=saw_sdk_error,
        )


def _effective_timeout(app: AgentAppSpec, override: float | None) -> float:
    configured = float(app.model.timeout_seconds)
    if override is None:
        return configured
    if override <= 0:
        raise ValueError("timeout_seconds must be positive")
    # A caller may tighten but never widen the tenant's published deadline.
    return min(configured, override)


def _as_sdk_input(message: str | Content | list[Content]) -> Content | list[Content]:
    if isinstance(message, str):
        if not message.strip():
            raise ValueError("new_message must not be empty")
        return Content(role="user", parts=[Part.from_text(text=message)])
    return message


def _new_agent_context(
    tenant_context: TenantContext,
    timeout_seconds: float,
) -> AgentContext:
    timeout_ms = max(1, int(timeout_seconds * 1_000))
    return new_agent_context(
        timeout=timeout_ms,
        metadata={TENANT_CONTEXT_METADATA_KEY: tenant_context},
    )


def _record_token_metrics(
    tenant_context: TenantContext,
    events: tuple[Event, ...],
) -> None:
    input_tokens = 0
    output_tokens = 0
    for event in events:
        usage = event.usage_metadata
        if usage is None:
            continue
        prompt = usage.prompt_token_count
        candidates = usage.candidates_token_count
        if isinstance(prompt, int) and not isinstance(prompt, bool) and prompt > 0:
            input_tokens += prompt
        if isinstance(candidates, int) and not isinstance(candidates, bool) and candidates > 0:
            output_tokens += candidates
    if input_tokens:
        METRICS.token_total.labels(
            tenant_context.tenant_id,
            tenant_context.app_id,
            "input",
        ).inc(input_tokens)
    if output_tokens:
        METRICS.token_total.labels(
            tenant_context.tenant_id,
            tenant_context.app_id,
            "output",
        ).inc(output_tokens)
