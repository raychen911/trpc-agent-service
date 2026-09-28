"""Observability decorators around provider-neutral Agent runtime ports."""

import asyncio
from dataclasses import replace
import logging
from time import perf_counter

from trpc_service.agent.contracts import AgentExecutionContext, AgentRunResult
from trpc_service.agent.governance import AgentAuditRecorder
from trpc_service.agent.ports import AgentRunner, AgentToolInvoker
from trpc_service.agent.usage import UsageRecorder
from trpc_service.metrics import PlatformTelemetry

logger = logging.getLogger(__name__)


def _nonnegative_rate(value: object) -> float | None:
    """Normalize one configured model price without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return None
    return float(value)


class ObservedAgentRunner:
    """Trace model execution and emit token, latency, cost, and audit facts."""

    def __init__(
        self,
        delegate: AgentRunner,
        telemetry: PlatformTelemetry,
        audit: AgentAuditRecorder,
        timeout_seconds: float = 120,
        usage_recorder: UsageRecorder | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("model timeout must be positive")
        self._delegate = delegate
        self._telemetry = telemetry
        self._audit = audit
        self._timeout_seconds = timeout_seconds
        self._usage_recorder = usage_recorder

    async def run(
        self,
        context: AgentExecutionContext,
        tools: AgentToolInvoker,
    ) -> AgentRunResult:
        """Observe one model turn without recording prompt or response bodies."""

        request = context.request
        provider = str(context.config.model.get("provider", "unknown"))
        started = perf_counter()
        with self._telemetry.start_span(
                "runner.run",
                attributes={
                    "tenant.id": str(request.tenant.tenant_id),
                    "agent.name":
                    str(context.config.application.get("name", request.tenant.agent_app_id)),
                    "channel.type": request.channel.channel_type,
                    "request.id": request.tenant.request_id,
                    "config.version": context.config.config_version,
                    "model.provider": provider,
                },
        ) as span:
            try:
                configured_timeout = context.config.model.get(
                    "timeout_seconds",
                    self._timeout_seconds,
                )
                if (isinstance(configured_timeout, bool)
                        or not isinstance(configured_timeout,
                                          (int, float)) or configured_timeout <= 0):
                    raise ValueError("model timeout_seconds must be positive")
                timeout_seconds = min(float(configured_timeout), self._timeout_seconds)
                # Bound every provider implementation at the project boundary;
                # SDK-specific timeout defaults cannot keep a lease alive forever.
                async with asyncio.timeout(timeout_seconds):
                    result = await self._delegate.run(context, tools)
            except Exception as error:
                duration = perf_counter() - started
                if self._usage_recorder is not None:
                    try:
                        await self._usage_recorder.release(context)
                    except Exception as release_error:
                        logger.error(
                            "Usage reservation release failed error_type=%s",
                            type(release_error).__name__,
                        )
                span.set_attribute("result", "error")
                span.set_attribute("error.type", type(error).__name__)
                self._telemetry.record_agent_execution(
                    channel_type=request.channel.channel_type,
                    model_provider=provider,
                    result="error",
                    duration_seconds=duration,
                )
                await self._audit.record(
                    request,
                    context.config,
                    action="agent.runner.execute",
                    decision="error",
                    reason_code="RUNNER_FAILED",
                    latency_ms=duration * 1000,
                    error_type=type(error).__name__,
                )
                raise

            duration = perf_counter() - started
            span.set_attribute("result", "success")
            usage = result.usage
            input_rate = _nonnegative_rate(context.config.model.get("input_cost_per_million", 0))
            output_rate = _nonnegative_rate(context.config.model.get("output_cost_per_million", 0))
            if (usage.estimated_cost == 0 and input_rate is not None and output_rate is not None):
                estimated_cost = (
                    (usage.input_tokens * input_rate + usage.output_tokens * output_rate) /
                    1_000_000)
                if estimated_cost:
                    usage = replace(usage, estimated_cost=estimated_cost)
                    result = replace(result, usage=usage)
            if self._usage_recorder is not None:
                await self._usage_recorder.record(context, result)
            self._telemetry.record_agent_execution(
                channel_type=request.channel.channel_type,
                model_provider=provider,
                result="succeeded",
                duration_seconds=duration,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
            )
            await self._audit.record(
                request,
                context.config,
                action="agent.runner.execute",
                decision="allow",
                reason_code="MODEL_COMPLETED",
                latency_ms=duration * 1000,
                cost_amount=usage.estimated_cost,
                details={
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "total_tokens": usage.total_tokens,
                },
            )
            return result
