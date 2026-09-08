import time
from dataclasses import dataclass, field
from typing import Any

from opentelemetry.trace import Status, StatusCode

from trpc_service.metrics import PlatformMetrics, tracer
from trpc_service.storage.contracts import AuditEntry, AuditStore


@dataclass(frozen=True, slots=True)
class ToolPolicy:
    name: str
    requires_confirmation: bool
    constraints: dict[str, Any] = field(default_factory=dict)


class ToolGovernanceCallbacks:
    def __init__(
        self,
        policies: tuple[ToolPolicy, ...],
        audit: AuditStore | None,
        metrics: PlatformMetrics | None = None,
    ) -> None:
        self._policies = {item.name: item for item in policies}
        self._audit = audit
        self._metrics = metrics
        self._observations: dict[tuple[str, str], tuple[float, Any]] = {}

    async def before_tool(self, invocation_ctx, tool, args: dict[str, Any], _result) -> dict | None:
        with tracer.start_as_current_span("tool.governance") as span:
            name = str(tool.name)
            policy = self._policies.get(name)
            custom_data = invocation_ctx.run_config.custom_data
            confirmed = set(map(str, custom_data.get("confirmed_tools", [])))
            allowed = policy is not None and (not policy.requires_confirmation or name in confirmed)
            decision = "allow" if allowed else "confirmation_required"
            span.set_attribute("gen_ai.tool.name", name)
            span.set_attribute("trpc.governance.decision", decision)
        if self._metrics is not None:
            self._metrics.tool_decisions.labels(
                str(custom_data.get("tenant_id", "")), name, decision
            ).inc()
        if self._audit is not None:
            await self._audit.append_audit(
                AuditEntry(
                    tenant_id=str(custom_data.get("tenant_id", "")),
                    agent_app_id=str(custom_data.get("agent_app_id", "")),
                    agent_name=invocation_ctx.agent.name,
                    tool_name=name,
                    decision=decision,
                    trace_id=str(custom_data.get("trace_id", invocation_ctx.invocation_id)),
                    request_id=str(
                        custom_data.get("external_message_id", invocation_ctx.invocation_id)
                    ),
                    channel=custom_data.get("channel"),
                    user_id=custom_data.get("sender_user_id"),
                    session_id=custom_data.get("session_id"),
                    details={"arguments": sorted(args)},
                )
            )
        if allowed:
            tool_span = tracer.start_span("tool.execute")
            tool_span.set_attribute("gen_ai.tool.name", name)
            tool_span.set_attribute("trpc.tenant_id", str(custom_data.get("tenant_id", "")))
            self._observations[(str(invocation_ctx.invocation_id), name)] = (
                time.perf_counter(),
                tool_span,
            )
            return None
        return {
            "error": "tool_confirmation_required",
            "tool_name": name,
            "message": f"Tool {name} requires explicit user confirmation.",
        }

    async def after_tool(
        self, invocation_ctx, tool, args: dict[str, Any], result: dict
    ) -> dict | None:
        name = str(tool.name)
        custom_data = invocation_ctx.run_config.custom_data
        observation = self._observations.pop((str(invocation_ctx.invocation_id), name), None)
        started, tool_span = observation if observation else (time.perf_counter(), None)
        latency_seconds = max(0.0, time.perf_counter() - started)
        error = result.get("error") if isinstance(result, dict) else None
        status = "error" if error else "ok"
        if tool_span is not None:
            tool_span.set_attribute("trpc.tool.status", status)
            if error:
                tool_span.set_status(Status(StatusCode.ERROR, str(error)[:256]))
            tool_span.end()
        if self._metrics is not None:
            self._metrics.tool_latency.labels(
                str(custom_data.get("tenant_id", "")), name, status
            ).observe(latency_seconds)
        if self._audit is not None:
            await self._audit.append_audit(
                AuditEntry(
                    tenant_id=str(custom_data.get("tenant_id", "")),
                    agent_app_id=str(custom_data.get("agent_app_id", "")),
                    agent_name=invocation_ctx.agent.name,
                    tool_name=name,
                    decision="failed" if error else "executed",
                    trace_id=str(custom_data.get("trace_id", invocation_ctx.invocation_id)),
                    request_id=str(
                        custom_data.get("external_message_id", invocation_ctx.invocation_id)
                    ),
                    channel=custom_data.get("channel"),
                    user_id=custom_data.get("sender_user_id"),
                    session_id=custom_data.get("session_id"),
                    latency_ms=round(latency_seconds * 1000),
                    error_type="tool_error" if error else None,
                    details={"arguments": sorted(args)},
                )
            )
        return None
