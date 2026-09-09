# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Metrics and audit records for tools that actually reach execution."""

from __future__ import annotations

import logging
import time
from typing import Any

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.filter import BaseFilter

from trpc_service.log import AuditEvent
from trpc_service.metrics import current_trace_context
from trpc_service.tenant.context import get_current_tenant

logger = logging.getLogger(__name__)


class ToolObservabilityFilter(BaseFilter):
    """Record bounded metrics and a body-free audit event around one tool call."""

    def __init__(self, tool_name: str, metrics: Any = None, audit: Any = None) -> None:
        super().__init__()
        self._type = FilterType.TOOL
        self._name = "tool_observability"
        self._tool_name = tool_name
        self._metrics = metrics
        self._audit = audit

    async def run(self, ctx, req, handle):
        del req
        started = time.perf_counter()
        result_name = "failed"
        error_type = ""
        try:
            result = await handle()
            if isinstance(result, FilterResult):
                if result.error:
                    error_type = type(result.error).__name__
                else:
                    result_name = "succeeded"
            elif isinstance(result, tuple) and len(result) == 2 and result[1]:
                error_type = type(result[1]).__name__
            else:
                result_name = "succeeded"
            return result
        except BaseException as error:
            error_type = type(error).__name__
            raise
        finally:
            await self._record(ctx, started, result_name, error_type)

    async def _record(self, ctx, started: float, result: str, error_type: str) -> None:
        latency_seconds = max(0.0, time.perf_counter() - started)
        if self._metrics is not None:
            self._metrics.increment("trpc_service_tool_calls_total", tool=self._tool_name, result=result)
            self._metrics.observe("trpc_service_tool_duration_seconds",
                                  latency_seconds,
                                  tool=self._tool_name,
                                  result=result)
        if self._audit is None:
            return
        try:
            trusted = get_current_tenant()
            channel = getattr(trusted.channel, "value", trusted.channel)
            traceparent = current_trace_context().traceparent
            trace_id = traceparent.split("-")[1] if traceparent.count("-") >= 3 else ""
            await self._audit.write(
                AuditEvent(
                    tenant_id=trusted.tenant_id,
                    channel=str(channel),
                    user_id=str(ctx.get_metadata("user_id", "")),
                    session_id=str(ctx.get_metadata("session_id", "")),
                    agent_name=trusted.app_id,
                    action="tool_execute",
                    decision="allow" if result == "succeeded" else "error",
                    tool_name=self._tool_name,
                    latency_ms=latency_seconds * 1000,
                    error_type=error_type,
                    trace_id=trace_id,
                    request_id=trusted.request_id,
                ))
        except Exception as error:  # Audit failure must not change the tool result.
            if self._metrics is not None:
                self._metrics.increment("trpc_service_audit_failures_total", action="tool_execute")
            logger.warning("tool audit write failed: %s", type(error).__name__)
