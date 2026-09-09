# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import pytest
from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.context import new_agent_context

from trpc_service.metrics import MetricsRegistry
from trpc_service.storage.observability import TracingMemoryService
from trpc_service.storage.session_wrapper import RequestTaggingSessionService
from trpc_service.tenant import TenantContext
from trpc_service.tenant import tenant_scope
from trpc_service.tool import ToolObservabilityFilter


class CapturingAudit:

    def __init__(self):
        self.events = []

    async def write(self, event):
        self.events.append(event)


class SessionDelegate:

    async def get_session(self, **kwargs):
        return kwargs


class MemoryDelegate:
    enabled = True

    async def search_memory(self, key, query, limit=10, agent_context=None):
        return [key, query, limit, agent_context]


@pytest.mark.asyncio
async def test_tool_execution_emits_metric_and_body_free_audit():
    metrics = MetricsRegistry()
    audit = CapturingAudit()
    observer = ToolObservabilityFilter("safe_tool", metrics, audit)
    context = new_agent_context(metadata={"user_id": "user-1", "session_id": "session-1"})
    with tenant_scope(TenantContext("tenant-1", "app-1", 1, "request-1", channel="web")):
        result = await observer.run(context, {}, lambda: _successful_tool())
    assert result.rsp == "ok"
    rendered = metrics.render()
    assert 'trpc_service_tool_calls_total{result="succeeded",tool="safe_tool"} 1' in rendered
    assert "trpc_service_tool_duration_seconds_count" in rendered
    assert len(audit.events) == 1
    assert audit.events[0].action == "tool_execute"
    assert audit.events[0].user_id == "user-1"
    assert audit.events[0].session_id == "session-1"


async def _successful_tool():
    return FilterResult(rsp="ok")


@pytest.mark.asyncio
async def test_session_and_memory_wrappers_emit_backend_latency_metrics():
    metrics = MetricsRegistry()
    session = RequestTaggingSessionService(SessionDelegate(), "redis", metrics)
    memory = TracingMemoryService(MemoryDelegate(), "sql", metrics)
    assert await session.get_session(app_name="app", user_id="user", session_id="session")
    assert await memory.search_memory("key", "query")
    rendered = metrics.render()
    assert ('trpc_service_storage_duration_seconds_count{backend="redis",operation="get",'
            'result="succeeded",store="session"} 1') in rendered
    assert ('trpc_service_storage_duration_seconds_count{backend="sql",operation="search",'
            'result="succeeded",store="memory"} 1') in rendered
