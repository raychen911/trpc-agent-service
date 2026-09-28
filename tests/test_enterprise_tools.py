from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from tests.test_trpc_agent_runner import _context
from trpc_service.agent.contracts import AgentToolCall, AgentToolKind
from trpc_service.tool.enterprise import EnterpriseToolInvoker
from trpc_service.workspace import LocalWorkspaceProvider


@pytest.fixture
async def enterprise_http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _call(name: str, arguments: dict[str, object], *, resource: str | None = None) -> AgentToolCall:
    return AgentToolCall(
        call_id=f"request-1:0:{name}",
        name=name,
        kind=AgentToolKind.TOOL,
        logical_call_index=0,
        resource=resource,
        arguments=arguments,
    )


@pytest.mark.anyio
async def test_current_time_uses_an_explicit_iana_timezone(
    tmp_path: Path,
    enterprise_http_client: httpx.AsyncClient,
) -> None:
    invoker = EnterpriseToolInvoker(
        LocalWorkspaceProvider(tmp_path),
        http_client=enterprise_http_client,
        clock=lambda: datetime(2026, 9, 3, 0, 0, tzinfo=timezone.utc),
    )

    result = await invoker.invoke(_context(), _call("current_time", {"timezone": "Asia/Shanghai"}))

    assert result.content == "2026-09-03T08:00:00+08:00"


@pytest.mark.anyio
async def test_workspace_tools_read_only_the_current_tenant_workspace(
    tmp_path: Path,
    enterprise_http_client: httpx.AsyncClient,
) -> None:
    context = _context()
    workspace = LocalWorkspaceProvider(tmp_path)
    handle = await workspace.acquire(context.request.tenant, context.request.tenant.request_id)
    context = replace(context, workspace=handle)
    file_path = workspace.resolve_path(handle, "reports/status.txt")
    file_path.parent.mkdir(parents=True)
    file_path.write_text("healthy", encoding="utf-8")
    invoker = EnterpriseToolInvoker(workspace, http_client=enterprise_http_client)

    listed = await invoker.invoke(
        context,
        _call("workspace.list", {"path": "reports"}, resource="reports"),
    )
    read = await invoker.invoke(
        context,
        _call("workspace.read", {"path": "reports/status.txt"}, resource="reports/status.txt"),
    )

    assert listed.content == "reports/status.txt"
    assert read.content == "healthy"
    with pytest.raises(ValueError, match="workspace path"):
        await invoker.invoke(
            context,
            _call("workspace.read", {"path": "../../secret"}, resource="../../secret"),
        )

    with pytest.raises(RuntimeError, match="no bound workspace"):
        await invoker.invoke(
            replace(context, workspace=None),
            _call("workspace.list", {"path": "."}, resource="."),
        )


@pytest.mark.anyio
async def test_http_get_requires_https_allowlist_and_public_dns(tmp_path: Path) -> None:
    context = replace(
        _context(),
        config=replace(
            _context().config,
            tools={"http_allowed_hosts": ["api.example.com"]},
        ),
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200,
                              text='{"status":"ok"}',
                              headers={"content-type": "application/json"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    invoker = EnterpriseToolInvoker(
        LocalWorkspaceProvider(tmp_path),
        http_client=client,
        resolve_host=lambda host: ("93.184.216.34", ),
    )

    result = await invoker.invoke(
        context,
        _call("http.get", {"url": "https://api.example.com/health"}, resource="api.example.com"),
    )

    assert result.content == '{"status":"ok"}'
    with pytest.raises(PermissionError, match="allowlisted"):
        await invoker.invoke(
            context,
            _call("http.get", {"url": "https://metadata.internal/"}, resource="metadata.internal"),
        )
    await client.aclose()
