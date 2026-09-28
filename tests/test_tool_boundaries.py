"""Boundary tests for deterministic, enterprise, and SDK Tool surfaces."""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from tests.test_trpc_agent_runner import _context
from trpc_service.agent.adapters.trpc_tools import CapabilityCallSequence, TRPCToolBridge
from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentToolCall,
    AgentToolKind,
    AgentToolResult,
)
from trpc_service.agent.ports import AgentToolInvoker
from trpc_service.tool import BuiltinToolInvoker
from trpc_service.tool.enterprise import EnterpriseToolInvoker
from trpc_service.workspace import LocalWorkspaceProvider


@pytest.fixture
async def enterprise_http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _bridge(context, invoker):  # type: ignore[no-untyped-def]
    return TRPCToolBridge(context, invoker, CapabilityCallSequence())


def _call(name: str, arguments: dict[str, object]) -> AgentToolCall:
    return AgentToolCall(
        call_id=f"call:{name}",
        name=name,
        kind=AgentToolKind.TOOL,
        logical_call_index=0,
        arguments=arguments,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("5 - 8", "-3"),
        ("+5", "5"),
        ("-0", "0"),
        ("7 / 2", "3.5"),
        ("7 // 2", "3"),
        ("7 % 4", "3"),
        ("2 ** 12", "4096"),
        ("1.500 + 0", "1.5"),
    ],
)
async def test_calculator_supports_only_bounded_arithmetic(expression: str, expected: str) -> None:
    result = await BuiltinToolInvoker().invoke(_context(),
                                               _call("calculate", {"expression": expression}))
    assert result.content == expected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ("", "non-empty"),
        ("1 +", "unsupported"),
        ("True", "unsupported"),
        ("'text'", "unsupported"),
        ("2 ** 1.5", "exponent"),
        ("2 ** 13", "exponent"),
        ("2 << 1", "unsupported"),
        ("1 / 0", "could not be completed"),
        ("1e100 * 10", "outside the supported range"),
        ("1e101", "outside the supported range"),
        (" + ".join(["1"] * 33), "too complex"),
        ("1" * 201, "too long"),
    ],
)
async def test_calculator_rejects_unsafe_or_unbounded_expressions(expression: str,
                                                                  message: str) -> None:
    with pytest.raises(ValueError, match=message):
        await BuiltinToolInvoker().invoke(_context(), _call("calculate",
                                                            {"expression": expression}))


@pytest.mark.anyio
async def test_calculator_requires_a_string_argument() -> None:
    with pytest.raises(ValueError, match="string expression"):
        await BuiltinToolInvoker().invoke(_context(), _call("calculate", {"expression": 12}))


@pytest.mark.anyio
async def test_enterprise_time_and_workspace_fail_closed(
    tmp_path: Path,
    enterprise_http_client: httpx.AsyncClient,
) -> None:
    context = _context()
    workspace = LocalWorkspaceProvider(tmp_path)
    invoker = EnterpriseToolInvoker(
        workspace,
        http_client=enterprise_http_client,
        clock=lambda: datetime(2026, 9, 5, tzinfo=timezone.utc),
    )
    handle = await workspace.acquire(context.request.tenant, context.request.tenant.request_id)
    context = replace(context, workspace=handle)
    root = Path(handle.location)
    (root / "nested").mkdir()
    (root / "nested" / "valid.txt").write_text("有效内容", encoding="utf-8")
    (root / "binary.bin").write_bytes(b"\xff\xfe")
    (root / "large.txt").write_bytes(b"x" * (256 * 1024 + 1))

    listed = await invoker.invoke(context, _call("workspace.list", {"path": "."}))
    assert listed.content is not None and "nested/valid.txt" in listed.content
    assert (await invoker.invoke(context, _call("workspace.read",
                                                {"path": "nested/valid.txt"}))).content == "有效内容"
    assert (await invoker.invoke(context, _call("current_time",
                                                {}))).content == ("2026-09-05T00:00:00+00:00")

    cases = [
        (_call("current_time", {"timezone": ""}), ValueError, "IANA"),
        (_call("current_time", {"timezone": "Mars/Base"}), ValueError, "unknown"),
        (_call("workspace.list", {"path": 1}), ValueError, "path must be a string"),
        (_call("workspace.list", {"path": "missing"}), LookupError, "does not exist"),
        (_call("workspace.list", {"path": "nested/valid.txt"}), ValueError, "directory"),
        (_call("workspace.read", {}), ValueError, "path must be a string"),
        (_call("workspace.read", {"path": "missing"}), LookupError, "does not exist"),
        (_call("workspace.read", {"path": "binary.bin"}), ValueError, "UTF-8"),
        (_call("workspace.read", {"path": "large.txt"}), ValueError, "read limit"),
        (_call("unknown", {}), PermissionError, "not registered"),
    ]
    for call, error_type, message in cases:
        with pytest.raises(error_type, match=message):
            await invoker.invoke(context, call)

    naive = EnterpriseToolInvoker(
        workspace,
        http_client=enterprise_http_client,
        clock=lambda: datetime(2026, 9, 5),
    )
    with pytest.raises(RuntimeError, match="timezone-aware"):
        await naive.invoke(context, _call("current_time", {}))


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://api.example.com", "HTTPS URL"),
        ("https://user:password@api.example.com", "HTTPS URL"),
        ("https://api.example.com:8443", "HTTPS URL"),
        ("https:///missing-host", "HTTPS URL"),
        ("https://other.example.com", "allowlisted"),
    ],
)
async def test_enterprise_http_rejects_invalid_destinations(
        tmp_path: Path, url: str, message: str, enterprise_http_client: httpx.AsyncClient) -> None:
    context = replace(
        _context(),
        config=replace(_context().config, tools={"http_allowed_hosts": ["api.example.com"]}),
    )
    invoker = EnterpriseToolInvoker(
        LocalWorkspaceProvider(tmp_path),
        http_client=enterprise_http_client,
        resolve_host=lambda _: ("93.184.216.34", ),
    )
    with pytest.raises(PermissionError, match=message):
        await invoker.invoke(context, _call("http.get", {"url": url}))


@pytest.mark.anyio
async def test_enterprise_http_validates_policy_dns_and_response(
    tmp_path: Path,
    enterprise_http_client: httpx.AsyncClient,
) -> None:
    base = _context()
    workspace = LocalWorkspaceProvider(tmp_path)
    call = _call("http.get", {"url": "https://api.example.com/value"})

    with pytest.raises(ValueError, match="URL string"):
        await EnterpriseToolInvoker(workspace, http_client=enterprise_http_client).invoke(
            base,
            _call("http.get", {"url": 1}),
        )
    for policy in ["api.example.com", [""], [1]]:
        context = replace(base, config=replace(base.config, tools={"http_allowed_hosts": policy}))
        with pytest.raises(ValueError, match="array of host names"):
            await EnterpriseToolInvoker(workspace,
                                        http_client=enterprise_http_client).invoke(context, call)

    context = replace(
        base,
        config=replace(base.config, tools={"http_allowed_hosts": ["api.example.com"]}),
    )
    with pytest.raises(ConnectionError, match="did not resolve"):
        await EnterpriseToolInvoker(
            workspace,
            http_client=enterprise_http_client,
            resolve_host=lambda _: (),
        ).invoke(context, call)
    for address in ["127.0.0.1", "10.0.0.1", "169.254.1.1", "224.0.0.1", "0.0.0.0"]:
        with pytest.raises(PermissionError, match="non-public"):
            await EnterpriseToolInvoker(
                workspace,
                http_client=enterprise_http_client,
                resolve_host=lambda _, address=address: (address, ),
            ).invoke(context, call)

    async def status_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    async def large_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * (256 * 1024 + 1))

    async def html_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html/>", headers={"content-type": "text/html"})

    for handler, error, message in [
        (status_handler, RuntimeError, "status 503"),
        (large_handler, ValueError, "size limit"),
        (html_handler, ValueError, "content type"),
    ]:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with pytest.raises(error, match=message):
            await EnterpriseToolInvoker(
                workspace,
                http_client=client,
                resolve_host=lambda _: ("93.184.216.34", ),
            ).invoke(context, call)
        await client.aclose()


class _RecordingInvoker(AgentToolInvoker):

    def __init__(self) -> None:
        self.calls: list[AgentToolCall] = []

    async def invoke(self, context: AgentExecutionContext, call: AgentToolCall) -> AgentToolResult:
        del context
        self.calls.append(call)
        return AgentToolResult(call.call_id, content=call.name)


@pytest.mark.anyio
async def test_trpc_tool_bridge_exposes_every_granted_public_tool() -> None:
    context = _context()
    allowlist = [
        "calculate",
        "current_time",
        "http.get",
        "workspace.list",
        "workspace.read",
        "knowledge.add",
        "knowledge.update",
        "knowledge.list",
        "knowledge.search",
        "knowledge.delete",
    ]
    context = replace(context, config=replace(context.config, tools={"allowlist": allowlist}))
    invoker = _RecordingInvoker()
    functions = {function.__name__: function for function in _bridge(context, invoker).functions()}

    assert set(functions) == {
        "calculate",
        "current_time",
        "http_get",
        "workspace_list",
        "workspace_read",
        "knowledge_list",
        "knowledge_search",
    }
    assert await functions["calculate"]("1 + 1") == {"result": "calculate"}
    assert await functions["current_time"]() == {"result": "current_time"}
    assert await functions["http_get"]("https://api.example.com/x") == {"result": "http.get"}
    assert await functions["workspace_list"]() == {"result": "workspace.list"}
    assert await functions["workspace_read"]("a.txt") == {"result": "workspace.read"}
    assert await functions["knowledge_list"]("handbook") == {"result": "knowledge.list"}
    assert await functions["knowledge_search"]("handbook", "年假") == {"result": "knowledge.search"}
    assert [call.logical_call_index for call in invoker.calls] == list(range(7))
    assert invoker.calls[2].resource == "api.example.com"
    assert invoker.calls[3].kind is AgentToolKind.WORKSPACE
    assert invoker.calls[4].kind is AgentToolKind.WORKSPACE


@pytest.mark.anyio
async def test_trpc_tool_bridge_honors_typed_workspace_grants() -> None:
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [
                    {
                        "kind": "workspace",
                        "name": "workspace.list",
                        "actions": ["execute"],
                        "resources": ["*"],
                    },
                    {
                        "kind": "workspace",
                        "name": "workspace.read",
                        "actions": ["execute"],
                        "resources": ["inputs/*"],
                    },
                ]
            },
        ),
    )
    invoker = _RecordingInvoker()
    functions = {function.__name__: function for function in _bridge(context, invoker).functions()}

    assert set(functions) == {"workspace_list", "workspace_read"}
    assert await functions["workspace_list"]() == {"result": "workspace.list"}
    assert await functions["workspace_read"]("inputs/spec.txt") == {"result": "workspace.read"}
    assert all(call.kind is AgentToolKind.WORKSPACE for call in invoker.calls)


def test_trpc_tool_bridge_rejects_malformed_grants_and_allowlists() -> None:
    context = _context()
    invalid_tools = [
        {
            "grants": "invalid"
        },
        {
            "grants": ["invalid"]
        },
        {
            "grants": [{
                "kind": "tool",
                "name": "calculate",
                "actions": "execute",
                "resources": []
            }]
        },
        {
            "grants": [{
                "kind": "tool",
                "name": "calculate",
                "actions": [],
                "resources": "all"
            }]
        },
        {
            "allowlist": "calculate"
        },
        {
            "allowlist": [""]
        },
        {
            "allowlist": [1]
        },
    ]
    for tools in invalid_tools:
        configured = replace(context, config=replace(context.config, tools=tools))
        with pytest.raises(ValueError):
            _bridge(configured, _RecordingInvoker()).functions()
