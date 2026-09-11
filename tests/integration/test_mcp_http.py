"""Real TCP MCP discovery, SDK invocation, persistence and tenant rejection."""

import asyncio
import hashlib
import socket
from types import SimpleNamespace

import pytest
import uvicorn
from mcp.server.fastmcp import FastMCP
from trpc_agent_sdk.context import AgentContext

from tests.reliability.test_durable_tools import tool_execution as tool_execution_fixture
from trpc_service.agent.mcp import discover_mcp_tools
from trpc_service.agent.tool_assembly import assemble_tools
from trpc_service.tenant.models import MCPServerConfig, ToolPolicy

tool_execution = tool_execution_fixture


@pytest.mark.asyncio
async def test_real_mcp_discovery_call_and_durable_replay(tool_execution, tenant):
    database, execution = tool_execution
    server = FastMCP("acceptance", stateless_http=True, json_response=True)
    invocations = []

    @server.tool()
    def sha256(text: str) -> str:
        """Calculate SHA-256 of UTF-8 text."""
        invocations.append(text)
        return hashlib.sha256(text.encode()).hexdigest()

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}/mcp"
    http = uvicorn.Server(uvicorn.Config(server.streamable_http_app(), log_level="critical", access_log=False))
    task = asyncio.create_task(http.serve(sockets=[listener]))
    try:
        for _ in range(200):
            if http.started:
                break
            if task.done():
                await task
            await asyncio.sleep(0.025)
        assert http.started
        tools = await discover_mcp_tools(database, tenant.tenant_id, "customer_support",
                                         [MCPServerConfig(server_id="checks", endpoint=endpoint)])
        assert len(tools) == 1
        assert tools[0]._get_declaration().parameters.required == ["text"]
        from trpc_agent_sdk.models import OpenAIModel
        from trpc_agent_sdk.types import Tool
        wire = OpenAIModel(model_name="test", api_key="unused")._convert_tools_to_openai_format(
            [Tool(function_declarations=[tools[0]._get_declaration()])])
        assert wire[0]["function"]["parameters"]["properties"]["text"]["type"] == "string"
        app = tenant.applications[0].model_copy(update={
            "tool_policy": ToolPolicy(allow={"mcp__checks__sha256"})})
        selected = assemble_tools(database, tenant, app, tools, model=SimpleNamespace())
        context = SimpleNamespace(function_call_id="mcp-call", agent_context=AgentContext(), agent=SimpleNamespace())
        for key, value in {"tenant_id": tenant.tenant_id, "agent_app_id": app.app_id,
                           "execution_id": execution}.items():
            context.agent_context.with_metadata(key, value)
        value = await selected[0].run_async(tool_context=context, args={"text": "lighthouse"})
        assert value == hashlib.sha256(b"lighthouse").hexdigest()
        assert await selected[0].run_async(tool_context=context, args={"text": "lighthouse"}) == value
        assert invocations == ["lighthouse"]
        context.agent_context.with_metadata("tenant_id", "tenant_other")
        with pytest.raises(PermissionError):
            await selected[0].run_async(tool_context=context, args={"text": "lighthouse"})
        denied = app.model_copy(update={"tool_policy": ToolPolicy(deny={"mcp__checks__sha256"})})
        assert assemble_tools(database, tenant, denied, tools, model=SimpleNamespace()) == ()
        assert invocations == ["lighthouse"]
    finally:
        http.should_exit = True
        await asyncio.wait_for(task, 10)
        listener.close()
