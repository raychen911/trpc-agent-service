"""Small, durable MCP-over-streamable-HTTP adapter for tenant applications."""

from __future__ import annotations

import httpx
import asyncio
import re

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from trpc_agent_sdk.tools import BaseTool
from trpc_agent_sdk.types import FunctionDeclaration, Schema

from trpc_service.reliability.tool_execution import DurableTools


def _headers(secret_ref):
    if secret_ref is None:
        return {}
    from trpc_service.storage.runtime_resources import resolve_env
    return {"Authorization": "Bearer " + resolve_env(secret_ref.uri)}


def _result_value(result):
    if getattr(result, "isError", False):
        raise RuntimeError("MCP tool returned an error")
    values = []
    for item in getattr(result, "content", ()):
        text = getattr(item, "text", None)
        if text is not None:
            values.append(text)
        elif hasattr(item, "model_dump"):
            values.append(item.model_dump(mode="json"))
    if len(values) == 1:
        return values[0]
    return values


def model_schema(value):
    """Use the schema field supported by the pinned SDK's OpenAI converter.

    Reject unsupported structural schemas instead of advertising empty arguments.
    The original JSON schema is still validated locally before execution.
    """
    if any(key in value for key in ("$ref", "$defs", "oneOf", "anyOf", "allOf")):
        raise ValueError("this SDK requires resolved, non-union MCP schemas")
    result = {key: item for key, item in value.items()
              if key in {"type", "description", "required", "additionalProperties"}}
    if "properties" in value:
        result["properties"] = {key: model_schema(item) for key, item in value["properties"].items()}
    if "items" in value:
        result["items"] = model_schema(value["items"])
    return Schema.model_validate(result)


class MCPTool(BaseTool):
    """Persist call results; an uncertain remote outcome is never replayed automatically."""

    def __init__(self, database, tenant_id, app_id, *, server_id, endpoint, secret_ref,
                 remote_name, description, schema, timeout_seconds=30, max_calls=20):
        name = f"mcp__{server_id}__{remote_name}"
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.:-]{0,127}", name) is None:
            raise ValueError("MCP tool name is invalid or too long")
        super().__init__(name=name, description=description or f"MCP tool {remote_name}")
        self.database = database
        self.tenant_id, self.app_id = tenant_id, app_id
        self.server_id, self.endpoint, self.secret_ref = server_id, endpoint, secret_ref
        self.remote_name, self.schema = remote_name, (schema or {"type": "object"})
        self.parameters = model_schema(self.schema)
        self.timeout_seconds, self.max_calls = timeout_seconds, max_calls

    def _get_declaration(self):
        return FunctionDeclaration(name=self.name,
                                   description=self.description,
                                   parameters=self.parameters)

    async def _call_remote(self, args):
        headers = _headers(self.secret_ref)
        async with httpx.AsyncClient(headers=headers, timeout=self.timeout_seconds) as client:
            async with streamable_http_client(self.endpoint, http_client=client) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(self.remote_name, arguments=args)
                    return _result_value(result)

    async def _run_async_impl(self, *, tool_context, args):
        from jsonschema import validate
        validate(instance=args, schema=self.schema)
        metadata = tool_context.agent_context.metadata
        if metadata.get("tenant_id") != self.tenant_id or metadata.get("agent_app_id") != self.app_id:
            raise PermissionError("MCP tool context is outside its tenant binding")
        execution = metadata.get("execution_id")
        call_id = tool_context.function_call_id
        if not execution or not call_id:
            raise PermissionError("MCP tool execution identity missing")

        async def operation(_idempotency_key):
            return await asyncio.wait_for(self._call_remote(args), self.timeout_seconds)

        return await DurableTools(self.database).invoke(self.tenant_id, execution, call_id, self.name, args,
                                                        operation,
                                                        max_calls=self.max_calls)


async def discover_mcp_tools(database, tenant_id, app_id, servers, *, timeout_seconds=30, max_calls=20):
    """Discover declarations once during versioned runtime construction."""
    tools = []
    for server in servers:
        if not server.enabled:
            continue
        async with httpx.AsyncClient(headers=_headers(server.secret_ref), timeout=timeout_seconds) as client:
            async with streamable_http_client(server.endpoint, http_client=client) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    cursor, seen = None, set()
                    while True:
                        page = await session.list_tools(cursor=cursor)
                        for item in page.tools:
                            tools.append(MCPTool(database, tenant_id, app_id,
                                                 server_id=server.server_id,
                                                 endpoint=server.endpoint,
                                                 secret_ref=server.secret_ref,
                                                 remote_name=item.name,
                                                 description=getattr(item, "description", None),
                                                 schema=getattr(item, "inputSchema", None),
                                                 timeout_seconds=timeout_seconds,
                                                 max_calls=max_calls))
                        cursor = page.nextCursor
                        if not cursor:
                            break
                        if cursor in seen or len(seen) >= 100:
                            raise ValueError("MCP tool pagination did not terminate")
                        seen.add(cursor)
    if len({tool.name for tool in tools}) != len(tools):
        raise ValueError("duplicate MCP tool declaration")
    return tuple(tools)
