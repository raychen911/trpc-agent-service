"""Tenant-scoped tool and MCP registry used by AgentWorker."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, get_args, get_origin, get_type_hints
from urllib.request import Request, urlopen

from trpc_service.security.ssrf import validate_outbound_url


@dataclass(slots=True)
class ToolResult:
    name: str
    content: str
    metadata: dict[str, Any]


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Callable[..., ToolResult]] = {}
        self._mcp_servers: dict[str, dict[str, Any]] = {}

    def register(self, name: str, handler: Callable[..., ToolResult]) -> None:
        self._tools[name] = handler

    @property
    def registered_names(self) -> set[str]:
        names = set(self._tools)
        for server in self._mcp_servers.values():
            names.update(server["tools"])
        return names

    def has_local_tool(self, name: str) -> bool:
        return name in self._tools

    def local_handler(self, name: str) -> Callable[..., ToolResult] | None:
        return self._tools.get(name)

    def tool_schemas(self, allowed_names: set[str] | None = None) -> list[dict[str, Any]]:
        """Return OpenAI-compatible declarations for policy-approved tools."""
        allowed = self.registered_names if allowed_names is None else allowed_names
        schemas: list[dict[str, Any]] = []
        if "search_knowledge" in allowed and "search_knowledge" in self._tools:
            schemas.append(
                {
                    "type": "function",
                    "function": {
                        "name": "search_knowledge",
                        "description": "Search tenant-scoped knowledge.",
                        "parameters": {
                            "type": "object",
                            "properties": {"query": {"type": "string"}},
                            "required": ["query"],
                            "additionalProperties": False,
                        },
                    },
                }
            )
        for name, handler in self._tools.items():
            if name == "search_knowledge" or name not in allowed:
                continue
            schemas.append(
                {
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": (inspect.getdoc(handler) or f"Execute tenant tool {name}.").strip(),
                        "parameters": _handler_schema(handler),
                    },
                }
            )
        for server in self._mcp_servers.values():
            for name in server["tools"]:
                if name in allowed and name != "search_knowledge":
                    schemas.append(
                        {
                            "type": "function",
                            "function": {
                                "name": name,
                                "description": f"MCP tool from {server['name'] if 'name' in server else 'server'}",
                                "parameters": {"type": "object", "additionalProperties": True},
                            },
                        }
                    )
        return schemas

    def register_mcp_server(
        self,
        name: str,
        endpoint: str,
        tools: list[str],
        headers: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> None:
        validate_outbound_url(endpoint)
        self._mcp_servers[name] = {
            "endpoint": endpoint,
            "tools": list(tools),
            "headers": dict(headers or {}),
            "timeout": timeout,
        }

    def call(self, name: str, **kwargs: Any) -> ToolResult:
        idempotency_key = kwargs.get("idempotency_key")
        if name in self._tools:
            handler = self._tools[name]
            parameters = inspect.signature(handler).parameters
            accepts_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
            )
            if not accepts_kwargs:
                for injected_name in ("idempotency_key", "request_id"):
                    if injected_name not in parameters:
                        kwargs.pop(injected_name, None)
            return handler(**kwargs)
        for server_name, server in self._mcp_servers.items():
            if name in server["tools"]:
                body = {
                    "jsonrpc": "2.0",
                    "id": kwargs.get("request_id", name),
                    "method": "tools/call",
                    "params": {
                        "name": name,
                        "arguments": kwargs.get("arguments", {}),
                        "metadata": {"idempotency_key": idempotency_key} if idempotency_key else {},
                    },
                }
                request = Request(
                    validate_outbound_url(server["endpoint"]),
                    data=json.dumps(body).encode("utf-8"),
                    headers={"Content-Type": "application/json", **server.get("headers", {})},
                    method="POST",
                )
                with urlopen(request, timeout=float(server.get("timeout", 10))) as response:
                    result = json.loads(response.read().decode("utf-8"))
                if result.get("error"):
                    raise RuntimeError(str(result["error"]))
                value = result.get("result", {})
                content = value.get("content", value) if isinstance(value, dict) else value
                return ToolResult(name, str(content), {"mcp_server": server_name})
        raise KeyError(f"tool not registered: {name}")

    @property
    def mcp_servers(self) -> dict[str, dict[str, Any]]:
        return dict(self._mcp_servers)


def _handler_schema(handler: Callable[..., Any]) -> dict[str, Any]:
    """Derive a conservative JSON schema while hiding platform-only kwargs."""
    try:
        hints = get_type_hints(handler)
    except Exception:
        hints = {}
    properties: dict[str, Any] = {}
    required: list[str] = []
    hidden = {"storage", "tenant_id", "request_id", "idempotency_key", "tool_context", "arguments"}
    accepts_arbitrary_arguments = False
    for parameter in inspect.signature(handler).parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            accepts_arbitrary_arguments = True
            continue
        if parameter.name in hidden or parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        annotation = hints.get(parameter.name, parameter.annotation)
        properties[parameter.name] = {"type": _json_type(annotation)}
        if parameter.default is inspect.Parameter.empty:
            required.append(parameter.name)
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": accepts_arbitrary_arguments,
    }
    if required:
        schema["required"] = required
    return schema


def _json_type(annotation: Any) -> str:
    if annotation in {str}:
        return "string"
    if annotation in {int}:
        return "integer"
    if annotation in {float}:
        return "number"
    if annotation in {bool}:
        return "boolean"
    origin = get_origin(annotation)
    if origin in {list, tuple, set}:
        return "array"
    if origin is dict or annotation in {dict, Any}:
        return "object"
    if origin is not None:
        args = get_args(annotation)
        if type(None) in args and len(args) == 2:
            return _json_type(next(arg for arg in args if arg is not type(None)))
    return "object"
