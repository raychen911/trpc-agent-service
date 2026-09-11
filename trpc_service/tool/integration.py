"""Governed FunctionTool and MCP integration for tRPC-Agent-Python."""

from __future__ import annotations

import contextvars
import functools
import importlib
import inspect
import json
import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from ..config import resolve_secret
from ..metrics.prometheus import observe_operation, record_tool
from ..metrics.telemetry import span
from ..tenant import AgentApp, InboundMessage, TenantConfig
from .confirmation import ConfirmationScope, ConfirmationService, arguments_hash
from .execution import InMemoryExecutionLedger, ToolExecutor
from .policy import TenantPolicyFilter
from .postgres import PostgresConfirmationStore, PostgresExecutionLedger

_ACTIVE_TURN: contextvars.ContextVar[InboundMessage | None] = contextvars.ContextVar(
    "trpc_service_active_tool_turn", default=None
)


@contextmanager
def active_tool_turn(message: InboundMessage) -> Iterator[None]:
    token = _ACTIVE_TURN.set(message)
    try:
        yield
    finally:
        _ACTIVE_TURN.reset(token)


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    function: Callable[..., Any]
    idempotent: bool = True


class GovernedToolRuntime:
    def __init__(
        self,
        config: TenantConfig,
        key: bytes,
        *,
        repository: Any | None = None,
        audit_store: Any | None = None,
    ) -> None:
        self.config = config
        self.key = key
        self.repository = repository
        self.audit_store = audit_store
        confirmation_store = PostgresConfirmationStore(repository) if repository is not None else None
        self.confirmations = ConfirmationService(key, store=confirmation_store)
        self.memory_ledger = InMemoryExecutionLedger()

    async def execute(
        self,
        tool_name: str,
        supplied_arguments: dict[str, Any],
        call: Callable[[dict[str, Any]], Any],
        *,
        idempotent: bool,
    ) -> Any:
        message = _ACTIVE_TURN.get()
        if message is None or message.tenant_id != self.config.tenant_id:
            raise RuntimeError("tool execution is outside an authenticated agent turn")
        arguments = dict(supplied_arguments)
        confirmation_token = arguments.pop("confirmation_token", None)
        decision = TenantPolicyFilter(self.config).check_tool(tool_name, confirmed=False)
        if not decision.allowed and not decision.requires_confirmation:
            record_tool(message.tenant_id, tool_name, "denied")
            await self._audit(message, tool_name, "deny", decision.reason)
            return {"error": decision.reason}
        if decision.requires_confirmation:
            scope = ConfirmationScope(
                message.tenant_id,
                message.external_user_id,
                message.session_id,
                tool_name,
                arguments_hash(arguments),
            )
            if not isinstance(confirmation_token, str) or not confirmation_token:
                token = await self.confirmations.issue(scope)
                record_tool(message.tenant_id, tool_name, "confirmation_required")
                await self._audit(message, tool_name, "confirm", "confirmation_required")
                return {
                    "error": "confirmation_required",
                    "confirmation_token": token,
                    "arguments_hash": scope.arguments_hash,
                }
            if confirmation_token not in message.text:
                record_tool(message.tenant_id, tool_name, "confirmation_rejected")
                await self._audit(message, tool_name, "deny", "confirmation_not_user_supplied")
                return {"error": "confirmation_token_must_be_supplied_by_user"}
            try:
                await self.confirmations.consume(confirmation_token, scope)
            except ValueError as exc:
                record_tool(message.tenant_id, tool_name, "confirmation_rejected")
                await self._audit(message, tool_name, "deny", "confirmation_rejected")
                return {"error": str(exc)}
        turn_id = message.external_message_id
        digest = arguments_hash(arguments)
        if self.repository is None:
            ledger = self.memory_ledger
        else:
            ledger = PostgresExecutionLedger(
                self.repository,
                message.tenant_id,
                message.session_id,
                turn_id,
                tool_name,
                digest,
            )
        executor = ToolExecutor(self.key, ledger)

        async def invoke() -> Any:
            result = call(arguments)
            return await result if inspect.isawaitable(result) else result

        try:
            with (
                span("tool", tenant_id=message.tenant_id, tool=tool_name, trace_id=message.trace_id),
                observe_operation("tool", message.tenant_id, tool_name),
            ):
                result = await executor.execute(
                    message.tenant_id,
                    message.session_id,
                    turn_id,
                    tool_name,
                    arguments,
                    invoke,
                    idempotent=idempotent,
                )
        except BaseException:
            record_tool(message.tenant_id, tool_name, "failed")
            await self._audit(message, tool_name, "fail", "execution_failed")
            raise
        record_tool(message.tenant_id, tool_name, "succeeded")
        await self._audit(message, tool_name, "execute", "ok")
        return result

    async def _audit(self, message: InboundMessage, tool_name: str, decision: str, reason: str) -> None:
        if self.audit_store is None:
            return
        await self.audit_store.write(
            {
                "tenant_id": message.tenant_id,
                "channel": message.channel,
                "user_id": message.external_user_id,
                "session_id": message.session_id,
                "agent_name": message.app_id,
                "tool_name": tool_name,
                "decision": decision,
                "reason": reason,
                "trace_id": message.trace_id,
            }
        )

    def function_tool(self, definition: ToolDefinition) -> Any:
        from trpc_agent_sdk.tools import FunctionTool

        original = definition.function
        signature = inspect.signature(original)
        if "confirmation_token" in signature.parameters:
            raise ValueError(f"tool {definition.name} reserves the confirmation_token parameter")

        @functools.wraps(original)
        async def governed(**kwargs: Any) -> Any:
            return await self.execute(
                definition.name,
                kwargs,
                lambda arguments: original(**arguments),
                idempotent=definition.idempotent,
            )

        governed.__name__ = definition.name
        parameters = list(signature.parameters.values())
        if definition.name in self.config.policy.dangerous_tools:
            confirmation = inspect.Parameter(
                "confirmation_token",
                inspect.Parameter.KEYWORD_ONLY,
                default=None,
                annotation=Optional[str],  # noqa: UP045 - tRPC-Agent 1.1 parser rejects PEP 604 here.
            )
            variadic = next(
                (index for index, parameter in enumerate(parameters) if parameter.kind == inspect.Parameter.VAR_KEYWORD),
                len(parameters),
            )
            parameters.insert(variadic, confirmation)
        governed.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]
        return FunctionTool(governed)


class ToolCatalog:
    def __init__(self, functions: Mapping[str, ToolDefinition] | None = None, mcp_servers: list[dict[str, Any]] | None = None) -> None:
        self.functions = dict(functions or {})
        self.mcp_servers = list(mcp_servers or [])

    @classmethod
    def from_env(cls) -> ToolCatalog:
        function_payload = os.getenv("TRPC_AGENT_FUNCTION_TOOLS_JSON", "{}").strip() or "{}"
        mcp_payload = os.getenv("TRPC_AGENT_MCP_SERVERS_JSON", "[]").strip() or "[]"
        try:
            raw_functions = json.loads(function_payload)
            raw_mcp = json.loads(mcp_payload)
        except json.JSONDecodeError as exc:
            raise ValueError("tool configuration must be valid JSON") from exc
        if not isinstance(raw_functions, dict) or not isinstance(raw_mcp, list):
            raise TypeError("tool configuration has an invalid shape")
        functions: dict[str, ToolDefinition] = {}
        for name, value in raw_functions.items():
            config = value if isinstance(value, dict) else {"callable": value}
            target = config.get("callable")
            if not isinstance(name, str) or not isinstance(target, str):
                raise TypeError("function tool entries require a name and callable")
            functions[name] = ToolDefinition(name, _load_callable(target), bool(config.get("idempotent", True)))
        return cls(functions, [dict(value) for value in raw_mcp if isinstance(value, dict)])

    def build(
        self,
        config: TenantConfig,
        app: AgentApp,
        key: bytes,
        *,
        repository: Any | None = None,
        storage_router: Any | None = None,
    ) -> list[Any]:
        selected = set(app.tools)
        disallowed = selected - set(config.policy.allowed_tools)
        if disallowed:
            raise ValueError(f"app tools are not tenant-allowlisted: {sorted(disallowed)}")
        audit_store = storage_router.route(config).audit if storage_router is not None else None
        runtime = GovernedToolRuntime(config, key, repository=repository, audit_store=audit_store)
        tools = [runtime.function_tool(self.functions[name]) for name in sorted(selected & self.functions.keys())]
        provided = set(self.functions)
        for server in self.mcp_servers:
            declared = {str(value) for value in server.get("tools", [])}
            chosen = sorted(selected & declared)
            provided.update(declared)
            if chosen:
                tools.append(_mcp_toolset(server, chosen, runtime))
        missing = selected - provided
        if missing:
            raise ValueError(f"app references unregistered tools: {sorted(missing)}")
        return tools


def _load_callable(target: str) -> Callable[..., Any]:
    module_name, separator, attribute = target.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError(f"invalid tool callable: {target}")
    value = getattr(importlib.import_module(module_name), attribute)
    if not callable(value):
        raise TypeError(f"tool target is not callable: {target}")
    return value


def _mcp_toolset(server: dict[str, Any], selected: list[str], runtime: GovernedToolRuntime) -> Any:
    from trpc_agent_sdk.tools.mcp_tool import (
        MCPTool,
        MCPToolset,
        SseConnectionParams,
        StreamableHTTPConnectionParams,
    )

    transport = str(server.get("transport", "streamable_http")).replace("-", "_")
    url = server.get("url")
    if not isinstance(url, str) or not url.startswith(("https://", "http://")):
        raise ValueError("MCP server URL must use HTTP(S)")
    if os.getenv("TRPC_SERVICE_ENV", "development") == "production" and not url.startswith("https://"):
        raise ValueError("production MCP server URL must use HTTPS")
    if os.getenv("TRPC_SERVICE_ENV", "development") == "production" and any(
        isinstance(value, str) and value.startswith("literal://")
        for value in dict(server.get("headers") or {}).values()
    ):
        raise ValueError("production MCP headers cannot use literal:// secrets")
    headers = {
        str(key): (
            resolve_secret(value)
            if isinstance(value, str) and value.startswith(("env://", "file://", "literal://"))
            else value
        )
        for key, value in dict(server.get("headers") or {}).items()
    }
    if transport == "sse":
        params = SseConnectionParams(url=url, headers=headers)
    elif transport == "streamable_http":
        params = StreamableHTTPConnectionParams(
            url=url,
            headers=headers,
            timeout=timedelta(seconds=float(server.get("timeout_seconds", 30))),
        )
    else:
        raise ValueError("MCP transport must be sse or streamable_http")
    non_idempotent = {str(value) for value in server.get("non_idempotent_tools", [])}

    class GovernedMCPTool(MCPTool):
        def _get_declaration(self) -> Any:
            from trpc_agent_sdk.types import Schema

            declaration = super()._get_declaration()
            if self.name in runtime.config.policy.dangerous_tools:
                if declaration.parameters is None:
                    declaration.parameters = Schema(type="OBJECT", properties={})
                properties = declaration.parameters.properties or {}
                declaration.parameters.properties = {
                    **properties,
                    "confirmation_token": Schema(
                        type="STRING",
                        nullable=True,
                        description="One-time confirmation token returned by the previous tool call.",
                    ),
                }
            return declaration

        async def _run_async_impl(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            async def call(arguments: dict[str, Any]) -> Any:
                return await super(GovernedMCPTool, self)._run_async_impl(args=arguments, tool_context=tool_context)

            return await runtime.execute(self.name, args, call, idempotent=self.name not in non_idempotent)

    return MCPToolset(connection_params=params, tool_filter=selected, mcp_tool_cls=GovernedMCPTool)
