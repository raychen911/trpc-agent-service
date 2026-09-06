"""Compatibility worker backed by the public tRPC-Agent-Python SDK.

The platform remains authoritative for tenant routing, policy, audit,
idempotency, and persistent Session/Memory state. The SDK SessionService used
below is deliberately ephemeral: it is only the SDK invocation context and is
never used as a second platform persistence layer.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import threading
from contextvars import ContextVar, copy_context
from typing import Any

from trpc_agent_sdk.tools import BaseTool
from trpc_agent_sdk.types import FunctionDeclaration

from trpc_service.agent.model_client import ModelResponse
from trpc_service.gateway.router import AgentWorker
from trpc_service.policy.tenant_filter import TenantPolicy
from trpc_service.storage.factory import StorageBundle
from trpc_service.telemetry.tracing import TraceRecorder
from trpc_service.tenant.models import AgentEvent, RunRequest, TenantConfig


class _ApprovalRequired(BaseException):
    def __init__(self, events: list[AgentEvent]) -> None:
        super().__init__("tool approval required")
        self.events = events


class _PlatformSdkTool(BaseTool):
    """Bridge one tenant tool into the SDK while keeping platform controls."""

    def __init__(self, worker: TrpcAgentWorker, name: str, schema: dict[str, Any]) -> None:
        function = schema.get("function", schema)
        super().__init__(
            name=name,
            description=str(function.get("description", f"Execute tenant tool {name}.")),
        )
        self.worker = worker
        self.schema = schema
        self.func = _signature_proxy(worker.tools.local_handler(name), name)

    def _get_declaration(self) -> FunctionDeclaration:
        function = self.schema.get("function", self.schema)
        parameters = function.get("parameters") or {"type": "object", "properties": {}}
        return FunctionDeclaration(
            name=str(function.get("name", self.name)),
            description=str(function.get("description", self.description)),
            # OpenAIModel consumes the SDK's typed ``parameters`` field when
            # it converts declarations to provider tool schemas. Keeping the
            # original JSON schema here also preserves required fields and
            # additionalProperties for MCP tools.
            parameters=parameters,
        )

    async def _run_async_impl(self, *, tool_context, args: dict[str, Any]) -> Any:
        request = self.worker._active_request_var.get()
        app = self.worker._active_app_var.get()
        policy = self.worker._active_policy_var.get()
        storage = self.worker._active_storage_var.get()
        if request is None or app is None or policy is None or storage is None:
            raise RuntimeError("tRPC platform tool invoked outside an active tenant request")

        call_id = str(getattr(tool_context, "function_call_id", "") or "")
        call = {
            "name": self.name,
            "arguments": dict(args or {}),
            "call_id": call_id,
            "_tool_key": f"{request.idempotency_key}:sdk:{call_id or self.name}",
            "side_effect": self.name != "search_knowledge",
        }
        events, _ = self.worker._run_tools(request, app, policy, storage, calls=[call])
        collected = self.worker._sdk_tool_events_var.get()
        if collected is not None:
            collected.extend(events)

        approval = next((event for event in events if event.event_type == "approval_required"), None)
        if approval is not None:
            raise _ApprovalRequired([approval])
        result = next((event for event in events if event.event_type == "tool_call"), None)
        if result is None:
            raise RuntimeError(f"tool {self.name} did not return a result")
        return {"content": result.content, "metadata": result.metadata}


def _signature_proxy(handler: Any, name: str):
    """Provide an introspectable callable without bypassing platform execution."""
    def proxy(**kwargs: Any) -> None:
        del kwargs

    proxy.__name__ = name
    if handler is not None:
        signature = inspect.signature(handler)
        if "tool_context" not in signature.parameters:
            signature = signature.replace(
                parameters=[
                    *signature.parameters.values(),
                    inspect.Parameter(
                        "tool_context",
                        inspect.Parameter.KEYWORD_ONLY,
                        annotation=Any,
                    ),
                ]
            )
        proxy.__signature__ = signature
    return proxy


class TrpcAgentWorker(AgentWorker):
    def __init__(
        self,
        storage: StorageBundle,
        telemetry: TraceRecorder | None = None,
        settings: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(storage, telemetry)
        self.settings = dict(settings or {})
        self._active_context_var: ContextVar[Any] = ContextVar(
            "trpc_agent_active_context",
            default=None,
        )
        self._active_request_var: ContextVar[Any] = ContextVar(
            "trpc_agent_active_request",
            default=None,
        )
        self._active_app_var: ContextVar[Any] = ContextVar(
            "trpc_agent_active_app",
            default=None,
        )
        self._active_policy_var: ContextVar[Any] = ContextVar(
            "trpc_agent_active_policy",
            default=None,
        )
        self._active_storage_var: ContextVar[Any] = ContextVar(
            "trpc_agent_active_storage",
            default=None,
        )
        self._sdk_tool_events_var: ContextVar[list[AgentEvent] | None] = ContextVar(
            "trpc_agent_sdk_tool_events",
            default=None,
        )

    def run(
        self,
        request: RunRequest,
        config: TenantConfig,
        storage: StorageBundle | None = None,
    ) -> list[AgentEvent]:
        events_token = self._sdk_tool_events_var.set([])
        approval_events: list[AgentEvent] | None = None
        try:
            events = super().run(request, config, storage)
        except _ApprovalRequired as exc:
            approval_events = exc.events
        finally:
            sdk_events = self._sdk_tool_events_var.get() or []
            self._sdk_tool_events_var.reset(events_token)

        if approval_events is not None:
            return sdk_events or approval_events

        if sdk_events:
            message_end = next(
                (event for event in reversed(events) if event.event_type == "message_end"),
                None,
            )
            if message_end is not None:
                events = [event for event in events if event is not message_end]
                events.extend(sdk_events)
                events.append(message_end)
            else:
                events.extend(sdk_events)
        return events

    def _generate_answer(
        self,
        text: str,
        app,
        prior,
        tool_context: str = "",
        summary_context: str = "",
        memory_context: str = "",
        redact=None,
        tools=None,
    ):
        if not text.strip():
            return "Please enter a message."
        if text.lower().startswith("/help"):
            return "This tenant supports shared sessions, memory, tools, and audit records."
        conversation = self._build_conversation(
            text,
            prior,
            tool_context,
            summary_context,
            memory_context,
            redact,
        )
        allowed_tools = {
            str(item.get("function", {}).get("name", ""))
            for item in (tools or [])
            if item.get("function", {}).get("name")
        }
        return _run_coroutine_sync(self._run_sdk_async(app, conversation, allowed_tools))

    async def _run_sdk_async(
        self,
        app,
        conversation: list[dict],
        tool_names: set[str] | None = None,
    ) -> ModelResponse:
        # Imports are intentionally lazy so explicit local mode and unit tests
        # do not require the SDK to be imported before the runtime is selected.
        from trpc_agent_sdk.agents import LlmAgent
        from trpc_agent_sdk.configs import RunConfig
        from trpc_agent_sdk.context import AgentContext
        from trpc_agent_sdk.models import OpenAIModel
        from trpc_agent_sdk.runners import Runner
        from trpc_agent_sdk.types import Content, Part

        model_config = app.model_config
        api_key = _resolve_api_key(model_config.api_key_ref, model_config.api_key_env)
        if not api_key:
            raise RuntimeError(
                "tRPC-Agent-Python runtime requires a model API key; "
                "set the tenant model api_key_ref or configured environment variable"
            )
        if not model_config.model:
            raise RuntimeError("tRPC-Agent-Python runtime requires a tenant model name")
        if not model_config.base_url and not os.getenv("CPA_BASE_URL", "").strip():
            raise RuntimeError("tRPC-Agent-Python runtime requires a model base URL")

        # Platform StorageBundle is the sole source of truth. The SDK receives
        # the already-rendered platform history and uses an ephemeral service
        # for this invocation, so backend credentials and tenant state cannot
        # diverge between two Session implementations.
        session_service = _build_session_service()
        context = self._runtime_context()
        sdk_tools = self._build_sdk_tools(None, tool_names or set())
        app_name = f"trpc-agent:{context.tenant_id}:{app.agent_app_id}"
        model = OpenAIModel(
            model_config.model,
            api_key=api_key,
            base_url=model_config.base_url or os.getenv("CPA_BASE_URL", "").strip(),
            use_responses_api=model_config.wire_api.lower() == "responses",
        )
        agent = LlmAgent(
            name=app.agent_name,
            model=model,
            instruction=app.prompt,
            tools=sdk_tools,
            parallel_tool_calls=False,
            include_previous_history=False,
        )
        runner = Runner(
            app_name=app_name,
            agent=agent,
            session_service=session_service,
            close_session_service_on_close=True,
        )
        session_id = context.session_id or context.trace_id
        user_id = context.user_id or "anonymous"
        if await session_service.get_session(app_name=app_name, user_id=user_id, session_id=session_id) is None:
            await session_service.create_session(
                app_name=app_name,
                user_id=user_id,
                session_id=session_id,
            )
        content = Content(
            role="user",
            parts=[Part(text=_render_conversation(conversation))],
        )
        texts: list[str] = []
        try:
            async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=content,
                run_config=RunConfig(
                    max_llm_calls=_max_sdk_llm_calls(),
                    max_tool_calls=_max_sdk_tool_calls(len(sdk_tools)),
                    streaming=False,
                ),
                agent_context=AgentContext(trpc_ctx=context),
            ):
                text = event.get_text()
                if text:
                    texts.append(text)
        finally:
            await _close_async(runner)
        answer = "".join(texts).strip()
        tool_events = self._sdk_tool_events_var.get() or []
        approval_events = [event for event in tool_events if event.event_type == "approval_required"]
        if approval_events:
            raise _ApprovalRequired(approval_events)
        if not answer:
            raise RuntimeError("tRPC-Agent-Python runtime returned no assistant text")
        input_tokens = len(_render_conversation(conversation).split())
        output_tokens = len(answer.split())
        return ModelResponse(answer, input_tokens, output_tokens, input_tokens + output_tokens)

    def _runtime_context(self):
        # AgentWorker builds this just before model generation; keeping it on
        # a context variable keeps concurrent requests isolated on one worker.
        return self._active_context_var.get()

    def _run_unlocked(self, request, config, storage=None, lease=None):
        context_token = self._active_context_var.set(request.tenant_context)
        request_token = self._active_request_var.set(request)
        app_token = self._active_app_var.set(config.app(request.tenant_context.agent_app_id))
        policy_token = self._active_policy_var.set(TenantPolicy(config, request.tenant_context.agent_app_id))
        storage_token = self._active_storage_var.set(storage or self.storage)
        try:
            return super()._run_unlocked(request, config, storage, lease)
        finally:
            self._active_context_var.reset(context_token)
            self._active_request_var.reset(request_token)
            self._active_app_var.reset(app_token)
            self._active_policy_var.reset(policy_token)
            self._active_storage_var.reset(storage_token)

    def _build_sdk_tools(self, tool_cls=None, allowed_names: set[str] | None = None) -> list[_PlatformSdkTool]:
        del tool_cls  # Platform execution is required for policy and audit enforcement.
        allowed = set(allowed_names or set()).intersection(self.tools.registered_names)
        return [
            _PlatformSdkTool(self, name, schema)
            for schema in self.tools.tool_schemas(allowed)
            for name in [str(schema.get("function", {}).get("name", ""))]
            if name
        ]


def _resolve_api_key(ref: str, env_name: str) -> str:
    if ref.startswith("env://"):
        return os.getenv(ref[6:], "").strip()
    if ref.startswith("secret://"):
        from trpc_service.security.secrets import SecretManager

        return SecretManager().resolve(ref).strip()
    return os.getenv(env_name, "").strip()


def _max_sdk_llm_calls() -> int:
    try:
        value = int(os.getenv("AGENT_MAX_LLM_CALLS", "16"))
    except ValueError as exc:
        raise RuntimeError("AGENT_MAX_LLM_CALLS must be an integer") from exc
    if value < 2 or value > 500:
        raise RuntimeError("AGENT_MAX_LLM_CALLS must be between 2 and 500")
    return value


def _max_sdk_tool_calls(tool_count: int) -> int:
    try:
        rounds = int(os.getenv("AGENT_MAX_TOOL_ROUNDS", "8"))
    except ValueError as exc:
        raise RuntimeError("AGENT_MAX_TOOL_ROUNDS must be an integer") from exc
    if rounds < 1 or rounds > 32:
        raise RuntimeError("AGENT_MAX_TOOL_ROUNDS must be between 1 and 32")
    return max(1, rounds * max(1, tool_count))


def _build_session_service(profile=None):
    """Build the SDK invocation context without creating a second durable store.

    ``profile`` remains an optional compatibility argument for callers that
    used the old helper. Tenant-specific durable Session selection belongs to
    ``StorageBundle``; the SDK must not independently resolve Redis/SQL URLs.
    """
    del profile
    from trpc_agent_sdk.sessions import InMemorySessionService

    return InMemorySessionService()


async def _close_async(value: Any) -> None:
    close = getattr(value, "close", None)
    if close is None:
        return
    result = close()
    if hasattr(result, "__await__"):
        await result


def _run_coroutine_sync(coroutine):
    """Run the SDK coroutine from both sync workers and ASGI worker threads."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    result: list[Any] = []
    failure: list[BaseException] = []
    context = copy_context()

    def run_in_thread() -> None:
        try:
            result.append(context.run(asyncio.run, coroutine))
        except BaseException as exc:  # re-raise the original SDK failure
            failure.append(exc)

    thread = threading.Thread(
        target=run_in_thread,
        name="trpc-agent-sdk-call",
        daemon=True,
    )
    thread.start()
    thread.join()
    if failure:
        raise failure[0]
    return result[0]


def _render_conversation(conversation: list[dict]) -> str:
    parts = []
    for item in conversation:
        role = str(item.get("role", "user"))
        content = str(item.get("content", ""))
        if content:
            parts.append(f"{role}: {content}")
    return "\n\n".join(parts)
