"""The single shared ``tool.execute`` tracing boundary.

Both normal tool execution (SDK ``FunctionTool`` calls) and approved
execution (:class:`trpc_service.governance.approved_execution.ApprovedToolExecutor`)
funnel through :func:`traced_tool_execution`, so every tool run produces
exactly one fixed-name span with a fixed, safe attribute set::

    status          "ok" | "error"
    error_code      fixed category (e.g. "tenant_agent_configuration")
    exception.type  Python type NAME of the raised exception

Never the tool name, arguments, result, or exception message.

:func:`traced_tool_function` wraps a plain function for use inside
``FunctionTool``; it preserves ``__name__``/``__doc__``/signature via
``functools.wraps`` and — critically — preserves the function's sync/async
flavor, because the SDK selects worker-thread vs. await based on
``inspect.iscoroutinefunction``.
"""

from __future__ import annotations

import functools
import inspect
from typing import Callable

from .runtime import (
    ATTR_ERROR_CODE,
    SPAN_TOOL_EXECUTE,
    TelemetryRuntime,
    safe_span,
)

ERROR_CODE_TENANT_CONFIGURATION = "tenant_agent_configuration"


def tracer_for(runtime: TelemetryRuntime | None, name: str):
    """Return a tracer from ``runtime`` only when tracing is usable.

    Disabled/missing runtimes return ``None`` so callers take the zero-cost
    unwrapped path (original functions, no proxies, no headers).
    """
    if runtime is None or not runtime.enabled:
        return None
    return runtime.tracer(name)


async def traced_tool_execution(tracer, call: Callable[[], object]):
    """Await ``call()`` (and its awaitable result) inside a tool.execute span."""
    if tracer is None:
        result = call()
        if inspect.isawaitable(result):
            result = await result
        return result
    with safe_span(tracer, SPAN_TOOL_EXECUTE) as span:
        try:
            result = call()
            if inspect.isawaitable(result):
                result = await result
            return result
        except Exception as exc:
            # Lazy import: trpc_service.agent.errors pulls the agent package
            # __init__, which imports this module back (import cycle).
            from trpc_service.agent.errors import TenantAgentConfigurationError
            if span is not None and isinstance(exc, TenantAgentConfigurationError):
                try:
                    span.set_attribute(ATTR_ERROR_CODE, ERROR_CODE_TENANT_CONFIGURATION)
                except Exception:
                    pass
            raise


def _traced_tool_execution_sync(tracer, call: Callable[[], object]):
    """Sync sibling of :func:`traced_tool_execution` (tool bodies are sync)."""
    if tracer is None:
        return call()
    with safe_span(tracer, SPAN_TOOL_EXECUTE):
        return call()


def traced_tool_function(tracer, func):
    """Wrap ``func`` in the shared tool.execute boundary, SDK-compatible.

    ``tracer is None`` returns ``func`` unchanged.  Otherwise the returned
    callable keeps ``func``'s name/doc/signature and its sync or async flavor
    (so ``FunctionTool`` builds the same declaration and the SDK keeps the
    same threading choice), while every invocation creates one
    ``tool.execute`` span.
    """
    if tracer is None:
        return func
    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def _async_wrapper(*args, **kwargs):
            return await traced_tool_execution(tracer, lambda: func(*args, **kwargs))

        return _async_wrapper

    @functools.wraps(func)
    def _sync_wrapper(*args, **kwargs):
        return _traced_tool_execution_sync(tracer, lambda: func(*args, **kwargs))

    return _sync_wrapper


__all__ = [
    "ERROR_CODE_TENANT_CONFIGURATION",
    "traced_tool_execution",
    "traced_tool_function",
    "tracer_for",
]
