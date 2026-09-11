"""Explicit approved-execution boundary (Stage 6A2, Codex P1-4).

`func(**args)` on an arbitrary callable is NOT an acceptable execution
mechanism for potentially dangerous tools.  :class:`ApprovedToolExecutor`
is the single, deliberate boundary through which an approved tool runs:

- only names present in the static originals whitelist resolve at all;
- arguments are validated against the function signature BEFORE any call
  (missing required, unknown extras, non-mapping payload => rejected, and
  the function is never invoked);
- functions declaring a ``tool_context`` parameter are explicitly NOT
  supported in this stage (the SDK InvocationContext cannot be reconstructed
  outside a live turn) — they are rejected, not executed;
- synchronous functions run on a worker thread via ``asyncio.to_thread`` so
  a slow tool can never block the event loop; coroutine functions are
  awaited directly.

Any rejection raises the fixed TenantAgentConfigurationError; the caller
must fail-closed the approval, never retry across endpoints.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from typing import Any, Callable

from trpc_service.agent.errors import TenantAgentConfigurationError

_UNSUPPORTED_PARAMS = frozenset({"tool_context"})


class ApprovedToolExecutor:

    def __init__(self, originals: Mapping[str, Callable[..., Any]], *, tracer: object | None = None) -> None:
        self._originals: dict[str, Callable[..., Any]] = dict(originals)
        self._tracer = tracer

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._originals)

    async def execute(self, name: str, args: Any) -> Any:
        """Run one approved tool invocation.

        When a tracer was injected, the whole operation (validation included)
        runs inside the shared ``tool.execute`` span boundary — rejection and
        execution alike, with fixed safe attributes only.
        """
        if self._tracer is None:
            return await self._execute(name, args)
        # Local import: keeps the telemetry package out of this module's
        # static import cycle (agent -> registry -> executor).
        from trpc_service.telemetry.tool import traced_tool_execution
        return await traced_tool_execution(self._tracer, lambda: self._execute(name, args))

    async def _execute(self, name: str, args: Any) -> Any:
        func = self._originals.get(name) if isinstance(name, str) else None
        if func is None or not callable(func) or not isinstance(args, Mapping):
            raise TenantAgentConfigurationError()
        kwargs = self._validate(func, dict(args))
        if inspect.iscoroutinefunction(func):
            return await func(**kwargs)
        return await asyncio.to_thread(func, **kwargs)

    @staticmethod
    def _validate(func: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
        try:
            signature = inspect.signature(func)
        except (TypeError, ValueError):
            raise TenantAgentConfigurationError() from None
        params = signature.parameters
        accepts_var_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        for unsupported in _UNSUPPORTED_PARAMS:
            if unsupported in params:
                # Tools needing the live InvocationContext are not supported
                # by the approved-execution boundary (6A2).
                raise TenantAgentConfigurationError()
        for key in kwargs:
            if key not in params and not accepts_var_kwargs:
                raise TenantAgentConfigurationError()
        try:
            signature.bind(**kwargs)
        except TypeError:
            raise TenantAgentConfigurationError() from None
        return kwargs


__all__ = ["ApprovedToolExecutor"]
