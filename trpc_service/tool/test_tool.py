"""A deterministic, side-effect-free tool reserved for fault-stage tests."""

from __future__ import annotations

import asyncio

from trpc_agent_sdk.tools import BaseTool, FunctionTool

from trpc_service.config.settings import Environment

DETERMINISTIC_FAULT_TOOL_NAME = "deterministic_fault_stage_probe"
_MAX_DELAY_SECONDS = 0.2


def build_fault_stage_test_tools(
    *,
    environment: Environment,
    fault_injection_enabled: bool,
    delay_seconds: float = 0.0,
) -> dict[str, BaseTool]:
    """Return the test tool only for an explicitly enabled test runtime.

    The returned callable accepts no arguments and always returns the same
    content-free value.  In particular, it cannot receive or echo a user
    message.  Any non-test attempt to enable this registry fails closed.
    """

    if environment != Environment.TEST:
        if fault_injection_enabled:
            raise ValueError("deterministic fault tools are restricted to test environment")
        return {}
    if not fault_injection_enabled:
        return {}
    if not 0 <= delay_seconds <= _MAX_DELAY_SECONDS:
        raise ValueError(f"test tool delay must be between 0 and {_MAX_DELAY_SECONDS:g} seconds")

    async def deterministic_fault_stage_probe() -> dict[str, str]:
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        return {"status": "ok", "result": "deterministic"}

    deterministic_fault_stage_probe.__name__ = DETERMINISTIC_FAULT_TOOL_NAME
    deterministic_fault_stage_probe.__doc__ = (
        "Return a fixed response for a controlled TOOL-stage fault test."
    )
    tool = FunctionTool(deterministic_fault_stage_probe)
    return {DETERMINISTIC_FAULT_TOOL_NAME: tool}


__all__ = ["DETERMINISTIC_FAULT_TOOL_NAME", "build_fault_stage_test_tools"]
