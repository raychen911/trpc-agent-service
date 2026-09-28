"""Concrete Agent adapters with cycle-safe public imports.

Keeping this package initializer free of eager re-exports is intentional:
``mcp.service`` imports the lightweight Tool bridge, while the tRPC Runner
imports the MCP service. Importing both implementations here would turn that
valid dependency direction into a package-initialization cycle.
"""

__all__ = ["TRPCAgentRunner", "TRPCToolBridge"]


def __getattr__(name: str) -> object:
    """Resolve compatibility exports only when a caller requests them."""

    if name == "TRPCAgentRunner":
        from trpc_service.agent.adapters.trpc import TRPCAgentRunner

        return TRPCAgentRunner
    if name == "TRPCToolBridge":
        from trpc_service.agent.adapters.trpc_tools import TRPCToolBridge

        return TRPCToolBridge
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
