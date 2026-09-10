"""Execution bus boundary between gateways and workers."""

from __future__ import annotations

from typing import Protocol

from trpc_service.agent.execution import AgentReply, RunAgentCommand
from trpc_service.worker.service import WorkerService


class ExecutionBus(Protocol):
    async def submit(self, command: RunAgentCommand) -> AgentReply: ...


class InlineExecutionBus:
    """Execute commands in the current process for the small-scale deployment."""

    def __init__(self, worker: WorkerService) -> None:
        self._worker = worker

    async def submit(self, command: RunAgentCommand) -> AgentReply:
        return await self._worker.run(command)


__all__ = ["ExecutionBus", "InlineExecutionBus"]
