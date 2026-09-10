"""Worker entry point shared by inline and future queued execution."""

from __future__ import annotations

from trpc_service.agent.execution import AgentExecutionService, AgentReply, RunAgentCommand


class WorkerService:
    def __init__(self, execution: AgentExecutionService) -> None:
        self._execution = execution

    async def run(self, command: RunAgentCommand) -> AgentReply:
        return await self._execution.execute(command)


__all__ = ["WorkerService"]
