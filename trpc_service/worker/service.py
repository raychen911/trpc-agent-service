"""Adapter around the official tRPC-Agent Runner."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent._deterministic_validation_model import DeterministicValidationModel
from trpc_service.storage.contracts import AccessDenied, AgentExecutionFailed, OutcomeUnknown
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.tenant.models import VerifiedTenantContext
from trpc_service.tenant.session_identity import SessionIdentity, assert_session_ownership


@dataclass(frozen=True, slots=True)
class AgentExecution:
    final_text: str
    event_count: int
    final_response_count: int


class PreparedAgentRun:
    def __init__(self, runner: Runner, identity: SessionIdentity, text: str) -> None:
        self._runner = runner
        self._identity = identity
        self._text = text
        self.execution_started = False

    async def execute(self, timeout_seconds: float = 30) -> AgentExecution:
        self.execution_started = True
        message = Content(role="user", parts=[Part.from_text(text=self._text)])

        async def collect() -> list[object]:
            return [
                event
                async for event in self._runner.run_async(
                    user_id=self._identity.sdk_user_id,
                    session_id=self._identity.platform_session_id,
                    new_message=message,
                )
            ]

        try:
            events = await asyncio.wait_for(collect(), timeout=timeout_seconds)
        except (TimeoutError, asyncio.CancelledError):
            raise OutcomeUnknown("Agent outcome is unknown.") from None
        except Exception:
            raise AgentExecutionFailed("Agent execution failed.") from None
        visible = [event for event in events if getattr(event, "visible", False)]
        finals = [event for event in visible if event.is_final_response() and (event.get_text() or "").strip()]
        if len(finals) != 1:
            raise AgentExecutionFailed("Agent execution failed.")
        return AgentExecution((finals[0].get_text() or "").strip(), len(visible), len(finals))


class AgentExecutor:
    def __init__(self, backends: SessionBackendFactory) -> None:
        self.backends = backends
        # Snapshot-aware Runner cache: one Runner per
        # (tenant, agent, config snapshot) so a pinned execution never
        # reuses a Runner built from a different snapshot (FR-020).
        self._runners: dict[tuple[str, str, str], Runner] = {}
        self._models: dict[tuple[str, str, str], DeterministicValidationModel] = {}
        self.external_model_calls = 0

    async def prepare(
        self,
        context: VerifiedTenantContext,
        identity: SessionIdentity,
        text: str,
        *,
        snapshot_id: str | None = None,
    ) -> PreparedAgentRun:
        try:
            assert_session_ownership(context, identity)
        except ValueError:
            raise AccessDenied("Session ownership mismatch.")
        key = (context.tenant_id, context.agent_id, snapshot_id or "")
        backend_key = (context.tenant_id, context.agent_id)
        if key not in self._runners:
            model = DeterministicValidationModel()
            agent = LlmAgent(
                name="agent_" + __import__("hashlib").sha256("|".join(key).encode()).hexdigest()[:16],
                model=model,
                instruction="Perform only the deterministic local validation exchange.",
            )
            self._models[key] = model
            self._runners[key] = Runner(
                app_name=identity.sdk_app_name,
                agent=agent,
                session_service=self.backends.get_backend(*backend_key),
                enable_post_turn_processing=False,
            )
        return PreparedAgentRun(self._runners[key], identity, text)

    @property
    def call_count(self) -> int:
        return sum(model.call_count for model in self._models.values())

    async def close(self) -> None:
        for runner in self._runners.values():
            await runner.close()
        self._runners.clear()
        self._models.clear()
        await self.backends.close()
