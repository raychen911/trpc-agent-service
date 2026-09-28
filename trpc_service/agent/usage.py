"""Durable usage accounting port kept separate from operational metrics."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentExecutionRequest,
    AgentRunResult,
    AgentRuntimeConfig,
)


class UsageRecorder(ABC):
    """Persist one idempotent business usage fact for a completed model turn."""

    @abstractmethod
    async def record(self, context: AgentExecutionContext, result: AgentRunResult) -> None:
        """Record usage using the logical request ID as the idempotency key."""

    async def release(self, context: AgentExecutionContext) -> None:
        """Release a prior budget reservation after a failed model call."""

        await self.release_request(context.request)

    async def release_request(self, request: AgentExecutionRequest) -> None:
        """Release by immutable request when no execution context exists yet."""

        del request


@dataclass(frozen=True, slots=True)
class UsageTotals:
    """Aggregated usage within one policy window."""

    calls: int
    total_tokens: int


class UsageReader(ABC):
    """Read tenant usage for runtime budget decisions."""

    @abstractmethod
    async def totals(self, tenant_id: UUID, *, since: datetime) -> UsageTotals:
        """Return calls and tokens recorded since the inclusive UTC boundary."""

    async def reserve(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        *,
        since: datetime,
        daily_calls: int | None,
        daily_tokens: int | None,
    ) -> str | None:
        """Reserve one turn, returning a stable denial code when exhausted.

        Non-database test readers retain a deterministic aggregate fallback.
        Production implementations must override this method atomically.
        """

        totals = await self.totals(request.tenant.tenant_id, since=since)
        if daily_calls is not None and totals.calls >= daily_calls:
            return "DAILY_CALL_BUDGET_EXCEEDED"
        reservation = token_reservation(request, config)
        if daily_tokens is not None and totals.total_tokens + reservation > daily_tokens:
            return "DAILY_TOKEN_BUDGET_EXCEEDED"
        return None


def token_reservation(
    request: AgentExecutionRequest,
    config: AgentRuntimeConfig,
) -> int:
    """Conservatively reserve prompt estimate plus configured maximum output."""

    context_window = config.model.get("context_window_tokens")
    if (isinstance(context_window, int) and not isinstance(context_window, bool)
            and context_window > 0):
        # Reserving the trusted provider context limit is the only safe upper
        # bound before the SDK composes instructions, history and Tool schemas.
        return context_window
    configured = config.model.get("max_output_tokens", 0)
    maximum_output = (configured if isinstance(configured, int)
                      and not isinstance(configured, bool) and configured > 0 else 0)
    # A byte-level upper bound is intentionally stricter than the common
    # chars/4 estimate and remains safe for Chinese and tokenizer fallbacks.
    prompt_bytes = len((request.incoming.text or "").encode("utf-8"))
    estimated_input = max(1, prompt_bytes)
    return estimated_input + maximum_output
