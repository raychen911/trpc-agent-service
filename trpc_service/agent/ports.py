"""Abstract stages implemented around the stable Agent execution pipeline."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Protocol
from uuid import UUID

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentExecutionClaim,
    AgentExecutionReceipt,
    AgentExecutionRequest,
    AgentRuntimeConfig,
    AgentRunResult,
    AgentTaskClaim,
    AgentTaskSnapshot,
    AgentToolCall,
    AgentToolResult,
    PolicyDecision,
)
from trpc_service.tenant.context import TenantContext


class AgentTaskQueue(ABC):
    """Durable dispatch boundary shared by Gateway and Worker processes."""

    @abstractmethod
    async def enqueue(self, request: AgentExecutionRequest) -> str:
        """Persist a normalized callback before acknowledging its provider."""

        ...

    @abstractmethod
    async def claim(self, worker_id: str, *, lease_until: datetime) -> AgentTaskClaim | None:
        """Lease the oldest due task without requiring a sticky Session."""

        ...

    @abstractmethod
    async def renew(
        self,
        task_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        lease_until: datetime,
    ) -> bool:
        """Extend only a lease still owned by the calling Worker."""

        ...

    @abstractmethod
    async def complete(self, task_id: str, *, worker_id: str, fencing_token: int) -> None:
        """Record terminal success after the pipeline and delivery commit."""

        ...

    @abstractmethod
    async def fail(
        self,
        task_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
    ) -> None:
        """Release retryable work or record a safe permanent failure."""

        ...

    @abstractmethod
    async def get(
        self,
        context: TenantContext,
        binding_id: UUID,
        external_message_id: str,
    ) -> AgentTaskSnapshot | None:
        """Read task state inside the supplied Tenant and Agent boundary."""

        ...


class AgentConfigProvider(ABC):
    """Load the immutable configuration version named by a request."""

    @abstractmethod
    async def load(self, request: AgentExecutionRequest) -> AgentRuntimeConfig:
        """Load Tenant, Agent, model, Tool, policy and backend configuration."""

        ...


class AgentExecutionCoordinator(ABC):
    """Own Inbox idempotency, execution leases and retry state transitions."""

    @abstractmethod
    async def begin(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> AgentExecutionClaim:
        """Claim new work or return the durable receipt of a completed request."""

        ...

    @abstractmethod
    async def reject(
        self,
        claim: AgentExecutionClaim,
        decision: PolicyDecision,
    ) -> AgentExecutionReceipt:
        """Atomically persist rejection, Inbox state and Audit/Reply Outbox."""

        ...

    @abstractmethod
    async def fail(self, claim: AgentExecutionClaim, error: Exception) -> None:
        """Persist retryable or terminal failure state without claiming success."""

        ...

    @property
    def lease_renewal_interval_seconds(self) -> float | None:
        """Return a renewal interval, or None for coordinators without leases."""

        return None

    async def renew(self, claim: AgentExecutionClaim) -> bool:
        """Renew a claimed execution; non-leased coordinators need no action."""

        del claim
        return True


class AgentPolicyEngine(ABC):
    """Apply tenant permissions, budgets, redaction and approval rules."""

    @abstractmethod
    async def evaluate(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> PolicyDecision:
        """Return allow, deny or review before invoking the Runner."""

        ...


class AgentContextBuilder(ABC):
    """Build Runner context from Session, Memory, Summary and Knowledge ports."""

    @abstractmethod
    async def build(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        policy: PolicyDecision,
        claim: AgentExecutionClaim,
    ) -> AgentExecutionContext:
        """Load context and apply the approved governance transformations."""

        ...


class AgentRunner(Protocol):
    """Structural contract used by the pipeline and its observability wrapper.

    The production implementation is :class:`TRPCAgentRunner`.  This remains a
    protocol instead of an abstract base class so the tRPC adapter does not need
    to inherit a project-owned runtime hierarchy merely to expose ``run``.
    """

    async def run(
        self,
        context: AgentExecutionContext,
        tools: "AgentToolInvoker",
    ) -> AgentRunResult:
        """Invoke model, Tool, MCP, Skill and RAG implementations as configured."""

        ...


class AgentToolInvoker(ABC):
    """Govern every Tool/MCP call before delegating to its concrete adapter."""

    @abstractmethod
    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        """Apply whitelist, parameter, approval, ledger and audit policies."""

        ...


class AgentOutputFilter(ABC):
    """Apply tenant output safety and redaction before durable commit."""

    @abstractmethod
    async def apply(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentRunResult:
        """Return the approved result or raise before any success commit."""

        ...


class AgentResultCommitter(ABC):
    """Atomically persist execution facts and every required Outbox message."""

    @abstractmethod
    async def commit(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentExecutionReceipt:
        """Commit Session, Inbox, checkpoint and Outbox or fail as one unit."""

        ...


class AgentResultPublisher(ABC):
    """Notify downstream workers that committed Outbox work is available."""

    @abstractmethod
    async def publish(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        receipt: AgentExecutionReceipt,
    ) -> None:
        """Idempotently wake downstream workers for one durable receipt."""

        ...
