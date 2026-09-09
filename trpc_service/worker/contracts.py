# mypy: disable-error-code="import-untyped"
"""Narrow, testable contracts for the claim-bound Worker data plane.

The Worker depends on capabilities instead of a concrete SQL repository.  This is
deliberate: the production :class:`ReliabilityRepository` satisfies these protocols,
while tests can model races without weakening the fencing contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content

from trpc_service.agent.runtime import TurnResult
from trpc_service.reliability.types import (
    AuditData,
    ClaimInput,
    CommittedSessionView,
    EventAppend,
    EventData,
    FinalizeResult,
    ReplyPart,
    SessionClaim,
)
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import AgentAppSpec, TenantSpec


class WorkerPort(Protocol):
    """Durability operations required by one Worker turn."""

    async def claim_next(
        self,
        tenant_id: str,
        worker_id: str,
        *,
        lease_ttl: timedelta,
    ) -> SessionClaim | None:
        """Claim the next ordered Inbox item for one tenant."""

    async def load_claim_input(self, claim: SessionClaim) -> ClaimInput:
        """Load normalized input after revalidating the live claim."""

    async def load_committed_session(
        self,
        claim: SessionClaim,
    ) -> CommittedSessionView:
        """Load the committed replay view, excluding staged/aborted attempts."""

    async def renew_lease(
        self,
        claim: SessionClaim,
        *,
        lease_ttl: timedelta,
    ) -> bool:
        """Renew only if the same worker and fencing token still own the lease."""

    async def append_event_cas(
        self,
        claim: SessionClaim,
        expected_version: int,
        event: EventData,
    ) -> EventAppend:
        """Append under both an optimistic version and a fencing capability."""

    async def abort_staged_events(self, claim: SessionClaim) -> int:
        """Make this attempt's unpublished events permanently invisible."""

    async def defer_run_retry(
        self,
        claim: SessionClaim,
        *,
        next_attempt_at: datetime,
        error_type: str,
    ) -> None:
        """Atomically abort, schedule retry, and release the current lease."""

    async def finalize_run(
        self,
        claim: SessionClaim,
        *,
        final_state: dict[str, Any],
        final_event_id: str | None,
        reply_parts: tuple[ReplyPart, ...],
        audit: AuditData,
    ) -> FinalizeResult:
        """Atomically commit events, state, Outbox records, and Audit."""


@dataclass(frozen=True, slots=True)
class EventCodecContext:
    """Authenticated context for an encrypted, externally stored SDK event."""

    tenant_id: str
    session_id: str
    event_id: str
    seq: int


class EncryptedEventCodec(Protocol):
    """Persist and recover complete SDK events behind opaque encrypted references.

    Implementations must use authenticated encryption bound to every field in
    :class:`EventCodecContext`.  ``seal`` returns an opaque reference only; it must
    never return plaintext JSON, a raw prompt, tool arguments, or a bearer URL.
    ``open`` must fail closed on missing objects, context mismatch, or authentication
    failure, without placing plaintext in exception messages.
    """

    async def seal(self, event: Event, *, context: EventCodecContext) -> str:
        """Encrypt/store a normalized non-partial event and return its opaque ref."""

    async def open(self, content_ref: str, *, context: EventCodecContext) -> Event:
        """Authenticate/decrypt one complete event without logging its content."""


class TenantSpecLoader(Protocol):
    """Load the tenant configuration snapshot used to resolve a claimed turn."""

    async def load_revision(self, tenant_id: str, revision: int) -> TenantSpec:
        """Return the exact immutable revision pinned at Inbox acceptance."""


@dataclass(frozen=True, slots=True)
class ResolvedTenantTurn:
    """Trusted configuration and identity snapshot for one claimed execution."""

    tenant_context: TenantContext
    app: AgentAppSpec = field(repr=False)
    channel: str
    config_revision: int
    policy_revision: int
    approved_tools: frozenset[str] = field(default_factory=frozenset)
    app_state: dict[str, Any] = field(default_factory=dict, repr=False)
    user_state: dict[str, Any] = field(default_factory=dict, repr=False)


class TenantTurnResolver(Protocol):
    """Resolve an immutable tenant/app/binding view for a claimed Inbox."""

    async def resolve(self, claim_input: ClaimInput) -> ResolvedTenantTurn:
        """Return a view whose identity exactly matches the durable claim input."""


class TurnExecutor(Protocol):
    """Per-turn Agent executor implemented by ``TenantAgentRunner`` or a test fake."""

    async def run_turn(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        new_message: str | Content | list[Content],
        run_id: str,
        in_reply_to_delivery_id: str,
        attempt_no: int,
        approved_tools: frozenset[str],
        timeout_seconds: float | None = None,
    ) -> TurnResult:
        """Run and fully consume exactly one Agent turn."""


class TurnExecutorFactory(Protocol):
    """Create an executor bound to a fresh, claim-scoped SessionService."""

    def app_name_for(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        approved_tools: frozenset[str],
    ) -> str:
        """Return the exact SDK app name the executor will pass to the service."""

    def create(
        self,
        *,
        session_service: BaseSessionService,
    ) -> TurnExecutor:
        """Return an executor that does not close the supplied shared dependencies."""


class InboundMessageDecoder(Protocol):
    """Convert the normalized Inbox payload into a safe SDK user input."""

    def decode(self, claim_input: ClaimInput) -> str | Content | list[Content]:
        """Decode trusted normalized content; never consume raw webhook bytes."""


class FailureClass(StrEnum):
    """Operational action for a sanitized execution failure."""

    LOST_CLAIM = "lost_claim"
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


class FailureClassifier(Protocol):
    """Classify errors without inspecting or returning secret-bearing messages."""

    def classify(self, error: Exception) -> FailureClass:
        """Return the safe operational class for ``error``."""
