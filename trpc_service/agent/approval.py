"""Durable human approval state shared by every Worker and IM connector."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
import json
import secrets
from uuid import UUID, uuid4

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentToolCall,
)


class ApprovalStatus(StrEnum):
    """Stable lifecycle of one high-risk capability authorization."""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    EXECUTING = "EXECUTING"
    EXECUTED = "EXECUTED"
    UNKNOWN = "UNKNOWN"


class ApprovalDecision(StrEnum):
    """Decisions accepted from a trusted Channel confirmation."""

    APPROVE = "approve"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class ApprovalRequestCreate:
    """Immutable approval facts produced before any dangerous side effect."""

    approval_id: UUID
    short_code: str
    tenant_id: UUID
    agent_app_id: UUID
    binding_id: UUID
    principal_id: str
    session_id: str
    tool_call_id: str
    capability_kind: str
    capability_name: str
    action: str
    resource: str | None
    arguments_hash: str
    risk_level: int
    expires_at: datetime
    config_version: int = 1
    logical_call_index: int = 0
    artifact_refs: tuple[str, ...] = ()
    operation_arguments: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ApprovalRequestSnapshot:
    """Safe state returned to runtime and Channel adapters."""

    approval_id: UUID
    short_code: str
    tenant_id: UUID
    agent_app_id: UUID
    binding_id: UUID
    principal_id: str
    session_id: str
    tool_call_id: str
    capability_kind: str
    capability_name: str
    action: str
    resource: str | None
    arguments_hash: str
    risk_level: int
    status: ApprovalStatus
    expires_at: datetime
    config_version: int = 1
    logical_call_index: int = 0
    artifact_refs: tuple[str, ...] = ()
    operation_arguments: dict[str, object] = field(default_factory=dict)
    decided_by: str | None = None


class ApprovalStore(ABC):
    """Persistence port for cross-node approval ownership transitions."""

    @abstractmethod
    async def create_or_get(
        self,
        request: ApprovalRequestCreate,
    ) -> ApprovalRequestSnapshot:
        """Create an idempotent pending request for one logical Tool call."""

    @abstractmethod
    async def decide(
        self,
        *,
        short_code: str,
        tenant_id: UUID,
        agent_app_id: UUID,
        principal_id: str,
        session_id: str,
        decision: ApprovalDecision,
        now: datetime,
    ) -> ApprovalRequestSnapshot:
        """Apply exactly one decision from the original requester."""

    @abstractmethod
    async def claim_execution(
        self,
        *,
        approval_id: UUID,
        tenant_id: UUID,
        agent_app_id: UUID,
        principal_id: str,
        session_id: str,
        arguments_hash: str,
        now: datetime,
    ) -> ApprovalRequestSnapshot:
        """Atomically move one approved request to EXECUTING."""

    @abstractmethod
    async def complete_execution(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        """Record a known successful side effect."""

    @abstractmethod
    async def mark_unknown(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        """Prevent retries when an executing side effect has no known result."""

    @abstractmethod
    async def get(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        """Read one safe approval state by its internal identifier."""


class ApprovalService:
    """Bind human decisions to an exact tenant, caller, session, and operation."""

    def __init__(self, store: ApprovalStore, *, ttl_seconds: int = 600) -> None:
        if ttl_seconds < 30:
            raise ValueError("approval TTL must be at least 30 seconds")
        self._store = store
        self._ttl_seconds = ttl_seconds

    @staticmethod
    def _bound_artifact_refs(context: AgentExecutionContext) -> tuple[str, ...]:
        """Bind any request attachments to the exact approved operation."""

        return context.request.incoming.artifact_refs

    @staticmethod
    def _arguments_hash(context: AgentExecutionContext, call: AgentToolCall) -> str:
        payload = {
            "tenant_id": str(context.request.tenant.tenant_id),
            "agent_app_id": str(context.request.tenant.agent_app_id),
            "principal_id": context.request.incoming.principal_id,
            "session_id": context.request.session_id,
            "config_version": context.request.tenant.config_version,
            "kind": call.kind.value,
            "name": call.name,
            "action": call.action,
            "resource": call.resource,
            "arguments": call.arguments,
            # Attachments are trusted request context and therefore participate
            # in the approval hash even though raw Tool arguments are not stored.
            "artifact_refs": list(ApprovalService._bound_artifact_refs(context)),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    async def request(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        *,
        risk_level: int,
    ) -> ApprovalRequestSnapshot:
        """Create a short-lived approval without storing raw Tool arguments."""

        if risk_level not in {2, 3}:
            raise ValueError("only L2/L3 capabilities require human approval")
        now = datetime.now(timezone.utc)
        return await self._store.create_or_get(
            ApprovalRequestCreate(
                approval_id=uuid4(),
                short_code=secrets.token_hex(4).upper(),
                tenant_id=context.request.tenant.tenant_id,
                agent_app_id=context.request.tenant.agent_app_id,
                binding_id=context.request.channel.binding_id,
                principal_id=context.request.incoming.principal_id,
                session_id=context.request.session_id,
                tool_call_id=call.call_id,
                capability_kind=call.kind.value,
                capability_name=call.name,
                action=call.action,
                resource=call.resource,
                arguments_hash=self._arguments_hash(context, call),
                risk_level=risk_level,
                expires_at=now + timedelta(seconds=self._ttl_seconds),
                config_version=context.request.tenant.config_version,
                logical_call_index=call.logical_call_index,
                artifact_refs=self._bound_artifact_refs(context),
                # The approval subsystem never persists raw model arguments.
                operation_arguments={},
            ))

    async def decide(
        self,
        *,
        short_code: str,
        tenant_id: UUID,
        agent_app_id: UUID,
        principal_id: str,
        session_id: str,
        decision: ApprovalDecision,
    ) -> ApprovalRequestSnapshot:
        """Approve or reject from the same identity and conversation scope."""

        return await self._store.decide(
            short_code=short_code.strip().upper(),
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            principal_id=principal_id,
            session_id=session_id,
            decision=decision,
            now=datetime.now(timezone.utc),
        )

    async def claim_execution(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> ApprovalRequestSnapshot:
        """Consume trusted approval evidence attached by a Channel service."""

        raw_approval_id = context.attributes.get("approval_id")
        if not isinstance(raw_approval_id, str):
            raise PermissionError("capability execution has no trusted approval evidence")
        try:
            approval_id = UUID(raw_approval_id)
        except ValueError as error:
            raise PermissionError("capability approval evidence is invalid") from error
        return await self._store.claim_execution(
            approval_id=approval_id,
            tenant_id=context.request.tenant.tenant_id,
            agent_app_id=context.request.tenant.agent_app_id,
            principal_id=context.request.incoming.principal_id,
            session_id=context.request.session_id,
            arguments_hash=self._arguments_hash(context, call),
            now=datetime.now(timezone.utc),
        )

    async def complete_execution(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        """Mark the exact claimed operation as successfully executed."""

        return await self._store.complete_execution(approval_id)

    async def mark_unknown(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        """Quarantine an operation whose external outcome cannot be proven."""

        return await self._store.mark_unknown(approval_id)

    async def get(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        """Return approval state for runtime recovery and management views."""

        return await self._store.get(approval_id)
