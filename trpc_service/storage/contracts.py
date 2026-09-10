"""Replaceable repository and adapter ports for the local message flow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.tenant.models import ResourceStatus, VerifiedTenantContext
from trpc_service.storage.data_models import DataScope


class PlatformPortError(RuntimeError):
    """Stable base error translated at the gateway boundary."""


class DataDomainError(PlatformPortError):
    """Safe, provider-neutral error returned by phase-seven data ports."""

    code = "data_error"
    retryable = False

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code.replace("_", " ").capitalize() + ".")


class InvalidRequest(PlatformPortError):
    pass


class Unauthorized(PlatformPortError):
    pass


class AccessDenied(PlatformPortError):
    pass


class NotFound(PlatformPortError):
    pass


class SecretUnavailable(PlatformPortError):
    pass


class IdempotencyConflict(DataDomainError):
    code = "idempotency_conflict"
    retryable = False


class Processing(PlatformPortError):
    pass


class AgentPreparationFailed(PlatformPortError):
    pass


class AgentExecutionFailed(PlatformPortError):
    pass


class OutcomeUnknown(PlatformPortError):
    pass


class AuditUnavailable(DataDomainError):
    code = "audit_unavailable"
    retryable = True


class AuditIncomplete(PlatformPortError):
    pass


class ConditionalWriteFailed(PlatformPortError):
    pass


class ConfigurationUnavailable(PlatformPortError):
    def __init__(self, message: str = "Configuration is unavailable.") -> None:
        super().__init__(message)


class StateBackendUnavailable(DataDomainError):
    code = "state_backend_unavailable"
    retryable = True

    def __init__(self, message: str = "Shared state is unavailable.") -> None:
        super().__init__(message)


class SessionBusy(PlatformPortError):
    pass


class SessionQuarantined(PlatformPortError):
    pass


class LeaseLost(PlatformPortError):
    pass


class LeaseBusy(PlatformPortError):
    pass


class StaleFence(DataDomainError):
    code = "stale_fence"
    retryable = False


class RecoveryConflict(PlatformPortError):
    pass


# Phase-seven stable domain errors.  They intentionally expose only a safe
# code/retryability pair; adapter-specific details stay out of the envelope.
class TenantScopeInvalid(DataDomainError):
    code = "tenant_scope_invalid"
    retryable = False


class SequenceGap(DataDomainError):
    code = "sequence_gap"
    retryable = True


class VersionConflict(DataDomainError):
    code = "version_conflict"
    retryable = True


class SummaryConflict(DataDomainError):
    code = "summary_conflict"
    retryable = False


class ContentTooLarge(DataDomainError):
    code = "content_too_large"
    retryable = False


class DigestMismatch(DataDomainError):
    code = "digest_mismatch"
    retryable = False


class TenantFilterUnsupported(DataDomainError):
    code = "tenant_filter_unsupported"
    retryable = False


class MigrationWritePaused(DataDomainError):
    code = "migration_write_paused"
    retryable = True


class MigrationConflict(DataDomainError):
    code = "migration_conflict"
    retryable = True


class ForwardRepairRequired(DataDomainError):
    code = "forward_repair_required"
    retryable = False


@dataclass(frozen=True, slots=True)
class ResolvedChannelBinding:
    scope: VerifiedBindingScope
    context: VerifiedTenantContext
    secret_ref: str
    config_version: int


@dataclass(frozen=True, slots=True)
class BindingAuthMaterial:
    binding_id: str
    secret_ref: str
    signature_version: str
    status: ResourceStatus


class SecretBytes:
    """Short-lived secret wrapper that cannot be serialized or printed."""

    __slots__ = ("__value",)

    def __init__(self, value: bytes) -> None:
        if not value:
            raise ValueError("secret must not be empty")
        self.__value = bytes(value)

    def reveal(self) -> bytes:
        return self.__value

    def __repr__(self) -> str:
        return "SecretBytes(<redacted>)"

    def __reduce__(self) -> object:
        raise TypeError("SecretBytes cannot be serialized")


@runtime_checkable
class BindingAuthRegistry(Protocol):
    async def get_auth_material(self, binding_id: str, channel: Channel) -> BindingAuthMaterial: ...


@runtime_checkable
class SecretResolver(Protocol):
    def resolve(self, secret_ref: str) -> SecretBytes: ...


@runtime_checkable
class TenantDirectory(Protocol):
    async def resolve_active_context(
        self,
        scope: VerifiedBindingScope,
        *,
        external_user_id: str,
        trace_id: Any,
    ) -> VerifiedTenantContext: ...


@runtime_checkable
class IdempotencyRepository(Protocol):
    async def claim(self, key: Any, fingerprint: str, trace_id: UUID, now: datetime) -> Any: ...
    async def mark_running(self, key: Any, owner_token: str, execution_trace_id: UUID, now: datetime) -> Any: ...
    async def mark_pre_start_failed(self, key: Any, owner_token: str, safe_error: str, now: datetime) -> Any: ...
    async def complete(self, key: Any, owner_token: str, result: Any, now: datetime) -> Any: ...
    async def mark_post_start_failed(self, key: Any, owner_token: str, result: Any, now: datetime) -> Any: ...
    async def mark_outcome_unknown(self, key: Any, owner_token: str, result: Any, now: datetime) -> Any: ...
    async def get(self, key: Any) -> Any: ...
    async def reset(self) -> None: ...


@runtime_checkable
class SessionLockManager(Protocol):
    def acquire(self, platform_session_id: str) -> Any: ...


@runtime_checkable
class SessionBackendFactory(Protocol):
    def get_backend(self, tenant_id: str, agent_id: str) -> Any: ...
    async def close(self) -> None: ...


@runtime_checkable
class AuditRepository(Protocol):
    async def append(self, scope: Any, record: Any, fence_proof: Any | None = None) -> Any: ...
    async def append_diagnostic(self, scope: Any, record: Any) -> Any: ...
    async def update_final(self, scope: Any, audit_id: UUID, trace_id: UUID, decision: Any, **fields: object) -> Any: ...
    async def list_by_trace(self, scope: Any, trace_id: UUID) -> list[Any]: ...
    async def list_by_session(self, scope: Any, platform_session_id: str) -> list[Any]: ...
    async def list_by_tenant(self, scope: Any) -> list[Any]: ...
    async def list_preauth(self, scope: Any) -> list[Any]: ...
    async def reset(self) -> None: ...


@runtime_checkable
class SessionLeaseManager(Protocol):
    async def acquire(
        self,
        tenant_scope: Any,
        agent_id: str,
        platform_session_id: str,
        message_key_digest: str,
        node_identity: Any,
        lease_ms: int,
        wait_ms: int,
    ) -> Any: ...


@runtime_checkable
class SharedSessionRepository(Protocol):
    async def get_session(self, session_identity: Any, session_fence: Any) -> Any: ...
    async def create_session(self, session_identity: Any, initial_state: Any, session_fence: Any) -> Any: ...
    async def append_event(self, session_identity: Any, event: Any, message_fence: Any, session_fence: Any) -> Any: ...
    async def update_session(self, session_identity: Any, expected_version: int, state_delta: Any, message_fence: Any, session_fence: Any) -> Any: ...


@runtime_checkable
class RecoveryRepository(Protocol):
    async def find_blocking(self, tenant_scope: Any, idempotency_key_digest: str) -> Any: ...
    async def get_pending(self, tenant_scope: Any, limit: int) -> list[Any]: ...
    async def mark_reconciled(self, tenant_scope: Any, recovery_id: UUID, expected_result_digest: str) -> Any: ...
    async def mark_conflict_review(self, tenant_scope: Any, recovery_id: UUID, safe_reason: str) -> Any: ...


@runtime_checkable
class GovernancePolicyRepository(Protocol):
    async def get_active(self, *, tenant_id: str, agent_name: str, binding_id: str) -> Any: ...
    async def create_version(self, *, tenant_id: str, scope: Any, document: Any, actor_digest: str) -> Any: ...
    async def activate(self, *, tenant_id: str, policy_id: str, expected_generation: int) -> Any: ...
    async def disable(self, *, tenant_id: str, policy_id: str, expected_generation: int) -> Any: ...


@runtime_checkable
class PrincipalGrantRepository(Protocol):
    async def evaluate(self, *, principal: Any, agent_name: str, binding_id: str, at: datetime) -> Any: ...
    async def put(self, grant: Any) -> Any: ...
    async def disable(self, *, tenant_id: str, grant_id: str, at: datetime) -> Any: ...


@runtime_checkable
class BudgetRepository(Protocol):
    async def reserve_maximum(self, *, tenant_id: str, execution_id: str, policy: Any, maximums: Any, owner_generation: int, trace_id: str) -> Any: ...
    async def mark_execution_started(self, *, tenant_id: str, execution_id: str, owner_generation: int) -> Any: ...
    async def settle(self, *, tenant_id: str, execution_id: str, actuals: Any, owner_generation: int) -> Any: ...
    async def release_before_execution(self, *, tenant_id: str, execution_id: str, owner_generation: int, reason: str) -> Any: ...
    async def get_by_execution(self, *, tenant_id: str, execution_id: str) -> Any: ...


@runtime_checkable
class PendingConfirmationRepository(Protocol):
    async def create_once(self, pending: Any) -> Any: ...
    async def claim(self, *, intent: Any, owner_node_id: str, owner_generation: int, now: datetime) -> Any: ...
    async def mark_executing(self, *, confirmation_id: str, claim_token: str, owner_generation: int) -> Any: ...
    async def complete(self, *, confirmation_id: str, claim_token: str, owner_generation: int, result_digest: str) -> Any: ...
    async def cancel(self, *, confirmation_id: str, reason: str) -> Any: ...


@runtime_checkable
class GovernanceRecoveryRepository(Protocol):
    async def record_stage(self, marker: Any) -> None: ...
    async def list_recoverable(self, *, before: datetime, limit: int) -> list[Any]: ...
    async def claim(self, *, marker_id: str, node_id: str, generation: int) -> Any: ...
    async def complete(self, *, marker_id: str, generation: int, outcome: str) -> None: ...


@runtime_checkable
class SessionEventRepository(Protocol):
    async def append(self, scope: DataScope, event: Any, *, expected_watermark: int) -> Any: ...
    async def list_metadata(self, scope: DataScope, session_key: str, *, after_sequence: int = 0, limit: int = 100) -> list[Any]: ...
    async def read_content(self, scope: DataScope, session_key: str, *, after_sequence: int = 0, limit: int = 100) -> list[Any]: ...
    async def get_watermark(self, scope: DataScope, session_key: str) -> int: ...


@runtime_checkable
class MemoryRepository(Protocol):
    async def compare_and_set(self, scope: DataScope, record: Any, *, expected_version: int | None) -> Any: ...
    async def get_metadata(self, scope: DataScope, namespace: str, key: str) -> Any: ...
    async def read_content(self, scope: DataScope, namespace: str, key: str) -> Any: ...


@runtime_checkable
class SummaryRepository(Protocol):
    async def compare_and_set(self, scope: DataScope, summary: Any, *, expected_version: int | None) -> Any: ...
    async def get_metadata(self, scope: DataScope, session_key: str) -> Any: ...
    async def read_content(self, scope: DataScope, session_key: str) -> Any: ...


@runtime_checkable
class ArtifactRepository(Protocol):
    async def publish(self, scope: DataScope, request: Any, *, expected_version: int | None) -> Any: ...
    async def get_metadata(self, scope: DataScope, artifact_id: str) -> Any: ...
    async def read_content(self, scope: DataScope, artifact_id: str) -> Any: ...
    async def collect_orphans(self, scope: DataScope, *, before: datetime, limit: int) -> Any: ...


@runtime_checkable
class KnowledgeRepository(Protocol):
    async def stage(self, scope: DataScope, document: Any, *, expected_version: int | None) -> Any: ...
    async def mark_indexed(self, scope: DataScope, document_id: str, digest: str, *, expected_version: int) -> Any: ...
    async def search(self, scope: DataScope, query: str, *, limit: int = 10) -> list[Any]: ...


@runtime_checkable
class MigrationRepository(Protocol):
    async def get(self, scope: DataScope, stream: str) -> Any: ...
    async def transition(self, scope: DataScope, stream: str, *, expected_state: str, expected_generation: int, target_state: str, fields: dict[str, Any] | None = None) -> Any: ...
    async def checkpoint(self, scope: DataScope, stream: str, *, expected_generation: int, copied_watermark: int, target_digest: str | None = None) -> Any: ...
    async def activate(self, scope: DataScope, stream: str, *, expected_generation: int, verified_watermark: int, verified_digest: str) -> Any: ...
    async def mark_first_authoritative_write(self, scope: DataScope, stream: str, *, expected_generation: int, transaction: Any = None) -> Any: ...


@runtime_checkable
class ObjectStorePort(Protocol):
    async def put_temporary(self, scope: DataScope, upload_id: str, content: bytes) -> Any: ...
    async def read(self, scope: DataScope, storage_ref: str) -> bytes: ...
    async def delete_temporary(self, scope: DataScope, storage_ref: str) -> Any: ...


@runtime_checkable
class VectorStorePort(Protocol):
    supports_tenant_prefilter: bool
    async def upsert(self, scope: DataScope, document_id: str, digest: str, vector: Any) -> None: ...
    async def search(self, scope: DataScope, query_vector: Any, limit: int) -> list[Any]: ...


@runtime_checkable
class DataUnitOfWork(Protocol):
    events: SessionEventRepository
    memories: MemoryRepository
    summaries: SummaryRepository
    audits: AuditRepository
    migrations: MigrationRepository

    async def __aenter__(self) -> "DataUnitOfWork": ...
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


@runtime_checkable
class DataUnitOfWorkFactory(Protocol):
    def __call__(self, scope: DataScope) -> DataUnitOfWork: ...


@runtime_checkable
class ChannelBindingRepository(Protocol):
    async def resolve_by_channel_identity(
        self,
        identity: Any,
        *,
        external_user_id: str,
        trace_id: UUID,
    ) -> ResolvedChannelBinding: ...


@runtime_checkable
class DeliveryRepository(Protocol):
    async def create_or_get(
        self,
        tenant_scope: Any,
        binding_scope: Any,
        execution_result: Any,
        reply_context: Any,
        adapter_fence: Any,
    ) -> Any: ...
    async def begin_attempt(
        self,
        tenant_scope: Any,
        delivery_id: UUID,
        expected_status: Any,
        adapter_fence: Any,
        trace_id: UUID,
    ) -> Any: ...
    async def finish_attempt(
        self,
        tenant_scope: Any,
        attempt_id: UUID,
        outcome: Any,
        safe_error_code: str | None,
        retry_delay_seconds: int | None,
        adapter_fence: Any,
    ) -> Any: ...
    async def get(self, tenant_scope: Any, delivery_id: UUID) -> Any: ...
    async def list_due(self, tenant_scope: Any, now: datetime, limit: int) -> list[Any]: ...


@runtime_checkable
class AdapterLeaseHandle(Protocol):
    async def renew(self, lease_ms: int) -> Any: ...
    async def mark_ready(self, runtime_bot_identity: Any) -> Any: ...
    async def release(self, reason: str) -> None: ...


@runtime_checkable
class AdapterOwnershipRepository(Protocol):
    async def acquire(self, identity_digest: str, node_id: str, lease_ms: int) -> Any: ...
    async def inspect(self, identity_digest: str) -> Any: ...


@runtime_checkable
class PlatformAdapters(BindingAuthRegistry, TenantDirectory, Protocol):
    audit: AuditRepository
    idempotency: IdempotencyRepository


@runtime_checkable
class AgentExecutorPort(Protocol):
    async def prepare(self, context: VerifiedTenantContext, identity: Any, text: str) -> Any: ...
    async def close(self) -> None: ...


__all__ = [name for name in globals() if not name.startswith("_")]
