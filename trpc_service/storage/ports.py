"""Capability-specific storage ports implemented by concrete backends."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from datetime import datetime

from trpc_service.storage.types import (
    ArtifactMetadata,
    ArtifactRef,
    AuditRecord,
    ExecutionClaim,
    ExecutionCommit,
    InboxClaimRequest,
    KnowledgeDocument,
    KnowledgeHit,
    MemoryHit,
    MemoryRecord,
    OutboxMessage,
    SessionSnapshot,
    SessionSummary,
)
from trpc_service.tenant.context import TenantContext


class SessionStore(ABC):
    """Abstract storage for Session state and atomic Agent commits."""

    @abstractmethod
    async def load(self, context: TenantContext, session_id: str) -> SessionSnapshot | None:
        """Load a Session snapshot visible to every Worker."""

        ...

    @abstractmethod
    async def claim_execution(
        self,
        context: TenantContext,
        request: InboxClaimRequest,
        worker_id: str,
        lease_until: datetime,
    ) -> ExecutionClaim:
        """Claim one inbound message or return its previously committed result."""

        ...

    async def renew_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        lease_until: datetime,
    ) -> bool:
        """Extend an execution lease; adapters without leases fail closed."""

        del context, inbox_id, worker_id, fencing_token, lease_until
        return False

    @abstractmethod
    async def commit_execution(
        self,
        context: TenantContext,
        commit: ExecutionCommit,
    ) -> SessionSnapshot:
        """Commit facts, Inbox completion, checkpoint and Outbox atomically."""

        ...

    @abstractmethod
    async def fail_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        *,
        fencing_token: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
    ) -> None:
        """Release an Inbox claim into retryable or permanent failure."""

        ...


class OutboxStore(ABC):
    """Abstract lifecycle for reliable asynchronous delivery tasks."""

    @abstractmethod
    async def claim_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        lease_until: datetime,
    ) -> OutboxMessage | None:
        """Claim one pending task; terminal or actively leased tasks return None."""

        ...

    @abstractmethod
    async def complete_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        external_receipt_id: str,
        completed_at: datetime,
    ) -> None:
        """Record a successful delivery and its provider receipt."""

        ...

    @abstractmethod
    async def fail_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
        completed_at: datetime,
        outcome_unknown: bool = False,
    ) -> None:
        """Record retry, unknown outcome or dead-letter completion."""

        ...


class MemoryStore(ABC):
    """Abstract storage for cross-Session long-term memory."""

    @abstractmethod
    async def upsert(
        self,
        context: TenantContext,
        records: Sequence[MemoryRecord],
    ) -> None:
        """Idempotently store memory records."""

        ...

    @abstractmethod
    async def search(
        self,
        context: TenantContext,
        principal_id: str,
        query: str,
        limit: int,
    ) -> Sequence[MemoryHit]:
        """Return tenant-scoped memory matches."""

        ...


class SummaryStore(ABC):
    """Abstract storage for rebuildable Session summaries."""

    @abstractmethod
    async def put_if_newer(
        self,
        context: TenantContext,
        summary: SessionSummary,
    ) -> bool:
        """Persist a summary only when its source Event sequence is newer."""

        ...


class KnowledgeStore(ABC):
    """Abstract storage for tenant-scoped knowledge documents."""

    @abstractmethod
    async def index(
        self,
        context: TenantContext,
        documents: Sequence[KnowledgeDocument],
    ) -> None:
        """Idempotently index normalized documents."""

        ...

    @abstractmethod
    async def search(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        query: str,
        limit: int,
    ) -> Sequence[KnowledgeHit]:
        """Return tenant-scoped knowledge matches."""

        ...

    async def delete(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        document_ids: Sequence[str],
    ) -> None:
        """Delete indexed chunks by stable ID inside one tenant and base."""

        raise NotImplementedError("knowledge backend does not support deletion")


class ArtifactStore(ABC):
    """Abstract storage for files independent of provider APIs."""

    @abstractmethod
    async def put(
        self,
        context: TenantContext,
        content: AsyncIterator[bytes],
        metadata: ArtifactMetadata,
    ) -> ArtifactRef:
        """Store one validated Artifact and return its stable reference."""

        ...

    @abstractmethod
    def open(self, context: TenantContext, artifact_id: str) -> AsyncIterator[bytes]:
        """Stream one tenant-owned Artifact."""

        ...

    @abstractmethod
    async def create_download_url(
        self,
        context: TenantContext,
        artifact_id: str,
        ttl_seconds: int,
    ) -> str:
        """Create a short-lived tenant-scoped download URL."""

        ...


class AuditStore(ABC):
    """Abstract storage for immutable governance and execution records."""

    @abstractmethod
    async def append(self, context: TenantContext, record: AuditRecord) -> None:
        """Append one immutable audit record."""

        ...
