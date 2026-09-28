"""Process-local storage implementations for tests and single-node development."""

import asyncio
import hashlib
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

from trpc_service.storage.errors import (
    ArtifactIntegrityError,
    ExecutionAlreadyRunning,
    IdempotencyConflict,
    SessionVersionConflict,
    StaleExecutionLease,
    StoredObjectNotFound,
)
from trpc_service.storage.ports import (
    ArtifactStore,
    AuditStore,
    KnowledgeStore,
    MemoryStore,
    OutboxStore,
    SessionStore,
    SummaryStore,
)
from trpc_service.storage.registry import StorageBackend
from trpc_service.storage.scoring import lexical_score
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

TenantScope = tuple[UUID, UUID]
SessionKey = tuple[UUID, UUID, str]
InboxKey = tuple[UUID, UUID, str]


@dataclass(slots=True)
class _InMemoryInbox:
    """Mutable local representation of one durable Inbox lifecycle."""

    inbox_id: str
    scope: TenantScope
    request_id: str
    request: InboxClaimRequest
    status: str
    fencing_token: int
    lease_owner: str
    lease_until: datetime | None
    next_attempt_at: datetime | None = None
    snapshot: SessionSnapshot | None = None
    outbox_ids: tuple[str, ...] = ()


@dataclass(slots=True)
class _InMemoryOutbox:
    """Mutable local representation of one reliable delivery task."""

    message: OutboxMessage
    status: str = "PENDING"
    lease_owner: str | None = None
    lease_until: datetime | None = None
    next_attempt_at: datetime | None = None
    external_receipt_id: str | None = None
    attempt_count: int = 0
    retry_count: int = 0


@dataclass(slots=True)
class _InMemoryFence:
    """Latest issued Session token and its current lease ownership."""

    issued_token: int = 0
    lease_owner: str | None = None
    lease_until: datetime | None = None


@dataclass(slots=True)
class _InMemoryState:
    """Share isolated state across capability-specific in-memory adapters."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    sessions: dict[SessionKey, SessionSnapshot] = field(default_factory=dict)
    committed_fencing_tokens: dict[SessionKey, int] = field(default_factory=dict)
    session_fences: dict[SessionKey, _InMemoryFence] = field(default_factory=dict)
    inbox: dict[InboxKey, _InMemoryInbox] = field(default_factory=dict)
    inbox_by_id: dict[str, _InMemoryInbox] = field(default_factory=dict)
    outbox: dict[TenantScope, dict[str, _InMemoryOutbox]] = field(default_factory=dict)
    outbox_keys: dict[tuple[UUID, UUID, str, str], str] = field(default_factory=dict)
    memories: dict[TenantScope, dict[str, MemoryRecord]] = field(default_factory=dict)
    summaries: dict[SessionKey, SessionSummary] = field(default_factory=dict)
    # Knowledge and Artifacts belong to a Tenant rather than one Agent App.
    # Agent-level access is enforced by the versioned knowledge configuration.
    knowledge: dict[UUID, dict[str, KnowledgeDocument]] = field(default_factory=dict)
    artifacts: dict[tuple[UUID, str], tuple[bytes, ArtifactRef]] = field(default_factory=dict)
    audits: dict[TenantScope, list[AuditRecord]] = field(default_factory=dict)


class _ScopedAdapter:
    """Provide the mandatory tenant and Agent scope used by every adapter."""

    def __init__(self, state: _InMemoryState) -> None:
        self._state = state

    @staticmethod
    def _scope(context: TenantContext) -> TenantScope:
        return context.tenant_id, context.agent_app_id


class InMemorySessionStore(_ScopedAdapter, SessionStore, OutboxStore):
    """Keep versioned Session facts with local Inbox and Outbox guarantees."""

    def _session_key(self, context: TenantContext, session_id: str) -> SessionKey:
        return (*self._scope(context), session_id)

    async def load(
        self,
        context: TenantContext,
        session_id: str,
    ) -> SessionSnapshot | None:
        """Load a Session only from the caller's tenant and Agent scope."""

        async with self._state.lock:
            return self._state.sessions.get(self._session_key(context, session_id))

    async def claim_execution(
        self,
        context: TenantContext,
        request: InboxClaimRequest,
        worker_id: str,
        lease_until: datetime,
    ) -> ExecutionClaim:
        """Claim one provider identity or return its prior exact result."""

        if not worker_id.strip() or lease_until <= datetime.now(timezone.utc):
            raise ValueError("execution worker and future lease are required")
        key = context.tenant_id, request.binding_id, request.external_message_id
        async with self._state.lock:
            inbox = self._state.inbox.get(key)
            now = datetime.now(timezone.utc)
            if inbox is None:
                fencing_token = self._claim_session_fence(
                    context,
                    request.session_id,
                    worker_id,
                    lease_until,
                    now,
                )
                inbox = _InMemoryInbox(
                    inbox_id=str(uuid4()),
                    scope=self._scope(context),
                    request_id=context.request_id,
                    request=request,
                    status="RUNNING",
                    fencing_token=fencing_token,
                    lease_owner=worker_id,
                    lease_until=lease_until,
                )
                self._state.inbox[key] = inbox
                self._state.inbox_by_id[inbox.inbox_id] = inbox
                return ExecutionClaim(
                    inbox_id=inbox.inbox_id,
                    request_id=inbox.request_id,
                    fencing_token=fencing_token,
                    session_version=self._state.sessions.get(
                        self._session_key(context, request.session_id),
                        SessionSnapshot(session_id=request.session_id, version=0),
                    ).version,
                )
            if inbox.request.payload_hash != request.payload_hash:
                raise IdempotencyConflict("external message ID was reused with a different payload")
            if inbox.status in {"SUCCEEDED", "REPLIED"}:
                assert inbox.snapshot is not None
                return ExecutionClaim(
                    inbox_id=inbox.inbox_id,
                    request_id=inbox.request_id,
                    session_version=inbox.snapshot.version,
                    replayed=True,
                    completed=inbox.snapshot,
                    committed_outbox_ids=inbox.outbox_ids,
                )
            if (inbox.status == "RUNNING" and inbox.lease_until is not None
                    and inbox.lease_until > now):
                raise ExecutionAlreadyRunning("Inbox message is owned by another Worker")
            if (inbox.next_attempt_at is not None and inbox.next_attempt_at > now):
                raise ExecutionAlreadyRunning("Inbox retry backoff has not elapsed")
            if inbox.status == "PERMANENT_FAILED":
                raise RuntimeError("Inbox message has permanently failed")
            fencing_token = self._claim_session_fence(
                context,
                request.session_id,
                worker_id,
                lease_until,
                now,
            )
            inbox.status = "RUNNING"
            inbox.fencing_token = fencing_token
            inbox.lease_owner = worker_id
            inbox.lease_until = lease_until
            inbox.next_attempt_at = None
            return ExecutionClaim(
                inbox_id=inbox.inbox_id,
                request_id=inbox.request_id,
                fencing_token=fencing_token,
                session_version=self._state.sessions.get(
                    self._session_key(context, request.session_id),
                    SessionSnapshot(session_id=request.session_id, version=0),
                ).version,
            )

    async def renew_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        lease_until: datetime,
    ) -> bool:
        """Extend matching local Inbox and Session fence leases together."""

        now = datetime.now(timezone.utc)
        if lease_until <= now:
            raise ValueError("renewed execution lease must expire in the future")
        async with self._state.lock:
            inbox = self._state.inbox_by_id.get(inbox_id)
            if inbox is None or inbox.scope != self._scope(context) or inbox.status != "RUNNING":
                return False
            fence = self._state.session_fences.get((*inbox.scope, inbox.request.session_id))
            if (inbox.lease_owner != worker_id or inbox.fencing_token != fencing_token
                    or inbox.lease_until is None or inbox.lease_until <= now or fence is None
                    or fence.lease_owner != worker_id or fence.issued_token != fencing_token
                    or fence.lease_until is None or fence.lease_until <= now):
                return False
            inbox.lease_until = lease_until
            fence.lease_until = lease_until
            return True

    def _claim_session_fence(
        self,
        context: TenantContext,
        session_id: str,
        worker_id: str,
        lease_until: datetime,
        now: datetime,
    ) -> int:
        """Issue the next token only when the preceding Session lease expired."""

        key = self._session_key(context, session_id)
        fence = self._state.session_fences.setdefault(
            key,
            _InMemoryFence(issued_token=self._state.committed_fencing_tokens.get(key, 0)),
        )
        if fence.lease_until is not None and fence.lease_until > now:
            raise ExecutionAlreadyRunning("Session is owned by another Worker")
        fence.issued_token += 1
        fence.lease_owner = worker_id
        fence.lease_until = lease_until
        return fence.issued_token

    async def commit_execution(
        self,
        context: TenantContext,
        commit: ExecutionCommit,
    ) -> SessionSnapshot:
        """Atomically apply CAS, Inbox idempotency, Session and Outbox updates."""

        async with self._state.lock:
            inbox = None
            if commit.inbox_id is not None:
                inbox = self._state.inbox_by_id.get(commit.inbox_id)
                if inbox is None or inbox.scope != self._scope(context):
                    raise LookupError("Inbox claim does not exist in the execution scope")
                if inbox.status in {"SUCCEEDED", "REPLIED"}:
                    assert inbox.snapshot is not None
                    return inbox.snapshot
                if inbox.status != "RUNNING":
                    raise StaleExecutionLease("Inbox claim is no longer running")
                now = datetime.now(timezone.utc)
                session_key = self._session_key(context, commit.session_id)
                fence = self._state.session_fences.get(session_key)
                if (commit.fencing_token is None or commit.fencing_token != inbox.fencing_token
                        or fence is None or fence.issued_token != commit.fencing_token
                        or fence.lease_until is None or fence.lease_until <= now):
                    raise StaleExecutionLease("Session execution lease is stale or expired")

            session_key = self._session_key(context, commit.session_id)
            current = self._state.sessions.get(session_key)
            current_version = 0 if current is None else current.version
            if current_version != commit.expected_version:
                raise SessionVersionConflict(
                    f"expected Session version {commit.expected_version}, got {current_version}")
            last_fencing_token = self._state.committed_fencing_tokens.get(session_key)
            if last_fencing_token is not None and (commit.fencing_token is None
                                                   or commit.fencing_token <= last_fencing_token):
                raise StaleExecutionLease(f"fencing token must be newer than {last_fencing_token}")

            existing_events = () if current is None else current.events
            snapshot = SessionSnapshot(
                session_id=commit.session_id,
                version=current_version + 1,
                events=existing_events + commit.events,
                state=dict(commit.state),
            )
            self._state.sessions[session_key] = snapshot
            if commit.fencing_token is not None:
                self._state.committed_fencing_tokens[session_key] = commit.fencing_token
            if inbox is not None:
                inbox.status = "SUCCEEDED"
                inbox.lease_until = None
                inbox.snapshot = snapshot
                fence = self._state.session_fences[session_key]
                fence.lease_owner = None
                fence.lease_until = None
            scope_outbox = self._state.outbox.setdefault(self._scope(context), {})
            committed_outbox_ids: list[str] = []
            for message in commit.outbox:
                semantic_key = (*self._scope(context), message.category, message.idempotency_key)
                existing_id = self._state.outbox_keys.get(semantic_key)
                if existing_id is None:
                    scope_outbox[message.outbox_id] = _InMemoryOutbox(message=message)
                    self._state.outbox_keys[semantic_key] = message.outbox_id
                    committed_outbox_ids.append(message.outbox_id)
                else:
                    committed_outbox_ids.append(existing_id)
            if inbox is not None:
                inbox.outbox_ids = tuple(committed_outbox_ids)
            return snapshot

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
        """Release a local Inbox claim without retaining sensitive error data."""

        del error_code, error_summary
        async with self._state.lock:
            inbox = self._state.inbox_by_id.get(inbox_id)
            if inbox is None or inbox.scope != self._scope(context):
                return
            if inbox.status not in {"SUCCEEDED", "REPLIED"}:
                session_key = (*inbox.scope, inbox.request.session_id)
                fence = self._state.session_fences.get(session_key)
                if (fencing_token != inbox.fencing_token or fence is None
                        or fence.issued_token != fencing_token or fence.lease_until is None
                        or fence.lease_until <= datetime.now(timezone.utc)):
                    raise StaleExecutionLease("Session execution lease is stale")
                inbox.status = ("RETRYABLE_FAILED"
                                if next_attempt_at is not None else "PERMANENT_FAILED")
                inbox.lease_until = None
                inbox.next_attempt_at = next_attempt_at
                fence.lease_owner = None
                fence.lease_until = None

    async def claim_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        lease_until: datetime,
    ) -> OutboxMessage | None:
        """Claim one pending local delivery task."""

        if not worker_id.strip() or lease_until <= datetime.now(timezone.utc):
            raise ValueError("Outbox worker and future lease are required")
        async with self._state.lock:
            row = self._state.outbox.get(self._scope(context), {}).get(outbox_id)
            if row is None or row.status in {"DELIVERED", "DEAD_LETTER", "CANCELLED", "UNKNOWN"}:
                return None
            if (row.status == "PROCESSING" and row.lease_until is not None
                    and row.lease_until > datetime.now(timezone.utc)):
                return None
            if (row.next_attempt_at is not None
                    and row.next_attempt_at > datetime.now(timezone.utc)):
                return None
            row.status = "PROCESSING"
            row.lease_owner = worker_id
            row.lease_until = lease_until
            row.next_attempt_at = None
            row.attempt_count += 1
            row.retry_count += 1
            return OutboxMessage(
                outbox_id=row.message.outbox_id,
                category=row.message.category,
                idempotency_key=row.message.idempotency_key,
                destination=row.message.destination,
                binding_id=row.message.binding_id,
                request_id=row.message.request_id,
                session_id=row.message.session_id,
                sequence_no=row.message.sequence_no,
                attempt_count=row.attempt_count,
                retry_count=row.retry_count,
                payload=row.message.payload,
            )

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
        """Mark one local delivery complete and close its Inbox lifecycle."""

        async with self._state.lock:
            row = self._state.outbox.get(self._scope(context), {}).get(outbox_id)
            if row is None:
                raise LookupError("Outbox task does not exist in the execution scope")
            if row.status == "DELIVERED":
                return
            if (row.status != "PROCESSING" or row.lease_owner != worker_id
                    or row.attempt_count != attempt_no or row.lease_until is None
                    or row.lease_until <= datetime.now(timezone.utc)):
                raise StaleExecutionLease("Outbox delivery lease is stale or expired")
            row.status = "DELIVERED"
            row.lease_owner = None
            row.lease_until = None
            row.next_attempt_at = None
            row.external_receipt_id = external_receipt_id
            for inbox in self._state.inbox.values():
                if inbox.scope == self._scope(context) and outbox_id in inbox.outbox_ids:
                    inbox.status = "REPLIED"

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
        """Release or dead-letter one failed local delivery task."""

        del error_code, error_summary, completed_at
        async with self._state.lock:
            row = self._state.outbox.get(self._scope(context), {}).get(outbox_id)
            if row is None or row.status == "DELIVERED":
                return
            if (row.status != "PROCESSING" or row.lease_owner != worker_id
                    or row.attempt_count != attempt_no or row.lease_until is None
                    or row.lease_until <= datetime.now(timezone.utc)):
                raise StaleExecutionLease("Outbox delivery lease is stale")
            if outcome_unknown:
                row.status = "UNKNOWN"
            else:
                row.status = ("RETRYABLE_FAILED" if next_attempt_at is not None else "DEAD_LETTER")
            row.lease_owner = None
            row.lease_until = None
            row.next_attempt_at = next_attempt_at


class InMemoryMemoryStore(_ScopedAdapter, MemoryStore):
    """Store long-term memory records inside one process."""

    async def upsert(
        self,
        context: TenantContext,
        records: Sequence[MemoryRecord],
    ) -> None:
        """Replace records by stable ID within one tenant and Agent scope."""

        async with self._state.lock:
            scoped = self._state.memories.setdefault(self._scope(context), {})
            for record in records:
                scoped[record.memory_id] = record

    async def search(
        self,
        context: TenantContext,
        principal_id: str,
        query: str,
        limit: int,
    ) -> Sequence[MemoryHit]:
        """Search principal memory with a deterministic local lexical score."""

        async with self._state.lock:
            records = self._state.memories.get(self._scope(context), {}).values()
            hits = [
                MemoryHit(record=record, score=lexical_score(record.content, query))
                for record in records if record.principal_id == principal_id
            ]
        return sorted(hits, key=lambda hit: (-hit.score, hit.record.memory_id))[:limit]


class InMemorySummaryStore(_ScopedAdapter, SummaryStore):
    """Keep the newest summary for each scoped Session."""

    async def put_if_newer(
        self,
        context: TenantContext,
        summary: SessionSummary,
    ) -> bool:
        """Reject stale or duplicate summary writes."""

        key = (*self._scope(context), summary.session_id)
        async with self._state.lock:
            current = self._state.summaries.get(key)
            if current is not None and current.source_event_seq >= summary.source_event_seq:
                return False
            self._state.summaries[key] = summary
            return True


class InMemoryKnowledgeStore(_ScopedAdapter, KnowledgeStore):
    """Provide deterministic knowledge retrieval without a vector database."""

    async def index(
        self,
        context: TenantContext,
        documents: Sequence[KnowledgeDocument],
    ) -> None:
        """Replace documents by stable ID within one tenant scope."""

        async with self._state.lock:
            scoped = self._state.knowledge.setdefault(context.tenant_id, {})
            for document in documents:
                scoped[document.document_id] = document

    async def search(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        query: str,
        limit: int,
    ) -> Sequence[KnowledgeHit]:
        """Search one knowledge base using the local lexical fallback."""

        async with self._state.lock:
            documents = self._state.knowledge.get(context.tenant_id, {}).values()
            hits = [
                KnowledgeHit(document=document, score=lexical_score(document.content, query))
                for document in documents if document.knowledge_base_id == knowledge_base_id
            ]
        return sorted(hits, key=lambda hit: (-hit.score, hit.document.document_id))[:limit]

    async def delete(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        document_ids: Sequence[str],
    ) -> None:
        """Remove only matching tenant/base chunks from the local index."""

        async with self._state.lock:
            scoped = self._state.knowledge.get(context.tenant_id, {})
            for document_id in document_ids:
                document = scoped.get(document_id)
                if document is not None and document.knowledge_base_id == knowledge_base_id:
                    del scoped[document_id]


class InMemoryArtifactStore(_ScopedAdapter, ArtifactStore):
    """Validate and retain small development Artifacts in process memory."""

    async def put(
        self,
        context: TenantContext,
        content: AsyncIterator[bytes],
        metadata: ArtifactMetadata,
    ) -> ArtifactRef:
        """Buffer and validate content before making it visible."""

        payload = b"".join([chunk async for chunk in content])
        checksum = hashlib.sha256(payload).hexdigest()
        if len(payload) != metadata.size_bytes or checksum != metadata.checksum:
            raise ArtifactIntegrityError(
                "Artifact size or SHA-256 checksum does not match metadata")
        artifact_id = str(uuid5(NAMESPACE_URL, f"{context.tenant_id}:{checksum}"))
        reference = ArtifactRef(
            artifact_id=artifact_id,
            uri=f"memory://{context.tenant_id}/{artifact_id}",
            checksum=checksum,
        )
        async with self._state.lock:
            self._state.artifacts[(context.tenant_id, artifact_id)] = payload, reference
        return reference

    async def open(
        self,
        context: TenantContext,
        artifact_id: str,
    ) -> AsyncIterator[bytes]:
        """Yield the complete local payload as one chunk."""

        async with self._state.lock:
            stored = self._state.artifacts.get((context.tenant_id, artifact_id))
        if stored is None:
            raise StoredObjectNotFound(f"Artifact does not exist: {artifact_id}")
        yield stored[0]

    async def create_download_url(
        self,
        context: TenantContext,
        artifact_id: str,
        ttl_seconds: int,
    ) -> str:
        """Return the internal URI after checking tenant ownership."""

        del ttl_seconds
        async with self._state.lock:
            stored = self._state.artifacts.get((context.tenant_id, artifact_id))
        if stored is None:
            raise StoredObjectNotFound(f"Artifact does not exist: {artifact_id}")
        return stored[1].uri


class InMemoryAuditStore(_ScopedAdapter, AuditStore):
    """Retain append-only audit records for local verification."""

    async def append(self, context: TenantContext, record: AuditRecord) -> None:
        """Append without exposing a mutation path for prior records."""

        async with self._state.lock:
            self._state.audits.setdefault(self._scope(context), []).append(record)


def build_inmemory_backend(name: str = "inmemory") -> StorageBackend:
    """Compose all in-memory capabilities over one isolated state instance."""

    state = _InMemoryState()
    session = InMemorySessionStore(state)
    return StorageBackend(
        name=name,
        session=session,
        outbox=session,
        memory=InMemoryMemoryStore(state),
        summary=InMemorySummaryStore(state),
        knowledge=InMemoryKnowledgeStore(state),
        artifact=InMemoryArtifactStore(state),
        audit=InMemoryAuditStore(state),
    )
