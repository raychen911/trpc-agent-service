"""Provider-neutral values exchanged through storage ports."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True, slots=True)
class SessionEvent:
    """Immutable event appended to one Session."""

    event_id: str
    event_type: str
    occurred_at: datetime
    payload: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """Reliable asynchronous work committed with Session facts."""

    outbox_id: str
    category: str
    idempotency_key: str
    destination: str = ""
    binding_id: UUID | None = None
    request_id: str | None = None
    session_id: str | None = None
    sequence_no: int = 0
    # attempt_count is monotonic because it identifies immutable audit rows;
    # retry_count is the resettable budget for the current delivery cycle.
    attempt_count: int = 0
    retry_count: int = 0
    payload: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.outbox_id.strip() or not self.category.strip(
        ) or not self.idempotency_key.strip():
            raise ValueError("Outbox identifiers and category cannot be empty")
        if self.sequence_no < 0 or self.attempt_count < 0 or self.retry_count < 0:
            raise ValueError("Outbox sequence and attempt counts cannot be negative")


@dataclass(frozen=True, slots=True)
class InboxClaimRequest:
    """Normalized inbound identity used for durable claim and deduplication."""

    binding_id: UUID
    external_message_id: str
    payload_hash: str
    session_id: str
    received_at: datetime
    id_source: str = "PROVIDER"

    def __post_init__(self) -> None:
        if not self.external_message_id.strip() or not self.session_id.strip():
            raise ValueError("Inbox message and Session identifiers cannot be empty")
        if len(self.payload_hash) != 64 or any(character not in "0123456789abcdef"
                                               for character in self.payload_hash):
            raise ValueError("Inbox payload hash must be a lowercase SHA-256 digest")
        if self.id_source not in {"PROVIDER", "DERIVED"}:
            raise ValueError("Inbox ID source must be PROVIDER or DERIVED")


@dataclass(frozen=True, slots=True)
class ExecutionCommit:
    """Facts that a SessionStore must commit as one atomic operation."""

    session_id: str
    expected_version: int
    fencing_token: int | None = None
    events: tuple[SessionEvent, ...] = ()
    state: Mapping[str, object] = field(default_factory=dict)
    inbox_id: str | None = None
    runner_request_id: str | None = None
    outbox: tuple[OutboxMessage, ...] = ()

    def __post_init__(self) -> None:
        if self.expected_version < 0:
            raise ValueError("expected Session version cannot be negative")
        if self.fencing_token is not None and self.fencing_token < 1:
            raise ValueError("fencing token must be positive")


@dataclass(frozen=True, slots=True)
class SessionSnapshot:
    """Shared Session state loadable by any Worker node."""

    session_id: str
    version: int
    events: tuple[SessionEvent, ...] = ()
    state: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    """Durable Inbox claim or the exact result of an earlier successful claim."""

    inbox_id: str
    request_id: str
    fencing_token: int | None = None
    session_version: int | None = None
    replayed: bool = False
    completed: SessionSnapshot | None = None
    committed_outbox_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.fencing_token is not None and self.fencing_token < 1:
            raise ValueError("execution fencing token must be positive")
        if self.session_version is not None and self.session_version < 0:
            raise ValueError("claimed Session version cannot be negative")


@dataclass(frozen=True, slots=True)
class MemoryRecord:
    """Long-term memory associated with a tenant principal."""

    memory_id: str
    principal_id: str
    content: str
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MemoryHit:
    """One scored result returned by a MemoryStore."""

    record: MemoryRecord
    score: float


@dataclass(frozen=True, slots=True)
class SessionSummary:
    """Rebuildable Session summary ordered by its source Event sequence."""

    session_id: str
    source_event_seq: int
    content: str
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnowledgeDocument:
    """Normalized document accepted by a KnowledgeStore."""

    document_id: str
    knowledge_base_id: str
    content: str
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnowledgeHit:
    """One scored document returned by knowledge retrieval."""

    document: KnowledgeDocument
    score: float


@dataclass(frozen=True, slots=True)
class ArtifactMetadata:
    """Metadata stored alongside file content."""

    filename: str
    media_type: str
    checksum: str
    size_bytes: int
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Stable reference returned after an Artifact is stored."""

    artifact_id: str
    uri: str
    checksum: str


@dataclass(frozen=True, slots=True)
class AuditRecord:
    """Append-only governance decision emitted by the runtime."""

    action: str
    decision: str
    occurred_at: datetime
    attributes: Mapping[str, object] = field(default_factory=dict)
