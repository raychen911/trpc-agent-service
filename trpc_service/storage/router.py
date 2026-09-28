"""Resolve one tenant Backend Profile into capability-specific storage ports."""

from collections.abc import Mapping
from dataclasses import dataclass

from trpc_service.storage.ports import (
    ArtifactStore,
    AuditStore,
    KnowledgeStore,
    MemoryStore,
    OutboxStore,
    SessionStore,
    SummaryStore,
)
from trpc_service.storage.registry import StorageBackendRegistry


class StorageCapabilityMissing(LookupError):
    """Raised when a selected backend does not implement a required capability."""


@dataclass(frozen=True, slots=True)
class BackendProfile:
    """Backend names selected independently for each storage capability."""

    session: str
    memory: str | None = None
    summary: str | None = None
    knowledge: str | None = None
    artifact: str | None = None
    audit: str | None = None

    def __post_init__(self) -> None:
        for capability in ("session", "memory", "summary", "knowledge", "artifact", "audit"):
            name = getattr(self, capability)
            if name is None:
                continue
            normalized = name.strip().lower()
            if normalized == "":
                raise ValueError(f"backend name cannot be empty for {capability}")
            object.__setattr__(self, capability, normalized)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "BackendProfile":
        """Validate the flexible Agent configuration at the routing boundary."""

        session = value.get("session")
        if not isinstance(session, str):
            raise ValueError("backend profile requires a session backend")

        optional_names: dict[str, str | None] = {}
        for capability in ("memory", "summary", "knowledge", "artifact", "audit"):
            name = value.get(capability)
            if name is not None and not isinstance(name, str):
                raise ValueError(f"backend name must be a string for {capability}")
            optional_names[capability] = name
        return cls(session=session, **optional_names)


@dataclass(frozen=True, slots=True)
class ResolvedStorage:
    """Typed concrete stores used during one configuration version."""

    profile: BackendProfile
    session: SessionStore
    outbox: OutboxStore
    memory: MemoryStore | None
    summary: SummaryStore | None
    knowledge: KnowledgeStore | None
    artifact: ArtifactStore | None
    audit: AuditStore | None


class StorageRouter:
    """Route each storage capability without leaking backend selection to callers."""

    def __init__(self, registry: StorageBackendRegistry) -> None:
        self._registry = registry

    @staticmethod
    def _missing(backend_name: str, capability: str) -> StorageCapabilityMissing:
        return StorageCapabilityMissing(
            f"storage backend {backend_name!r} does not provide {capability}")

    def resolve(self, profile: BackendProfile) -> ResolvedStorage:
        """Resolve and validate all capabilities before starting Agent execution."""

        session_backend = self._registry.resolve(profile.session)
        session = session_backend.session
        if session is None:
            raise self._missing(profile.session, "session")
        # Outbox must share the Session fact backend so one adapter can commit
        # both sides atomically without a distributed transaction.
        outbox = session_backend.outbox
        if outbox is None:
            raise self._missing(profile.session, "outbox")
        memory = None
        if profile.memory is not None:
            memory = self._registry.resolve(profile.memory).memory
            if memory is None:
                raise self._missing(profile.memory, "memory")
        summary = None
        if profile.summary is not None:
            summary = self._registry.resolve(profile.summary).summary
            if summary is None:
                raise self._missing(profile.summary, "summary")
        knowledge = None
        if profile.knowledge is not None:
            knowledge = self._registry.resolve(profile.knowledge).knowledge
            if knowledge is None:
                raise self._missing(profile.knowledge, "knowledge")
        artifact = None
        if profile.artifact is not None:
            artifact = self._registry.resolve(profile.artifact).artifact
            if artifact is None:
                raise self._missing(profile.artifact, "artifact")
        audit = None
        if profile.audit is not None:
            audit = self._registry.resolve(profile.audit).audit
            if audit is None:
                raise self._missing(profile.audit, "audit")

        return ResolvedStorage(
            profile=profile,
            session=session,
            outbox=outbox,
            memory=memory,
            summary=summary,
            knowledge=knowledge,
            artifact=artifact,
            audit=audit,
        )
