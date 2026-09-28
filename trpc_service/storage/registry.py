"""Explicit registration of concrete storage backends and their capabilities."""

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


class StorageBackendNotFound(LookupError):
    """Raised when a Backend Profile references an unavailable backend."""


class StorageBackendAlreadyRegistered(ValueError):
    """Raised when startup registers the same backend name twice."""


@dataclass(frozen=True, slots=True)
class StorageBackend:
    """Concrete backend with one or more independently selectable capabilities."""

    name: str
    session: SessionStore | None = None
    outbox: OutboxStore | None = None
    memory: MemoryStore | None = None
    summary: SummaryStore | None = None
    knowledge: KnowledgeStore | None = None
    artifact: ArtifactStore | None = None
    audit: AuditStore | None = None

    def __post_init__(self) -> None:
        normalized = self.name.strip().lower()
        if normalized == "":
            raise ValueError("storage backend name cannot be empty")
        if all(port is None for port in (
                self.session,
                self.outbox,
                self.memory,
                self.summary,
                self.knowledge,
                self.artifact,
                self.audit,
        )):
            raise ValueError("storage backend must provide at least one capability")
        object.__setattr__(self, "name", normalized)


class StorageBackendRegistry:
    """Hold concrete storage implementations selected by Backend Profiles."""

    def __init__(self) -> None:
        self._backends: dict[str, StorageBackend] = {}

    def register(self, backend: StorageBackend, *, replace: bool = False) -> None:
        """Register one backend during application composition."""

        if backend.name in self._backends and not replace:
            raise StorageBackendAlreadyRegistered(
                f"storage backend is already registered: {backend.name}")
        self._backends[backend.name] = backend

    def resolve(self, name: str) -> StorageBackend:
        """Resolve a normalized backend name or fail before Agent execution."""

        normalized = name.strip().lower()
        try:
            return self._backends[normalized]
        except KeyError as error:
            raise StorageBackendNotFound(
                f"storage backend is not available on this node: {normalized}") from error

    @property
    def names(self) -> tuple[str, ...]:
        """Return deterministic backend names for diagnostics."""

        return tuple(sorted(self._backends))
