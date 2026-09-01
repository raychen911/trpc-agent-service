"""Fail-closed backend selection from a validated TenantSpec."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, TypeVar

from trpc_service.backends.contracts import (
    ArtifactBackend,
    ConsistencyMetadata,
    KnowledgeBackend,
    MemoryBackend,
    ScopedStateBackend,
    SessionProjectionBackend,
    SummaryBackend,
)
from trpc_service.tenant.models import TenantSpec


class BackendRoutingError(RuntimeError):
    """Base class for explicit backend routing failures."""


class BackendNotRegisteredError(BackendRoutingError):
    """A TenantSpec selected a backend for which no adapter was registered."""


class UnsafeBackendError(BackendRoutingError):
    """A development-only backend was selected in production mode."""


class BackendMode(StrEnum):
    """Runtime safety mode for backend routing."""

    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


@dataclass(frozen=True, slots=True)
class TenantBackendBundle:
    """Resolved adapters for exactly one published tenant revision."""

    tenant_id: str
    tenant_revision: int
    session_name: str
    session: SessionProjectionBackend
    scoped_state_name: str
    scoped_state: ScopedStateBackend
    memory_name: str
    memory: MemoryBackend
    summary_name: str
    summary: SummaryBackend
    knowledge_name: str
    knowledge: KnowledgeBackend
    artifact_name: str
    artifact: ArtifactBackend


class _MetadataBackend(Protocol):
    @property
    def consistency(self) -> ConsistencyMetadata: ...


_BackendT = TypeVar("_BackendT", bound=_MetadataBackend)


class BackendRouter:
    """Resolve only explicitly registered adapters; never guess or fall back."""

    def __init__(
        self,
        *,
        sessions: Mapping[str, SessionProjectionBackend] | None = None,
        scoped_states: Mapping[str, ScopedStateBackend] | None = None,
        memories: Mapping[str, MemoryBackend] | None = None,
        summaries: Mapping[str, SummaryBackend] | None = None,
        knowledge: Mapping[str, KnowledgeBackend] | None = None,
        artifacts: Mapping[str, ArtifactBackend] | None = None,
        mode: BackendMode = BackendMode.PRODUCTION,
    ) -> None:
        self._sessions = dict(sessions or {})
        self._scoped_states = dict(scoped_states or {})
        self._memories = dict(memories or {})
        self._summaries = dict(summaries or {})
        self._knowledge = dict(knowledge or {})
        self._artifacts = dict(artifacts or {})
        self._mode = mode

    def route(self, spec: TenantSpec) -> TenantBackendBundle:
        """Resolve a complete bundle or reject the TenantSpec atomically."""

        session_name = spec.storage.session
        # ScopedState follows the Session backend selector, but requires its own
        # explicitly registered adapter. Redis Session projection alone is not
        # silently treated as an app/user state implementation.
        scoped_state_name = session_name
        memory_name = spec.storage.memory
        summary_name = spec.storage.summary
        knowledge_name = spec.storage.knowledge
        artifact_name = spec.storage.artifact
        return TenantBackendBundle(
            tenant_id=spec.tenant_id,
            tenant_revision=spec.revision,
            session_name=session_name,
            session=self._resolve(self._sessions, session_name, "session"),
            scoped_state_name=scoped_state_name,
            scoped_state=self._resolve(
                self._scoped_states,
                scoped_state_name,
                "scoped_state",
            ),
            memory_name=memory_name,
            memory=self._resolve(self._memories, memory_name, "memory"),
            summary_name=summary_name,
            summary=self._resolve(self._summaries, summary_name, "summary"),
            knowledge_name=knowledge_name,
            knowledge=self._resolve(self._knowledge, knowledge_name, "knowledge"),
            artifact_name=artifact_name,
            artifact=self._resolve(self._artifacts, artifact_name, "artifact"),
        )

    def _resolve(
        self,
        registry: Mapping[str, _BackendT],
        backend_name: str,
        category: str,
    ) -> _BackendT:
        backend = registry.get(backend_name)
        if backend is None:
            raise BackendNotRegisteredError(
                f"tenant selected unregistered {category} backend {backend_name!r}"
            )
        if self._mode is BackendMode.PRODUCTION and backend.consistency.development_only:
            raise UnsafeBackendError(
                f"development-only backend {backend.consistency.backend_id!r} "
                f"cannot serve production {category}"
            )
        return backend
