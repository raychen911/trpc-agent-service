"""Injectable contracts for durable Summary and Memory projection algorithms."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol

from trpc_service.reliability import (
    ProjectionClaim,
    ProjectionFinalizeResult,
    ProjectionInput,
)
from trpc_service.storage import MemoryProjection, SummaryProjection


@dataclass(frozen=True, slots=True)
class MemoryCandidate:
    """Content selected by an extractor for one visible source event."""

    source_event_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


class SummaryAlgorithm(Protocol):
    """Pure/injectable summary algorithm; transport and durability stay outside it."""

    @property
    def version(self) -> str:
        """Return the immutable implementation/configuration version."""

    async def summarize(self, projection_input: ProjectionInput) -> str | None:
        """Return summary text, or ``None`` when no summary should be emitted."""


class MemoryAlgorithm(Protocol):
    """Pure/injectable memory extraction algorithm over committed events only."""

    @property
    def version(self) -> str:
        """Return the immutable implementation/configuration version."""

    async def extract(self, projection_input: ProjectionInput) -> Sequence[MemoryCandidate]:
        """Return event-keyed candidates; do not perform storage side effects."""


class ProjectionPort(Protocol):
    """Narrow durability capabilities used by :class:`ProjectionWorker`."""

    async def claim_projection(
        self,
        tenant_id: str,
        worker_id: str,
        *,
        lease_ttl: timedelta,
    ) -> ProjectionClaim | None: ...

    async def renew_projection_claim(
        self,
        claim: ProjectionClaim,
        *,
        lease_ttl: timedelta,
    ) -> bool: ...

    async def load_projection_input(self, claim: ProjectionClaim) -> ProjectionInput: ...

    async def complete_projection(
        self,
        claim: ProjectionClaim,
        *,
        summary: SummaryProjection | None,
        memories: Sequence[MemoryProjection],
    ) -> ProjectionFinalizeResult: ...

    async def defer_projection_retry(
        self,
        claim: ProjectionClaim,
        *,
        retry_delay: timedelta,
        error_type: str,
    ) -> None: ...

    async def dead_letter_projection(
        self,
        claim: ProjectionClaim,
        *,
        error_type: str,
    ) -> None: ...
