"""Tenant-prefiltering deterministic vector-store fixture."""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Sequence

from .contracts import StateBackendUnavailable
from .data_models import DataScope


@dataclass(frozen=True, slots=True)
class VectorHit:
    document_id: str
    digest: str
    score: float


class DeterministicVectorStore:
    supports_tenant_prefilter = True
    is_deterministic_fixture = True

    def __init__(self, *, supports_tenant_prefilter: bool = True) -> None:
        self.supports_tenant_prefilter = supports_tenant_prefilter
        self._vectors: dict[tuple[str, str, str], tuple[float, ...]] = {}
        self.fail_operations: set[str] = set()
        self.last_filter_tenant: str | None = None

    def _fail(self, operation: str) -> None:
        if operation in self.fail_operations:
            raise StateBackendUnavailable()

    async def upsert(self, scope: DataScope, document_id: str, digest: str, vector: Sequence[float]) -> None:
        self._fail("upsert")
        self._vectors[(scope.tenant_id, document_id, digest)] = tuple(float(x) for x in vector)

    async def search(self, scope: DataScope, query_vector: Sequence[float], limit: int) -> list[VectorHit]:
        self._fail("search")
        self.last_filter_tenant = scope.tenant_id
        query = tuple(float(x) for x in query_vector)
        q_norm = sqrt(sum(x * x for x in query)) or 1.0
        hits: list[VectorHit] = []
        # The tenant predicate is applied before scoring or candidate creation.
        for (tenant_id, document_id, digest), vector in self._vectors.items():
            if tenant_id != scope.tenant_id:
                continue
            norm = sqrt(sum(x * x for x in vector)) or 1.0
            score = sum(a * b for a, b in zip(query, vector, strict=False)) / (q_norm * norm)
            hits.append(VectorHit(document_id, digest, score))
        return sorted(hits, key=lambda x: (-x.score, x.document_id))[:limit]
