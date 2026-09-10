"""Tenant-scoped durable-recovery marker reference implementation."""
from __future__ import annotations

from datetime import datetime, timezone
from .data_decisions import can_fence_write
from trpc_service.storage.data_models import DataRecoveryMarker, DataScope
from trpc_service.storage.contracts import StaleFence

class InMemoryDataRecoveryRepository:
    def __init__(self) -> None: self._markers: dict[tuple[str,str], DataRecoveryMarker] = {}
    async def create_once(self, scope: DataScope, marker: DataRecoveryMarker) -> DataRecoveryMarker:
        if marker.tenant_id != scope.tenant_id: raise StaleFence()
        key=(scope.tenant_id,marker.marker_id)
        return self._markers.setdefault(key,marker)
    async def claim(self, scope: DataScope, marker_id: str, *, expected_generation: int) -> DataRecoveryMarker:
        marker=self._markers[(scope.tenant_id,marker_id)]
        if not can_fence_write(expected_generation, marker.generation): raise StaleFence()
        return marker
    async def mark_complete(self, scope: DataScope, marker_id: str, *, generation: int, result_digest: str | None = None) -> DataRecoveryMarker:
        marker=await self.claim(scope,marker_id,expected_generation=generation)
        updated=marker.model_copy(update={"confirmed":True,"result_digest":result_digest or marker.result_digest})
        self._markers[(scope.tenant_id,marker_id)]=updated; return updated
    async def mark_review(self, scope: DataScope, marker_id: str, *, generation: int, reason: str) -> DataRecoveryMarker:
        marker=await self.claim(scope,marker_id,expected_generation=generation)
        updated=marker.model_copy(update={"review_reason":reason}); self._markers[(scope.tenant_id,marker_id)]=updated; return updated
