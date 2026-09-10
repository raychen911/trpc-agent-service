"""Backend migration and synchronization primitives."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Protocol
from trpc_service.storage.data_models import Authority, MigrationState
from trpc_service.storage.contracts import ForwardRepairRequired, MigrationConflict, MigrationWritePaused

def transition_migration(state: MigrationState, target_state: str, *, expected_generation: int) -> MigrationState:
    if state.generation != expected_generation:
        raise MigrationConflict()
    allowed = {
        "PLANNED": {"PAUSING"}, "PAUSING": {"SNAPSHOT_LOCKED"},
        "SNAPSHOT_LOCKED": {"COPYING"}, "COPYING": {"VERIFYING"},
        "VERIFYING": {"CUTOVER_READY"}, "CUTOVER_READY": {"ACTIVE_ROLLBACK_ELIGIBLE", "ROLLED_BACK"},
        "ACTIVE_ROLLBACK_ELIGIBLE": {"ACTIVE_FORWARD_ONLY", "ROLLED_BACK"},
        "ACTIVE_FORWARD_ONLY": {"FORWARD_REPAIR_REQUIRED"},
    }
    if target_state not in allowed.get(state.state, set()):
        if state.state == "ACTIVE_FORWARD_ONLY" and target_state == "ROLLED_BACK":
            raise ForwardRepairRequired()
        raise MigrationConflict()
    return state.model_copy(update={"state": target_state, "generation": state.generation + 1})

class InMemoryMigrationRepository:
    def __init__(self): self._states = {}
    async def get(self, scope: Any, stream: str) -> Any:
        key=(scope.tenant_id,stream)
        if key not in self._states: self._states[key]=MigrationState(tenant_id=scope.tenant_id,stream=stream)
        return self._states[key]
    async def transition(self, scope, stream, *, expected_state, expected_generation, target_state, fields=None):
        state=await self.get(scope,stream)
        if state.state != expected_state: raise MigrationConflict()
        state=transition_migration(state,target_state,expected_generation=expected_generation)
        if fields: state=state.model_copy(update=fields)
        self._states[(scope.tenant_id,stream)]=state; return state
    async def checkpoint(self, scope, stream, *, expected_generation, copied_watermark, target_digest=None):
        state=await self.get(scope,stream)
        if state.generation != expected_generation: raise MigrationConflict()
        state=state.model_copy(update={"copied_watermark":copied_watermark,"target_digest":target_digest,"generation":state.generation+1,"state":"VERIFYING"})
        self._states[(scope.tenant_id,stream)]=state; return state
    async def activate(self, scope, stream, *, expected_generation, verified_watermark, verified_digest):
        state=await self.get(scope,stream)
        if state.generation != expected_generation or state.copied_watermark != verified_watermark or state.target_digest != verified_digest: raise MigrationConflict()
        state=state.model_copy(update={"state":"ACTIVE_ROLLBACK_ELIGIBLE","authority":Authority.POSTGRES,"generation":state.generation+1})
        self._states[(scope.tenant_id,stream)]=state; return state
    async def mark_first_authoritative_write(self, scope, stream, *, expected_generation, transaction=None):
        state=await self.get(scope,stream)
        if state.generation != expected_generation: raise MigrationConflict()
        state=state.model_copy(update={"state":"ACTIVE_FORWARD_ONLY","rollback_eligible":False,"generation":state.generation+1})
        self._states[(scope.tenant_id,stream)]=state; return state

@dataclass(frozen=True, slots=True)
class MigrationWatermark:
    tenant_id: str
    stream: str
    sequence: int

class VectorSearchAdapter(Protocol):
    async def upsert(self, document: Any) -> Any: ...
    async def search(self, tenant_id: str, query: str, limit: int = 10) -> list[Any]: ...

class ObjectStorageAdapter(Protocol):
    async def put(self, tenant_id: str, key: str, content: bytes) -> str: ...
    async def get(self, tenant_id: str, key: str) -> bytes: ...

class DualReadMigrator:
    def __init__(self, source: Any, target: Any) -> None:
        self.source, self.target = source, target

    async def verify(self, *, tenant_id: str, key: str) -> bool:
        source = await self.source.get_memory(tenant_id=tenant_id, namespace="default", key=key)
        target = await self.target.get_memory(tenant_id=tenant_id, namespace="default", key=key)
        return source is None and target is None or (source is not None and target is not None and source.version == target.version and source.value == target.value)
