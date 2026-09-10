"""Tenant/stream scoped migration coordinator for deterministic tests."""
from .contracts import MigrationWritePaused, ForwardRepairRequired
from .sync import transition_migration

class MigrationCoordinator:
    def __init__(self, repository): self.repository = repository
    async def pause(self, scope, stream): return await self.repository.transition(scope,stream,expected_state="PLANNED",expected_generation=(await self.repository.get(scope,stream)).generation,target_state="PAUSING")
    async def assert_write_allowed(self, scope, stream):
        state=await self.repository.get(scope,stream)
        if state.state in {"PAUSING","SNAPSHOT_LOCKED","COPYING","VERIFYING","CUTOVER_READY"}: raise MigrationWritePaused()
        return state
    async def lock_snapshot(self, scope, stream, generation, *, watermark, digest): return await self.repository.transition(scope,stream,expected_state="PAUSING",expected_generation=generation,target_state="SNAPSHOT_LOCKED",fields={"source_watermark":watermark,"source_digest":digest})
    async def checkpoint(self, scope, stream, generation, *, watermark, digest): return await self.repository.checkpoint(scope,stream,expected_generation=generation,copied_watermark=watermark,target_digest=digest)
    async def cutover(self, scope, stream, generation): return await self.repository.activate(scope,stream,expected_generation=generation,verified_watermark=(await self.repository.get(scope,stream)).copied_watermark,verified_digest=(await self.repository.get(scope,stream)).target_digest)
    async def rollback(self, scope, stream, generation): return await self.repository.transition(scope,stream,expected_state="CUTOVER_READY",expected_generation=generation,target_state="ROLLED_BACK")
    async def require_forward_repair(self, scope, stream, generation): return await self.repository.transition(scope,stream,expected_state="ACTIVE_FORWARD_ONLY",expected_generation=generation,target_state="FORWARD_REPAIR_REQUIRED")
