"""Resumable, tenant-scoped storage migration state machine."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class MigrationPhase(str, Enum):
    PREPARE = "prepare"
    BACKFILL = "backfill"
    CHECKSUM = "checksum"
    DUAL_WRITE = "dual_write"
    SHADOW_READ = "shadow_read"
    CUTOVER = "cutover"
    OBSERVE = "observe"
    COMPLETE = "complete"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class MigrationCheckpoint:
    tenant_id: str
    migration_id: str
    phase: MigrationPhase
    source_count: int = 0
    target_count: int = 0
    checksum: str = ""
    differences: tuple[str, ...] = ()
    status: str = "pending"


class MigrationCoordinator:
    phases = tuple(MigrationPhase)

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    async def save(self, checkpoint: MigrationCheckpoint) -> None:
        async with self.repository._scoped(checkpoint.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO migration_checkpoints(tenant_id,migration_id,phase,source_count,target_count,checksum,differences,status)
                VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
                ON CONFLICT (tenant_id,migration_id) DO UPDATE SET phase=EXCLUDED.phase,source_count=EXCLUDED.source_count,
                    target_count=EXCLUDED.target_count,checksum=EXCLUDED.checksum,differences=EXCLUDED.differences,
                    status=EXCLUDED.status,updated_at=now()
                """,
                checkpoint.tenant_id,
                checkpoint.migration_id,
                checkpoint.phase.value,
                checkpoint.source_count,
                checkpoint.target_count,
                checkpoint.checksum,
                json.dumps(checkpoint.differences),
                checkpoint.status,
            )

    async def advance(
        self,
        checkpoint: MigrationCheckpoint,
        operation: Callable[[MigrationCheckpoint], Awaitable[MigrationCheckpoint]],
    ) -> MigrationCheckpoint:
        result = await operation(checkpoint)
        current = self.phases.index(checkpoint.phase)
        target = self.phases.index(result.phase)
        if target not in {current, current + 1} and result.phase != MigrationPhase.ROLLED_BACK:
            raise ValueError("migration phase transition is not allowed")
        if result.phase in {MigrationPhase.CUTOVER, MigrationPhase.OBSERVE, MigrationPhase.COMPLETE} and (
            result.source_count != result.target_count or result.differences
        ):
            raise ValueError("migration validation failed")
        await self.save(result)
        return result


def checksum_rows(rows: list[dict[str, object]]) -> str:
    canonical = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()
