"""Executable reference migration for JSON records (not an SDK backend adapter).

The source retains authority until cutover. Real Redis/SQL adapters must provide
their own event-preserving export/import and shared write-routing barrier.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy

from trpc_service.migration.coordinator import MigrationJob
from trpc_service.migration.coordinator import MigrationPhase


class RecordMigrationProvider:
    """Small deterministic data plane used to exercise the coordinator offline."""

    def __init__(self, source: dict[str, dict], target: dict[str, dict]) -> None:
        self.source = source
        self.target = target
        self.dual_write = False
        self.read_target = False

    def put(self, key: str, value: dict) -> None:
        self.source[key] = deepcopy(value)
        if self.dual_write:
            self.target[key] = deepcopy(value)

    def get(self, key: str) -> dict:
        return deepcopy((self.target if self.read_target else self.source)[key])

    @staticmethod
    def digest(records: dict) -> str:
        return hashlib.sha256(json.dumps(records, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

    @property
    def steps(self):
        return {phase: self.execute for phase in MigrationPhase if phase != MigrationPhase.COMPLETED}

    async def execute(self, job: MigrationJob) -> MigrationJob:
        if job.source_backend != "local-source" or job.target_backend != "local-target":
            raise ValueError("record reference provider only supports local-source/local-target")
        if job.phase == MigrationPhase.PREPARING:
            job.checkpoint["source_hash"] = self.digest(self.source)
        elif job.phase == MigrationPhase.DUAL_WRITE:
            self.dual_write = True
        elif job.phase == MigrationPhase.BACKFILLING:
            # Checkpoints are saved by the coordinator after each completed phase.
            for key, value in sorted(self.source.items()):
                self.target[key] = deepcopy(value)
                job.checkpoint["last_key"] = key
            job.source_count, job.target_count = len(self.source), len(self.target)
        elif job.phase in {MigrationPhase.VERIFYING, MigrationPhase.SHADOW_READ, MigrationPhase.CUTOVER}:
            job.mismatch_count = sum(
                self.source.get(key) != self.target.get(key) for key in self.source.keys() | self.target.keys())
            if job.mismatch_count:
                raise RuntimeError("migration verification has mismatches")
            job.checkpoint["target_hash"] = self.digest(self.target)
            if job.phase == MigrationPhase.CUTOVER:
                self.read_target = True
        elif job.phase == MigrationPhase.ROLLED_BACK:
            self.read_target = False
            self.dual_write = False
        return job
