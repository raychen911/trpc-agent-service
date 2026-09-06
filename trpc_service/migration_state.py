"""Resumable tenant migration state machine."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python 3.10 compatibility
    from enum import Enum

    class StrEnum(str, Enum):
        pass


def _now() -> str:
    return datetime.now(UTC).isoformat()


class MigrationPhase(StrEnum):
    PREPARE = "prepare"
    BACKFILL = "backfill"
    SHADOW_READ = "shadow_read"
    DUAL_WRITE = "dual_write"
    CUTOVER = "cutover"
    VERIFY = "verify"
    CLEANUP = "cleanup"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


_TRANSITIONS: dict[MigrationPhase, set[MigrationPhase]] = {
    MigrationPhase.PREPARE: {MigrationPhase.BACKFILL, MigrationPhase.FAILED, MigrationPhase.ROLLED_BACK},
    MigrationPhase.BACKFILL: {MigrationPhase.SHADOW_READ, MigrationPhase.FAILED, MigrationPhase.ROLLED_BACK},
    MigrationPhase.SHADOW_READ: {MigrationPhase.DUAL_WRITE, MigrationPhase.FAILED, MigrationPhase.ROLLED_BACK},
    MigrationPhase.DUAL_WRITE: {MigrationPhase.CUTOVER, MigrationPhase.FAILED, MigrationPhase.ROLLED_BACK},
    MigrationPhase.CUTOVER: {MigrationPhase.VERIFY, MigrationPhase.FAILED, MigrationPhase.ROLLED_BACK},
    MigrationPhase.VERIFY: {MigrationPhase.CLEANUP, MigrationPhase.FAILED, MigrationPhase.ROLLED_BACK},
    MigrationPhase.CLEANUP: set(),
    MigrationPhase.ROLLED_BACK: set(),
    MigrationPhase.FAILED: {MigrationPhase.PREPARE, MigrationPhase.ROLLED_BACK},
}


@dataclass(slots=True)
class MigrationState:
    migration_id: str
    tenant_id: str
    source_backend: str
    target_backend: str
    phase: MigrationPhase = MigrationPhase.PREPARE
    version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)
    updated_at: str = field(default_factory=_now)

    def transition(self, phase: MigrationPhase | str, *, actor: str = "system", **metadata: Any) -> MigrationState:
        next_phase = MigrationPhase(phase)
        if next_phase not in _TRANSITIONS[self.phase]:
            raise ValueError(f"invalid migration transition: {self.phase.value} -> {next_phase.value}")
        entry = {
            "from": self.phase.value,
            "to": next_phase.value,
            "actor": actor,
            "at": _now(),
            "metadata": dict(metadata),
        }
        self.history.append(entry)
        self.phase = next_phase
        self.metadata.update(metadata)
        self.version += 1
        self.updated_at = entry["at"]
        return self

    def fail(self, error: str, *, actor: str = "system") -> MigrationState:
        return self.transition(MigrationPhase.FAILED, actor=actor, error=str(error)[:1000])

    def rollback(self, reason: str, *, actor: str = "system") -> MigrationState:
        if self.phase == MigrationPhase.FAILED:
            return self.transition(MigrationPhase.ROLLED_BACK, actor=actor, reason=str(reason)[:1000])
        return self.transition(MigrationPhase.ROLLED_BACK, actor=actor, reason=str(reason)[:1000])

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["phase"] = self.phase.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MigrationState:
        return cls(
            migration_id=str(data["migration_id"]),
            tenant_id=str(data["tenant_id"]),
            source_backend=str(data["source_backend"]),
            target_backend=str(data["target_backend"]),
            phase=MigrationPhase(data.get("phase", MigrationPhase.PREPARE)),
            version=int(data.get("version", 1)),
            metadata=dict(data.get("metadata", {})),
            history=list(data.get("history", [])),
            updated_at=str(data.get("updated_at", _now())),
        )


def new_migration(
    migration_id: str,
    tenant_id: str,
    source_backend: str,
    target_backend: str,
    *,
    metadata: dict[str, Any] | None = None,
) -> MigrationState:
    return MigrationState(
        migration_id=str(migration_id),
        tenant_id=str(tenant_id),
        source_backend=str(source_backend),
        target_backend=str(target_backend),
        metadata=dict(metadata or {}),
    )
