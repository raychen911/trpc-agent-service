"""Migration states and verification reports."""

from dataclasses import dataclass
from enum import Enum


class MigrationState(str, Enum):
    PREPARING = "preparing"
    BACKFILLING = "backfilling"
    DUAL_WRITE = "dual_write"
    VERIFYING = "verifying"
    SHADOW_READ = "shadow_read"
    CUTOVER = "cutover"
    ROLLBACK_WINDOW = "rollback_window"
    COMPLETED = "completed"
    PAUSED = "paused"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_FINAL = "failed_final"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class VerificationReport:
    source_count: int
    target_count: int
    differences: tuple[str, ...]

    @property
    def matches(self) -> bool:
        return not self.differences and self.source_count == self.target_count


@dataclass(frozen=True)
class VectorMigrationReport:
    copied: int
    tombstones: int
