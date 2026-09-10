"""Stable phase-seven data error types.

The canonical definitions live in :mod:`trpc_service.storage.contracts` so
legacy callers and the new data facade share one exception hierarchy.  This
module is a discoverable, dependency-light import surface for data adapters.
"""
from .contracts import (
    AuditUnavailable,
    ContentTooLarge,
    DigestMismatch,
    ForwardRepairRequired,
    IdempotencyConflict,
    MigrationConflict,
    MigrationWritePaused,
    SequenceGap,
    StaleFence,
    StateBackendUnavailable,
    SummaryConflict,
    TenantFilterUnsupported,
    TenantScopeInvalid,
    VersionConflict,
)

__all__ = [
    "AuditUnavailable", "ContentTooLarge", "DigestMismatch", "ForwardRepairRequired",
    "IdempotencyConflict", "MigrationConflict", "MigrationWritePaused", "SequenceGap",
    "StaleFence", "StateBackendUnavailable", "SummaryConflict", "TenantFilterUnsupported",
    "TenantScopeInvalid", "VersionConflict",
]
