"""Storage-domain records and concurrency errors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


class FencingConflict(RuntimeError):
    """A stale worker attempted to commit after its lease was replaced."""


class LeaseBusy(RuntimeError):
    """Another live worker currently owns the session."""


@dataclass(frozen=True)
class SessionLease:
    tenant_id: str
    session_id: str
    owner_id: str
    epoch: int
    expected_version: int
    expires_at: datetime
