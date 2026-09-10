"""Persistent hard-gate enforcement point (FR-019, FR-010, DEC-004).

When a safety or consistency violation is detected the enforcement point
records a hard gate signal AND latches the tenant route inside the SAME
transaction as the formal audit — the latch is durable and does not depend
on droppable telemetry. Route resolution sees the latch and immediately
selects last-good for new requests.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from trpc_service.operations.gates import HARD_GATE_TYPES
from trpc_service.operations.models import ReleaseGateSignal


def _now() -> datetime:
    return datetime.now(timezone.utc)


def hard_signal_digest(
    tenant_id: str, release_id: str, gate_type: str, evidence_digest: str
) -> str:
    """Stable digest deduplicating identical violation reports."""

    return hashlib.sha256(
        f"hard|{tenant_id}|{release_id}|{gate_type}|{evidence_digest}".encode("utf-8")
    ).hexdigest()


class HardGateEnforcementPoint:
    """Durable latch writer bound to persistent enforcement evidence."""

    def __init__(self, store: Any, *, actor_digest: str = "governance_enforcement") -> None:
        self._store = store
        self._actor_digest = actor_digest

    async def report_violation(
        self,
        *,
        tenant_id: str,
        release_id: str,
        gate_type: str,
        evidence_digest: str | None,
    ) -> ReleaseGateSignal:
        if gate_type not in HARD_GATE_TYPES:
            raise ValueError(f"unknown hard gate type {gate_type!r}")
        if not evidence_digest:
            raise ValueError(
                "hard signals must be bound to persistent enforcement evidence"
            )
        signal_digest = hard_signal_digest(
            tenant_id, release_id, gate_type, evidence_digest
        )
        signal = ReleaseGateSignal(
            signal_id=str(uuid5(NAMESPACE_URL, f"hard-gate:{signal_digest}")),
            tenant_id=tenant_id,
            release_id=release_id,
            signal_digest=signal_digest,
            gate_type=gate_type,
            severity="hard",
            observation_window=300,
            sample_count=1,
            observed_value=None,
            evidence_digest=evidence_digest,
            observed_at=_now(),
        )
        async with self._store.transaction():
            recorded = await self._store.record_gate_signal(signal)
            await self._store.latch_hard_gate(
                tenant_id, signal=recorded, actor_digest=self._actor_digest
            )
        return recorded
