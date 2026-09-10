"""Terminal-only reconciliation. This module deliberately cannot execute Agents."""

from __future__ import annotations

from typing import Any
from trpc_service.storage.contracts import ConditionalWriteFailed


class RecoveryReconciler:
    def __init__(
        self,
        durable_repository: Any,
        terminal_repository: Any,
        *,
        config_pin_store: Any = None,
    ) -> None:
        self.durable_repository = durable_repository
        self.terminal_repository = terminal_repository
        # Optional authoritative pin store: recovery reuses the ORIGINAL
        # execution pin instead of re-resolving a possibly changed route
        # (FR-020, DEC-004).
        self.config_pin_store = config_pin_store

    async def _original_pin(self, marker: Any) -> Any | None:
        if self.config_pin_store is None:
            return None
        get = getattr(self.config_pin_store, "get_pin", None)
        if get is None:
            return None
        tenant_id = marker.get("tenant_id") if isinstance(marker, dict) else getattr(marker, "tenant_id", None)
        key_digest = (
            marker.get("idempotency_key_digest")
            if isinstance(marker, dict)
            else getattr(marker, "idempotency_key_digest", None)
        )
        if not tenant_id or not key_digest:
            return None
        return await get(tenant_id, key_digest)

    async def run_once(self, tenant_scope: Any, limit: int = 100) -> int:
        markers = await self.durable_repository.get_pending(tenant_scope, limit)
        completed = 0
        for marker in markers:
            marker_id = marker.get("id") if isinstance(marker, dict) else marker.recovery_id
            digest = marker.get("result_digest", "saved") if isinstance(marker, dict) else marker.result_digest
            try:
                pin = await self._original_pin(marker)
                if pin is not None:
                    if isinstance(marker, dict):
                        marker = dict(marker)
                        marker["config_snapshot_id"] = pin.snapshot_id
                        marker["route_generation"] = pin.route_generation
                    else:
                        marker.config_snapshot_id = pin.snapshot_id
                await self.terminal_repository.complete_from_recovery(marker)
                await self.durable_repository.mark_reconciled(tenant_scope, marker_id, digest)
                completed += 1
            except ConditionalWriteFailed:
                await self.durable_repository.mark_conflict_review(
                    tenant_scope, marker_id, "terminal_cas_conflict"
                )
        return completed
