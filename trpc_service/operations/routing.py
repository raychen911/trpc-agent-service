"""Fail-closed route resolution for new executions (FR-020, FR-021, DEC-004).

The PostgreSQL-backed route is the ONLY routing authority for new requests:
outages, missing routes, digest mismatches and contract incompatibilities
fail closed — never falling back to process defaults or the Redis cache.
The cache is refreshed after the authoritative read and its failure is
purely degraded, never authoritative.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from trpc_service.operations.canonical import contract_rank
from trpc_service.operations.models import (
    ConfigurationSnapshot,
    ExecutionConfigPin,
)
from trpc_service.operations.operations_errors import (
    ConfigurationIncompatible,
    ReleaseStateUnavailable,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def node_config_readiness(node_contract: str, snapshot: ConfigurationSnapshot) -> bool:
    """A mixed-version node below ``min_runtime_contract`` exits readiness."""

    return contract_rank(snapshot.min_runtime_contract) <= contract_rank(
        node_contract
    )


class RouteResolver:
    """Resolves the authoritative snapshot pin for one new execution."""

    def __init__(
        self,
        store: Any,
        *,
        cache: Any = None,
        node_contract: str | None = None,
    ) -> None:
        self._store = store
        self._cache = cache
        self._node_contract = node_contract

    async def resolve_for_new_execution(
        self, tenant_id: str, idempotency_key_digest: str, content_fingerprint: str
    ) -> ExecutionConfigPin:
        route = await self._store.get_route(tenant_id)
        if route is None:
            raise ReleaseStateUnavailable("no authoritative route for tenant")

        existing = await self._store.get_pin(tenant_id, idempotency_key_digest)
        if existing is not None:
            if existing.content_fingerprint != content_fingerprint:
                from trpc_service.operations.operations_errors import (
                    SnapshotDigestMismatch,
                )

                raise SnapshotDigestMismatch("pin fingerprint conflict")
            return existing

        if route.hard_gate_latched or route.candidate_snapshot_id is None:
            target = route.stable_snapshot_id  # last-good routing
        else:
            target = route.candidate_snapshot_id

        snapshot = await self._store.get_snapshot(tenant_id, target)
        if snapshot is None:
            raise ReleaseStateUnavailable("routed snapshot missing")
        if self._node_contract is not None and not node_config_readiness(
            self._node_contract, snapshot
        ):
            raise ConfigurationIncompatible(
                "node runtime contract below snapshot minimum"
            )

        pin = await self._store.create_pin(
            ExecutionConfigPin(
                tenant_id=tenant_id,
                idempotency_key_digest=idempotency_key_digest,
                content_fingerprint=content_fingerprint,
                snapshot_id=target,
                route_generation=route.route_generation,
                release_id=route.release_id,
                created_at=_now(),
            )
        )

        if self._cache is not None:
            try:
                await self._cache.refresh(route)
            except Exception:  # noqa: BLE001 - cache is never authoritative
                notes = getattr(self._store, "degraded_notes", None)
                if notes is not None:
                    notes.append(f"route_cache_refresh_failed:{tenant_id}")
        return pin
