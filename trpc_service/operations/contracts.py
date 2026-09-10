"""Async ports for release operations, capacity acceptance and drain
(contracts/release-operations-contracts.md).

All commands are idempotent by ``command_id``, fenced by ``expected_revision``
and raise the stable operations errors from ``operations_errors`` (DEC-004).
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from trpc_service.operations.models import (
    CanaryRelease,
    CapacityComparison,
    CapacityRun,
    CapacityScenario,
    ConfigurationSnapshot,
    DrainSnapshot,
    ExecutionConfigPin,
    ReleaseGateSignal,
    TenantConfigRoute,
)


@runtime_checkable
class ConfigurationSnapshotRepository(Protocol):
    """Immutable, tenant-scoped snapshot storage (secret-ref-only payloads)."""

    async def create(self, snapshot: ConfigurationSnapshot) -> ConfigurationSnapshot: ...
    async def get(self, tenant_id: str, snapshot_id: str) -> ConfigurationSnapshot: ...
    async def verify_compatible(
        self, tenant_id: str, snapshot_id: str, runtime_contract: str
    ) -> bool: ...


@runtime_checkable
class TenantConfigRouteRepository(Protocol):
    """Current routing authority for new executions; fails closed on outage."""

    async def resolve_for_new_execution(
        self, tenant_id: str, idempotency_key_digest: str, content_fingerprint: str
    ) -> ExecutionConfigPin: ...
    async def get_route(self, tenant_id: str) -> TenantConfigRoute: ...
    async def compare_and_route(
        self, tenant_id: str, expected_generation: int, candidate_snapshot_id: str | None
    ) -> TenantConfigRoute: ...


@runtime_checkable
class ReleaseRepository(Protocol):
    """Release state machine persistence with revision/fence CAS semantics."""

    async def create_release(self, release: CanaryRelease) -> CanaryRelease: ...
    async def validate(self, release_id: str, command_id: str) -> CanaryRelease: ...
    async def start_canary(self, release_id: str, command_id: str) -> CanaryRelease: ...
    async def advance(self, release_id: str, command_id: str) -> CanaryRelease: ...
    async def pause(
        self, release_id: str, command_id: str, reason_code: str
    ) -> CanaryRelease: ...
    async def resume(self, release_id: str, command_id: str) -> CanaryRelease: ...
    async def rollback(
        self, release_id: str, command_id: str, reason_code: str
    ) -> CanaryRelease: ...
    async def repair(self, release_id: str, command_id: str) -> CanaryRelease: ...


@runtime_checkable
class ReleaseCoordinator(Protocol):
    """Executes one idempotent release command end-to-end."""

    async def execute(
        self,
        release_id: str,
        command_id: str,
        action: str,
        expected_revision: int,
        actor_digest: str,
    ) -> CanaryRelease: ...


@runtime_checkable
class GateEvaluationPort(Protocol):
    """Evaluates quality/hard gates over the canary observation window."""

    async def evaluate(self, release_id: str) -> list[ReleaseGateSignal]: ...


@runtime_checkable
class CapacityHarnessPort(Protocol):
    """Formal capacity acceptance harness; results are local evidence only."""

    async def prepare(self, scenario: CapacityScenario) -> str: ...
    async def run(self, scenario: CapacityScenario, telemetry_mode: str) -> CapacityRun: ...
    async def compare(self, baseline: CapacityRun, enabled: CapacityRun) -> CapacityComparison: ...


@runtime_checkable
class DrainControllerPort(Protocol):
    """Forward-only drain lifecycle for graceful node shutdown."""

    async def begin(
        self, node_digest: str, role: str, deadline: datetime
    ) -> DrainSnapshot: ...
    async def snapshot(self, node_digest: str) -> DrainSnapshot: ...
    async def complete_or_handoff(self, node_digest: str) -> DrainSnapshot: ...
    async def expire(self, node_digest: str) -> DrainSnapshot: ...
