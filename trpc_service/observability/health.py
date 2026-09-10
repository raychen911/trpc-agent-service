"""Role-level readiness matrix and path-level health aggregation.

Implements ``HealthProbePort`` semantics (FR-012/FR-013, DEC-003): each
platform role declares its CRITICAL dependencies; a critical dependency
that is down, unknown or simply unobserved makes the role ``unready``.
Ordinary telemetry and foreign IM channels only ``degrade`` the affected
paths. Liveness is a process property, independent of every external
dependency. Observations expire after the staleness TTL so readiness can
never be extended indefinitely by stale data (SC-005: state changes
surface within 30 seconds).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable

from trpc_service.observability import taxonomy
from trpc_service.observability.models import (
    DependencyObservation,
    PlatformHealthSnapshot,
    RoleReadinessSnapshot,
)

DEFAULT_OBSERVATION_TTL = timedelta(seconds=30)

# FR-013 role-level critical dependency matrix. Extending this table (and
# taxonomy.DEPENDENCIES) is the only sanctioned way to change readiness
# semantics — adapters never invent their own matrix (NFR-007).
ROLE_CRITICAL_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    # Entry roles need trusted configuration, channel binding and
    # idempotency state (authoritative store) plus the audit boundary.
    "gateway": ("postgres", "channel_binding_identity", "governance"),
    # Execution roles need shared session/data state, leases/fencing,
    # the governance/audit boundary and an initialised Runner.
    "worker": ("postgres", "redis", "governance", "runner_initialisation"),
    # Channel roles need valid authentication, their own connection and a
    # reachable entry point.
    "feishu_adapter": ("postgres", "channel_binding_identity", "feishu_connection"),
    "wecom_adapter": ("postgres", "channel_binding_identity", "wecom_connection"),
    # Recovery needs the authoritative store and lease coordination.
    "recovery": ("postgres", "redis"),
}

# Dependencies that are business paths rather than cross-cutting health.
_CHANNEL_PATHS: dict[str, str] = {
    "feishu_connection": "feishu",
    "wecom_connection": "wecom",
}


class RoleReadinessMatrix:
    """Pure evaluation of role snapshots and path-level aggregation."""

    def __init__(
        self,
        *,
        observation_ttl: timedelta = DEFAULT_OBSERVATION_TTL,
    ) -> None:
        self.observation_ttl = observation_ttl

    def evaluate_role(
        self,
        *,
        node_digest: str,
        role: str,
        observations: dict[str, DependencyObservation],
        now: datetime,
    ) -> RoleReadinessSnapshot:
        from trpc_service.observability.service import node_digest_of

        taxonomy.validate_role(role)
        critical = ROLE_CRITICAL_DEPENDENCIES.get(role, ())
        dependency_states: dict[str, str] = {}
        reason_codes: list[str] = []
        degraded = False
        for dependency in critical:
            observation = observations.get(dependency)
            state = self._effective_state(observation, now)
            dependency_states[dependency] = state
            if state in ("down", "unknown"):
                reason_codes.append(f"{dependency}_{state}")
            elif state == "degraded":
                degraded = True
        # Non-critical observations (telemetry, other channels) only degrade.
        for dependency, observation in observations.items():
            if dependency in dependency_states:
                continue
            state = self._effective_state(observation, now)
            dependency_states[dependency] = state
            if state in ("down", "unknown", "degraded"):
                degraded = True
        unready = any(code for code in reason_codes)
        readiness = "unready" if unready else "ready"
        service_state = "unready" if unready else ("degraded" if degraded else "ready")
        return RoleReadinessSnapshot(
            node_digest=node_digest_of(node_digest),
            role=role,
            liveness="ready",
            readiness=readiness,
            service_state=service_state,
            changed_at=now,
            observed_at=now,
            dependency_states=dependency_states,
            reason_codes=tuple(reason_codes),
        )

    def _effective_state(
        self, observation: DependencyObservation | None, now: datetime
    ) -> str:
        if observation is None:
            return "unknown"
        if observation.expires_at is not None and observation.expires_at <= now:
            return "unknown"
        if observation.observed_at + self.observation_ttl <= now:
            # Stale data must not extend readiness indefinitely (SC-005).
            return "unknown"
        return observation.state

    def aggregate(
        self, snapshots: Iterable[RoleReadinessSnapshot], *, now: datetime
    ) -> PlatformHealthSnapshot:
        snapshots = list(snapshots)
        available_paths: set[str] = {"local_http"}
        unavailable_paths: set[str] = set()
        role_counts: dict[str, int] = {}
        reason_codes: set[str] = set()
        for snapshot in snapshots:
            role_counts[snapshot.role] = role_counts.get(snapshot.role, 0) + 1
            reason_codes.update(snapshot.reason_codes)
            channel_path = next(
                (
                    path
                    for dep, path in _CHANNEL_PATHS.items()
                    if snapshot.dependency_states.get(dep) in ("down", "unknown")
                    and dep in ROLE_CRITICAL_DEPENDENCIES.get(snapshot.role, ())
                ),
                None,
            )
            if channel_path is not None:
                unavailable_paths.add(channel_path)
        # A channel path is available when at least one of its roles is ready.
        for dep, path in _CHANNEL_PATHS.items():
            serving = [
                s
                for s in snapshots
                if s.role.endswith(f"{path.split('_')[0]}_adapter")
                and s.readiness == "ready"
            ]
            if serving:
                available_paths.add(path)
        state = "unready"
        if snapshots and all(s.readiness == "ready" for s in snapshots):
            state = "ready"
        elif any(s.readiness == "ready" for s in snapshots):
            state = "degraded"
        if not snapshots:
            state = "unready"
        if state == "ready" and unavailable_paths:
            # ready platform cannot report unavailable paths — degrade.
            state = "degraded"
        return PlatformHealthSnapshot(
            state=state,
            generated_at=now,
            available_paths=tuple(sorted(available_paths - unavailable_paths)),
            unavailable_paths=tuple(sorted(unavailable_paths)),
            role_counts=role_counts,
            reason_codes=tuple(sorted(reason_codes)),
        )


class HealthMonitor:
    """Binds a readiness matrix to live probe callables for one process.

    Each probe is a cheap, short-timeout callable returning a dependency
    state string; an absent probe yields ``unknown`` — never a fabricated
    ``up`` (FR-013).
    """

    def __init__(
        self,
        *,
        role: str,
        node_id: str,
        probes: dict[str, Any] | None = None,
        matrix: RoleReadinessMatrix | None = None,
        clock: Any = None,
    ) -> None:
        from datetime import timezone

        taxonomy.validate_role(role)
        self._role = role
        self._node_id = node_id
        self._probes = dict(probes or {})
        self._matrix = matrix or RoleReadinessMatrix()
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def readiness(self) -> RoleReadinessSnapshot:
        now = self._clock()
        observations: dict[str, DependencyObservation] = {}
        for dependency in ROLE_CRITICAL_DEPENDENCIES.get(self._role, ()):
            probe = self._probes.get(dependency)
            state = "unknown" if probe is None else str(probe())
            observations[dependency] = DependencyObservation(
                role=self._role,
                dependency=dependency,
                state=state,
                stable_reason=f"{dependency}_{state}",
                observed_at=now,
                expires_at=now + DEFAULT_OBSERVATION_TTL,
            )
        return self._matrix.evaluate_role(
            node_digest=self._node_id,
            role=self._role,
            observations=observations,
            now=now,
        )

    async def platform(self) -> PlatformHealthSnapshot:
        snapshot = await self.readiness()
        return self._matrix.aggregate([snapshot], now=self._clock())


class HealthProbeService:
    """Async in-process implementation of ``HealthProbePort``."""

    def __init__(
        self,
        *,
        probes: dict[tuple[str, str], Any] | None = None,
        nodes: Iterable[tuple[str, str]] | None = None,
        matrix: RoleReadinessMatrix | None = None,
        clock: Any = None,
    ) -> None:
        from datetime import timezone

        self._probes = dict(probes or {})
        self._nodes = list(nodes or [])
        self._matrix = matrix or RoleReadinessMatrix()
        self._observations: dict[tuple[str, str], DependencyObservation] = {}
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def probe_liveness(self) -> str:
        # The process answering the probe is, by construction, progressing.
        return "ready"

    async def probe_dependency(self, role: str, dependency: str) -> DependencyObservation:
        taxonomy.validate_role(role)
        taxonomy.validate_dependency(dependency)
        probe = self._probes.get((role, dependency))
        now = self._clock()
        if probe is None:
            return DependencyObservation(
                role=role,
                dependency=dependency,
                state="unknown",
                stable_reason=f"{dependency}_unobserved",
                observed_at=now,
                expires_at=now + DEFAULT_OBSERVATION_TTL,
            )
        state = probe() if callable(probe) else probe
        observation = DependencyObservation(
            role=role,
            dependency=dependency,
            state=str(state),
            stable_reason=f"{dependency}_{state}",
            observed_at=now,
            expires_at=now + DEFAULT_OBSERVATION_TTL,
        )
        self._observations[(role, dependency)] = observation
        return observation

    async def evaluate_role(self, node_digest: str, role: str) -> RoleReadinessSnapshot:
        now = self._clock()
        for dependency in ROLE_CRITICAL_DEPENDENCIES.get(role, ()):
            await self.probe_dependency(role, dependency)
        observations = {
            dep: obs
            for (obs_role, dep), obs in self._observations.items()
            if obs_role == role
        }
        return self._matrix.evaluate_role(
            node_digest=node_digest, role=role, observations=observations, now=now
        )

    async def aggregate(self) -> PlatformHealthSnapshot:
        now = self._clock()
        snapshots = [
            await self.evaluate_role(node_id, role)
            for node_id, role in self._nodes
        ]
        return self._matrix.aggregate(snapshots, now=now)
