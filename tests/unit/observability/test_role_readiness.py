"""T041 RED: role-level readiness matrix (FR-012/FR-013, DEC-003).

Table-driven: for every platform role, a critical dependency that is down
or unknown makes the role unready; ordinary telemetry or a single IM
channel only degrades affected paths; liveness is independent of external
dependencies; stale observations expire to unknown instead of extending
readiness forever.
"""

from __future__ import annotations

import importlib
from datetime import timedelta

from tests.observability_support import FIXED_OBS_UTC, obs_nodes


def _load():
    try:
        return importlib.import_module("trpc_service.observability.health")
    except ModuleNotFoundError:
        return None


ALL_ROLES = ("gateway", "worker", "feishu_adapter", "wecom_adapter", "recovery")


def _observation(role: str, dependency: str, state: str, *, age_seconds: float = 0):
    from trpc_service.observability.models import DependencyObservation

    observed_at = FIXED_OBS_UTC - timedelta(seconds=age_seconds)
    return DependencyObservation(
        role=role,
        dependency=dependency,
        state=state,
        stable_reason=f"{dependency}_{state}",
        observed_at=observed_at,
        expires_at=observed_at + timedelta(seconds=60),
    )


def _matrix():
    health = _load()
    assert health is not None, "trpc_service.observability.health is not implemented"
    return health.RoleReadinessMatrix()


def _all_up(role: str):
    matrix_mod = _load()
    critical = matrix_mod.ROLE_CRITICAL_DEPENDENCIES[role]
    return {dep: _observation(role, dep, "up") for dep in critical}


def test_every_role_has_a_critical_dependency_matrix() -> None:
    matrix_mod = _load()
    assert matrix_mod is not None
    for role in ALL_ROLES:
        critical = matrix_mod.ROLE_CRITICAL_DEPENDENCIES.get(role)
        assert critical, f"role {role} must declare critical dependencies"
        assert len(critical) >= 1


def test_all_critical_up_means_ready_for_every_role() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    for role in ALL_ROLES:
        snapshot = matrix.evaluate_role(
            node_digest=node.node_id,
            role=role,
            observations=_all_up(role),
            now=FIXED_OBS_UTC,
        )
        assert snapshot.readiness == "ready", role
        assert snapshot.service_state == "ready", role
        assert snapshot.liveness == "ready"


def test_critical_dependency_down_makes_role_unready() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    for role in ALL_ROLES:
        for critical in _load().ROLE_CRITICAL_DEPENDENCIES[role]:
            observations = _all_up(role)
            observations[critical] = _observation(role, critical, "down")
            snapshot = matrix.evaluate_role(
                node_digest=node.node_id,
                role=role,
                observations=observations,
                now=FIXED_OBS_UTC,
            )
            assert snapshot.readiness == "unready", (role, critical)
            assert snapshot.service_state == "unready", (role, critical)
            assert any(critical in code for code in snapshot.reason_codes), (
                role,
                critical,
            )


def test_critical_dependency_unknown_makes_role_unready() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    role = "worker"
    observations = _all_up(role)
    observations["redis"] = _observation(role, "redis", "unknown")
    snapshot = matrix.evaluate_role(
        node_digest=node.node_id, role=role, observations=observations,
        now=FIXED_OBS_UTC,
    )
    assert snapshot.readiness == "unready"
    assert snapshot.service_state == "unready"


def test_missing_critical_observation_is_unknown_not_ready() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    role = "gateway"
    observations = _all_up(role)
    observations.pop("postgres")
    snapshot = matrix.evaluate_role(
        node_digest=node.node_id, role=role, observations=observations,
        now=FIXED_OBS_UTC,
    )
    # An unobserved critical dependency is unknown — never fabricated ready.
    assert snapshot.readiness == "unready"
    assert "postgres" in snapshot.dependency_states
    assert snapshot.dependency_states["postgres"] == "unknown"


def test_telemetry_down_is_degraded_not_unready() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    for role in ALL_ROLES:
        observations = _all_up(role)
        observations["telemetry"] = _observation(role, "telemetry", "down")
        snapshot = matrix.evaluate_role(
            node_digest=node.node_id, role=role, observations=observations,
            now=FIXED_OBS_UTC,
        )
        assert snapshot.readiness == "ready", role
        assert snapshot.service_state == "degraded", role


def test_single_channel_down_only_degrades_other_roles() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    # feishu is down: the feishu adapter itself is unready ...
    feishu_obs = _all_up("feishu_adapter")
    feishu_obs["feishu_connection"] = _observation(
        "feishu_adapter", "feishu_connection", "down"
    )
    feishu_snapshot = matrix.evaluate_role(
        node_digest=node.node_id, role="feishu_adapter",
        observations=feishu_obs, now=FIXED_OBS_UTC,
    )
    assert feishu_snapshot.readiness == "unready"
    # ... but the wecom adapter (and gateway/worker) stay serviceable.
    wecom_snapshot = matrix.evaluate_role(
        node_digest=node.node_id, role="wecom_adapter",
        observations=_all_up("wecom_adapter"), now=FIXED_OBS_UTC,
    )
    assert wecom_snapshot.readiness == "ready"


def test_liveness_is_independent_of_dependencies() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    role = "gateway"
    observations = _all_up(role)
    for dep in list(observations):
        observations[dep] = _observation(role, dep, "down")
    snapshot = matrix.evaluate_role(
        node_digest=node.node_id, role=role, observations=observations,
        now=FIXED_OBS_UTC,
    )
    assert snapshot.readiness == "unready"
    assert snapshot.liveness == "ready"


def test_stale_observations_expire_to_unknown() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    role = "gateway"
    observations = _all_up(role)
    # Observation older than the 30-second staleness TTL must not extend
    # readiness indefinitely (SC-05-adjacent: 30s state change bound).
    observations["postgres"] = _observation(role, "postgres", "up", age_seconds=31)
    snapshot = matrix.evaluate_role(
        node_digest=node.node_id, role=role, observations=observations,
        now=FIXED_OBS_UTC,
    )
    assert snapshot.readiness == "unready"
    assert snapshot.dependency_states["postgres"] == "unknown"


def test_platform_aggregate_is_path_level() -> None:
    matrix = _matrix()
    node = obs_nodes()[0]
    all_ready = [
        matrix.evaluate_role(
            node_digest=node.node_id, role=role, observations=_all_up(role),
            now=FIXED_OBS_UTC,
        )
        for role in ALL_ROLES
    ]
    ready_platform = matrix.aggregate(all_ready, now=FIXED_OBS_UTC)
    assert ready_platform.state == "ready"

    feishu_down = _all_up("feishu_adapter")
    feishu_down["feishu_connection"] = _observation(
        "feishu_adapter", "feishu_connection", "down"
    )
    degraded = [
        matrix.evaluate_role(
            node_digest=node.node_id, role="feishu_adapter",
            observations=feishu_down, now=FIXED_OBS_UTC,
        ),
    ] + [
        matrix.evaluate_role(
            node_digest=node.node_id, role=role, observations=_all_up(role),
            now=FIXED_OBS_UTC,
        )
        for role in ("gateway", "worker", "wecom_adapter", "recovery")
    ]
    degraded_platform = matrix.aggregate(degraded, now=FIXED_OBS_UTC)
    # A single channel path failing is degradation, not platform death.
    assert degraded_platform.state == "degraded"
    assert "feishu" in degraded_platform.unavailable_paths
    assert "wecom" in degraded_platform.available_paths

    postgres_down_everywhere = []
    for role in ALL_ROLES:
        observations = _all_up(role)
        if "postgres" in observations:
            observations["postgres"] = _observation(role, "postgres", "down")
        postgres_down_everywhere.append(
            matrix.evaluate_role(
                node_digest=node.node_id, role=role, observations=observations,
                now=FIXED_OBS_UTC,
            )
        )
    unready_platform = matrix.aggregate(postgres_down_everywhere, now=FIXED_OBS_UTC)
    assert unready_platform.state == "unready"
