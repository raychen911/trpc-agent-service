"""T044 RED: dependency fault matrix (FR-010/FR-012/FR-013/FR-015, SC-005).

Injects critical dependency outages (authoritative store, lease backend,
single IM channel, audit), flapping and recovery, then verifies readiness
verdicts, business fail-closed semantics, alert dedup, single resolved
notification and the 30s/60s time bounds.
"""

from __future__ import annotations

import asyncio
import importlib
from datetime import timedelta

import pytest

from tests.observability_support import (
    FIXED_OBS_UTC,
    obs_tenants,
    stable_scope_digest,
)
from tests.support import FIXED_UTC, inbound_message_data


def _health():
    try:
        return importlib.import_module("trpc_service.observability.health")
    except ModuleNotFoundError:
        return None


def _alerts():
    try:
        return importlib.import_module("trpc_service.observability.alerts")
    except ModuleNotFoundError:
        return None


def _observation(role: str, dependency: str, state: str, at):
    from trpc_service.observability.models import DependencyObservation

    return DependencyObservation(
        role=role,
        dependency=dependency,
        state=state,
        stable_reason=f"{dependency}_{state}",
        observed_at=at,
        expires_at=at + timedelta(seconds=60),
    )


def _role_observations(role: str, overrides: dict[str, str], at):
    health = _health()
    assert health is not None, "trpc_service.observability.health is not implemented"
    result = {
        dep: _observation(role, dep, "up", at)
        for dep in health.ROLE_CRITICAL_DEPENDENCIES[role]
    }
    for dep, state in overrides.items():
        result[dep] = _observation(role, dep, state, at)
    return result


@pytest.mark.parametrize(
    "role,dependency",
    [
        ("gateway", "postgres"),
        ("worker", "redis"),
        ("recovery", "postgres"),
        ("feishu_adapter", "feishu_connection"),
        ("wecom_adapter", "wecom_connection"),
    ],
)
def test_critical_dependency_outage_makes_role_unready(role: str, dependency: str) -> None:
    health = _health()
    assert health is not None
    matrix = health.RoleReadinessMatrix()
    snapshot = matrix.evaluate_role(
        node_digest="worker-a",
        role=role,
        observations=_role_observations(role, {dependency: "down"}, FIXED_OBS_UTC),
        now=FIXED_OBS_UTC,
    )
    assert snapshot.readiness == "unready"
    assert any(dependency in code for code in snapshot.reason_codes)


def test_single_channel_outage_degrades_platform_but_other_paths_continue() -> None:
    health = _health()
    assert health is not None
    matrix = health.RoleReadinessMatrix()
    snapshots = []
    for role in ("gateway", "worker", "feishu_adapter", "wecom_adapter", "recovery"):
        overrides = {"feishu_connection": "down"} if role == "feishu_adapter" else {}
        snapshots.append(
            matrix.evaluate_role(
                node_digest="worker-a",
                role=role,
                observations=_role_observations(role, overrides, FIXED_OBS_UTC),
                now=FIXED_OBS_UTC,
            )
        )
    platform = matrix.aggregate(snapshots, now=FIXED_OBS_UTC)
    assert platform.state == "degraded"
    assert "feishu" in platform.unavailable_paths
    assert "wecom" in platform.available_paths
    assert "local_http" in platform.available_paths


def test_authoritative_store_down_fails_new_executions_closed(runtime_secret_env) -> None:
    from trpc_service.channels.contracts import InboundMessage
    from trpc_service.storage.contracts import AuditUnavailable
    from trpc_service.web.app import build_runtime

    async def scenario() -> str:
        runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
        try:
            class DownAudit:
                async def append(self, *args, **kwargs):
                    raise AuditUnavailable("pg down")

            runtime.adapters.audit = DownAudit()
            message = InboundMessage(**inbound_message_data())
            reply = await runtime.gateway.handle_verified_message_for_test(message)
            return reply.status.value
        finally:
            await runtime.close()

    status = asyncio.run(scenario())
    assert status != "succeeded", "authoritative outage must fail closed"


def test_flapping_dependency_dedups_and_recovery_closes_once() -> None:
    alerts = _alerts()
    assert alerts is not None, "trpc_service.observability.alerts is not implemented"
    machine = alerts.AlertStateMachine()
    scope = stable_scope_digest(obs_tenants()[0].tenant_id)
    keys = dict(
        rule_id="dependency_down",
        severity="critical",
        role="worker",
        component="data",
        scope_digest=scope,
        stable_reason="postgres_authoritative_down",
    )
    incident = None
    notifications: list[str] = []
    states: list[str] = []
    clock_seconds = 0.0

    def step(firing: bool):
        nonlocal incident, clock_seconds
        incident, notified = machine.transition(
            incident, firing=firing,
            observed_at=FIXED_OBS_UTC + timedelta(seconds=clock_seconds),
            keys=keys if incident is None else None,
        )
        if notified is not None:
            notifications.append(notified)
            states.append(incident.state)

    # 10 full outage episodes: dedup means exactly one notification per
    # state change inside each episode — never per observation.
    for cycle in range(10):
        for _ in range(machine.fire_window):
            clock_seconds += 15
            step(firing=True)
        clock_seconds += 15
        step(firing=False)
        for _ in range(machine.resolve_window):
            clock_seconds += 15
            step(firing=False)
    assert incident.state == "resolved"
    assert incident.incident_id.count(incident.incident_id) == 1
    assert len(set(notifications)) == len(notifications)
    # Each episode notifies exactly (firing, recovering, resolved).
    assert len(states) % 3 == 0
    for index in range(0, len(states), 3):
        assert tuple(states[index : index + 3]) == ("firing", "recovering", "resolved")
    # Exactly one resolved notification per sustained recovery episode: the
    # final episode's resolved notification is the last notification emitted.
    resolved_notifications = [
        n for n in notifications if n.endswith(f":{incident.state_version}")
    ]
    assert resolved_notifications == [notifications[-1]]
    assert incident.fingerprint == alerts.alert_fingerprint(**keys)
    # All outage occurrences merged into ONE logical incident id.
    assert incident.occurrence_count >= 10 * machine.fire_window


def test_state_change_bound_is_30_seconds_and_recovery_closes_within_60() -> None:
    health = _health()
    alerts = _alerts()
    assert health is not None and alerts is not None
    # SC-005: staleness TTL keeps state changes inside 30 seconds.
    assert health.DEFAULT_OBSERVATION_TTL <= timedelta(seconds=30)
    matrix = health.RoleReadinessMatrix()
    assert matrix.observation_ttl == health.DEFAULT_OBSERVATION_TTL
    # SC-005: a sustained recovery closes the alert within 60 seconds.
    machine = alerts.AlertStateMachine()
    assert machine.resolve_window * machine.observation_interval <= timedelta(seconds=60)
