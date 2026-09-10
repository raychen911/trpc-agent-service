"""T042 RED: deduplicated alert state machine (FR-014/FR-015, DEC-003).

PENDING → FIRING → RECOVERING → RESOLVED with sustained windows, fingerprint
dedup across roles/components/scopes, CAS state versions, at-least-once
notification ids (fingerprint:state_version) and a body that never contains
tenant raw values, user content or secrets.
"""

from __future__ import annotations

import importlib

from tests.observability_support import (
    FIXED_OBS_UTC,
    obs_tenants,
    stable_scope_digest,
)

from datetime import timedelta


def _load():
    try:
        return importlib.import_module("trpc_service.observability.alerts")
    except ModuleNotFoundError:
        return None


def _machine():
    alerts = _load()
    assert alerts is not None, "trpc_service.observability.alerts is not implemented"
    return alerts.AlertStateMachine()


def _keys() -> dict:
    alerts = _load()
    assert alerts is not None
    return dict(
        rule_id="dependency_down",
        severity="critical",
        role="worker",
        component="data",
        scope_digest=stable_scope_digest(obs_tenants()[0].tenant_id),
        stable_reason="postgres_authoritative_down",
    )


def _fingerprint() -> str:
    alerts = _load()
    assert alerts is not None
    return alerts.alert_fingerprint(**_keys())


def test_fingerprint_is_deterministic_and_distinguishing() -> None:
    alerts = _load()
    assert alerts is not None
    base = _fingerprint()
    assert base == alerts.alert_fingerprint(**_keys())
    different_scope = dict(_keys())
    different_scope["scope_digest"] = stable_scope_digest("tenant-beta")
    assert alerts.alert_fingerprint(**different_scope) != base
    different_reason = dict(_keys())
    different_reason["stable_reason"] = "redis_lease_down"
    assert alerts.alert_fingerprint(**different_reason) != base
    assert base.startswith("sha256:")


def test_pending_needs_sustained_window_before_firing() -> None:
    machine = _machine()
    incident, notified = machine.transition(None, firing=True, observed_at=FIXED_OBS_UTC, keys=_keys())
    assert incident.state == "pending"
    assert notified is None
    # A second firing observation inside the same sustained window still
    # keeps PENDING (occurrences merge, no notification yet).
    incident, notified = machine.transition(
        incident, firing=True, observed_at=FIXED_OBS_UTC + timedelta(seconds=5)
    )
    assert incident.state == "pending"
    assert notified is None
    assert incident.occurrence_count == 2


def test_firing_emits_exactly_one_notification_with_cas_version() -> None:
    machine = _machine()
    incident = None
    for _ in range(machine.fire_window):
        incident, notified = machine.transition(
            incident, firing=True, observed_at=FIXED_OBS_UTC,
            keys=_keys() if incident is None else None,
        )
    assert incident.state == "firing"
    assert notified is not None
    assert notified == f"{incident.fingerprint}:{incident.state_version}"
    assert incident.state_version >= 2
    # Continued firing merges occurrences: no new notification, no version bump.
    incident, notified = machine.transition(
        incident, firing=True, observed_at=FIXED_OBS_UTC + timedelta(seconds=10)
    )
    assert incident.state == "firing"
    assert notified is None
    assert incident.occurrence_count == machine.fire_window + 1


def test_recovery_requires_sustained_stability_before_resolved() -> None:
    machine = _machine()
    incident = None
    for _ in range(machine.fire_window):
        incident, _ = machine.transition(
            incident, firing=True, observed_at=FIXED_OBS_UTC,
            keys=_keys() if incident is None else None,
        )
    # Recovery starts immediately with one notification.
    incident, notified = machine.transition(
        incident, firing=False, observed_at=FIXED_OBS_UTC + timedelta(seconds=20)
    )
    assert incident.state == "recovering"
    assert notified is not None
    # Instability: a new firing observation during RECOVERING re-arms FIRING.
    incident, notified = machine.transition(
        incident, firing=True, observed_at=FIXED_OBS_UTC + timedelta(seconds=25)
    )
    assert incident.state == "firing"
    # Sustained recovery through the resolve window closes the alert once.
    for seconds in range(30, 30 + machine.resolve_window):
        incident, notified = machine.transition(
            incident, firing=False,
            observed_at=FIXED_OBS_UTC + timedelta(seconds=seconds),
        )
    assert incident.state == "resolved"
    assert notified is not None
    assert notified == f"{incident.fingerprint}:{incident.state_version}"
    assert incident.resolved_at is not None


def test_same_fingerprint_observations_merge_into_one_incident() -> None:
    machine = _machine()
    alerts = _load()
    keys = _keys()
    first = None
    for _ in range(machine.fire_window):
        first, _ = machine.transition(
            first, firing=True, observed_at=FIXED_OBS_UTC,
            keys=_keys() if first is None else None,
        )
    assert first.state == "firing"
    # The SAME cause re-observed after resolution reopens the SAME incident
    # id (merged), not a second logical alert.
    resolved = first
    for seconds in range(20, 20 + machine.resolve_window):
        resolved, _ = machine.transition(
            resolved, firing=False, observed_at=FIXED_OBS_UTC + timedelta(seconds=seconds)
        )
    assert resolved.state == "resolved"
    assert resolved.incident_id == first.incident_id
    reopened, _ = machine.transition(
        resolved, firing=True, observed_at=FIXED_OBS_UTC + timedelta(seconds=90)
    )
    assert reopened.incident_id == first.incident_id
    assert reopened.state == "pending"


def test_flapping_dependency_dedups_to_single_logical_alert() -> None:
    machine = _machine()
    keys = _keys()
    incident = None
    notifications: list[str] = []
    states: list[str] = []
    # A dependency flapping up/down for a long time never sustains long
    # enough to fire: zero notifications, all observations merged into one
    # logical incident's occurrence counter.
    for step in range(40):
        firing = step % 4 < 2  # alternate bursts
        incident, notified = machine.transition(
            incident,
            firing=firing,
            observed_at=FIXED_OBS_UTC + timedelta(seconds=step * 15),
            keys=keys if incident is None else None,
        )
        if notified is not None:
            notifications.append(notified)
            states.append(incident.state)
    assert notifications == [], "a pure flap must not sustain into a firing alert"
    assert incident.state == "pending"
    assert incident.occurrence_count == 20


def test_notification_body_is_safe() -> None:
    alerts = _load()
    assert alerts is not None
    machine = _machine()
    incident = None
    for _ in range(machine.fire_window):
        incident, _ = machine.transition(
            incident, firing=True, observed_at=FIXED_OBS_UTC,
            keys=_keys() if incident is None else None,
        )
    notifier = alerts.SafeAlertNotifier()
    import asyncio

    notification_id = asyncio.run(notifier.notify(incident))
    assert notification_id == f"{incident.fingerprint}:{incident.state_version}"
    body = notifier.last_body()
    blob = str(body)
    for tenant in obs_tenants():
        assert tenant.tenant_id not in blob
    for forbidden in ("postgres://", "redis://", "password", "secret", "dsn", "select "):
        assert forbidden not in blob.lower()
    # Required impact fields per FR-014.
    for key in ("rule_id", "severity", "state", "first_observed_at", "last_observed_at",
                "occurrence_count", "scope_digest", "stable_reason", "recommended_action"):
        assert key in body, key


def test_severity_is_a_bounded_domain() -> None:
    alerts = _load()
    assert alerts is not None
    assert alerts.SEVERITIES == ("critical", "warning", "info")
    for bad in ("panic", "sev1", ""):
        try:
            alerts.AlertStateMachine().validate_severity(bad)
        except ValueError:
            continue
        raise AssertionError(f"severity {bad!r} must be rejected")
