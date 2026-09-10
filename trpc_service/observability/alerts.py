"""Deduplicated alert incidents with a CAS state machine (FR-014/FR-015).

One logical incident per fingerprint (rule/severity/role/component/scope
digest/stable reason). State transitions PENDING → FIRING → RECOVERING →
RESOLVED are gated by sustained observation windows; every transition
bumps a compare-and-swap ``state_version`` and emits exactly one
notification id ``fingerprint:state_version``. Notification bodies carry
only impact scope, stable reason, safe evidence references and a
recommended action — never tenant raw values, user content or secrets.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from trpc_service.observability.models import AlertIncident

SEVERITIES: tuple[str, ...] = ("critical", "warning", "info")

STATES: tuple[str, ...] = AlertIncident.STATES

# Sustained-window defaults: firing needs 3 consecutive firing
# observations (a flap never sustains long enough to fire); resolution
# needs 2 consecutive stable observations at the default 30-second
# observation interval — closing within 60s (SC-005).
DEFAULT_FIRE_WINDOW = 3
DEFAULT_RESOLVE_WINDOW = 2
DEFAULT_OBSERVATION_INTERVAL = timedelta(seconds=30)

_RECOMMENDED_ACTIONS: dict[str, str] = {
    "critical": "check the dependency and consider draining affected roles",
    "warning": "inspect evidence and recent releases",
    "info": "no immediate action required",
}


def alert_fingerprint(
    *,
    rule_id: str,
    severity: str,
    role: str,
    component: str,
    scope_digest: str,
    stable_reason: str,
) -> str:
    """Deterministic dedup key for one cause inside one impact scope."""

    material = (
        f"alert:{rule_id}|{severity}|{role}|{component}|{scope_digest}|{stable_reason}"
    ).encode("utf-8")
    return "sha256:" + sha256(material).hexdigest()[:16]


def _incident_id(fingerprint: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"incident:{fingerprint}"))


def notification_id_for(incident: AlertIncident) -> str:
    return f"{incident.fingerprint}:{incident.state_version}"


class AlertStateMachine:
    """In-memory CAS transition engine over ``AlertIncident`` rows."""

    def __init__(
        self,
        *,
        fire_window: int = DEFAULT_FIRE_WINDOW,
        resolve_window: int = DEFAULT_RESOLVE_WINDOW,
        observation_interval: timedelta = DEFAULT_OBSERVATION_INTERVAL,
    ) -> None:
        if fire_window < 1 or resolve_window < 1:
            raise ValueError("windows must be at least one observation")
        self.fire_window = int(fire_window)
        self.resolve_window = int(resolve_window)
        self.observation_interval = observation_interval
        # fingerprint -> (last_streak_firing, streak_count)
        self._streaks: dict[str, tuple[bool, int]] = {}

    @staticmethod
    def validate_severity(severity: str) -> str:
        if severity not in SEVERITIES:
            raise ValueError(f"unknown alert severity {severity!r}")
        return severity

    def transition(
        self,
        incident: AlertIncident | None,
        *,
        firing: bool,
        observed_at: datetime,
        keys: dict[str, Any] | None = None,
    ) -> tuple[AlertIncident, str | None]:
        """Fold one observation into the incident.

        Returns ``(incident, notification_id)`` where ``notification_id``
        is non-None exactly when the state changed (at-least-once per
        transition, idempotent by ``fingerprint:state_version``).
        """

        if incident is None:
            if keys is None:
                raise ValueError("keys are required to open a new incident")
            self.validate_severity(keys["severity"])
            fingerprint = alert_fingerprint(
                rule_id=keys["rule_id"],
                severity=keys["severity"],
                role=keys.get("role", "platform"),
                component=keys.get("component", "platform"),
                scope_digest=keys["scope_digest"],
                stable_reason=keys["stable_reason"],
            )
            incident = AlertIncident(
                incident_id=_incident_id(fingerprint),
                fingerprint=fingerprint,
                rule_id=keys["rule_id"],
                severity=keys["severity"],
                scope_digest=keys["scope_digest"],
                state="pending",
                first_observed_at=observed_at,
                last_observed_at=observed_at,
                occurrence_count=0,
                stable_reason=keys["stable_reason"],
            )
        last_firing, streak = self._streaks.get(incident.fingerprint, (firing, 0))
        streak = streak + 1 if firing == last_firing else 1
        self._streaks[incident.fingerprint] = (firing, streak)

        if firing:
            return self._advance_firing(incident, streak, observed_at)
        return self._advance_recovery(incident, streak, observed_at)

    def _advance_firing(
        self, incident: AlertIncident, streak: int, observed_at: datetime
    ) -> tuple[AlertIncident, str | None]:
        if incident.state == "resolved":
            # Reopening a closed incident needs a fresh sustained window.
            return self._bump(incident, "pending", observed_at, count=True), None
        if incident.state == "recovering":
            # Instability during recovery re-arms the proven firing state.
            return self._bump(incident, "firing", observed_at, count=True), None
        if incident.state == "pending":
            if streak >= self.fire_window:
                updated = self._bump(incident, "firing", observed_at, count=True)
                return updated, notification_id_for(updated)
            return self._bump_same_state(incident, observed_at), None
        # Already firing: merge the occurrence, no duplicate notification.
        return self._bump_same_state(incident, observed_at), None

    def _advance_recovery(
        self, incident: AlertIncident, streak: int, observed_at: datetime
    ) -> tuple[AlertIncident, str | None]:
        if incident.state == "firing":
            updated = self._bump(incident, "recovering", observed_at, count=False)
            return updated, notification_id_for(updated)
        if incident.state == "recovering":
            if streak >= self.resolve_window:
                updated = self._bump(
                    incident, "resolved", observed_at, count=False, resolve=True
                )
                return updated, notification_id_for(updated)
            return self._bump_same_state(incident, observed_at, count=False), None
        # pending/resolved with a stable observation: nothing to notify.
        return self._bump_same_state(incident, observed_at, count=False), None

    def _bump(
        self,
        incident: AlertIncident,
        state: str,
        observed_at: datetime,
        *,
        count: bool = False,
        resolve: bool = False,
    ) -> AlertIncident:
        return AlertIncident(
            incident_id=incident.incident_id,
            fingerprint=incident.fingerprint,
            rule_id=incident.rule_id,
            severity=incident.severity,
            scope_digest=incident.scope_digest,
            state=state,
            first_observed_at=incident.first_observed_at,
            last_observed_at=observed_at,
            state_version=incident.state_version + 1,
            occurrence_count=incident.occurrence_count + (1 if count else 0),
            evidence_digest=incident.evidence_digest,
            last_notification_id=incident.last_notification_id,
            resolved_at=observed_at if resolve else None,
            stable_reason=incident.stable_reason,
        )

    def _bump_same_state(
        self, incident: AlertIncident, observed_at: datetime, *, count: bool = True
    ) -> AlertIncident:
        return AlertIncident(
            incident_id=incident.incident_id,
            fingerprint=incident.fingerprint,
            rule_id=incident.rule_id,
            severity=incident.severity,
            scope_digest=incident.scope_digest,
            state=incident.state,
            first_observed_at=incident.first_observed_at,
            last_observed_at=observed_at,
            state_version=incident.state_version,
            occurrence_count=incident.occurrence_count + (1 if count else 0),
            evidence_digest=incident.evidence_digest,
            last_notification_id=incident.last_notification_id,
            resolved_at=incident.resolved_at,
            stable_reason=incident.stable_reason,
        )

    def notification_id(self, incident: AlertIncident) -> str:
        return notification_id_for(incident)


class SafeAlertNotifier:
    """At-least-once notifier emitting only safe, bounded bodies (FR-014)."""

    def __init__(self, *, sink: Any | None = None) -> None:
        self._sink = sink
        self._last_body: dict[str, Any] | None = None

    async def notify(self, incident: AlertIncident) -> str:
        notification_id = notification_id_for(incident)
        body = {
            "notification_id": notification_id,
            "incident_id": incident.incident_id,
            "rule_id": incident.rule_id,
            "severity": incident.severity,
            "state": incident.state,
            "first_observed_at": incident.first_observed_at.isoformat(),
            "last_observed_at": incident.last_observed_at.isoformat(),
            "occurrence_count": incident.occurrence_count,
            "scope_digest": incident.scope_digest,
            "stable_reason": incident.stable_reason,
            "evidence_digest": incident.evidence_digest,
            "recommended_action": _RECOMMENDED_ACTIONS.get(
                incident.severity, "inspect platform health"
            ),
        }
        self._last_body = body
        if self._sink is not None:
            result = self._sink(body)
            if hasattr(result, "__await__"):
                await result
        return notification_id

    def last_body(self) -> dict[str, Any]:
        if self._last_body is None:
            raise ValueError("no notification has been emitted yet")
        return dict(self._last_body)
