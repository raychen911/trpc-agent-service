"""Control-plane facts for the local reference deployment.

This module deliberately keeps behavioural releases separate from the mutable
security envelope.  ``ControlPlane`` is used by the deterministic memory mode;
the SQL repositories expose the same data model in production.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from threading import RLock
from typing import Any


class ControlPlaneError(ValueError):
    code = "control_plane_error"


class ControlNotFound(ControlPlaneError):
    code = "not_found"


class ControlConflict(ControlPlaneError):
    code = "conflict"


def _now() -> datetime:
    return datetime.now(UTC)


def webhook_key_hash(webhook_key: str) -> str:
    return sha256(webhook_key.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class Tenant:
    tenant_id: str
    display_name: str
    status: str = "active"
    audit_policy: dict[str, Any] = field(default_factory=dict)
    budget_policy: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)


@dataclass(slots=True)
class Agent:
    tenant_id: str
    agent_id: str
    name: str
    status: str = "draft"
    active_config_version: int | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)


@dataclass(frozen=True, slots=True)
class Release:
    tenant_id: str
    agent_id: str
    version: int
    app_config: dict[str, Any]
    model_config: dict[str, Any]
    tool_policy: dict[str, Any]
    knowledge_config: dict[str, Any]
    created_by: str
    change_reason: str
    release_status: str = "staged"
    created_at: datetime = field(default_factory=_now)


@dataclass(slots=True)
class Binding:
    tenant_id: str
    binding_id: str
    agent_id: str
    provider: str
    external_account_id: str
    key_hash: str
    secret_ref: str
    capabilities: dict[str, Any]
    status: str = "active"
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)


class ControlPlane:
    """Thread-safe control-plane implementation used by API and demo mode.

    The webhook index intentionally exposes just a single exact-match lookup.
    There is no method to list it, mirroring the SQL ``SECURITY DEFINER``
    locator function used before a tenant RLS context exists.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._tenants: dict[str, Tenant] = {}
        self._agents: dict[tuple[str, str], Agent] = {}
        self._releases: dict[tuple[str, str, int], Release] = {}
        self._bindings: dict[tuple[str, str], Binding] = {}
        self._binding_locator: dict[tuple[str, str], tuple[str, str]] = {}

    @staticmethod
    def _primitive(value: object) -> dict[str, Any]:
        data = asdict(value)  # type: ignore[arg-type]
        for key, item in list(data.items()):
            if isinstance(item, datetime):
                data[key] = item.isoformat()
        return data

    def create_tenant(
        self,
        tenant_id: str,
        display_name: str,
        *,
        audit_policy: dict[str, Any] | None = None,
        budget_policy: dict[str, Any] | None = None,
    ) -> Tenant:
        with self._lock:
            if tenant_id in self._tenants:
                raise ControlConflict(f"tenant {tenant_id!r} already exists")
            tenant = Tenant(
                tenant_id,
                display_name,
                audit_policy=audit_policy or {},
                budget_policy=budget_policy or {},
            )
            self._tenants[tenant_id] = tenant
            return tenant

    def tenant(self, tenant_id: str) -> Tenant:
        with self._lock:
            try:
                return self._tenants[tenant_id]
            except KeyError as exc:
                raise ControlNotFound(f"tenant {tenant_id!r} does not exist") from exc

    def tenant_ids(self) -> list[str]:
        """Return control-plane tenants for local dispatcher scheduling only."""

        with self._lock:
            return sorted(self._tenants)

    def create_agent(self, tenant_id: str, agent_id: str, name: str) -> Agent:
        with self._lock:
            self.tenant(tenant_id)
            key = (tenant_id, agent_id)
            if key in self._agents:
                raise ControlConflict(f"agent {agent_id!r} already exists")
            agent = Agent(tenant_id, agent_id, name)
            self._agents[key] = agent
            return agent

    def agent(self, tenant_id: str, agent_id: str) -> Agent:
        with self._lock:
            try:
                return self._agents[(tenant_id, agent_id)]
            except KeyError as exc:
                raise ControlNotFound(f"agent {agent_id!r} does not exist") from exc

    def create_release(self, tenant_id: str, agent_id: str, version: int, **values: Any) -> Release:
        with self._lock:
            self.agent(tenant_id, agent_id)
            key = (tenant_id, agent_id, version)
            if key in self._releases:
                raise ControlConflict(f"release {version} already exists")
            release = Release(tenant_id=tenant_id, agent_id=agent_id, version=version, **values)
            self._releases[key] = release
            return release

    def release(self, tenant_id: str, agent_id: str, version: int) -> Release:
        with self._lock:
            try:
                return self._releases[(tenant_id, agent_id, version)]
            except KeyError as exc:
                raise ControlNotFound(f"release {version} does not exist") from exc

    def activate_release(self, tenant_id: str, agent_id: str, version: int) -> Release:
        with self._lock:
            release = self.release(tenant_id, agent_id, version)
            agent = self.agent(tenant_id, agent_id)
            agent.active_config_version = version
            agent.status = "active"
            agent.updated_at = _now()
            return release

    def rollback_agent(self, tenant_id: str, agent_id: str) -> Release:
        with self._lock:
            agent = self.agent(tenant_id, agent_id)
            candidates = sorted(
                version
                for ten, agt, version in self._releases
                if ten == tenant_id and agt == agent_id
            )
            if not candidates:
                raise ControlNotFound("agent has no release")
            current = agent.active_config_version
            prior = [version for version in candidates if current is None or version < current]
            if not prior:
                raise ControlConflict("agent has no earlier release to roll back to")
            return self.activate_release(tenant_id, agent_id, prior[-1])

    def active_release(self, tenant_id: str, agent_id: str) -> Release:
        agent = self.agent(tenant_id, agent_id)
        if agent.active_config_version is None:
            raise ControlConflict("agent does not have an active release")
        return self.release(tenant_id, agent_id, agent.active_config_version)

    def create_binding(self, tenant_id: str, *, webhook_key: str, **values: Any) -> Binding:
        with self._lock:
            self.agent(tenant_id, str(values["agent_id"]))
            binding_id = str(values["binding_id"])
            key = (tenant_id, binding_id)
            if key in self._bindings:
                raise ControlConflict(f"binding {binding_id!r} already exists")
            digest = webhook_key_hash(webhook_key)
            locator_key = (str(values["provider"]), digest)
            if locator_key in self._binding_locator:
                raise ControlConflict("webhook key is already bound")
            binding = Binding(tenant_id=tenant_id, key_hash=digest, **values)
            self._bindings[key] = binding
            self._binding_locator[locator_key] = key
            return binding

    def binding(self, tenant_id: str, binding_id: str) -> Binding:
        with self._lock:
            try:
                return self._bindings[(tenant_id, binding_id)]
            except KeyError as exc:
                raise ControlNotFound(f"binding {binding_id!r} does not exist") from exc

    def bindings_for_provider(self, provider: str) -> list[Binding]:
        """Scheduler-only active bindings for an outbound connection transport."""

        with self._lock:
            return [
                binding
                for binding in self._bindings.values()
                if binding.provider == provider and binding.status == "active"
            ]

    def resolve_callback_binding(self, provider: str, webhook_key: str) -> Binding:
        """Protected exact-match locator: only public callback paths call this."""

        with self._lock:
            found = self._binding_locator.get((provider, webhook_key_hash(webhook_key)))
            if found is None:
                raise ControlNotFound("unknown channel binding")
            binding = self._bindings[found]
            if binding.status != "active":
                raise ControlNotFound("channel binding is disabled")
            return binding

    def serialize(self, value: object) -> dict[str, Any]:
        return self._primitive(value)
